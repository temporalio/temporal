// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package matching

import (
	"context"
	"fmt"
	"github.com/emirpasic/gods/maps/treemap"
	godsutils "github.com/emirpasic/gods/utils"
	"go.uber.org/atomic"
	"sync"
	"time"

	"go.temporal.io/server/common/metrics"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	initialRangeID     = 1 // Id of the first range of a new task queue
	stickyTaskQueueTTL = 24 * time.Hour
)

type (
	taskQueueDB struct {
		sync.Mutex
		backlogMgr              *backlogManagerImpl // accessing taskWriter and taskReader
		queue                   *PhysicalTaskQueueKey
		rangeID                 int64
		store                   persistence.TaskManager
		logger                  log.Logger
		approximateBacklogCount atomic.Int64
		maxReadLevel            atomic.Int64 // note that even though this is an atomic, it should only be written to while holding the db lock

		outstandingTasks *treemap.Map  // TaskID->acked
		readLevel        *atomic.Int64 // Maximum TaskID inserted into outstandingTasks
		ackLevel         *atomic.Int64 // Maximum TaskID below which all tasks are acked
		backlogCounter   atomic.Int64  // already present backlogCounter

	}
	taskQueueState struct {
		rangeID  int64
		ackLevel int64
	}
)

// newTaskQueueDB returns an instance of an object that represents
// persistence view of a physical task queue. All mutations / reads to queues
// wrt persistence go through this object.
//
// This class will serialize writes to persistence that do condition updates. There are
// two reasons for doing this:
//   - To work around known Cassandra issue where concurrent LWT to the same partition cause timeout errors
//   - To provide the guarantee that there is only writer who updates queue in persistence at any given point in time
//     This guarantee makes some of the other code simpler and there is no impact to perf because updates to taskqueue are
//     spread out and happen in background routines
func newTaskQueueDB(
	backlogMgr *backlogManagerImpl,
	store persistence.TaskManager,
	queue *PhysicalTaskQueueKey,
	logger log.Logger,
) *taskQueueDB {
	return &taskQueueDB{
		backlogMgr:       backlogMgr,
		queue:            queue,
		store:            store,
		logger:           logger,
		outstandingTasks: treemap.NewWith(godsutils.Int64Comparator),
		readLevel:        atomic.NewInt64(-1),
		ackLevel:         atomic.NewInt64(-1),
	}
}

// Registers task as in-flight and moves read level to it. Tasks can be added in increasing order of taskID only.
func (db *taskQueueDB) addTask(taskID int64) {
	db.Lock()
	defer db.Unlock()
	if db.readLevel.Load() >= taskID {
		db.logger.Fatal("Next task ID is less than current read level.",
			tag.TaskID(taskID),
			tag.ReadLevel(db.readLevel.Load()))
	}
	db.readLevel.Store(taskID)
	if _, found := db.outstandingTasks.Get(taskID); found {
		db.logger.Fatal("Already present in outstanding tasks", tag.TaskID(taskID))
	}
	db.outstandingTasks.Put(taskID, false)
	db.backlogCounter.Inc()
}

func (db *taskQueueDB) getReadLevel() int64 {
	return db.readLevel.Load()
}

func (db *taskQueueDB) setReadLevel(readLevel int64) {
	db.Lock()
	defer db.Unlock()
	db.readLevel.Store(readLevel)
}

func (db *taskQueueDB) setReadLevelAfterGap(newReadLevel int64) {
	db.Lock()
	defer db.Unlock()
	if db.ackLevel.Load() == db.readLevel.Load() {
		// This is called after we read a range and find no tasks. The range we read was m.readLevel to newReadLevel.
		// (We know this because nothing should change m.readLevel except the getTasksPump loop itself, after initialization.
		// And getTasksPump doesn't start until it gets a signal from taskWriter that it's initialized the levels.)
		// If we've acked all tasks up to m.readLevel, and there are no tasks between that and newReadLevel, then we've
		// acked all tasks up to newReadLevel too. This lets us advance the ack level on a task queue with no activity
		// but where the rangeid has moved higher, to prevent excessive reads on the next load.
		db.ackLevel.Store(newReadLevel)
	}
	db.readLevel.Store(newReadLevel)
}

func (db *taskQueueDB) getAckLevel() int64 {
	return db.ackLevel.Load()
}

// Moves ack level to the new level if it is higher than the current one.
// Also updates the read level if it is lower than the ackLevel.
func (db *taskQueueDB) setAckLevel(ackLevel int64) {
	db.Lock()
	defer db.Unlock()
	if ackLevel > db.ackLevel.Load() {
		db.ackLevel.Store(ackLevel)
	}
	if ackLevel > db.readLevel.Load() {
		db.readLevel.Store(ackLevel)
	}
}

func (db *taskQueueDB) completeTask(taskID int64) int64 {
	db.Lock()
	defer db.Unlock()

	macked, found := db.outstandingTasks.Get(taskID)
	if !found {
		return db.ackLevel.Load()
	}

	acked := macked.(bool)
	if acked {
		// don't adjust ack level if nothing has changed
		return db.ackLevel.Load()
	}

	// TODO the ack level management should be done by a dedicated coroutine
	//  this is only a temporarily solution
	db.outstandingTasks.Put(taskID, true)
	db.backlogCounter.Dec()

	// Adjust the ack level as far as we can
	for {
		min, acked := db.outstandingTasks.Min()
		if min == nil || !acked.(bool) {
			return db.ackLevel.Load()
		}
		db.ackLevel.Store(min.(int64))
		db.outstandingTasks.Remove(min)
		// reducing our backlog since a task gets acked
		db.backlogMgr.db.approximateBacklogCount.Add(int64(-1))
	}

}

func (db *taskQueueDB) getBacklogCountHint() int64 {
	return db.backlogCounter.Load()
}

// RangeID returns the current persistence view of rangeID
func (db *taskQueueDB) RangeID() int64 {
	db.Lock()
	defer db.Unlock()
	return db.rangeID
}

// GetMaxReadLevel returns the current maxReadLevel
func (db *taskQueueDB) GetMaxReadLevel() int64 {
	return db.maxReadLevel.Load()
}

// SetMaxReadLevel sets the current maxReadLevel
func (db *taskQueueDB) SetMaxReadLevel(maxReadLevel int64) {
	db.Lock()
	defer db.Unlock()
	db.maxReadLevel.Store(maxReadLevel)
}

// RenewLease renews the lease on a taskqueue. If there is no previous lease,
// this method will attempt to steal taskqueue from current owner
func (db *taskQueueDB) RenewLease(
	ctx context.Context,
) (taskQueueState, error) {
	db.Lock()
	defer db.Unlock()

	if db.rangeID == 0 {
		if err := db.takeOverTaskQueueLocked(ctx); err != nil {
			return taskQueueState{}, err
		}
	} else {
		if err := db.renewTaskQueueLocked(ctx, db.rangeID+1); err != nil {
			return taskQueueState{}, err
		}
	}
	return taskQueueState{rangeID: db.rangeID, ackLevel: db.ackLevel.Load()}, nil
}

func (db *taskQueueDB) takeOverTaskQueueLocked(
	ctx context.Context,
) error {
	response, err := db.store.GetTaskQueue(ctx, &persistence.GetTaskQueueRequest{
		NamespaceID: db.queue.NamespaceId().String(),
		TaskQueue:   db.queue.PersistenceName(),
		TaskType:    db.queue.TaskType(),
	})
	switch err.(type) {
	case nil:
		response.TaskQueueInfo.Kind = db.queue.Partition().Kind()
		response.TaskQueueInfo.ExpiryTime = db.expiryTime()
		response.TaskQueueInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
		if _, err := db.store.UpdateTaskQueue(ctx, &persistence.UpdateTaskQueueRequest{
			RangeID:       response.RangeID + 1,
			TaskQueueInfo: response.TaskQueueInfo,
			PrevRangeID:   response.RangeID,
		}); err != nil {
			return err
		}
		db.ackLevel.Store(response.TaskQueueInfo.AckLevel)
		db.rangeID = response.RangeID + 1
		db.approximateBacklogCount.Store(response.TaskQueueInfo.ApproximateBacklogCount)
		return nil

	case *serviceerror.NotFound:
		if _, err := db.store.CreateTaskQueue(ctx, &persistence.CreateTaskQueueRequest{
			RangeID:       initialRangeID,
			TaskQueueInfo: db.cachedQueueInfo(),
		}); err != nil {
			return err
		}
		db.rangeID = initialRangeID
		return nil

	default:
		return err
	}
}

func (db *taskQueueDB) renewTaskQueueLocked(
	ctx context.Context,
	rangeID int64,
) error {
	if _, err := db.store.UpdateTaskQueue(ctx, &persistence.UpdateTaskQueueRequest{
		RangeID:       rangeID,
		TaskQueueInfo: db.cachedQueueInfo(),
		PrevRangeID:   db.rangeID,
	}); err != nil {
		return err
	}

	db.rangeID = rangeID
	return nil
}

// UpdateState updates the queue state with the given values
func (db *taskQueueDB) UpdateState(
	ctx context.Context,
) error {
	db.Lock()
	defer db.Unlock()

	// Resetting approximateBacklogCounter to fix the count divergence issue
	if db.getAckLevel() == db.GetMaxReadLevel() {
		db.approximateBacklogCount.Store(0)
	}

	queueInfo := db.cachedQueueInfo()
	_, err := db.store.UpdateTaskQueue(ctx, &persistence.UpdateTaskQueueRequest{
		RangeID:       db.rangeID,
		TaskQueueInfo: queueInfo,
		PrevRangeID:   db.rangeID,
	})
	if err == nil {
		db.emitTaskLagMetric()
		db.emitApproximateBacklogCount()
	}
	return err
}

// updateApproximateBacklogCount updates the in-memory DB state with the given delta value
func (db *taskQueueDB) updateApproximateBacklogCount(
	delta int64,
) {
	db.Lock()
	defer db.Unlock()

	// Preventing under-counting
	if db.approximateBacklogCount.Load()+delta < 0 {
		// logging as an error here since our counter is becoming negative which means it's undercounting
		// Undercounting should never happen.
		db.logger.Error("ApproximateBacklogCounter could have under-counted. This should never happen",
			tag.WorkerBuildId(db.queue.BuildId()), tag.WorkerBuildId(db.queue.Partition().NamespaceId().String()))
		db.approximateBacklogCount.Store(0)
	} else {
		db.approximateBacklogCount.Add(delta)
	}
}

func (db *taskQueueDB) getApproximateBacklogCount() int64 {
	return db.approximateBacklogCount.Load()
}

// CreateTasks creates a batch of given tasks for this task queue
func (db *taskQueueDB) CreateTasks(
	ctx context.Context,
	taskIDs []int64,
	reqs []*writeTaskRequest,
) (*persistence.CreateTasksResponse, error) {
	db.Lock()
	defer db.Unlock()

	if len(reqs) == 0 {
		return &persistence.CreateTasksResponse{}, nil
	}

	maxReadLevel := int64(0)
	var tasks []*persistencespb.AllocatedTaskInfo
	for i, req := range reqs {
		tasks = append(tasks, &persistencespb.AllocatedTaskInfo{
			TaskId: taskIDs[i],
			Data:   req.taskInfo,
		})
		maxReadLevel = taskIDs[i]
	}
	db.approximateBacklogCount.Add(int64(len(tasks)))

	resp, err := db.store.CreateTasks(
		ctx,
		&persistence.CreateTasksRequest{
			TaskQueueInfo: &persistence.PersistedTaskQueueInfo{
				Data:    db.cachedQueueInfo(),
				RangeID: db.rangeID,
			},
			Tasks: tasks,
		})

	// Update the maxReadLevel after the writes are completed, but before we send the response,
	// so that taskReader is guaranteed to see the new read level when SpoolTask wakes it up.
	db.maxReadLevel.Store(maxReadLevel)

	if _, ok := err.(*persistence.ConditionFailedError); ok {
		// tasks definitely were not created, restore the counter. For other errors tasks may or may not be created.
		// In those cases we keep the count incremented, hence it may be an overestimate.
		db.approximateBacklogCount.Add(-int64(len(tasks)))
	}
	return resp, err
}

// GetTasks returns a batch of tasks between the given range
func (db *taskQueueDB) GetTasks(
	ctx context.Context,
	inclusiveMinTaskID int64,
	exclusiveMaxTaskID int64,
	batchSize int,
) (*persistence.GetTasksResponse, error) {
	return db.store.GetTasks(ctx, &persistence.GetTasksRequest{
		NamespaceID:        db.queue.NamespaceId().String(),
		TaskQueue:          db.queue.PersistenceName(),
		TaskType:           db.queue.TaskType(),
		PageSize:           batchSize,
		InclusiveMinTaskID: inclusiveMinTaskID,
		ExclusiveMaxTaskID: exclusiveMaxTaskID,
	})
}

// CompleteTasksLessThan deletes of tasks less than the given taskID. Limit is
// the upper bound of number of tasks that can be deleted by this method. It may
// or may not be honored
func (db *taskQueueDB) CompleteTasksLessThan(
	ctx context.Context,
	exclusiveMaxTaskID int64,
	limit int,
) (int, error) {
	n, err := db.store.CompleteTasksLessThan(ctx, &persistence.CompleteTasksLessThanRequest{
		NamespaceID:        db.queue.NamespaceId().String(),
		TaskQueueName:      db.queue.PersistenceName(),
		TaskType:           db.queue.TaskType(),
		ExclusiveMaxTaskID: exclusiveMaxTaskID,
		Limit:              limit,
	})
	if err != nil {
		db.logger.Error("Persistent store operation failure",
			tag.StoreOperationCompleteTasksLessThan,
			tag.Error(err),
			tag.TaskID(exclusiveMaxTaskID),
			tag.WorkflowTaskQueueType(db.queue.TaskType()),
			tag.WorkflowTaskQueueName(db.queue.PersistenceName()),
		)
	}
	return n, err
}

func (db *taskQueueDB) expiryTime() *timestamppb.Timestamp {
	switch db.queue.Partition().Kind() {
	case enumspb.TASK_QUEUE_KIND_NORMAL:
		return nil
	case enumspb.TASK_QUEUE_KIND_STICKY:
		return timestamppb.New(time.Now().UTC().Add(stickyTaskQueueTTL))
	default:
		panic(fmt.Sprintf("taskQueueDB encountered unknown task kind: %v", db.queue.Partition().Kind()))
	}
}

func (db *taskQueueDB) cachedQueueInfo() *persistencespb.TaskQueueInfo {
	return &persistencespb.TaskQueueInfo{
		NamespaceId:             db.queue.NamespaceId().String(),
		Name:                    db.queue.PersistenceName(),
		TaskType:                db.queue.TaskType(),
		Kind:                    db.queue.Partition().Kind(),
		AckLevel:                db.ackLevel.Load(),
		ExpiryTime:              db.expiryTime(),
		LastUpdateTime:          timestamp.TimeNowPtrUtc(),
		ApproximateBacklogCount: db.approximateBacklogCount.Load(),
	}
}

func (db *taskQueueDB) emitApproximateBacklogCount() {
	// note: this metric is called after persisting the updated BacklogCount
	approximateBacklogCount := db.getApproximateBacklogCount()
	db.backlogMgr.metricsHandler.Gauge(metrics.ApproximateBacklogCount.Name()).Record(float64(approximateBacklogCount))
}

func (db *taskQueueDB) emitTaskLagMetric() {
	// note: this metric is only an estimation for the lag.
	// taskID in DB may not be continuous, especially when task list ownership changes.
	db.backlogMgr.metricsHandler.Gauge(metrics.TaskLagPerTaskQueueGauge.Name()).Record(float64(db.GetMaxReadLevel() - db.getAckLevel()))
}
