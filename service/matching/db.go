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
	"sync"
	"time"

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
		queue    *PhysicalTaskQueueKey
		rangeID  int64
		ackLevel int64
		store    persistence.TaskManager
		logger   log.Logger
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
	store persistence.TaskManager,
	queue *PhysicalTaskQueueKey,
	logger log.Logger,
) *taskQueueDB {
	return &taskQueueDB{
		queue:  queue,
		store:  store,
		logger: logger,
	}
}

// RangeID returns the current persistence view of rangeID
func (db *taskQueueDB) RangeID() int64 {
	db.Lock()
	defer db.Unlock()
	return db.rangeID
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
	return taskQueueState{rangeID: db.rangeID, ackLevel: db.ackLevel}, nil
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
		db.ackLevel = response.TaskQueueInfo.AckLevel
		db.rangeID = response.RangeID + 1
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

// UpdateState updates the queue state with the given value
func (db *taskQueueDB) UpdateState(
	ctx context.Context,
	ackLevel int64,
) error {
	db.Lock()
	defer db.Unlock()
	queueInfo := db.cachedQueueInfo()
	queueInfo.AckLevel = ackLevel
	_, err := db.store.UpdateTaskQueue(ctx, &persistence.UpdateTaskQueueRequest{
		RangeID:       db.rangeID,
		TaskQueueInfo: queueInfo,
		PrevRangeID:   db.rangeID,
	})
	if err == nil {
		db.ackLevel = ackLevel
	}
	return err
}

// CreateTasks creates a batch of given tasks for this task queue
func (db *taskQueueDB) CreateTasks(
	ctx context.Context,
	tasks []*persistencespb.AllocatedTaskInfo,
) (*persistence.CreateTasksResponse, error) {
	db.Lock()
	defer db.Unlock()
	return db.store.CreateTasks(
		ctx,
		&persistence.CreateTasksRequest{
			TaskQueueInfo: &persistence.PersistedTaskQueueInfo{
				Data:    db.cachedQueueInfo(),
				RangeID: db.rangeID,
			},
			Tasks: tasks,
		})
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
		NamespaceId:    db.queue.NamespaceId().String(),
		Name:           db.queue.PersistenceName(),
		TaskType:       db.queue.TaskType(),
		Kind:           db.queue.Partition().Kind(),
		AckLevel:       db.ackLevel,
		ExpiryTime:     db.expiryTime(),
		LastUpdateTime: timestamp.TimeNowPtrUtc(),
	}
}
