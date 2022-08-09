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

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
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
		namespaceID    namespace.ID
		taskQueueName  string
		taskQueueKind  enumspb.TaskQueueKind
		taskType       enumspb.TaskQueueType
		rangeID        int64
		ackLevel       int64
		versioningData *persistencespb.VersioningData
		store          persistence.TaskManager
		logger         log.Logger
	}
	taskQueueState struct {
		rangeID  int64
		ackLevel int64
	}
)

// newTaskQueueDB returns an instance of an object that represents
// persistence view of a taskQueue. All mutations / reads to taskQueues
// wrt persistence go through this object.
//
// This class will serialize writes to persistence that do condition updates. There are
// two reasons for doing this:
// - To work around known Cassandra issue where concurrent LWT to the same partition cause timeout errors
// - To provide the guarantee that there is only writer who updates taskQueue in persistence at any given point in time
//   This guarantee makes some of the other code simpler and there is no impact to perf because updates to taskqueue are
//   spread out and happen in background routines
func newTaskQueueDB(store persistence.TaskManager, namespaceID namespace.ID, name string, taskType enumspb.TaskQueueType, kind enumspb.TaskQueueKind, logger log.Logger) *taskQueueDB {
	return &taskQueueDB{
		namespaceID:   namespaceID,
		taskQueueName: name,
		taskQueueKind: kind,
		taskType:      taskType,
		store:         store,
		logger:        logger,
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
		NamespaceID: db.namespaceID.String(),
		TaskQueue:   db.taskQueueName,
		TaskType:    db.taskType,
	})
	switch err.(type) {
	case nil:
		response.TaskQueueInfo.Kind = db.taskQueueKind
		response.TaskQueueInfo.ExpiryTime = db.expiryTime()
		response.TaskQueueInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
		_, err := db.store.UpdateTaskQueue(ctx, &persistence.UpdateTaskQueueRequest{
			RangeID:       response.RangeID + 1,
			TaskQueueInfo: response.TaskQueueInfo,
			PrevRangeID:   response.RangeID,
		})
		if err != nil {
			return err
		}
		db.ackLevel = response.TaskQueueInfo.AckLevel
		db.rangeID = response.RangeID + 1
		db.versioningData = response.TaskQueueInfo.VersioningData
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

// UpdateState updates the taskQueue state with the given value
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
		NamespaceID:        db.namespaceID.String(),
		TaskQueue:          db.taskQueueName,
		TaskType:           db.taskType,
		PageSize:           batchSize,
		InclusiveMinTaskID: inclusiveMinTaskID,
		ExclusiveMaxTaskID: exclusiveMaxTaskID,
	})
}

// CompleteTask deletes a single task from this task queue
func (db *taskQueueDB) CompleteTask(
	ctx context.Context,
	taskID int64,
) error {
	err := db.store.CompleteTask(ctx, &persistence.CompleteTaskRequest{
		TaskQueue: &persistence.TaskQueueKey{
			NamespaceID:   db.namespaceID.String(),
			TaskQueueName: db.taskQueueName,
			TaskQueueType: db.taskType,
		},
		TaskID: taskID,
	})
	if err != nil {
		db.logger.Error("Persistent store operation failure",
			tag.StoreOperationCompleteTask,
			tag.Error(err),
			tag.TaskID(taskID),
			tag.WorkflowTaskQueueType(db.taskType),
			tag.WorkflowTaskQueueName(db.taskQueueName))
	}
	return err
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
		NamespaceID:        db.namespaceID.String(),
		TaskQueueName:      db.taskQueueName,
		TaskType:           db.taskType,
		ExclusiveMaxTaskID: exclusiveMaxTaskID,
		Limit:              limit,
	})
	if err != nil {
		db.logger.Error("Persistent store operation failure",
			tag.StoreOperationCompleteTasksLessThan,
			tag.Error(err),
			tag.TaskID(exclusiveMaxTaskID),
			tag.WorkflowTaskQueueType(db.taskType),
			tag.WorkflowTaskQueueName(db.taskQueueName))
	}
	return n, err
}

// GetVersioningData returns the versioning data for this task queue. Do not mutate the returned pointer, as doing so
// will cause cache inconsistency.
func (db *taskQueueDB) GetVersioningData(
	ctx context.Context,
) (*persistencespb.VersioningData, error) {
	db.Lock()
	defer db.Unlock()
	return db.getVersioningDataLocked(ctx)
}

// db.Lock() must be held before calling
func (db *taskQueueDB) getVersioningDataLocked(
	ctx context.Context,
) (*persistencespb.VersioningData, error) {
	if db.versioningData != nil {
		return db.versioningData, nil
	}

	tqInfo, err := db.store.GetTaskQueue(ctx, &persistence.GetTaskQueueRequest{
		NamespaceID: db.namespaceID.String(),
		TaskQueue:   db.taskQueueName,
		TaskType:    db.taskType,
	})
	if err != nil {
		return nil, err
	}
	db.versioningData = tqInfo.TaskQueueInfo.GetVersioningData()

	return tqInfo.TaskQueueInfo.GetVersioningData(), nil
}

// MutateVersioningData allows callers to update versioning data for this task queue. The pointer passed to the
// mutating function is guaranteed to be non-nil.
func (db *taskQueueDB) MutateVersioningData(ctx context.Context, mutator func(*persistencespb.VersioningData) error) error {
	db.Lock()
	defer db.Unlock()

	verDat, err := db.getVersioningDataLocked(ctx)
	if err != nil {
		return err
	}

	if verDat == nil {
		verDat = &persistencespb.VersioningData{}
	}
	if err := mutator(verDat); err != nil {
		return err
	}

	queueInfo := db.cachedQueueInfo()
	queueInfo.VersioningData = verDat

	_, err = db.store.UpdateTaskQueue(ctx, &persistence.UpdateTaskQueueRequest{
		RangeID:       db.rangeID,
		TaskQueueInfo: queueInfo,
		PrevRangeID:   db.rangeID,
	})
	if err == nil {
		db.versioningData = verDat
	}
	return err
}

func (db *taskQueueDB) expiryTime() *time.Time {
	switch db.taskQueueKind {
	case enumspb.TASK_QUEUE_KIND_NORMAL:
		return nil
	case enumspb.TASK_QUEUE_KIND_STICKY:
		return timestamp.TimePtr(time.Now().UTC().Add(stickyTaskQueueTTL))
	default:
		panic(fmt.Sprintf("taskQueueDB encountered unknown task kind: %v", db.taskQueueKind))
	}
}

func (db *taskQueueDB) cachedQueueInfo() *persistencespb.TaskQueueInfo {
	return &persistencespb.TaskQueueInfo{
		NamespaceId:    db.namespaceID.String(),
		Name:           db.taskQueueName,
		TaskType:       db.taskType,
		Kind:           db.taskQueueKind,
		AckLevel:       db.ackLevel,
		VersioningData: db.versioningData,
		ExpiryTime:     db.expiryTime(),
		LastUpdateTime: timestamp.TimeNowPtrUtc(),
	}
}
