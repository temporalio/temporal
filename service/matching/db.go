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
	"sync"
	"sync/atomic"

	enumspb "go.temporal.io/api/enums/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
)

type (
	taskQueueDB struct {
		sync.Mutex
		namespaceID   string
		taskQueueName string
		taskQueueKind enumspb.TaskQueueKind
		taskType      enumspb.TaskQueueType
		rangeID       int64
		ackLevel      int64
		store         persistence.TaskManager
		logger        log.Logger
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
func newTaskQueueDB(store persistence.TaskManager, namespaceID string, name string, taskType enumspb.TaskQueueType, kind enumspb.TaskQueueKind, logger log.Logger) *taskQueueDB {
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
func (db *taskQueueDB) RenewLease() (taskQueueState, error) {
	db.Lock()
	defer db.Unlock()
	resp, err := db.store.LeaseTaskQueue(&persistence.LeaseTaskQueueRequest{
		NamespaceID:   db.namespaceID,
		TaskQueue:     db.taskQueueName,
		TaskType:      db.taskType,
		TaskQueueKind: db.taskQueueKind,
		RangeID:       atomic.LoadInt64(&db.rangeID),
	})
	if err != nil {
		return taskQueueState{}, err
	}
	db.ackLevel = resp.TaskQueueInfo.Data.AckLevel
	db.rangeID = resp.TaskQueueInfo.RangeID
	return taskQueueState{rangeID: db.rangeID, ackLevel: db.ackLevel}, nil
}

// UpdateState updates the taskQueue state with the given value
func (db *taskQueueDB) UpdateState(ackLevel int64) error {
	db.Lock()
	defer db.Unlock()
	_, err := db.store.UpdateTaskQueue(&persistence.UpdateTaskQueueRequest{
		TaskQueueInfo: &persistencespb.TaskQueueInfo{
			NamespaceId: db.namespaceID,
			Name:        db.taskQueueName,
			TaskType:    db.taskType,
			AckLevel:    ackLevel,
			Kind:        db.taskQueueKind,
		},
		RangeID: db.rangeID,
	})
	if err == nil {
		db.ackLevel = ackLevel
	}
	return err
}

// CreateTasks creates a batch of given tasks for this task queue
func (db *taskQueueDB) CreateTasks(tasks []*persistencespb.AllocatedTaskInfo) (*persistence.CreateTasksResponse, error) {
	db.Lock()
	defer db.Unlock()
	return db.store.CreateTasks(
		&persistence.CreateTasksRequest{
			TaskQueueInfo: &persistence.PersistedTaskQueueInfo{
				Data: &persistencespb.TaskQueueInfo{
					NamespaceId: db.namespaceID,
					Name:        db.taskQueueName,
					TaskType:    db.taskType,
					AckLevel:    db.ackLevel,
					Kind:        db.taskQueueKind,
				},
				RangeID: db.rangeID,
			},
			Tasks: tasks,
		})
}

// GetTasks returns a batch of tasks between the given range
func (db *taskQueueDB) GetTasks(minTaskID int64, maxTaskID int64, batchSize int) (*persistence.GetTasksResponse, error) {
	return db.store.GetTasks(&persistence.GetTasksRequest{
		NamespaceID:  db.namespaceID,
		TaskQueue:    db.taskQueueName,
		TaskType:     db.taskType,
		BatchSize:    batchSize,
		ReadLevel:    minTaskID,  // exclusive
		MaxReadLevel: &maxTaskID, // inclusive
	})
}

// CompleteTask deletes a single task from this task queue
func (db *taskQueueDB) CompleteTask(taskID int64) error {
	err := db.store.CompleteTask(&persistence.CompleteTaskRequest{
		TaskQueue: &persistence.TaskQueueKey{
			NamespaceID: db.namespaceID,
			Name:        db.taskQueueName,
			TaskType:    db.taskType,
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
func (db *taskQueueDB) CompleteTasksLessThan(taskID int64, limit int) (int, error) {
	n, err := db.store.CompleteTasksLessThan(&persistence.CompleteTasksLessThanRequest{
		NamespaceID:   db.namespaceID,
		TaskQueueName: db.taskQueueName,
		TaskType:      db.taskType,
		TaskID:        taskID,
		Limit:         limit,
	})
	if err != nil {
		db.logger.Error("Persistent store operation failure",
			tag.StoreOperationCompleteTasksLessThan,
			tag.Error(err),
			tag.TaskID(taskID),
			tag.WorkflowTaskQueueType(db.taskType),
			tag.WorkflowTaskQueueName(db.taskQueueName))
	}
	return n, err
}
