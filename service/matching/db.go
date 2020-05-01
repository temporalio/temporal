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

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
)

type (
	taskListDB struct {
		sync.Mutex
		namespaceID  primitives.UUID
		taskListName string
		taskListKind tasklistpb.TaskListKind
		taskType     tasklistpb.TaskListType
		rangeID      int64
		ackLevel     int64
		store        persistence.TaskManager
		logger       log.Logger
	}
	taskListState struct {
		rangeID  int64
		ackLevel int64
	}
)

// newTaskListDB returns an instance of an object that represents
// persistence view of a taskList. All mutations / reads to taskLists
// wrt persistence go through this object.
//
// This class will serialize writes to persistence that do condition updates. There are
// two reasons for doing this:
// - To work around known Cassandra issue where concurrent LWT to the same partition cause timeout errors
// - To provide the guarantee that there is only writer who updates taskList in persistence at any given point in time
//   This guarantee makes some of the other code simpler and there is no impact to perf because updates to tasklist are
//   spread out and happen in background routines
func newTaskListDB(store persistence.TaskManager, namespaceID primitives.UUID, name string, taskType tasklistpb.TaskListType, kind tasklistpb.TaskListKind, logger log.Logger) *taskListDB {
	return &taskListDB{
		namespaceID:  namespaceID,
		taskListName: name,
		taskListKind: kind,
		taskType:     taskType,
		store:        store,
		logger:       logger,
	}
}

// RangeID returns the current persistence view of rangeID
func (db *taskListDB) RangeID() int64 {
	db.Lock()
	defer db.Unlock()
	return db.rangeID
}

// RenewLease renews the lease on a tasklist. If there is no previous lease,
// this method will attempt to steal tasklist from current owner
func (db *taskListDB) RenewLease() (taskListState, error) {
	db.Lock()
	defer db.Unlock()
	resp, err := db.store.LeaseTaskList(&persistence.LeaseTaskListRequest{
		NamespaceID:  db.namespaceID,
		TaskList:     db.taskListName,
		TaskType:     db.taskType,
		TaskListKind: db.taskListKind,
		RangeID:      atomic.LoadInt64(&db.rangeID),
	})
	if err != nil {
		return taskListState{}, err
	}
	db.ackLevel = resp.TaskListInfo.Data.AckLevel
	db.rangeID = resp.TaskListInfo.RangeID
	return taskListState{rangeID: db.rangeID, ackLevel: db.ackLevel}, nil
}

// UpdateState updates the taskList state with the given value
func (db *taskListDB) UpdateState(ackLevel int64) error {
	db.Lock()
	defer db.Unlock()
	_, err := db.store.UpdateTaskList(&persistence.UpdateTaskListRequest{
		TaskListInfo: &persistenceblobs.TaskListInfo{
			NamespaceId: db.namespaceID,
			Name:        db.taskListName,
			TaskType:    db.taskType,
			AckLevel:    ackLevel,
			Kind:        db.taskListKind,
		},
		RangeID: db.rangeID,
	})
	if err == nil {
		db.ackLevel = ackLevel
	}
	return err
}

// CreateTasks creates a batch of given tasks for this task list
func (db *taskListDB) CreateTasks(tasks []*persistenceblobs.AllocatedTaskInfo) (*persistence.CreateTasksResponse, error) {
	db.Lock()
	defer db.Unlock()
	return db.store.CreateTasks(
		&persistence.CreateTasksRequest{
			TaskListInfo: &persistence.PersistedTaskListInfo{
				Data: &persistenceblobs.TaskListInfo{
					NamespaceId: db.namespaceID,
					Name:        db.taskListName,
					TaskType:    db.taskType,
					AckLevel:    db.ackLevel,
					Kind:        db.taskListKind,
				},
				RangeID: db.rangeID,
			},
			Tasks: tasks,
		})
}

// GetTasks returns a batch of tasks between the given range
func (db *taskListDB) GetTasks(minTaskID int64, maxTaskID int64, batchSize int) (*persistence.GetTasksResponse, error) {
	return db.store.GetTasks(&persistence.GetTasksRequest{
		NamespaceID:  db.namespaceID,
		TaskList:     db.taskListName,
		TaskType:     db.taskType,
		BatchSize:    batchSize,
		ReadLevel:    minTaskID,  // exclusive
		MaxReadLevel: &maxTaskID, // inclusive
	})
}

// CompleteTask deletes a single task from this task list
func (db *taskListDB) CompleteTask(taskID int64) error {
	err := db.store.CompleteTask(&persistence.CompleteTaskRequest{
		TaskList: &persistence.TaskListKey{
			NamespaceID: db.namespaceID,
			Name:        db.taskListName,
			TaskType:    db.taskType,
		},
		TaskID: taskID,
	})
	if err != nil {
		db.logger.Error("Persistent store operation failure",
			tag.StoreOperationCompleteTask,
			tag.Error(err),
			tag.TaskID(taskID),
			tag.WorkflowTaskListType(db.taskType),
			tag.WorkflowTaskListName(db.taskListName))
	}
	return err
}

// CompleteTasksLessThan deletes of tasks less than the given taskID. Limit is
// the upper bound of number of tasks that can be deleted by this method. It may
// or may not be honored
func (db *taskListDB) CompleteTasksLessThan(taskID int64, limit int) (int, error) {
	n, err := db.store.CompleteTasksLessThan(&persistence.CompleteTasksLessThanRequest{
		NamespaceID:  db.namespaceID,
		TaskListName: db.taskListName,
		TaskType:     db.taskType,
		TaskID:       taskID,
		Limit:        limit,
	})
	if err != nil {
		db.logger.Error("Persistent store operation failure",
			tag.StoreOperationCompleteTasksLessThan,
			tag.Error(err),
			tag.TaskID(taskID),
			tag.WorkflowTaskListType(db.taskType),
			tag.WorkflowTaskListName(db.taskListName))
	}
	return n, err
}
