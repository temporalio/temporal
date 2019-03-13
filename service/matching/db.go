// Copyright (c) 2017 Uber Technologies, Inc.
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
	"fmt"
	"sync"

	"sync/atomic"

	"github.com/uber-common/bark"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/persistence"
)

type (
	taskListDB struct {
		sync.Mutex
		domainID     string
		taskListName string
		taskListKind int
		taskType     int
		rangeID      int64
		ackLevel     int64
		store        persistence.TaskManager
		logger       bark.Logger
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
func newTaskListDB(store persistence.TaskManager, domainID string, name string, taskType int, kind int, logger bark.Logger) *taskListDB {
	return &taskListDB{
		domainID:     domainID,
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
		DomainID:     db.domainID,
		TaskList:     db.taskListName,
		TaskType:     db.taskType,
		TaskListKind: db.taskListKind,
		RangeID:      atomic.LoadInt64(&db.rangeID),
	})
	if err != nil {
		return taskListState{}, err
	}
	db.ackLevel = resp.TaskListInfo.AckLevel
	db.rangeID = resp.TaskListInfo.RangeID
	return taskListState{rangeID: db.rangeID, ackLevel: db.ackLevel}, nil
}

// UpdateState updates the taskList state with the given value
func (db *taskListDB) UpdateState(ackLevel int64) error {
	db.Lock()
	defer db.Unlock()
	_, err := db.store.UpdateTaskList(&persistence.UpdateTaskListRequest{
		TaskListInfo: &persistence.TaskListInfo{
			DomainID: db.domainID,
			Name:     db.taskListName,
			TaskType: db.taskType,
			AckLevel: ackLevel,
			RangeID:  db.rangeID,
			Kind:     db.taskListKind,
		},
	})
	if err == nil {
		db.ackLevel = ackLevel
	}
	return err
}

// CreateTasks creates a batch of given tasks for this task list
func (db *taskListDB) CreateTasks(tasks []*persistence.CreateTaskInfo) (*persistence.CreateTasksResponse, error) {
	db.Lock()
	defer db.Unlock()
	return db.store.CreateTasks(&persistence.CreateTasksRequest{
		TaskListInfo: &persistence.TaskListInfo{
			DomainID: db.domainID,
			Name:     db.taskListName,
			TaskType: db.taskType,
			AckLevel: db.ackLevel,
			RangeID:  db.rangeID,
			Kind:     db.taskListKind,
		},
		Tasks: tasks,
	})
}

// GetTasks returns a batch of tasks between the given range
func (db *taskListDB) GetTasks(minTaskID int64, maxTaskID int64, batchSize int) (*persistence.GetTasksResponse, error) {
	return db.store.GetTasks(&persistence.GetTasksRequest{
		DomainID:     db.domainID,
		TaskList:     db.taskListName,
		TaskType:     db.taskType,
		BatchSize:    batchSize,
		ReadLevel:    minTaskID, // exclusive
		MaxReadLevel: maxTaskID, // inclusive
	})
}

// CompleteTask deletes a single task from this task list
func (db *taskListDB) CompleteTask(taskID int64) error {
	err := db.store.CompleteTask(&persistence.CompleteTaskRequest{
		TaskList: &persistence.TaskListInfo{
			DomainID: db.domainID,
			Name:     db.taskListName,
			TaskType: db.taskType,
		},
		TaskID: taskID,
	})
	if err != nil {
		logging.LogPersistantStoreErrorEvent(db.logger, logging.TagValueStoreOperationCompleteTask, err,
			fmt.Sprintf("{taskID: %v, taskType: %v, taskList: %v}",
				taskID, db.taskType, db.taskListName))
	}
	return err
}

// CompleteTasksLessThan deletes of tasks less than the given taskID. Limit is
// the upper bound of number of tasks that can be deleted by this method. It may
// or may not be honored
func (db *taskListDB) CompleteTasksLessThan(taskID int64, limit int) (int, error) {
	n, err := db.store.CompleteTasksLessThan(&persistence.CompleteTasksLessThanRequest{
		DomainID:     db.domainID,
		TaskListName: db.taskListName,
		TaskType:     db.taskType,
		TaskID:       taskID,
		Limit:        limit,
	})
	if err != nil {
		logging.LogPersistantStoreErrorEvent(db.logger, logging.TagValueStoreOperationCompleteTasksLessThan, err,
			fmt.Sprintf("{taskID: %v, taskType: %v, taskList: %v}",
				taskID, db.taskType, db.taskListName))
	}
	return n, err
}
