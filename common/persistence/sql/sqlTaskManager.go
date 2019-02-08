// Copyright (c) 2018 Uber Technologies, Inc.
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

package sql

import (
	"fmt"

	"github.com/uber-common/bark"

	"database/sql"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/sql/storage"
	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
	"github.com/uber/cadence/common/service/config"
)

type sqlTaskManager struct {
	sqlStore
}

// newTaskPersistence creates a new instance of TaskManager
func newTaskPersistence(cfg config.SQL, log bark.Logger) (persistence.TaskManager, error) {
	var db, err = storage.NewSQLDB(&cfg)
	if err != nil {
		return nil, err
	}
	return &sqlTaskManager{
		sqlStore: sqlStore{
			db:     db,
			logger: log,
		},
	}, nil
}

func (m *sqlTaskManager) LeaseTaskList(request *persistence.LeaseTaskListRequest) (*persistence.LeaseTaskListResponse, error) {
	var rangeID int64
	var ackLevel int64
	row, err := m.db.SelectFromTaskLists(&sqldb.TaskListsFilter{
		DomainID: request.DomainID, Name: request.TaskList, TaskType: int64(request.TaskType)})
	if err != nil {
		if err == sql.ErrNoRows {
			row = &sqldb.TaskListsRow{
				DomainID: request.DomainID,
				Name:     request.TaskList,
				TaskType: int64(request.TaskType),
				AckLevel: ackLevel,
				Kind:     int64(request.TaskListKind),
				ExpiryTs: time.Time{},
			}
			if _, err := m.db.InsertIntoTaskLists(row); err != nil {
				return nil, &workflow.InternalServiceError{
					Message: fmt.Sprintf("LeaseTaskList operation failed. Failed to make task list %v of type %v. Error: %v", request.TaskList, request.TaskType, err),
				}
			}
		} else {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("LeaseTaskList operation failed. Failed to check if task list existed. Error: %v", err),
			}
		}
	}

	var resp *persistence.LeaseTaskListResponse
	err = m.txExecute("LeaseTaskList", func(tx sqldb.Tx) error {
		rangeID = row.RangeID
		ackLevel = row.AckLevel
		// We need to separately check the condition and do the
		// update because we want to throw different error codes.
		// Since we need to do things separately (in a transaction), we need to take a lock.
		err1 := lockTaskList(tx, request.DomainID, request.TaskList, request.TaskType, rangeID)
		if err1 != nil {
			return err1
		}
		result, err1 := tx.UpdateTaskLists(&sqldb.TaskListsRow{
			DomainID: row.DomainID,
			RangeID:  row.RangeID + 1,
			Name:     row.Name,
			TaskType: row.TaskType,
			AckLevel: row.AckLevel,
			Kind:     row.Kind,
			ExpiryTs: row.ExpiryTs,
		})
		if err1 != nil {
			return err1
		}
		rowsAffected, err1 := result.RowsAffected()
		if err1 != nil {
			return fmt.Errorf("rowsAffected error: %v", err1)
		}
		if rowsAffected == 0 {
			return fmt.Errorf("%v rows affected instead of 1", rowsAffected)
		}
		resp = &persistence.LeaseTaskListResponse{TaskListInfo: &persistence.TaskListInfo{
			DomainID: request.DomainID,
			Name:     request.TaskList,
			TaskType: request.TaskType,
			RangeID:  rangeID + 1,
			AckLevel: ackLevel,
			Kind:     request.TaskListKind,
		}}
		return nil
	})
	return resp, err
}

func (m *sqlTaskManager) UpdateTaskList(request *persistence.UpdateTaskListRequest) (*persistence.UpdateTaskListResponse, error) {
	if request.TaskListInfo.Kind == persistence.TaskListKindSticky {
		if _, err := m.db.ReplaceIntoTaskLists(&sqldb.TaskListsRow{
			DomainID: request.TaskListInfo.DomainID,
			RangeID:  request.TaskListInfo.RangeID,
			Name:     request.TaskListInfo.Name,
			TaskType: int64(request.TaskListInfo.TaskType),
			AckLevel: request.TaskListInfo.AckLevel,
			Kind:     int64(request.TaskListInfo.Kind),
			ExpiryTs: stickyTaskListTTL(),
		}); err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("UpdateTaskList operation failed. Failed to make sticky task list. Error: %v", err),
			}
		}
	}
	var resp *persistence.UpdateTaskListResponse
	err := m.txExecute("UpdateTaskList", func(tx sqldb.Tx) error {
		err1 := lockTaskList(
			tx, request.TaskListInfo.DomainID, request.TaskListInfo.Name, request.TaskListInfo.TaskType, request.TaskListInfo.RangeID)
		if err1 != nil {
			return err1
		}
		result, err1 := tx.UpdateTaskLists(&sqldb.TaskListsRow{
			DomainID: request.TaskListInfo.DomainID,
			RangeID:  request.TaskListInfo.RangeID,
			Name:     request.TaskListInfo.Name,
			TaskType: int64(request.TaskListInfo.TaskType),
			AckLevel: request.TaskListInfo.AckLevel,
			Kind:     int64(request.TaskListInfo.Kind),
			ExpiryTs: time.Time{},
		})
		if err1 != nil {
			return err1
		}
		rowsAffected, err1 := result.RowsAffected()
		if err1 != nil {
			return err1
		}
		if rowsAffected != 1 {
			return fmt.Errorf("%v rows were affected instead of 1", rowsAffected)
		}
		resp = &persistence.UpdateTaskListResponse{}
		return nil
	})
	return resp, err
}

func (m *sqlTaskManager) CreateTasks(request *persistence.CreateTasksRequest) (*persistence.CreateTasksResponse, error) {
	tasksRows := make([]sqldb.TasksRow, len(request.Tasks))
	for i, v := range request.Tasks {
		var expiryTime time.Time
		if v.Data.ScheduleToStartTimeout > 0 {
			expiryTime = time.Now().Add(time.Second * time.Duration(v.Data.ScheduleToStartTimeout))
		}
		tasksRows[i] = sqldb.TasksRow{
			DomainID:     v.Data.DomainID,
			WorkflowID:   v.Data.WorkflowID,
			RunID:        v.Data.RunID,
			ScheduleID:   v.Data.ScheduleID,
			TaskListName: request.TaskListInfo.Name,
			TaskType:     int64(request.TaskListInfo.TaskType),
			TaskID:       v.TaskID,
			ExpiryTs:     expiryTime,
		}
	}
	var resp *persistence.CreateTasksResponse
	err := m.txExecute("CreateTasks", func(tx sqldb.Tx) error {
		if _, err1 := tx.InsertIntoTasks(tasksRows); err1 != nil {
			return err1
		}
		// Lock task list before committing.
		err1 := lockTaskList(tx,
			request.TaskListInfo.DomainID, request.TaskListInfo.Name, request.TaskListInfo.TaskType, request.TaskListInfo.RangeID)
		if err1 != nil {
			return err1
		}
		resp = &persistence.CreateTasksResponse{}
		return nil
	})
	return resp, err
}

func (m *sqlTaskManager) GetTasks(request *persistence.GetTasksRequest) (*persistence.GetTasksResponse, error) {
	rows, err := m.db.SelectFromTasks(&sqldb.TasksFilter{
		DomainID:     request.DomainID,
		TaskListName: request.TaskList,
		TaskType:     int64(request.TaskType),
		MinTaskID:    &request.ReadLevel,
		MaxTaskID:    &request.MaxReadLevel,
		PageSize:     &request.BatchSize,
	})
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetTasks operation failed. Failed to get rows. Error: %v", err),
		}
	}

	var tasks = make([]*persistence.TaskInfo, len(rows))
	for i, v := range rows {
		tasks[i] = &persistence.TaskInfo{
			DomainID:   request.DomainID,
			WorkflowID: v.WorkflowID,
			RunID:      v.RunID,
			TaskID:     v.TaskID,
			ScheduleID: v.ScheduleID,
		}
	}

	return &persistence.GetTasksResponse{Tasks: tasks}, nil
}

// Deprecated
func (m *sqlTaskManager) CompleteTask(request *persistence.CompleteTaskRequest) error {
	taskID := request.TaskID
	taskList := request.TaskList
	_, err := m.db.DeleteFromTasks(&sqldb.TasksFilter{
		DomainID: taskList.DomainID, TaskListName: taskList.Name, TaskType: int64(taskList.TaskType), TaskID: &taskID})
	if err != nil && err != sql.ErrNoRows {
		return &workflow.InternalServiceError{Message: err.Error()}
	}
	return nil
}

func lockTaskList(tx sqldb.Tx, domainID, name string, taskListType int, oldRangeID int64) error {
	rangeID, err := tx.LockTaskLists(&sqldb.TaskListsFilter{DomainID: domainID, Name: name, TaskType: int64(taskListType)})
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to lock task list. Error: %v", err),
		}
	}
	if rangeID != oldRangeID {
		return &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("Task list range ID was %v when it was should have been %v", rangeID, oldRangeID),
		}
	}
	return nil
}

func stickyTaskListTTL() time.Time {
	return time.Now().Add(24 * time.Hour)
}
