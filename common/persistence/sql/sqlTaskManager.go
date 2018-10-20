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

	"github.com/jmoiron/sqlx"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/config"
)

type (
	sqlTaskManager struct {
		sqlStore
	}

	tasksRow struct {
		DomainID     string
		WorkflowID   string
		RunID        string
		ScheduleID   int64
		TaskID       int64
		TaskListName string
		TaskListType int64
		ExpiryTs     time.Time
	}

	tasksListsRow struct {
		DomainID string
		RangeID  int64
		Name     string
		TaskType int64
		AckLevel int64
		Kind     int64
		ExpiryTs time.Time
	}

	updateTaskListsRow struct {
		tasksListsRow
		OldRangeID int64
	}
)

const (
	taskListCreatePart = `INTO task_lists(domain_id, range_id, name, task_type, ack_level, kind, expiry_ts) ` +
		`VALUES (:domain_id, :range_id, :name, :task_type, :ack_level, :kind, :expiry_ts)`

	// (default range ID: initialRangeID == 1)
	createTaskListSQLQuery = `INSERT ` + taskListCreatePart

	updateTaskListWithTTLSQLQuery = `REPLACE ` + taskListCreatePart

	updateTaskListSQLQuery = `UPDATE task_lists SET
domain_id = :domain_id,
range_id = :range_id,
name = :name,
task_type = :task_type,
ack_level = :ack_level,
kind = :kind,
expiry_ts = :expiry_ts
WHERE
domain_id = :domain_id AND
name = :name AND
task_type = :task_type
`

	getTaskListSQLQuery = `SELECT domain_id, range_id, name, task_type, ack_level, kind, expiry_ts ` +
		`FROM task_lists ` +
		`WHERE domain_id = ? AND name = ? AND task_type = ?`

	lockTaskListSQLQuery = `SELECT range_id FROM task_lists ` +
		`WHERE domain_id = ? AND name = ? AND task_type = ? FOR UPDATE`

	getTaskSQLQuery = `SELECT workflow_id, run_id, schedule_id, task_id ` +
		`FROM tasks ` +
		`WHERE domain_id = ? AND task_list_name = ? AND task_list_type = ? AND task_id > ? AND task_id <= ?`

	createTaskSQLQuery = `INSERT INTO ` +
		`tasks(domain_id, workflow_id, run_id, schedule_id, task_list_name, task_list_type, task_id, expiry_ts) ` +
		`VALUES(:domain_id, :workflow_id, :run_id, :schedule_id, :task_list_name, :task_list_type, :task_id, :expiry_ts)`

	deleteTaskSQLQuery = `DELETE FROM tasks ` +
		`WHERE domain_id = ? AND task_list_name = ? AND task_list_type = ? AND task_id = ?`
)

// newTaskPersistence creates a new instance of TaskManager
func newTaskPersistence(cfg config.SQL, log bark.Logger) (persistence.TaskManager, error) {
	var db, err = newConnection(cfg)
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

func (m *sqlTaskManager) Close() {
	if m.db != nil {
		m.db.Close()
	}
}

func (m *sqlTaskManager) LeaseTaskList(request *persistence.LeaseTaskListRequest) (*persistence.LeaseTaskListResponse, error) {
	var row tasksListsRow
	var rangeID int64
	var ackLevel int64
	if err := m.db.Get(&row, getTaskListSQLQuery, request.DomainID, request.TaskList, request.TaskType); err != nil {
		if err == sql.ErrNoRows {
			row = tasksListsRow{
				DomainID: request.DomainID,
				Name:     request.TaskList,
				TaskType: int64(request.TaskType),
				AckLevel: ackLevel,
				Kind:     int64(request.TaskListKind),
				ExpiryTs: time.Time{},
			}
			if _, err := m.db.NamedExec(createTaskListSQLQuery, &row); err != nil {
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
	err := m.txExecute("LeaseTaskList", func(tx *sqlx.Tx) error {
		rangeID = row.RangeID
		ackLevel = row.AckLevel
		// We need to separately check the condition and do the
		// update because we want to throw different error codes.
		// Since we need to do things separately (in a transaction), we need to take a lock.
		if err1 := lockTaskList(tx, request.DomainID, request.TaskList, request.TaskType, row.RangeID); err1 != nil {
			return err1
		}
		result, err1 := tx.NamedExec(updateTaskListSQLQuery,
			&updateTaskListsRow{
				tasksListsRow{
					DomainID: row.DomainID,
					RangeID:  row.RangeID + 1,
					Name:     row.Name,
					TaskType: row.TaskType,
					AckLevel: row.AckLevel,
					Kind:     row.Kind,
					ExpiryTs: row.ExpiryTs,
				},
				row.RangeID,
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
		// If sticky, update with TTL
		if _, err := m.db.NamedExec(updateTaskListWithTTLSQLQuery, &tasksListsRow{
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
	err := m.txExecute("UpdateTaskList", func(tx *sqlx.Tx) error {
		err1 := lockTaskList(
			tx, request.TaskListInfo.DomainID, request.TaskListInfo.Name, request.TaskListInfo.TaskType, request.TaskListInfo.RangeID)
		if err1 != nil {
			return err1
		}
		result, err1 := tx.NamedExec(updateTaskListSQLQuery,
			&updateTaskListsRow{
				tasksListsRow{
					request.TaskListInfo.DomainID,
					request.TaskListInfo.RangeID,
					request.TaskListInfo.Name,
					int64(request.TaskListInfo.TaskType),
					request.TaskListInfo.AckLevel,
					int64(request.TaskListInfo.Kind),
					time.Time{},
				},
				request.TaskListInfo.RangeID,
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
	tasksRows := make([]tasksRow, len(request.Tasks))
	for i, v := range request.Tasks {
		var expiryTime time.Time
		if v.Data.ScheduleToStartTimeout > 0 {
			expiryTime = time.Now().Add(time.Second * time.Duration(v.Data.ScheduleToStartTimeout))
		}
		tasksRows[i] = tasksRow{
			DomainID:     v.Data.DomainID,
			WorkflowID:   v.Data.WorkflowID,
			RunID:        v.Data.RunID,
			ScheduleID:   v.Data.ScheduleID,
			TaskListName: request.TaskListInfo.Name,
			TaskListType: int64(request.TaskListInfo.TaskType),
			TaskID:       v.TaskID,
			ExpiryTs:     expiryTime,
		}
	}
	var resp *persistence.CreateTasksResponse
	err := m.txExecute("CreateTasks", func(tx *sqlx.Tx) error {
		query, args, err1 := m.db.BindNamed(createTaskSQLQuery, tasksRows)
		if err1 != nil {
			return err1
		}
		if _, err1 := tx.Exec(query, args...); err1 != nil {
			return err1
		}
		// Lock task list before committing.
		err1 = lockTaskList(tx,
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
	var rows []tasksRow
	if err := m.db.Select(&rows, getTaskSQLQuery, request.DomainID, request.TaskList, request.TaskType, request.ReadLevel, request.MaxReadLevel); err != nil {
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
	_, err := m.db.Exec(deleteTaskSQLQuery, taskList.DomainID, taskList.Name, int64(taskList.TaskType), taskID)
	if err != nil && err != sql.ErrNoRows {
		return &workflow.InternalServiceError{Message: err.Error()}
	}
	return nil
}

func lockTaskList(tx *sqlx.Tx, domainID, name string, taskListType int, oldRangeID int64) error {
	var rangeID int64
	if err := tx.Get(&rangeID, lockTaskListSQLQuery, domainID, name, taskListType); err != nil {
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
