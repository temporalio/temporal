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

package sql

import (
	"database/sql"
	"fmt"
	"math"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/gogo/protobuf/types"
	enumspb "go.temporal.io/temporal-proto/enums/v1"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs/v1"
	"github.com/temporalio/temporal/common/convert"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/persistence/serialization"
	"github.com/temporalio/temporal/common/persistence/sql/sqlplugin"
	"github.com/temporalio/temporal/common/primitives"
)

type sqlTaskManager struct {
	sqlStore
	nShards int
}

var (
	minUUID = "00000000-0000-0000-0000-000000000000"
)

// newTaskPersistence creates a new instance of TaskManager
func newTaskPersistence(db sqlplugin.DB, nShards int, log log.Logger) (persistence.TaskManager, error) {
	return &sqlTaskManager{
		sqlStore: sqlStore{
			db:     db,
			logger: log,
		},
		nShards: nShards,
	}, nil
}

func (m *sqlTaskManager) LeaseTaskList(request *persistence.LeaseTaskListRequest) (*persistence.LeaseTaskListResponse, error) {
	var rangeID int64
	var ackLevel int64
	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}
	shardID := m.shardID(nidBytes, request.TaskList)
	namespaceID := request.NamespaceID
	rows, err := m.db.SelectFromTaskLists(&sqlplugin.TaskListsFilter{
		ShardID:     shardID,
		NamespaceID: &nidBytes,
		Name:        &request.TaskList,
		TaskType:    convert.Int64Ptr(int64(request.TaskType))})
	if err != nil {
		if err == sql.ErrNoRows {
			tlInfo := &persistenceblobs.TaskListInfo{
				NamespaceId: namespaceID,
				Name:        request.TaskList,
				TaskType:    request.TaskType,
				AckLevel:    ackLevel,
				Kind:        request.TaskListKind,
				Expiry:      nil,
				LastUpdated: types.TimestampNow(),
			}
			blob, err := serialization.TaskListInfoToBlob(tlInfo)
			if err != nil {
				return nil, err
			}
			row := sqlplugin.TaskListsRow{
				ShardID:      shardID,
				NamespaceID:  nidBytes,
				Name:         request.TaskList,
				TaskType:     int64(request.TaskType),
				Data:         blob.Data,
				DataEncoding: blob.Encoding.String(),
			}
			rows = []sqlplugin.TaskListsRow{row}
			if _, err := m.db.InsertIntoTaskLists(&row); err != nil {
				return nil, serviceerror.NewInternal(fmt.Sprintf("LeaseTaskList operation failed. Failed to make task list %v of type %v. Error: %v", request.TaskList, request.TaskType, err))
			}
		} else {
			return nil, serviceerror.NewInternal(fmt.Sprintf("LeaseTaskList operation failed. Failed to check if task list existed. Error: %v", err))
		}
	}

	row := rows[0]
	if request.RangeID > 0 && request.RangeID != row.RangeID {
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("leaseTaskList:renew failed:taskList:%v, taskListType:%v, haveRangeID:%v, gotRangeID:%v",
				request.TaskList, request.TaskType, rangeID, row.RangeID),
		}
	}

	tlInfo, err := serialization.TaskListInfoFromBlob(row.Data, row.DataEncoding)
	if err != nil {
		return nil, err
	}

	var resp *persistence.LeaseTaskListResponse
	err = m.txExecute("LeaseTaskList", func(tx sqlplugin.Tx) error {
		rangeID = row.RangeID
		// We need to separately check the condition and do the
		// update because we want to throw different error codes.
		// Since we need to do things separately (in a transaction), we need to take a lock.
		err1 := lockTaskList(tx, shardID, nidBytes, request.TaskList, request.TaskType, rangeID)
		if err1 != nil {
			return err1
		}

		// todo: we shoudnt edit protobufs
		tlInfo.LastUpdated = types.TimestampNow()

		blob, err1 := serialization.TaskListInfoToBlob(tlInfo)
		if err1 != nil {
			return err1
		}
		result, err1 := tx.UpdateTaskLists(&sqlplugin.TaskListsRow{
			ShardID:      shardID,
			NamespaceID:  row.NamespaceID,
			RangeID:      row.RangeID + 1,
			Name:         row.Name,
			TaskType:     row.TaskType,
			Data:         blob.Data,
			DataEncoding: string(blob.Encoding),
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
		resp = &persistence.LeaseTaskListResponse{TaskListInfo: &persistence.PersistedTaskListInfo{
			Data:    tlInfo,
			RangeID: row.RangeID + 1,
		}}
		return nil
	})
	return resp, err
}

func (m *sqlTaskManager) UpdateTaskList(request *persistence.UpdateTaskListRequest) (*persistence.UpdateTaskListResponse, error) {
	nidBytes, err := primitives.ParseUUID(request.TaskListInfo.GetNamespaceId())
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}

	shardID := m.shardID(nidBytes, request.TaskListInfo.Name)

	tl := request.TaskListInfo
	tl.LastUpdated = types.TimestampNow()

	var blob serialization.DataBlob
	if request.TaskListInfo.Kind == enumspb.TASK_LIST_KIND_STICKY {
		tl.Expiry, err = types.TimestampProto(stickyTaskListTTL())
		if err != nil {
			return nil, err
		}
		blob, err = serialization.TaskListInfoToBlob(tl)
		if err != nil {
			return nil, err
		}
		if _, err := m.db.ReplaceIntoTaskLists(&sqlplugin.TaskListsRow{
			ShardID:      shardID,
			NamespaceID:  nidBytes,
			RangeID:      request.RangeID,
			Name:         request.TaskListInfo.Name,
			TaskType:     int64(request.TaskListInfo.TaskType),
			Data:         blob.Data,
			DataEncoding: string(blob.Encoding),
		}); err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("UpdateTaskList operation failed. Failed to make sticky task list. Error: %v", err))
		}
	}
	var resp *persistence.UpdateTaskListResponse
	blob, err = serialization.TaskListInfoToBlob(tl)
	if err != nil {
		return nil, err
	}
	err = m.txExecute("UpdateTaskList", func(tx sqlplugin.Tx) error {
		err1 := lockTaskList(
			tx, shardID, nidBytes, request.TaskListInfo.Name, request.TaskListInfo.TaskType, request.RangeID)
		if err1 != nil {
			return err1
		}
		result, err1 := tx.UpdateTaskLists(&sqlplugin.TaskListsRow{
			ShardID:      shardID,
			NamespaceID:  nidBytes,
			RangeID:      request.RangeID,
			Name:         request.TaskListInfo.Name,
			TaskType:     int64(request.TaskListInfo.TaskType),
			Data:         blob.Data,
			DataEncoding: string(blob.Encoding),
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

type taskListPageToken struct {
	ShardID     int
	NamespaceID string
	Name        string
	TaskType    int64
}

func (m *sqlTaskManager) ListTaskList(request *persistence.ListTaskListRequest) (*persistence.ListTaskListResponse, error) {
	pageToken := taskListPageToken{TaskType: math.MinInt16, NamespaceID: minUUID}
	if request.PageToken != nil {
		if err := gobDeserialize(request.PageToken, &pageToken); err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("error deserializing page token: %v", err))
		}
	}
	var err error
	var rows []sqlplugin.TaskListsRow
	namespaceID := primitives.MustParseUUID(pageToken.NamespaceID)
	for pageToken.ShardID < m.nShards {
		rows, err = m.db.SelectFromTaskLists(&sqlplugin.TaskListsFilter{
			ShardID:                pageToken.ShardID,
			NamespaceIDGreaterThan: &namespaceID,
			NameGreaterThan:        &pageToken.Name,
			TaskTypeGreaterThan:    &pageToken.TaskType,
			PageSize:               &request.PageSize,
		})
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}
		if len(rows) > 0 {
			break
		}
		pageToken = taskListPageToken{ShardID: pageToken.ShardID + 1, TaskType: math.MinInt16, NamespaceID: minUUID}
	}

	var nextPageToken []byte
	switch {
	case len(rows) >= request.PageSize:
		lastRow := &rows[request.PageSize-1]
		nextPageToken, err = gobSerialize(&taskListPageToken{
			ShardID:     pageToken.ShardID,
			NamespaceID: lastRow.NamespaceID.String(),
			Name:        lastRow.Name,
			TaskType:    lastRow.TaskType,
		})
	case pageToken.ShardID+1 < m.nShards:
		nextPageToken, err = gobSerialize(&taskListPageToken{ShardID: pageToken.ShardID + 1, TaskType: math.MinInt16})
	}

	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("error serializing nextPageToken:%v", err))
	}

	resp := &persistence.ListTaskListResponse{
		Items:         make([]*persistence.PersistedTaskListInfo, len(rows)),
		NextPageToken: nextPageToken,
	}

	for i := range rows {
		info, err := serialization.TaskListInfoFromBlob(rows[i].Data, rows[i].DataEncoding)
		if err != nil {
			return nil, err
		}
		resp.Items[i] = &persistence.PersistedTaskListInfo{
			Data:    info,
			RangeID: rows[i].RangeID,
		}
	}

	return resp, nil
}

func (m *sqlTaskManager) DeleteTaskList(request *persistence.DeleteTaskListRequest) error {
	nidBytes, err := primitives.ParseUUID(request.TaskList.NamespaceID)
	if err != nil {
		return serviceerror.NewInternal(err.Error())
	}

	result, err := m.db.DeleteFromTaskLists(&sqlplugin.TaskListsFilter{
		ShardID:     m.shardID(nidBytes, request.TaskList.Name),
		NamespaceID: &nidBytes,
		Name:        &request.TaskList.Name,
		TaskType:    convert.Int64Ptr(int64(request.TaskList.TaskType)),
		RangeID:     &request.RangeID,
	})
	if err != nil {
		return serviceerror.NewInternal(err.Error())
	}
	nRows, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("rowsAffected returned error:%v", err))
	}
	if nRows != 1 {
		return serviceerror.NewInternal(fmt.Sprintf("delete failed: %v rows affected instead of 1", nRows))
	}
	return nil
}

func (m *sqlTaskManager) CreateTasks(request *persistence.CreateTasksRequest) (*persistence.CreateTasksResponse, error) {
	tasksRows := make([]sqlplugin.TasksRow, len(request.Tasks))
	for i, v := range request.Tasks {
		nidBytes, err := primitives.ParseUUID(v.Data.GetNamespaceId())
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}

		blob, err := serialization.TaskInfoToBlob(v)

		if err != nil {
			return nil, err
		}
		tasksRows[i] = sqlplugin.TasksRow{
			NamespaceID:  nidBytes,
			TaskListName: request.TaskListInfo.Data.Name,
			TaskType:     int64(request.TaskListInfo.Data.TaskType),
			TaskID:       v.GetTaskId(),
			Data:         blob.Data,
			DataEncoding: string(blob.Encoding),
		}
	}
	var resp *persistence.CreateTasksResponse
	err := m.txExecute("CreateTasks", func(tx sqlplugin.Tx) error {
		nidBytes, err := primitives.ParseUUID(request.TaskListInfo.Data.GetNamespaceId())
		if err != nil {
			return serviceerror.NewInternal(err.Error())
		}

		if _, err1 := tx.InsertIntoTasks(tasksRows); err1 != nil {
			return err1
		}
		// Lock task list before committing.
		err1 := lockTaskList(tx,
			m.shardID(nidBytes, request.TaskListInfo.Data.Name),
			nidBytes,
			request.TaskListInfo.Data.Name,
			request.TaskListInfo.Data.TaskType,
			request.TaskListInfo.RangeID)
		if err1 != nil {
			return err1
		}
		resp = &persistence.CreateTasksResponse{}
		return nil
	})
	return resp, err
}

func (m *sqlTaskManager) GetTasks(request *persistence.GetTasksRequest) (*persistence.GetTasksResponse, error) {
	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}

	rows, err := m.db.SelectFromTasks(&sqlplugin.TasksFilter{
		NamespaceID:  nidBytes,
		TaskListName: request.TaskList,
		TaskType:     int64(request.TaskType),
		MinTaskID:    &request.ReadLevel,
		MaxTaskID:    request.MaxReadLevel,
		PageSize:     &request.BatchSize,
	})
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetTasks operation failed. Failed to get rows. Error: %v", err))
	}

	var tasks = make([]*persistenceblobs.AllocatedTaskInfo, len(rows))
	for i, v := range rows {
		info, err := serialization.TaskInfoFromBlob(v.Data, v.DataEncoding)
		if err != nil {
			return nil, err
		}
		tasks[i] = info
	}

	return &persistence.GetTasksResponse{Tasks: tasks}, nil
}

func (m *sqlTaskManager) CompleteTask(request *persistence.CompleteTaskRequest) error {
	nidBytes, err := primitives.ParseUUID(request.TaskList.NamespaceID)
	if err != nil {
		return serviceerror.NewInternal(err.Error())
	}

	taskID := request.TaskID
	taskList := request.TaskList
	_, err = m.db.DeleteFromTasks(&sqlplugin.TasksFilter{
		NamespaceID:  nidBytes,
		TaskListName: taskList.Name,
		TaskType:     int64(taskList.TaskType),
		TaskID:       &taskID})
	if err != nil && err != sql.ErrNoRows {
		return serviceerror.NewInternal(err.Error())
	}
	return nil
}

func (m *sqlTaskManager) CompleteTasksLessThan(request *persistence.CompleteTasksLessThanRequest) (int, error) {
	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return 0, serviceerror.NewInternal(err.Error())
	}
	result, err := m.db.DeleteFromTasks(&sqlplugin.TasksFilter{
		NamespaceID:          nidBytes,
		TaskListName:         request.TaskListName,
		TaskType:             int64(request.TaskType),
		TaskIDLessThanEquals: &request.TaskID,
		Limit:                &request.Limit,
	})
	if err != nil {
		return 0, serviceerror.NewInternal(err.Error())
	}
	nRows, err := result.RowsAffected()
	if err != nil {
		return 0, serviceerror.NewInternal(fmt.Sprintf("rowsAffected returned error: %v", err))
	}
	return int(nRows), nil
}

func (m *sqlTaskManager) shardID(namespaceID primitives.UUID, name string) int {
	id := farm.Hash32(append(namespaceID, []byte("_"+name)...)) % uint32(m.nShards)
	return int(id)
}

func lockTaskList(tx sqlplugin.Tx, shardID int, namespaceID primitives.UUID, name string, taskListType enumspb.TaskListType, oldRangeID int64) error {
	rangeID, err := tx.LockTaskLists(&sqlplugin.TaskListsFilter{
		ShardID: shardID, NamespaceID: &namespaceID, Name: &name, TaskType: convert.Int64Ptr(int64(taskListType))})
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to lock task list. Error: %v", err))
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
