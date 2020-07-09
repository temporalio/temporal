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
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
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

func (m *sqlTaskManager) LeaseTaskQueue(request *persistence.LeaseTaskQueueRequest) (*persistence.LeaseTaskQueueResponse, error) {
	var rangeID int64
	var ackLevel int64
	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}
	shardID := m.shardID(nidBytes, request.TaskQueue)
	namespaceID := request.NamespaceID
	rows, err := m.db.SelectFromTaskQueues(&sqlplugin.TaskQueuesFilter{
		ShardID:     shardID,
		NamespaceID: &nidBytes,
		Name:        &request.TaskQueue,
		TaskType:    convert.Int64Ptr(int64(request.TaskType))})
	if err != nil {
		if err == sql.ErrNoRows {
			tqInfo := &persistenceblobs.TaskQueueInfo{
				NamespaceId: namespaceID,
				Name:        request.TaskQueue,
				TaskType:    request.TaskType,
				AckLevel:    ackLevel,
				Kind:        request.TaskQueueKind,
				Expiry:      nil,
				LastUpdated: types.TimestampNow(),
			}
			blob, err := serialization.TaskQueueInfoToBlob(tqInfo)
			if err != nil {
				return nil, err
			}
			row := sqlplugin.TaskQueuesRow{
				ShardID:      shardID,
				NamespaceID:  nidBytes,
				Name:         request.TaskQueue,
				TaskType:     int64(request.TaskType),
				Data:         blob.Data,
				DataEncoding: blob.Encoding.String(),
			}
			rows = []sqlplugin.TaskQueuesRow{row}
			if _, err := m.db.InsertIntoTaskQueues(&row); err != nil {
				return nil, serviceerror.NewInternal(fmt.Sprintf("LeaseTaskQueue operation failed. Failed to make task queue %v of type %v. Error: %v", request.TaskQueue, request.TaskType, err))
			}
		} else {
			return nil, serviceerror.NewInternal(fmt.Sprintf("LeaseTaskQueue operation failed. Failed to check if task queue existed. Error: %v", err))
		}
	}

	row := rows[0]
	if request.RangeID > 0 && request.RangeID != row.RangeID {
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("leaseTaskQueue:renew failed:taskQueue:%v, taskQueueType:%v, haveRangeID:%v, gotRangeID:%v",
				request.TaskQueue, request.TaskType, rangeID, row.RangeID),
		}
	}

	tqInfo, err := serialization.TaskQueueInfoFromBlob(row.Data, row.DataEncoding)
	if err != nil {
		return nil, err
	}

	var resp *persistence.LeaseTaskQueueResponse
	err = m.txExecute("LeaseTaskQueue", func(tx sqlplugin.Tx) error {
		rangeID = row.RangeID
		// We need to separately check the condition and do the
		// update because we want to throw different error codes.
		// Since we need to do things separately (in a transaction), we need to take a lock.
		err1 := lockTaskQueue(tx, shardID, nidBytes, request.TaskQueue, request.TaskType, rangeID)
		if err1 != nil {
			return err1
		}

		// todo: we shoudnt edit protobufs
		tqInfo.LastUpdated = types.TimestampNow()

		blob, err1 := serialization.TaskQueueInfoToBlob(tqInfo)
		if err1 != nil {
			return err1
		}
		result, err1 := tx.UpdateTaskQueues(&sqlplugin.TaskQueuesRow{
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
		resp = &persistence.LeaseTaskQueueResponse{TaskQueueInfo: &persistence.PersistedTaskQueueInfo{
			Data:    tqInfo,
			RangeID: row.RangeID + 1,
		}}
		return nil
	})
	return resp, err
}

func (m *sqlTaskManager) UpdateTaskQueue(request *persistence.UpdateTaskQueueRequest) (*persistence.UpdateTaskQueueResponse, error) {
	nidBytes, err := primitives.ParseUUID(request.TaskQueueInfo.GetNamespaceId())
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}

	shardID := m.shardID(nidBytes, request.TaskQueueInfo.Name)

	tq := request.TaskQueueInfo
	tq.LastUpdated = types.TimestampNow()

	var blob serialization.DataBlob
	if request.TaskQueueInfo.Kind == enumspb.TASK_QUEUE_KIND_STICKY {
		tq.Expiry, err = types.TimestampProto(stickyTaskQueueTTL())
		if err != nil {
			return nil, err
		}
		blob, err = serialization.TaskQueueInfoToBlob(tq)
		if err != nil {
			return nil, err
		}
		if _, err := m.db.ReplaceIntoTaskQueues(&sqlplugin.TaskQueuesRow{
			ShardID:      shardID,
			NamespaceID:  nidBytes,
			RangeID:      request.RangeID,
			Name:         request.TaskQueueInfo.Name,
			TaskType:     int64(request.TaskQueueInfo.TaskType),
			Data:         blob.Data,
			DataEncoding: string(blob.Encoding),
		}); err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("UpdateTaskQueue operation failed. Failed to make sticky task queue. Error: %v", err))
		}
	}
	var resp *persistence.UpdateTaskQueueResponse
	blob, err = serialization.TaskQueueInfoToBlob(tq)
	if err != nil {
		return nil, err
	}
	err = m.txExecute("UpdateTaskQueue", func(tx sqlplugin.Tx) error {
		err1 := lockTaskQueue(
			tx, shardID, nidBytes, request.TaskQueueInfo.Name, request.TaskQueueInfo.TaskType, request.RangeID)
		if err1 != nil {
			return err1
		}
		result, err1 := tx.UpdateTaskQueues(&sqlplugin.TaskQueuesRow{
			ShardID:      shardID,
			NamespaceID:  nidBytes,
			RangeID:      request.RangeID,
			Name:         request.TaskQueueInfo.Name,
			TaskType:     int64(request.TaskQueueInfo.TaskType),
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
		resp = &persistence.UpdateTaskQueueResponse{}
		return nil
	})
	return resp, err
}

type taskQueuePageToken struct {
	ShardID     int
	NamespaceID string
	Name        string
	TaskType    int64
}

func (m *sqlTaskManager) ListTaskQueue(request *persistence.ListTaskQueueRequest) (*persistence.ListTaskQueueResponse, error) {
	pageToken := taskQueuePageToken{TaskType: math.MinInt16, NamespaceID: minUUID}
	if request.PageToken != nil {
		if err := gobDeserialize(request.PageToken, &pageToken); err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("error deserializing page token: %v", err))
		}
	}
	var err error
	var rows []sqlplugin.TaskQueuesRow
	namespaceID := primitives.MustParseUUID(pageToken.NamespaceID)
	for pageToken.ShardID < m.nShards {
		rows, err = m.db.SelectFromTaskQueues(&sqlplugin.TaskQueuesFilter{
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
		pageToken = taskQueuePageToken{ShardID: pageToken.ShardID + 1, TaskType: math.MinInt16, NamespaceID: minUUID}
	}

	var nextPageToken []byte
	switch {
	case len(rows) >= request.PageSize:
		lastRow := &rows[request.PageSize-1]
		nextPageToken, err = gobSerialize(&taskQueuePageToken{
			ShardID:     pageToken.ShardID,
			NamespaceID: lastRow.NamespaceID.String(),
			Name:        lastRow.Name,
			TaskType:    lastRow.TaskType,
		})
	case pageToken.ShardID+1 < m.nShards:
		nextPageToken, err = gobSerialize(&taskQueuePageToken{ShardID: pageToken.ShardID + 1, TaskType: math.MinInt16})
	}

	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("error serializing nextPageToken:%v", err))
	}

	resp := &persistence.ListTaskQueueResponse{
		Items:         make([]*persistence.PersistedTaskQueueInfo, len(rows)),
		NextPageToken: nextPageToken,
	}

	for i := range rows {
		info, err := serialization.TaskQueueInfoFromBlob(rows[i].Data, rows[i].DataEncoding)
		if err != nil {
			return nil, err
		}
		resp.Items[i] = &persistence.PersistedTaskQueueInfo{
			Data:    info,
			RangeID: rows[i].RangeID,
		}
	}

	return resp, nil
}

func (m *sqlTaskManager) DeleteTaskQueue(request *persistence.DeleteTaskQueueRequest) error {
	nidBytes, err := primitives.ParseUUID(request.TaskQueue.NamespaceID)
	if err != nil {
		return serviceerror.NewInternal(err.Error())
	}

	result, err := m.db.DeleteFromTaskQueues(&sqlplugin.TaskQueuesFilter{
		ShardID:     m.shardID(nidBytes, request.TaskQueue.Name),
		NamespaceID: &nidBytes,
		Name:        &request.TaskQueue.Name,
		TaskType:    convert.Int64Ptr(int64(request.TaskQueue.TaskType)),
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
			NamespaceID:   nidBytes,
			TaskQueueName: request.TaskQueueInfo.Data.Name,
			TaskType:      int64(request.TaskQueueInfo.Data.TaskType),
			TaskID:        v.GetTaskId(),
			Data:          blob.Data,
			DataEncoding:  string(blob.Encoding),
		}
	}
	var resp *persistence.CreateTasksResponse
	err := m.txExecute("CreateTasks", func(tx sqlplugin.Tx) error {
		nidBytes, err := primitives.ParseUUID(request.TaskQueueInfo.Data.GetNamespaceId())
		if err != nil {
			return serviceerror.NewInternal(err.Error())
		}

		if _, err1 := tx.InsertIntoTasks(tasksRows); err1 != nil {
			return err1
		}
		// Lock task queue before committing.
		err1 := lockTaskQueue(tx,
			m.shardID(nidBytes, request.TaskQueueInfo.Data.Name),
			nidBytes,
			request.TaskQueueInfo.Data.Name,
			request.TaskQueueInfo.Data.TaskType,
			request.TaskQueueInfo.RangeID)
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
		NamespaceID:   nidBytes,
		TaskQueueName: request.TaskQueue,
		TaskType:      int64(request.TaskType),
		MinTaskID:     &request.ReadLevel,
		MaxTaskID:     request.MaxReadLevel,
		PageSize:      &request.BatchSize,
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
	nidBytes, err := primitives.ParseUUID(request.TaskQueue.NamespaceID)
	if err != nil {
		return serviceerror.NewInternal(err.Error())
	}

	taskID := request.TaskID
	taskQueue := request.TaskQueue
	_, err = m.db.DeleteFromTasks(&sqlplugin.TasksFilter{
		NamespaceID:   nidBytes,
		TaskQueueName: taskQueue.Name,
		TaskType:      int64(taskQueue.TaskType),
		TaskID:        &taskID})
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
		TaskQueueName:        request.TaskQueueName,
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

func lockTaskQueue(tx sqlplugin.Tx, shardID int, namespaceID primitives.UUID, name string, taskQueueType enumspb.TaskQueueType, oldRangeID int64) error {
	rangeID, err := tx.LockTaskQueues(&sqlplugin.TaskQueuesFilter{
		ShardID: shardID, NamespaceID: &namespaceID, Name: &name, TaskType: convert.Int64Ptr(int64(taskQueueType))})
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Failed to lock task queue. Error: %v", err))
	}
	if rangeID != oldRangeID {
		return &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("Task queue range ID was %v when it was should have been %v", rangeID, oldRangeID),
		}
	}
	return nil
}

func stickyTaskQueueTTL() time.Time {
	return time.Now().Add(24 * time.Hour)
}
