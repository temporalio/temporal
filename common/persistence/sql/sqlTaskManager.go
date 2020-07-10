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
	taskScanPartitions uint32
}

var (
	minUUID = primitives.MustParseUUID("00000000-0000-0000-0000-000000000000")
)

// newTaskPersistence creates a new instance of TaskManager
func newTaskPersistence(db sqlplugin.DB, taskScanPartitions int, log log.Logger) (persistence.TaskManager, error) {
	return &sqlTaskManager{
		sqlStore: sqlStore{
			db:     db,
			logger: log,
		},
		taskScanPartitions: uint32(taskScanPartitions),
	}, nil
}

func (m *sqlTaskManager) LeaseTaskQueue(request *persistence.LeaseTaskQueueRequest) (*persistence.LeaseTaskQueueResponse, error) {
	var rangeID int64
	var ackLevel int64
	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}
	taskQueueHash := m.calculateTaskHash(nidBytes, request.TaskQueue, request.TaskType)
	namespaceID := request.NamespaceID
	rows, err := m.db.SelectFromTaskQueues(&sqlplugin.TaskQueuesFilter{
		RangeHash:   taskQueueHash,
		NamespaceID: &nidBytes,
		Name:        &request.TaskQueue,
		TaskType:    convert.Int64Ptr(int64(request.TaskType))})
	if err != nil {
		if err == sql.ErrNoRows {
			tqInfo := &persistenceblobs.TaskQueueInfo{
				NamespaceId:    namespaceID,
				Name:           request.TaskQueue,
				TaskType:       request.TaskType,
				AckLevel:       ackLevel,
				Kind:           request.TaskQueueKind,
				ExpiryTime:     nil,
				LastUpdateTime: types.TimestampNow(),
			}
			blob, err := serialization.TaskQueueInfoToBlob(tqInfo)
			if err != nil {
				return nil, err
			}
			row := sqlplugin.TaskQueuesRow{
				RangeHash:    taskQueueHash,
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
		err1 := lockTaskQueue(tx, taskQueueHash, nidBytes, request.TaskQueue, request.TaskType, rangeID)
		if err1 != nil {
			return err1
		}

		// todo: we shoudnt edit protobufs
		tqInfo.LastUpdateTime = types.TimestampNow()

		blob, err1 := serialization.TaskQueueInfoToBlob(tqInfo)
		if err1 != nil {
			return err1
		}
		result, err1 := tx.UpdateTaskQueues(&sqlplugin.TaskQueuesRow{
			RangeHash:    taskQueueHash,
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

	taskQueueHash := m.calculateTaskHash(nidBytes, request.TaskQueueInfo.Name, request.TaskQueueInfo.TaskType)

	tq := request.TaskQueueInfo
	tq.LastUpdateTime = types.TimestampNow()

	var blob serialization.DataBlob
	if request.TaskQueueInfo.Kind == enumspb.TASK_QUEUE_KIND_STICKY {
		tq.ExpiryTime, err = types.TimestampProto(stickyTaskQueueTTL())
		if err != nil {
			return nil, err
		}
		blob, err = serialization.TaskQueueInfoToBlob(tq)
		if err != nil {
			return nil, err
		}
		if _, err := m.db.ReplaceIntoTaskQueues(&sqlplugin.TaskQueuesRow{
			RangeHash:    taskQueueHash,
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
			tx, taskQueueHash, nidBytes, request.TaskQueueInfo.Name, request.TaskQueueInfo.TaskType, request.RangeID)
		if err1 != nil {
			return err1
		}
		result, err1 := tx.UpdateTaskQueues(&sqlplugin.TaskQueuesRow{
			RangeHash:    taskQueueHash,
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
	MinRangeHash   uint32
	MinNamespaceID primitives.UUID
	MinName        string
	MinTaskType    int64
}

func (m *sqlTaskManager) ListTaskQueue(request *persistence.ListTaskQueueRequest) (*persistence.ListTaskQueueResponse, error) {
	pageToken := taskQueuePageToken{MinTaskType: math.MinInt16, MinNamespaceID: minUUID}
	if request.PageToken != nil {
		if err := gobDeserialize(request.PageToken, &pageToken); err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("error deserializing page token: %v", err))
		}
	}
	var err error
	var rows []sqlplugin.TaskQueuesRow
	var shardGreaterThan uint32
	var shardLessThan uint32

	// Last page was full if these were set, see page token creation below
	lastPageFull := pageToken.MinName != "" || pageToken.MinNamespaceID != nil
	if lastPageFull {
		// Since last page was full, for completeness we need to explicitly drain the last range_hash seen.
		// A better option would be to always add one to PageSize internally to peek ahead as this results in extra queries to ensure we can move forward.
		// todo: revisit @shawn
		rows, err =  m.db.SelectFromTaskQueues(&sqlplugin.TaskQueuesFilter{
			RangeHash: 				     pageToken.MinRangeHash,
			NamespaceIDGreaterThan:      &pageToken.MinNamespaceID,
			NameGreaterThan:             &pageToken.MinName,
			TaskTypeGreaterThan:         &pageToken.MinTaskType,
			PageSize:                    &request.PageSize,
		})
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}

		// Increment the shard as we exhausted this hash in the above query.
		pageToken.MinRangeHash++
	}

	i := uint32(0)
	if pageToken.MinRangeHash > 0 {
		// Resume partition position from page token, if exists, before entering loop
		i = getPartitionForRangeHash(pageToken.MinRangeHash, m.taskScanPartitions)
	}

	for ; i < m.taskScanPartitions; i++ {
		if len(rows) > 0 {
			break
		}

		// Get start/end boundaries for partition
		shardGreaterThan, shardLessThan = getBoundariesForPartition(i, m.taskScanPartitions)

		if pageToken.MinRangeHash > shardGreaterThan {
			// Use as offset within partition that we should resume at
			shardGreaterThan = pageToken.MinRangeHash
		}

		rows, err = m.db.SelectFromTaskQueues(&sqlplugin.TaskQueuesFilter{
			RangeHashGreaterThanEqualTo: shardGreaterThan,
			RangeHashLessThanEqualTo:    shardLessThan,
			PageSize:                    &request.PageSize,
		})
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}
	}

	var nextPageToken []byte
	switch {
	case len(rows) >= request.PageSize:
		// Store the details of the lastRow seen up to PageSize.
		// Note we don't increment the rangeHash as we do in the case below.
		// This is so we can exhaust this hash before moving forward.
		lastRow := &rows[request.PageSize-1]
		nextPageToken, err = gobSerialize(&taskQueuePageToken{
			MinRangeHash:   lastRow.RangeHash,
			MinNamespaceID: lastRow.NamespaceID,
			MinName:        lastRow.Name,
			MinTaskType:    lastRow.TaskType,
		})
	case shardLessThan < math.MaxUint32:
		var lastRangeHash uint32
		if len(rows) == 0 {
			lastRangeHash = shardLessThan
		} else {
			lastRangeHash = rows[len(rows)-1].RangeHash
		}

		// Create page token with +1 from the last rangeHash we have seen to prevent duplicating the last row.
		// Since we have not exceeded PageSize, we are confident we won't lose data here and we have exhausted this hash.
		nextPageToken, err = gobSerialize(&taskQueuePageToken{MinRangeHash: lastRangeHash + 1, MinTaskType: math.MinInt16})
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

func getPartitionForRangeHash(rangeHash uint32, totalPartitions uint32) uint32 {
	if totalPartitions == 0 {
		return 0
	}
	return rangeHash / getPartitionBoundaryStart(1, totalPartitions)
}

func getPartitionBoundaryStart(partition uint32, totalPartitions uint32) uint32 {
	if totalPartitions == 0  {
		return 0
	}

	if partition == totalPartitions {
		return math.MaxUint32
	}

	return uint32((float32(partition) / float32(totalPartitions)) * math.MaxUint32)
}

func getBoundariesForPartition(partition uint32, totalPartitions uint32) (uint32, uint32) {
	return getPartitionBoundaryStart(partition, totalPartitions), getPartitionBoundaryStart(partition+1, totalPartitions)
}

func (m *sqlTaskManager) DeleteTaskQueue(request *persistence.DeleteTaskQueueRequest) error {
	nidBytes, err := primitives.ParseUUID(request.TaskQueue.NamespaceID)
	if err != nil {
		return serviceerror.NewInternal(err.Error())
	}

	result, err := m.db.DeleteFromTaskQueues(&sqlplugin.TaskQueuesFilter{
		RangeHash:   m.calculateTaskHash(nidBytes, request.TaskQueue.Name, request.TaskQueue.TaskType),
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
			m.calculateTaskHash(nidBytes, request.TaskQueueInfo.Data.Name, request.TaskQueueInfo.Data.TaskType),
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

// Returns uint32 hash for a particular TaskQueue/Task given a Namespace, Name and TaskQueueType
func (m *sqlTaskManager) calculateTaskHash(namespaceID primitives.UUID, name string, taskType enumspb.TaskQueueType) uint32 {
	return farm.Hash32(append(namespaceID, []byte("_"+name+"_"+string(int(taskType)))...))
}

func lockTaskQueue(tx sqlplugin.Tx, rangeHash uint32, namespaceID primitives.UUID, name string, taskQueueType enumspb.TaskQueueType, oldRangeID int64) error {
	rangeID, err := tx.LockTaskQueues(&sqlplugin.TaskQueuesFilter{
		RangeHash: rangeHash, NamespaceID: &namespaceID, Name: &name, TaskType: convert.Int64Ptr(int64(taskQueueType))})
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
