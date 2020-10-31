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
	"bytes"
	"database/sql"
	"fmt"
	"math"
	"time"

	"github.com/dgryski/go-farm"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
)

type sqlTaskManager struct {
	sqlStore
	taskScanPartitions uint32
}

var (
	// minUUID = primitives.MustParseUUID("00000000-0000-0000-0000-000000000000")
	minTaskQueueId = make([]byte, 0, 0)
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
	tqId, tqHash := m.taskQueueIdAndHash(nidBytes, request.TaskQueue, request.TaskType)
	rows, err := m.db.SelectFromTaskQueues(sqlplugin.TaskQueuesFilter{RangeHash: tqHash, TaskQueueID: tqId})
	if err != nil {
		if err == sql.ErrNoRows {
			namespaceID := request.NamespaceID
			tqInfo := &persistencespb.TaskQueueInfo{
				NamespaceId:    namespaceID,
				Name:           request.TaskQueue,
				TaskType:       request.TaskType,
				AckLevel:       ackLevel,
				Kind:           request.TaskQueueKind,
				ExpiryTime:     nil,
				LastUpdateTime: timestamp.TimeNowPtrUtc(),
			}
			blob, err := serialization.TaskQueueInfoToBlob(tqInfo)
			if err != nil {
				return nil, err
			}
			row := sqlplugin.TaskQueuesRow{
				RangeHash:    tqHash,
				TaskQueueID:  tqId,
				Data:         blob.Data,
				DataEncoding: blob.EncodingType.String(),
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
		err1 := lockTaskQueue(tx, tqHash, tqId, rangeID)
		if err1 != nil {
			return err1
		}

		// todo: we shoudnt edit protobufs
		tqInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()

		blob, err1 := serialization.TaskQueueInfoToBlob(tqInfo)
		if err1 != nil {
			return err1
		}
		result, err1 := tx.UpdateTaskQueues(&sqlplugin.TaskQueuesRow{
			RangeHash:    tqHash,
			TaskQueueID:  tqId,
			RangeID:      row.RangeID + 1,
			Data:         blob.Data,
			DataEncoding: blob.EncodingType.String(),
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

	tqId, tqHash := m.taskQueueIdAndHash(nidBytes, request.TaskQueueInfo.Name, request.TaskQueueInfo.TaskType)

	tq := request.TaskQueueInfo
	tq.LastUpdateTime = timestamp.TimeNowPtrUtc()

	var blob commonpb.DataBlob
	if request.TaskQueueInfo.Kind == enumspb.TASK_QUEUE_KIND_STICKY {
		tq.ExpiryTime = stickyTaskQueueTTL()
		blob, err = serialization.TaskQueueInfoToBlob(tq)
		if err != nil {
			return nil, err
		}
		if _, err := m.db.ReplaceIntoTaskQueues(&sqlplugin.TaskQueuesRow{
			RangeHash:    tqHash,
			TaskQueueID:  tqId,
			RangeID:      request.RangeID,
			Data:         blob.Data,
			DataEncoding: blob.EncodingType.String(),
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
		err1 := lockTaskQueue(tx, tqHash, tqId, request.RangeID)
		if err1 != nil {
			return err1
		}
		result, err1 := tx.UpdateTaskQueues(&sqlplugin.TaskQueuesRow{
			RangeHash:    tqHash,
			TaskQueueID:  tqId,
			RangeID:      request.RangeID,
			Data:         blob.Data,
			DataEncoding: blob.EncodingType.String(),
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
	MinTaskQueueId []byte
}

func (m *sqlTaskManager) ListTaskQueue(request *persistence.ListTaskQueueRequest) (*persistence.ListTaskQueueResponse, error) {
	pageToken := taskQueuePageToken{MinTaskQueueId: minTaskQueueId}
	if request.PageToken != nil {
		if err := gobDeserialize(request.PageToken, &pageToken); err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("error deserializing page token: %v", err))
		}
	}
	var err error
	var rows []sqlplugin.TaskQueuesRow
	var shardGreaterThan uint32
	var shardLessThan uint32

	i := uint32(0)
	if pageToken.MinRangeHash > 0 {
		// Resume partition position from page token, if exists, before entering loop
		i = getPartitionForRangeHash(pageToken.MinRangeHash, m.taskScanPartitions)
	}

	lastPageFull := !bytes.Equal(pageToken.MinTaskQueueId, minTaskQueueId)
	for ; i < m.taskScanPartitions; i++ {
		// Get start/end boundaries for partition
		shardGreaterThan, shardLessThan = getBoundariesForPartition(i, m.taskScanPartitions)

		// If page token hash is greater than the boundaries for this partition, use the pageToken hash for resume point
		if pageToken.MinRangeHash > shardGreaterThan {
			shardGreaterThan = pageToken.MinRangeHash
		}

		filter := sqlplugin.TaskQueuesFilter{
			RangeHashGreaterThanEqualTo: shardGreaterThan,
			RangeHashLessThanEqualTo:    shardLessThan,
			TaskQueueIDGreaterThan:      minTaskQueueId,
			PageSize:                    &request.PageSize,
		}

		if lastPageFull {
			// Use page token TaskQueueID filter for this query and set this to false
			// in order for the next partition so we don't miss any results.
			filter.TaskQueueIDGreaterThan = pageToken.MinTaskQueueId
			lastPageFull = false
		}

		rows, err = m.db.SelectFromTaskQueues(filter)
		if err != nil {
			return nil, serviceerror.NewInternal(err.Error())
		}

		if len(rows) > 0 {
			break
		}
	}

	maxRangeHash := uint32(0)
	resp := &persistence.ListTaskQueueResponse{
		Items: make([]*persistence.PersistedTaskQueueInfo, len(rows)),
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

		// Only want to look at up to PageSize number of records to prevent losing data.
		if rows[i].RangeHash > maxRangeHash {
			maxRangeHash = rows[i].RangeHash
		}

		// Enforces PageSize
		if i >= request.PageSize-1 {
			break
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
			MinRangeHash:   shardGreaterThan,
			MinTaskQueueId: lastRow.TaskQueueID,
		})
	case shardLessThan < math.MaxUint32:
		// Create page token with +1 from the last rangeHash we have seen to prevent duplicating the last row.
		// Since we have not exceeded PageSize, we are confident we won't lose data here and we have exhausted this hash.
		nextPageToken, err = gobSerialize(&taskQueuePageToken{MinRangeHash: shardLessThan + 1, MinTaskQueueId: minTaskQueueId})
	}

	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("error serializing nextPageToken:%v", err))
	}

	resp.NextPageToken = nextPageToken
	return resp, nil
}

func getPartitionForRangeHash(rangeHash uint32, totalPartitions uint32) uint32 {
	if totalPartitions == 0 {
		return 0
	}
	return rangeHash / getPartitionBoundaryStart(1, totalPartitions)
}

func getPartitionBoundaryStart(partition uint32, totalPartitions uint32) uint32 {
	if totalPartitions == 0 {
		return 0
	}

	if partition >= totalPartitions {
		return math.MaxUint32
	}

	return uint32((float32(partition) / float32(totalPartitions)) * math.MaxUint32)
}

func getBoundariesForPartition(partition uint32, totalPartitions uint32) (uint32, uint32) {
	endBoundary := getPartitionBoundaryStart(partition+1, totalPartitions)

	if endBoundary != math.MaxUint32 {
		endBoundary--
	}

	return getPartitionBoundaryStart(partition, totalPartitions), endBoundary
}

func (m *sqlTaskManager) DeleteTaskQueue(request *persistence.DeleteTaskQueueRequest) error {
	nidBytes, err := primitives.ParseUUID(request.TaskQueue.NamespaceID)
	if err != nil {
		return serviceerror.NewInternal(err.Error())
	}
	tqId, tqHash := m.taskQueueIdAndHash(nidBytes, request.TaskQueue.Name, request.TaskQueue.TaskType)
	result, err := m.db.DeleteFromTaskQueues(sqlplugin.TaskQueuesFilter{
		RangeHash:   tqHash,
		TaskQueueID: tqId,
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

		tqId, tqHash := m.taskQueueIdAndHash(nidBytes, request.TaskQueueInfo.Data.Name, request.TaskQueueInfo.Data.TaskType)
		tasksRows[i] = sqlplugin.TasksRow{
			RangeHash:    tqHash,
			TaskQueueID:  tqId,
			TaskID:       v.GetTaskId(),
			Data:         blob.Data,
			DataEncoding: blob.EncodingType.String(),
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
		tqId, tqHash := m.taskQueueIdAndHash(nidBytes, request.TaskQueueInfo.Data.Name, request.TaskQueueInfo.Data.TaskType)
		// Lock task queue before committing.
		err1 := lockTaskQueue(tx, tqHash, tqId, request.TaskQueueInfo.RangeID)
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

	tqId, tqHash := m.taskQueueIdAndHash(nidBytes, request.TaskQueue, request.TaskType)
	rows, err := m.db.SelectFromTasks(sqlplugin.TasksFilter{
		RangeHash:   tqHash,
		TaskQueueID: tqId,
		MinTaskID:   &request.ReadLevel,
		MaxTaskID:   request.MaxReadLevel,
		PageSize:    &request.BatchSize,
	})
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetTasks operation failed. Failed to get rows. Error: %v", err))
	}

	var tasks = make([]*persistencespb.AllocatedTaskInfo, len(rows))
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
	tqId, tqHash := m.taskQueueIdAndHash(nidBytes, request.TaskQueue.Name, request.TaskQueue.TaskType)
	_, err = m.db.DeleteFromTasks(sqlplugin.TasksFilter{
		RangeHash:   tqHash,
		TaskQueueID: tqId,
		TaskID:      &taskID})
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
	tqId, tqHash := m.taskQueueIdAndHash(nidBytes, request.TaskQueueName, request.TaskType)
	result, err := m.db.DeleteFromTasks(sqlplugin.TasksFilter{
		RangeHash:            tqHash,
		TaskQueueID:          tqId,
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
func (m *sqlTaskManager) calculateTaskQueueHash(namespaceID primitives.UUID, name string, taskType enumspb.TaskQueueType) uint32 {
	return farm.Fingerprint32(m.taskQueueId(namespaceID, name, taskType))
}

// Returns uint32 hash for a particular TaskQueue/Task given a Namespace, Name and TaskQueueType
func (m *sqlTaskManager) taskQueueIdAndHash(namespaceID primitives.UUID, name string, taskType enumspb.TaskQueueType) ([]byte, uint32) {
	id := m.taskQueueId(namespaceID, name, taskType)
	return id, farm.Fingerprint32(id)
}

func (m *sqlTaskManager) taskQueueId(namespaceID primitives.UUID, name string, taskType enumspb.TaskQueueType) []byte {
	idBytes := make([]byte, 0, 16+len(name)+1)
	idBytes = append(idBytes, namespaceID...)
	idBytes = append(idBytes, []byte(name)...)
	idBytes = append(idBytes, uint8(taskType))
	return idBytes
}

func lockTaskQueue(tx sqlplugin.Tx, tqHash uint32, tqId []byte, oldRangeID int64) error {
	rangeID, err := tx.LockTaskQueues(sqlplugin.TaskQueuesFilter{RangeHash: tqHash, TaskQueueID: tqId})
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

func stickyTaskQueueTTL() *time.Time {
	return timestamp.TimePtr(time.Now().UTC().Add(24 * time.Hour))
}
