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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"

	"github.com/dgryski/go-farm"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

type (
	taskQueuePageToken struct {
		MinRangeHash   uint32
		MinTaskQueueId []byte
	}

	sqlTaskManager struct {
		SqlStore
		taskScanPartitions uint32
	}
)

var (
	// minUUID = primitives.MustParseUUID("00000000-0000-0000-0000-000000000000")
	minTaskQueueId = make([]byte, 0)
)

// newTaskPersistence creates a new instance of TaskManager
func newTaskPersistence(
	db sqlplugin.DB,
	taskScanPartitions int,
	logger log.Logger,
) (persistence.TaskStore, error) {
	return &sqlTaskManager{
		SqlStore:           NewSqlStore(db, logger),
		taskScanPartitions: uint32(taskScanPartitions),
	}, nil
}

func (m *sqlTaskManager) CreateTaskQueue(
	ctx context.Context,
	request *persistence.InternalCreateTaskQueueRequest,
) error {
	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return serviceerror.NewInternal(err.Error())
	}
	tqId, tqHash := m.taskQueueIdAndHash(nidBytes, request.TaskQueue, request.TaskType)

	row := sqlplugin.TaskQueuesRow{
		RangeHash:    tqHash,
		TaskQueueID:  tqId,
		RangeID:      request.RangeID,
		Data:         request.TaskQueueInfo.Data,
		DataEncoding: request.TaskQueueInfo.EncodingType.String(),
	}
	if _, err := m.Db.InsertIntoTaskQueues(ctx, &row); err != nil {
		if m.Db.IsDupEntryError(err) {
			return &persistence.ConditionFailedError{Msg: err.Error()}
		}
		return serviceerror.NewUnavailable(fmt.Sprintf("CreateTaskQueue operation failed. Failed to make task queue %v of type %v. Error: %v", request.TaskQueue, request.TaskType, err))
	}

	return nil
}

func (m *sqlTaskManager) GetTaskQueue(
	ctx context.Context,
	request *persistence.InternalGetTaskQueueRequest,
) (*persistence.InternalGetTaskQueueResponse, error) {
	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}
	tqId, tqHash := m.taskQueueIdAndHash(nidBytes, request.TaskQueue, request.TaskType)
	rows, err := m.Db.SelectFromTaskQueues(ctx, sqlplugin.TaskQueuesFilter{
		RangeHash:   tqHash,
		TaskQueueID: tqId,
	})

	switch err {
	case nil:
		if len(rows) != 1 {
			return nil, serviceerror.NewUnavailable(
				fmt.Sprintf("GetTaskQueue operation failed. Expect exactly one result row, but got %d for task queue %v of type %v",
					len(rows), request.TaskQueue, request.TaskType))
		}
		row := rows[0]
		return &persistence.InternalGetTaskQueueResponse{
			RangeID:       row.RangeID,
			TaskQueueInfo: persistence.NewDataBlob(row.Data, row.DataEncoding),
		}, nil
	case sql.ErrNoRows:
		return nil, serviceerror.NewNotFound(
			fmt.Sprintf("GetTaskQueue operation failed. TaskQueue: %v, TaskQueueType: %v, Error: %v",
				request.TaskQueue, request.TaskType, err))
	default:
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("GetTaskQueue operation failed. Failed to check if task queue %v of type %v existed. Error: %v",
				request.TaskQueue, request.TaskType, err))
	}
}

func (m *sqlTaskManager) UpdateTaskQueue(
	ctx context.Context,
	request *persistence.InternalUpdateTaskQueueRequest,
) (*persistence.UpdateTaskQueueResponse, error) {
	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}

	tqId, tqHash := m.taskQueueIdAndHash(nidBytes, request.TaskQueue, request.TaskType)
	var resp *persistence.UpdateTaskQueueResponse
	err = m.txExecute(ctx, "UpdateTaskQueue", func(tx sqlplugin.Tx) error {
		if err := lockTaskQueue(ctx,
			tx,
			tqHash,
			tqId,
			request.PrevRangeID,
		); err != nil {
			return err
		}
		result, err := tx.UpdateTaskQueues(ctx, &sqlplugin.TaskQueuesRow{
			RangeHash:    tqHash,
			TaskQueueID:  tqId,
			RangeID:      request.RangeID,
			Data:         request.TaskQueueInfo.Data,
			DataEncoding: request.TaskQueueInfo.EncodingType.String(),
		})
		if err != nil {
			return err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected != 1 {
			return fmt.Errorf("%v rows were affected instead of 1", rowsAffected)
		}
		resp = &persistence.UpdateTaskQueueResponse{}
		return nil
	})
	return resp, err
}

func (m *sqlTaskManager) ListTaskQueue(
	ctx context.Context,
	request *persistence.ListTaskQueueRequest,
) (*persistence.InternalListTaskQueueResponse, error) {
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

		rows, err = m.Db.SelectFromTaskQueues(ctx, filter)
		if err != nil {
			return nil, serviceerror.NewUnavailable(err.Error())
		}

		if len(rows) > 0 {
			break
		}
	}

	maxRangeHash := uint32(0)
	resp := &persistence.InternalListTaskQueueResponse{
		Items: make([]*persistence.InternalListTaskQueueItem, len(rows)),
	}

	for i, row := range rows {
		resp.Items[i] = &persistence.InternalListTaskQueueItem{
			RangeID:   row.RangeID,
			TaskQueue: persistence.NewDataBlob(row.Data, row.DataEncoding),
		}

		// Only want to look at up to PageSize number of records to prevent losing data.
		if row.RangeHash > maxRangeHash {
			maxRangeHash = row.RangeHash
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
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("error serializing nextPageToken:%v", err))
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

func (m *sqlTaskManager) DeleteTaskQueue(
	ctx context.Context,
	request *persistence.DeleteTaskQueueRequest,
) error {
	nidBytes, err := primitives.ParseUUID(request.TaskQueue.NamespaceID)
	if err != nil {
		return serviceerror.NewUnavailable(err.Error())
	}
	tqId, tqHash := m.taskQueueIdAndHash(nidBytes, request.TaskQueue.TaskQueueName, request.TaskQueue.TaskQueueType)
	result, err := m.Db.DeleteFromTaskQueues(ctx, sqlplugin.TaskQueuesFilter{
		RangeHash:   tqHash,
		TaskQueueID: tqId,
		RangeID:     &request.RangeID,
	})
	if err != nil {
		return serviceerror.NewUnavailable(err.Error())
	}
	nRows, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("rowsAffected returned error:%v", err))
	}
	if nRows != 1 {
		return &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("delete failed: %v rows affected instead of 1", nRows),
		}
	}
	return nil
}
func (m *sqlTaskManager) CreateTasks(
	ctx context.Context,
	request *persistence.InternalCreateTasksRequest,
) (*persistence.CreateTasksResponse, error) {
	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return nil, serviceerror.NewUnavailable(err.Error())
	}
	tqId, tqHash := m.taskQueueIdAndHash(nidBytes, request.TaskQueue, request.TaskType)

	tasksRows := make([]sqlplugin.TasksRow, len(request.Tasks))
	for i, v := range request.Tasks {
		tasksRows[i] = sqlplugin.TasksRow{
			RangeHash:    tqHash,
			TaskQueueID:  tqId,
			TaskID:       v.TaskId,
			Data:         v.Task.Data,
			DataEncoding: v.Task.EncodingType.String(),
		}
	}
	var resp *persistence.CreateTasksResponse
	err = m.txExecute(ctx, "CreateTasks", func(tx sqlplugin.Tx) error {
		if _, err1 := tx.InsertIntoTasks(ctx, tasksRows); err1 != nil {
			return err1
		}
		// Lock task queue before committing.
		if err := lockTaskQueue(ctx,
			tx,
			tqHash,
			tqId,
			request.RangeID,
		); err != nil {
			return err
		}
		resp = &persistence.CreateTasksResponse{}
		return nil
	})
	return resp, err
}

func (m *sqlTaskManager) GetTasks(
	ctx context.Context,
	request *persistence.GetTasksRequest,
) (*persistence.InternalGetTasksResponse, error) {
	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return nil, serviceerror.NewUnavailable(err.Error())
	}

	inclusiveMinTaskID := request.InclusiveMinTaskID
	exclusiveMaxTaskID := request.ExclusiveMaxTaskID
	if len(request.NextPageToken) != 0 {
		token, err := deserializeMatchingTaskPageToken(request.NextPageToken)
		if err != nil {
			return nil, err
		}
		inclusiveMinTaskID = token.TaskID
	}

	tqId, tqHash := m.taskQueueIdAndHash(nidBytes, request.TaskQueue, request.TaskType)
	rows, err := m.Db.SelectFromTasks(ctx, sqlplugin.TasksFilter{
		RangeHash:          tqHash,
		TaskQueueID:        tqId,
		InclusiveMinTaskID: &inclusiveMinTaskID,
		ExclusiveMaxTaskID: &exclusiveMaxTaskID,
		PageSize:           &request.PageSize,
	})
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetTasks operation failed. Failed to get rows. Error: %v", err))
	}

	response := &persistence.InternalGetTasksResponse{
		Tasks: make([]*commonpb.DataBlob, len(rows)),
	}
	for i, v := range rows {
		response.Tasks[i] = persistence.NewDataBlob(v.Data, v.DataEncoding)
	}
	if len(rows) == request.PageSize {
		nextTaskID := rows[len(rows)-1].TaskID + 1
		if nextTaskID < exclusiveMaxTaskID {
			token, err := serializeMatchingTaskPageToken(&matchingTaskPageToken{
				TaskID: nextTaskID,
			})
			if err != nil {
				return nil, err
			}
			response.NextPageToken = token
		}
	}

	return response, nil
}

func (m *sqlTaskManager) CompleteTask(
	ctx context.Context,
	request *persistence.CompleteTaskRequest,
) error {
	nidBytes, err := primitives.ParseUUID(request.TaskQueue.NamespaceID)
	if err != nil {
		return serviceerror.NewUnavailable(err.Error())
	}

	taskID := request.TaskID
	tqId, tqHash := m.taskQueueIdAndHash(nidBytes, request.TaskQueue.TaskQueueName, request.TaskQueue.TaskQueueType)
	_, err = m.Db.DeleteFromTasks(ctx, sqlplugin.TasksFilter{
		RangeHash:   tqHash,
		TaskQueueID: tqId,
		TaskID:      &taskID})
	if err != nil && err != sql.ErrNoRows {
		return serviceerror.NewUnavailable(err.Error())
	}
	return nil
}

func (m *sqlTaskManager) CompleteTasksLessThan(
	ctx context.Context,
	request *persistence.CompleteTasksLessThanRequest,
) (int, error) {
	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return 0, serviceerror.NewUnavailable(err.Error())
	}
	tqId, tqHash := m.taskQueueIdAndHash(nidBytes, request.TaskQueueName, request.TaskType)
	result, err := m.Db.DeleteFromTasks(ctx, sqlplugin.TasksFilter{
		RangeHash:          tqHash,
		TaskQueueID:        tqId,
		ExclusiveMaxTaskID: &request.ExclusiveMaxTaskID,
		Limit:              &request.Limit,
	})
	if err != nil {
		return 0, serviceerror.NewUnavailable(err.Error())
	}
	nRows, err := result.RowsAffected()
	if err != nil {
		return 0, serviceerror.NewUnavailable(fmt.Sprintf("rowsAffected returned error: %v", err))
	}
	return int(nRows), nil
}

// Returns uint32 hash for a particular TaskQueue/Task given a Namespace, TaskQueueName and TaskQueueType
func (m *sqlTaskManager) taskQueueIdAndHash(
	namespaceID primitives.UUID,
	name string,
	taskType enumspb.TaskQueueType,
) ([]byte, uint32) {
	id := m.taskQueueId(namespaceID, name, taskType)
	return id, farm.Fingerprint32(id)
}

func (m *sqlTaskManager) taskQueueId(
	namespaceID primitives.UUID,
	name string,
	taskType enumspb.TaskQueueType,
) []byte {
	idBytes := make([]byte, 0, 16+len(name)+1)
	idBytes = append(idBytes, namespaceID...)
	idBytes = append(idBytes, []byte(name)...)
	idBytes = append(idBytes, uint8(taskType))
	return idBytes
}

func lockTaskQueue(
	ctx context.Context,
	tx sqlplugin.Tx,
	tqHash uint32,
	tqId []byte,
	oldRangeID int64,
) error {
	rangeID, err := tx.LockTaskQueues(ctx, sqlplugin.TaskQueuesFilter{
		RangeHash:   tqHash,
		TaskQueueID: tqId,
	})
	switch err {
	case nil:
		if rangeID != oldRangeID {
			return &persistence.ConditionFailedError{
				Msg: fmt.Sprintf("Task queue range ID was %v when it was should have been %v", rangeID, oldRangeID),
			}
		}
		return nil

	case sql.ErrNoRows:
		return &persistence.ConditionFailedError{Msg: "Task queue does not exists"}

	default:
		return serviceerror.NewUnavailable(fmt.Sprintf("Failed to lock task queue. Error: %v", err))
	}
}

type matchingTaskPageToken struct {
	TaskID int64
}

func serializeMatchingTaskPageToken(token *matchingTaskPageToken) ([]byte, error) {
	return json.Marshal(token)
}

func deserializeMatchingTaskPageToken(payload []byte) (*matchingTaskPageToken, error) {
	var token matchingTaskPageToken
	if err := json.Unmarshal(payload, &token); err != nil {
		return nil, err
	}
	return &token, nil
}
