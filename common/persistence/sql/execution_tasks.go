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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/service/history/tasks"
)

func (m *sqlExecutionStore) AddHistoryTasks(
	_ context.Context,
	request *p.InternalAddHistoryTasksRequest,
) error {
	ctx, cancel := newExecutionContext()
	defer cancel()
	return m.txExecuteShardLocked(ctx,
		"AddHistoryTasks",
		request.ShardID,
		request.RangeID,
		func(tx sqlplugin.Tx) error {
			return applyTasks(ctx,
				tx,
				request.ShardID,
				request.Tasks,
			)
		})
}

func (m *sqlExecutionStore) GetHistoryTask(
	_ context.Context,
	request *p.GetHistoryTaskRequest,
) (*p.InternalGetHistoryTaskResponse, error) {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		return m.getTransferTask(request)
	case tasks.CategoryIDTimer:
		return m.getTimerTask(request)
	case tasks.CategoryIDVisibility:
		return m.getVisibilityTask(request)
	case tasks.CategoryIDReplication:
		return m.getReplicationTask(request)
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("unknown task category: %v", request.TaskCategory))
	}
}

func (m *sqlExecutionStore) GetHistoryTasks(
	_ context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		return m.getTransferTasks(request)
	case tasks.CategoryIDTimer:
		return m.getTimerTasks(request)
	case tasks.CategoryIDVisibility:
		return m.getVisibilityTasks(request)
	case tasks.CategoryIDReplication:
		return m.getReplicationTasks(request)
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("unknown task category: %v", request.TaskCategory))
	}
}

func (m *sqlExecutionStore) CompleteHistoryTask(
	_ context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		return m.completeTransferTask(request)
	case tasks.CategoryIDTimer:
		return m.completeTimerTask(request)
	case tasks.CategoryIDVisibility:
		return m.completeVisibilityTask(request)
	case tasks.CategoryIDReplication:
		return m.completeReplicationTask(request)
	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown task category: %v", request.TaskCategory))
	}
}

func (m *sqlExecutionStore) RangeCompleteHistoryTasks(
	_ context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		return m.rangeCompleteTransferTasks(request)
	case tasks.CategoryIDTimer:
		return m.rangeCompleteTimerTasks(request)
	case tasks.CategoryIDVisibility:
		return m.rangeCompleteVisibilityTasks(request)
	case tasks.CategoryIDReplication:
		return m.rangeCompleteReplicationTasks(request)
	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown task category: %v", request.TaskCategory))
	}
}

func (m *sqlExecutionStore) getTransferTask(
	request *p.GetHistoryTaskRequest,
) (*p.InternalGetHistoryTaskResponse, error) {
	ctx, cancel := newExecutionContext()
	defer cancel()
	rows, err := m.Db.SelectFromTransferTasks(ctx, sqlplugin.TransferTasksFilter{
		ShardID: request.ShardID,
		TaskID:  request.TaskKey.TaskID,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, serviceerror.NewNotFound(fmt.Sprintf("GetTransferTask operation failed. Task with ID %v not found. Error: %v", request.TaskKey.TaskID, err))
		}
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetTransferTask operation failed. Failed to get record. TaskId: %v. Error: %v", request.TaskKey.TaskID, err))
	}

	if len(rows) == 0 {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("GetTransferTask operation failed. Failed to get record. TaskId: %v", request.TaskKey.TaskID))
	}

	transferRow := rows[0]
	resp := &p.InternalGetHistoryTaskResponse{
		Task: *p.NewDataBlob(transferRow.Data, transferRow.DataEncoding),
	}
	return resp, nil
}

func (m *sqlExecutionStore) getTransferTasks(
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	ctx, cancel := newExecutionContext()
	defer cancel()
	inclusiveMinTaskID, exclusiveMaxTaskID, err := getImmediateTaskReadRange(request)
	if err != nil {
		return nil, err
	}

	rows, err := m.Db.RangeSelectFromTransferTasks(ctx, sqlplugin.TransferTasksRangeFilter{
		ShardID:            request.ShardID,
		InclusiveMinTaskID: inclusiveMinTaskID,
		ExclusiveMaxTaskID: exclusiveMaxTaskID,
		PageSize:           request.BatchSize,
	})
	if err != nil {
		if err != sql.ErrNoRows {
			return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetTransferTasks operation failed. Select failed. Error: %v", err))
		}
	}
	resp := &p.InternalGetHistoryTasksResponse{
		Tasks: make([]commonpb.DataBlob, len(rows)),
	}
	if len(rows) == 0 {
		return resp, nil
	}

	for i, row := range rows {
		resp.Tasks[i] = *p.NewDataBlob(row.Data, row.DataEncoding)
	}
	if len(rows) == request.BatchSize {
		resp.NextPageToken = getImmediateTaskNextPageToken(
			rows[len(rows)-1].TaskID,
			exclusiveMaxTaskID,
		)
	}

	return resp, nil
}

func (m *sqlExecutionStore) completeTransferTask(
	request *p.CompleteHistoryTaskRequest,
) error {
	ctx, cancel := newExecutionContext()
	defer cancel()
	if _, err := m.Db.DeleteFromTransferTasks(ctx, sqlplugin.TransferTasksFilter{
		ShardID: request.ShardID,
		TaskID:  request.TaskKey.TaskID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("CompleteTransferTask operation failed. Error: %v", err))
	}
	return nil
}

func (m *sqlExecutionStore) rangeCompleteTransferTasks(
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	ctx, cancel := newExecutionContext()
	defer cancel()
	if _, err := m.Db.RangeDeleteFromTransferTasks(ctx, sqlplugin.TransferTasksRangeFilter{
		ShardID:            request.ShardID,
		InclusiveMinTaskID: request.InclusiveMinTaskKey.TaskID,
		ExclusiveMaxTaskID: request.ExclusiveMaxTaskKey.TaskID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("RangeCompleteTransferTask operation failed. Error: %v", err))
	}
	return nil
}

func (m *sqlExecutionStore) getTimerTask(
	request *p.GetHistoryTaskRequest,
) (*p.InternalGetHistoryTaskResponse, error) {
	ctx, cancel := newExecutionContext()
	defer cancel()
	rows, err := m.Db.SelectFromTimerTasks(ctx, sqlplugin.TimerTasksFilter{
		ShardID:             request.ShardID,
		TaskID:              request.TaskKey.TaskID,
		VisibilityTimestamp: request.TaskKey.FireTime,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, serviceerror.NewNotFound(fmt.Sprintf("GetTimerTask operation failed. Task with ID %v not found. Error: %v", request.TaskKey.TaskID, err))
		}
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetTimerTask operation failed. Failed to get record. TaskId: %v. Error: %v", request.TaskKey.TaskID, err))
	}

	if len(rows) == 0 {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("GetTimerTask operation failed. Failed to get record. TaskId: %v", request.TaskKey.TaskID))
	}

	timerRow := rows[0]
	resp := &p.InternalGetHistoryTaskResponse{
		Task: *p.NewDataBlob(timerRow.Data, timerRow.DataEncoding),
	}
	return resp, nil
}

func (m *sqlExecutionStore) getTimerTasks(
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	ctx, cancel := newExecutionContext()
	defer cancel()
	pageToken := &timerTaskPageToken{TaskID: math.MinInt64, Timestamp: request.InclusiveMinTaskKey.FireTime}
	if len(request.NextPageToken) > 0 {
		if err := pageToken.deserialize(request.NextPageToken); err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("error deserializing timerTaskPageToken: %v", err))
		}
	}

	rows, err := m.Db.RangeSelectFromTimerTasks(ctx, sqlplugin.TimerTasksRangeFilter{
		ShardID:                         request.ShardID,
		InclusiveMinVisibilityTimestamp: pageToken.Timestamp,
		InclusiveMinTaskID:              pageToken.TaskID,
		ExclusiveMaxVisibilityTimestamp: request.ExclusiveMaxTaskKey.FireTime,
		PageSize:                        request.BatchSize,
	})

	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetTimerTasks operation failed. Select failed. Error: %v", err))
	}

	resp := &p.InternalGetHistoryTasksResponse{Tasks: make([]commonpb.DataBlob, len(rows))}
	for i, row := range rows {
		resp.Tasks[i] = *p.NewDataBlob(row.Data, row.DataEncoding)
	}

	if len(resp.Tasks) == request.BatchSize {
		pageToken = &timerTaskPageToken{
			TaskID:    rows[request.BatchSize-1].TaskID + 1,
			Timestamp: rows[request.BatchSize-1].VisibilityTimestamp,
		}
		nextToken, err := pageToken.serialize()
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("GetTimerTasks: error serializing page token: %v", err))
		}
		resp.NextPageToken = nextToken
	}

	return resp, nil
}

func (m *sqlExecutionStore) completeTimerTask(
	request *p.CompleteHistoryTaskRequest,
) error {
	ctx, cancel := newExecutionContext()
	defer cancel()
	if _, err := m.Db.DeleteFromTimerTasks(ctx, sqlplugin.TimerTasksFilter{
		ShardID:             request.ShardID,
		VisibilityTimestamp: request.TaskKey.FireTime,
		TaskID:              request.TaskKey.TaskID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("CompleteTimerTask operation failed. Error: %v", err))
	}
	return nil
}

func (m *sqlExecutionStore) rangeCompleteTimerTasks(
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	ctx, cancel := newExecutionContext()
	defer cancel()
	start := request.InclusiveMinTaskKey.FireTime
	end := request.ExclusiveMaxTaskKey.FireTime
	if _, err := m.Db.RangeDeleteFromTimerTasks(ctx, sqlplugin.TimerTasksRangeFilter{
		ShardID:                         request.ShardID,
		InclusiveMinVisibilityTimestamp: start,
		ExclusiveMaxVisibilityTimestamp: end,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("CompleteTimerTask operation failed. Error: %v", err))
	}
	return nil
}

func (m *sqlExecutionStore) getReplicationTask(
	request *p.GetHistoryTaskRequest,
) (*p.InternalGetHistoryTaskResponse, error) {
	ctx, cancel := newExecutionContext()
	defer cancel()
	rows, err := m.Db.SelectFromReplicationTasks(ctx, sqlplugin.ReplicationTasksFilter{
		ShardID: request.ShardID,
		TaskID:  request.TaskKey.TaskID,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, serviceerror.NewNotFound(fmt.Sprintf("GetReplicationTask operation failed. Task with ID %v not found. Error: %v", request.TaskKey.TaskID, err))
		}
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetReplicationTask operation failed. Failed to get record. TaskId: %v. Error: %v", request.TaskKey.TaskID, err))
	}

	if len(rows) == 0 {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("GetReplicationTask operation failed. Failed to get record. TaskId: %v", request.TaskKey.TaskID))
	}

	replicationRow := rows[0]
	resp := &p.InternalGetHistoryTaskResponse{Task: *p.NewDataBlob(replicationRow.Data, replicationRow.DataEncoding)}
	return resp, nil
}

func (m *sqlExecutionStore) getReplicationTasks(
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	ctx, cancel := newExecutionContext()
	defer cancel()
	inclusiveMinTaskID, exclusiveMaxTaskID, err := getImmediateTaskReadRange(request)
	if err != nil {
		return nil, err
	}

	rows, err := m.Db.RangeSelectFromReplicationTasks(ctx, sqlplugin.ReplicationTasksRangeFilter{
		ShardID:            request.ShardID,
		InclusiveMinTaskID: inclusiveMinTaskID,
		ExclusiveMaxTaskID: exclusiveMaxTaskID,
		PageSize:           request.BatchSize,
	})

	switch err {
	case nil:
		return m.populateGetReplicationTasksResponse(rows, request.ExclusiveMaxTaskKey.TaskID, request.BatchSize)
	case sql.ErrNoRows:
		return &p.InternalGetHistoryTasksResponse{}, nil
	default:
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetReplicationTasks operation failed. Select failed: %v", err))
	}
}

func getImmediateTaskReadRange(
	request *p.GetHistoryTasksRequest,
) (inclusiveMinTaskID int64, exclusiveMaxTaskID int64, err error) {
	inclusiveMinTaskID = request.InclusiveMinTaskKey.TaskID
	if len(request.NextPageToken) > 0 {
		inclusiveMinTaskID, err = deserializePageToken(request.NextPageToken)
		if err != nil {
			return 0, 0, err
		}
	}

	return inclusiveMinTaskID, request.ExclusiveMaxTaskKey.TaskID, nil
}

func getImmediateTaskNextPageToken(
	lastTaskID int64,
	exclusiveMaxTaskID int64,
) []byte {
	nextTaskID := lastTaskID + 1
	if nextTaskID < exclusiveMaxTaskID {
		return serializePageToken(nextTaskID)
	}
	return nil
}

func (m *sqlExecutionStore) populateGetReplicationTasksResponse(
	rows []sqlplugin.ReplicationTasksRow,
	exclusiveMaxTaskID int64,
	batchSize int,
) (*p.InternalGetHistoryTasksResponse, error) {
	if len(rows) == 0 {
		return &p.InternalGetHistoryTasksResponse{}, nil
	}

	var tasks = make([]commonpb.DataBlob, len(rows))
	for i, row := range rows {
		tasks[i] = *p.NewDataBlob(row.Data, row.DataEncoding)
	}
	var nextPageToken []byte
	if len(rows) == batchSize {
		nextPageToken = getImmediateTaskNextPageToken(
			rows[len(rows)-1].TaskID,
			exclusiveMaxTaskID,
		)
	}
	return &p.InternalGetHistoryTasksResponse{
		Tasks:         tasks,
		NextPageToken: nextPageToken,
	}, nil
}

func (m *sqlExecutionStore) populateGetReplicationDLQTasksResponse(
	rows []sqlplugin.ReplicationDLQTasksRow,
	exclusiveMaxTaskID int64,
	batchSize int,
) (*p.InternalGetHistoryTasksResponse, error) {
	if len(rows) == 0 {
		return &p.InternalGetHistoryTasksResponse{}, nil
	}

	var tasks = make([]commonpb.DataBlob, len(rows))
	for i, row := range rows {
		tasks[i] = *p.NewDataBlob(row.Data, row.DataEncoding)
	}
	var nextPageToken []byte
	if len(rows) == batchSize {
		nextPageToken = getImmediateTaskNextPageToken(
			rows[len(rows)-1].TaskID,
			exclusiveMaxTaskID,
		)
	}
	return &p.InternalGetHistoryTasksResponse{
		Tasks:         tasks,
		NextPageToken: nextPageToken,
	}, nil
}

func (m *sqlExecutionStore) completeReplicationTask(
	request *p.CompleteHistoryTaskRequest,
) error {
	ctx, cancel := newExecutionContext()
	defer cancel()
	if _, err := m.Db.DeleteFromReplicationTasks(ctx, sqlplugin.ReplicationTasksFilter{
		ShardID: request.ShardID,
		TaskID:  request.TaskKey.TaskID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("CompleteReplicationTask operation failed. Error: %v", err))
	}
	return nil
}

func (m *sqlExecutionStore) rangeCompleteReplicationTasks(
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	ctx, cancel := newExecutionContext()
	defer cancel()
	if _, err := m.Db.RangeDeleteFromReplicationTasks(ctx, sqlplugin.ReplicationTasksRangeFilter{
		ShardID:            request.ShardID,
		InclusiveMinTaskID: request.InclusiveMinTaskKey.TaskID,
		ExclusiveMaxTaskID: request.ExclusiveMaxTaskKey.TaskID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("RangeCompleteReplicationTask operation failed. Error: %v", err))
	}
	return nil
}

func (m *sqlExecutionStore) PutReplicationTaskToDLQ(
	_ context.Context,
	request *p.PutReplicationTaskToDLQRequest,
) error {
	ctx, cancel := newExecutionContext()
	defer cancel()
	replicationTask := request.TaskInfo
	blob, err := serialization.ReplicationTaskInfoToBlob(replicationTask)

	if err != nil {
		return err
	}

	_, err = m.Db.InsertIntoReplicationDLQTasks(ctx, []sqlplugin.ReplicationDLQTasksRow{{
		SourceClusterName: request.SourceClusterName,
		ShardID:           request.ShardID,
		TaskID:            replicationTask.GetTaskId(),
		Data:              blob.Data,
		DataEncoding:      blob.EncodingType.String(),
	}})

	// Tasks are immutable. So it's fine if we already persisted it before.
	// This can happen when tasks are retried (ack and cleanup can have lag on source side).
	if err != nil && !m.Db.IsDupEntryError(err) {
		return serviceerror.NewUnavailable(fmt.Sprintf("Failed to create replication tasks. Error: %v", err))
	}

	return nil
}

func (m *sqlExecutionStore) GetReplicationTasksFromDLQ(
	_ context.Context,
	request *p.GetReplicationTasksFromDLQRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	ctx, cancel := newExecutionContext()
	defer cancel()
	inclusiveMinTaskID, exclusiveMaxTaskID, err := getImmediateTaskReadRange(&request.GetHistoryTasksRequest)
	if err != nil {
		return nil, err
	}

	rows, err := m.Db.RangeSelectFromReplicationDLQTasks(ctx, sqlplugin.ReplicationDLQTasksRangeFilter{
		ShardID:            request.ShardID,
		InclusiveMinTaskID: inclusiveMinTaskID,
		ExclusiveMaxTaskID: exclusiveMaxTaskID,
		PageSize:           request.BatchSize,
		SourceClusterName:  request.SourceClusterName,
	})

	switch err {
	case nil:
		return m.populateGetReplicationDLQTasksResponse(rows, request.ExclusiveMaxTaskKey.TaskID, request.BatchSize)
	case sql.ErrNoRows:
		return &p.InternalGetHistoryTasksResponse{}, nil
	default:
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetReplicationTasks operation failed. Select failed: %v", err))
	}
}

func (m *sqlExecutionStore) DeleteReplicationTaskFromDLQ(
	_ context.Context,
	request *p.DeleteReplicationTaskFromDLQRequest,
) error {
	ctx, cancel := newExecutionContext()
	defer cancel()
	if _, err := m.Db.DeleteFromReplicationDLQTasks(ctx, sqlplugin.ReplicationDLQTasksFilter{
		ShardID:           request.ShardID,
		TaskID:            request.TaskKey.TaskID,
		SourceClusterName: request.SourceClusterName,
	}); err != nil {
		return err
	}
	return nil
}

func (m *sqlExecutionStore) RangeDeleteReplicationTaskFromDLQ(
	_ context.Context,
	request *p.RangeDeleteReplicationTaskFromDLQRequest,
) error {
	ctx, cancel := newExecutionContext()
	defer cancel()
	if _, err := m.Db.RangeDeleteFromReplicationDLQTasks(ctx, sqlplugin.ReplicationDLQTasksRangeFilter{
		ShardID:            request.ShardID,
		SourceClusterName:  request.SourceClusterName,
		InclusiveMinTaskID: request.InclusiveMinTaskKey.TaskID,
		ExclusiveMaxTaskID: request.ExclusiveMaxTaskKey.TaskID,
	}); err != nil {
		return err
	}
	return nil
}

func (m *sqlExecutionStore) getVisibilityTask(
	request *p.GetHistoryTaskRequest,
) (*p.InternalGetHistoryTaskResponse, error) {
	ctx, cancel := newExecutionContext()
	defer cancel()
	rows, err := m.Db.SelectFromVisibilityTasks(ctx, sqlplugin.VisibilityTasksFilter{
		ShardID: request.ShardID,
		TaskID:  request.TaskKey.TaskID,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, serviceerror.NewNotFound(fmt.Sprintf("GetVisibilityTask operation failed. Task with ID %v not found. Error: %v", request.TaskKey.TaskID, err))
		}
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetVisibilityTask operation failed. Failed to get record. TaskId: %v. Error: %v", request.TaskKey.TaskID, err))
	}

	if len(rows) == 0 {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("GetVisibilityTask operation failed. Failed to get record. TaskId: %v", request.TaskKey.TaskID))
	}

	visibilityRow := rows[0]
	resp := &p.InternalGetHistoryTaskResponse{Task: *p.NewDataBlob(visibilityRow.Data, visibilityRow.DataEncoding)}
	return resp, nil
}

func (m *sqlExecutionStore) getVisibilityTasks(
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	ctx, cancel := newExecutionContext()
	defer cancel()
	inclusiveMinTaskID, exclusiveMaxTaskID, err := getImmediateTaskReadRange(request)
	if err != nil {
		return nil, err
	}

	rows, err := m.Db.RangeSelectFromVisibilityTasks(ctx, sqlplugin.VisibilityTasksRangeFilter{
		ShardID:            request.ShardID,
		InclusiveMinTaskID: inclusiveMinTaskID,
		ExclusiveMaxTaskID: exclusiveMaxTaskID,
		PageSize:           request.BatchSize,
	})
	if err != nil {
		if err != sql.ErrNoRows {
			return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetVisibilityTasks operation failed. Select failed. Error: %v", err))
		}
	}
	resp := &p.InternalGetHistoryTasksResponse{
		Tasks: make([]commonpb.DataBlob, len(rows)),
	}
	if len(rows) == 0 {
		return resp, nil
	}

	for i, row := range rows {
		resp.Tasks[i] = *p.NewDataBlob(row.Data, row.DataEncoding)
	}
	if len(rows) == request.BatchSize {
		resp.NextPageToken = getImmediateTaskNextPageToken(
			rows[len(rows)-1].TaskID,
			exclusiveMaxTaskID,
		)
	}

	return resp, nil
}

func (m *sqlExecutionStore) completeVisibilityTask(
	request *p.CompleteHistoryTaskRequest,
) error {
	ctx, cancel := newExecutionContext()
	defer cancel()
	if _, err := m.Db.DeleteFromVisibilityTasks(ctx, sqlplugin.VisibilityTasksFilter{
		ShardID: request.ShardID,
		TaskID:  request.TaskKey.TaskID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("CompleteVisibilityTask operation failed. Error: %v", err))
	}
	return nil
}

func (m *sqlExecutionStore) rangeCompleteVisibilityTasks(
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	ctx, cancel := newExecutionContext()
	defer cancel()
	if _, err := m.Db.RangeDeleteFromVisibilityTasks(ctx, sqlplugin.VisibilityTasksRangeFilter{
		ShardID:            request.ShardID,
		InclusiveMinTaskID: request.InclusiveMinTaskKey.TaskID,
		ExclusiveMaxTaskID: request.ExclusiveMaxTaskKey.TaskID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("RangeCompleteVisibilityTask operation failed. Error: %v", err))
	}
	return nil
}

type timerTaskPageToken struct {
	TaskID    int64
	Timestamp time.Time
}

func (t *timerTaskPageToken) serialize() ([]byte, error) {
	return json.Marshal(t)
}

func (t *timerTaskPageToken) deserialize(payload []byte) error {
	return json.Unmarshal(payload, t)
}
