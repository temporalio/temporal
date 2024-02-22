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
	"errors"
	"fmt"
	"math"
	"time"

	"go.temporal.io/api/serviceerror"

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/service/history/tasks"
)

func (m *sqlExecutionStore) AddHistoryTasks(
	ctx context.Context,
	request *p.InternalAddHistoryTasksRequest,
) error {
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

func (m *sqlExecutionStore) GetHistoryTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	switch request.TaskCategory.Type() {
	case tasks.CategoryTypeImmediate:
		return m.getHistoryImmediateTasks(ctx, request)
	case tasks.CategoryTypeScheduled:
		return m.getHistoryScheduledTasks(ctx, request)
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown task category type: %v", request.TaskCategory))
	}
}

func (m *sqlExecutionStore) CompleteHistoryTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	switch request.TaskCategory.Type() {
	case tasks.CategoryTypeImmediate:
		return m.completeHistoryImmediateTask(ctx, request)
	case tasks.CategoryTypeScheduled:
		return m.completeHistoryScheduledTask(ctx, request)
	default:
		return serviceerror.NewInternal(fmt.Sprintf("Unknown task category type: %v", request.TaskCategory))
	}
}

func (m *sqlExecutionStore) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	switch request.TaskCategory.Type() {
	case tasks.CategoryTypeImmediate:
		return m.rangeCompleteHistoryImmediateTasks(ctx, request)
	case tasks.CategoryTypeScheduled:
		return m.rangeCompleteHistoryScheduledTasks(ctx, request)
	default:
		return serviceerror.NewInternal(fmt.Sprintf("Unknown task category type: %v", request.TaskCategory))
	}
}

func (m *sqlExecutionStore) getHistoryImmediateTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	// This is for backward compatiblity.
	// These task categories exist before the general history_immediate_tasks table is created,
	// so they have their own tables.
	categoryID := request.TaskCategory.ID()
	switch categoryID {
	case tasks.CategoryIDTransfer:
		return m.getTransferTasks(ctx, request)
	case tasks.CategoryIDVisibility:
		return m.getVisibilityTasks(ctx, request)
	case tasks.CategoryIDReplication:
		return m.getReplicationTasks(ctx, request)
	}

	inclusiveMinTaskID, exclusiveMaxTaskID, err := getImmediateTaskReadRange(request)
	if err != nil {
		return nil, err
	}

	rows, err := m.Db.RangeSelectFromHistoryImmediateTasks(ctx, sqlplugin.HistoryImmediateTasksRangeFilter{
		ShardID:            request.ShardID,
		CategoryID:         int32(categoryID),
		InclusiveMinTaskID: inclusiveMinTaskID,
		ExclusiveMaxTaskID: exclusiveMaxTaskID,
		PageSize:           request.BatchSize,
	})
	if err != nil {
		if err != sql.ErrNoRows {
			return nil, serviceerror.NewUnavailable(
				fmt.Sprintf("GetHistoryTasks operation failed. Select failed. CategoryID: %v. Error: %v", categoryID, err),
			)
		}
	}
	resp := &p.InternalGetHistoryTasksResponse{
		Tasks: make([]p.InternalHistoryTask, len(rows)),
	}
	if len(rows) == 0 {
		return resp, nil
	}

	for i, row := range rows {
		resp.Tasks[i] = p.InternalHistoryTask{
			Key:  tasks.NewImmediateKey(row.TaskID),
			Blob: p.NewDataBlob(row.Data, row.DataEncoding),
		}
	}
	if len(rows) == request.BatchSize {
		resp.NextPageToken = getImmediateTaskNextPageToken(
			rows[len(rows)-1].TaskID,
			exclusiveMaxTaskID,
		)
	}

	return resp, nil
}

func (m *sqlExecutionStore) completeHistoryImmediateTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	// This is for backward compatiblity.
	// These task categories exist before the general history_immediate_tasks table is created,
	// so they have their own tables.
	categoryID := request.TaskCategory.ID()
	switch categoryID {
	case tasks.CategoryIDTransfer:
		return m.completeTransferTask(ctx, request)
	case tasks.CategoryIDVisibility:
		return m.completeVisibilityTask(ctx, request)
	case tasks.CategoryIDReplication:
		return m.completeReplicationTask(ctx, request)
	}

	if _, err := m.Db.DeleteFromHistoryImmediateTasks(ctx, sqlplugin.HistoryImmediateTasksFilter{
		ShardID:    request.ShardID,
		CategoryID: int32(categoryID),
		TaskID:     request.TaskKey.TaskID,
	}); err != nil {
		return serviceerror.NewUnavailable(
			fmt.Sprintf("CompleteHistoryTask operation failed. CategoryID: %v. Error: %v", categoryID, err),
		)
	}
	return nil
}

func (m *sqlExecutionStore) rangeCompleteHistoryImmediateTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	// This is for backward compatiblity.
	// These task categories exist before the general history_immediate_tasks table is created,
	// so they have their own tables.
	categoryID := request.TaskCategory.ID()
	switch categoryID {
	case tasks.CategoryIDTransfer:
		return m.rangeCompleteTransferTasks(ctx, request)
	case tasks.CategoryIDVisibility:
		return m.rangeCompleteVisibilityTasks(ctx, request)
	case tasks.CategoryIDReplication:
		return m.rangeCompleteReplicationTasks(ctx, request)
	}

	if _, err := m.Db.RangeDeleteFromHistoryImmediateTasks(ctx, sqlplugin.HistoryImmediateTasksRangeFilter{
		ShardID:            request.ShardID,
		CategoryID:         int32(categoryID),
		InclusiveMinTaskID: request.InclusiveMinTaskKey.TaskID,
		ExclusiveMaxTaskID: request.ExclusiveMaxTaskKey.TaskID,
	}); err != nil {
		return serviceerror.NewUnavailable(
			fmt.Sprintf("RangeCompleteTransferTask operation failed. CategoryID: %v. Error: %v", categoryID, err),
		)
	}
	return nil
}

func (m *sqlExecutionStore) getHistoryScheduledTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	// This is for backward compatiblity.
	// These task categories exist before the general history_scheduled_tasks table is created,
	// so they have their own tables.
	categoryID := request.TaskCategory.ID()
	if categoryID == tasks.CategoryIDTimer {
		return m.getTimerTasks(ctx, request)
	}

	pageToken := &scheduledTaskPageToken{TaskID: math.MinInt64, Timestamp: request.InclusiveMinTaskKey.FireTime}
	if len(request.NextPageToken) > 0 {
		if err := pageToken.deserialize(request.NextPageToken); err != nil {
			return nil, serviceerror.NewInternal(
				fmt.Sprintf("categoryID: %v. error deserializing scheduledTaskPageToken: %v", categoryID, err),
			)
		}
	}

	rows, err := m.Db.RangeSelectFromHistoryScheduledTasks(ctx, sqlplugin.HistoryScheduledTasksRangeFilter{
		ShardID:                         request.ShardID,
		CategoryID:                      int32(categoryID),
		InclusiveMinVisibilityTimestamp: pageToken.Timestamp,
		InclusiveMinTaskID:              pageToken.TaskID,
		ExclusiveMaxVisibilityTimestamp: request.ExclusiveMaxTaskKey.FireTime,
		PageSize:                        request.BatchSize,
	})

	if err != nil && err != sql.ErrNoRows {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("GetHistoryTasks operation failed. Select failed. CategoryID: %v. Error: %v", categoryID, err),
		)
	}

	resp := &p.InternalGetHistoryTasksResponse{Tasks: make([]p.InternalHistoryTask, 0, len(rows))}
	for _, row := range rows {
		resp.Tasks = append(resp.Tasks, p.InternalHistoryTask{
			Key:  tasks.NewKey(row.VisibilityTimestamp, row.TaskID),
			Blob: p.NewDataBlob(row.Data, row.DataEncoding),
		})
	}

	if len(resp.Tasks) == request.BatchSize {
		pageToken = &scheduledTaskPageToken{
			TaskID:    rows[request.BatchSize-1].TaskID + 1,
			Timestamp: rows[request.BatchSize-1].VisibilityTimestamp,
		}
		nextToken, err := pageToken.serialize()
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("GetHistoryTasks: error serializing page token: %v", err))
		}
		resp.NextPageToken = nextToken
	}

	return resp, nil
}

func (m *sqlExecutionStore) completeHistoryScheduledTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	// This is for backward compatiblity.
	// These task categories exist before the general history_scheduled_tasks table is created,
	// so they have their own tables.
	categoryID := request.TaskCategory.ID()
	if categoryID == tasks.CategoryIDTimer {
		return m.completeTimerTask(ctx, request)
	}

	if _, err := m.Db.DeleteFromHistoryScheduledTasks(ctx, sqlplugin.HistoryScheduledTasksFilter{
		ShardID:             request.ShardID,
		CategoryID:          int32(categoryID),
		VisibilityTimestamp: request.TaskKey.FireTime,
		TaskID:              request.TaskKey.TaskID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("CompleteHistoryTask operation failed. CategoryID: %v. Error: %v", categoryID, err))
	}
	return nil
}

func (m *sqlExecutionStore) rangeCompleteHistoryScheduledTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	// This is for backward compatiblity.
	// These task categories exist before the general history_scheduled_tasks table is created,
	// so they have their own tables.
	categoryID := request.TaskCategory.ID()
	if categoryID == tasks.CategoryIDTimer {
		return m.rangeCompleteTimerTasks(ctx, request)
	}

	start := request.InclusiveMinTaskKey.FireTime
	end := request.ExclusiveMaxTaskKey.FireTime
	if _, err := m.Db.RangeDeleteFromHistoryScheduledTasks(ctx, sqlplugin.HistoryScheduledTasksRangeFilter{
		ShardID:                         request.ShardID,
		CategoryID:                      int32(categoryID),
		InclusiveMinVisibilityTimestamp: start,
		ExclusiveMaxVisibilityTimestamp: end,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("RangeCompleteHistoryTask operation failed. CategoryID: %v. Error: %v", categoryID, err))
	}
	return nil
}

func (m *sqlExecutionStore) getTransferTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
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
		Tasks: make([]p.InternalHistoryTask, len(rows)),
	}
	if len(rows) == 0 {
		return resp, nil
	}

	for i, row := range rows {
		resp.Tasks[i] = p.InternalHistoryTask{
			Key:  tasks.NewImmediateKey(row.TaskID),
			Blob: p.NewDataBlob(row.Data, row.DataEncoding),
		}
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
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	if _, err := m.Db.DeleteFromTransferTasks(ctx, sqlplugin.TransferTasksFilter{
		ShardID: request.ShardID,
		TaskID:  request.TaskKey.TaskID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("CompleteTransferTask operation failed. Error: %v", err))
	}
	return nil
}

func (m *sqlExecutionStore) rangeCompleteTransferTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	if _, err := m.Db.RangeDeleteFromTransferTasks(ctx, sqlplugin.TransferTasksRangeFilter{
		ShardID:            request.ShardID,
		InclusiveMinTaskID: request.InclusiveMinTaskKey.TaskID,
		ExclusiveMaxTaskID: request.ExclusiveMaxTaskKey.TaskID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("RangeCompleteTransferTask operation failed. Error: %v", err))
	}
	return nil
}

func (m *sqlExecutionStore) getTimerTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	pageToken := &scheduledTaskPageToken{TaskID: math.MinInt64, Timestamp: request.InclusiveMinTaskKey.FireTime}
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

	resp := &p.InternalGetHistoryTasksResponse{Tasks: make([]p.InternalHistoryTask, 0, len(rows))}
	for _, row := range rows {
		resp.Tasks = append(resp.Tasks, p.InternalHistoryTask{
			Key:  tasks.NewKey(row.VisibilityTimestamp, row.TaskID),
			Blob: p.NewDataBlob(row.Data, row.DataEncoding),
		})
	}

	if len(resp.Tasks) == request.BatchSize {
		pageToken = &scheduledTaskPageToken{
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
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
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
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	start := request.InclusiveMinTaskKey.FireTime
	end := request.ExclusiveMaxTaskKey.FireTime
	if _, err := m.Db.RangeDeleteFromTimerTasks(ctx, sqlplugin.TimerTasksRangeFilter{
		ShardID:                         request.ShardID,
		InclusiveMinVisibilityTimestamp: start,
		ExclusiveMaxVisibilityTimestamp: end,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("RangeCompleteTimerTask operation failed. Error: %v", err))
	}
	return nil
}

func (m *sqlExecutionStore) getReplicationTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
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

	var replicationTasks = make([]p.InternalHistoryTask, len(rows))
	for i, row := range rows {
		replicationTasks[i] = p.InternalHistoryTask{
			Key:  tasks.NewImmediateKey(row.TaskID),
			Blob: p.NewDataBlob(row.Data, row.DataEncoding),
		}
	}
	var nextPageToken []byte
	if len(rows) == batchSize {
		nextPageToken = getImmediateTaskNextPageToken(
			rows[len(rows)-1].TaskID,
			exclusiveMaxTaskID,
		)
	}
	return &p.InternalGetHistoryTasksResponse{
		Tasks:         replicationTasks,
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

	var dlqTasks = make([]p.InternalHistoryTask, len(rows))
	for i, row := range rows {
		dlqTasks[i] = p.InternalHistoryTask{
			Key:  tasks.NewImmediateKey(row.TaskID),
			Blob: p.NewDataBlob(row.Data, row.DataEncoding),
		}
	}
	var nextPageToken []byte
	if len(rows) == batchSize {
		nextPageToken = getImmediateTaskNextPageToken(
			rows[len(rows)-1].TaskID,
			exclusiveMaxTaskID,
		)
	}
	return &p.InternalGetHistoryTasksResponse{
		Tasks:         dlqTasks,
		NextPageToken: nextPageToken,
	}, nil
}

func (m *sqlExecutionStore) completeReplicationTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	if _, err := m.Db.DeleteFromReplicationTasks(ctx, sqlplugin.ReplicationTasksFilter{
		ShardID: request.ShardID,
		TaskID:  request.TaskKey.TaskID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("CompleteReplicationTask operation failed. Error: %v", err))
	}
	return nil
}

func (m *sqlExecutionStore) rangeCompleteReplicationTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
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
	ctx context.Context,
	request *p.PutReplicationTaskToDLQRequest,
) error {
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
	ctx context.Context,
	request *p.GetReplicationTasksFromDLQRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
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
	ctx context.Context,
	request *p.DeleteReplicationTaskFromDLQRequest,
) error {
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
	ctx context.Context,
	request *p.RangeDeleteReplicationTaskFromDLQRequest,
) error {
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

func (m *sqlExecutionStore) IsReplicationDLQEmpty(
	ctx context.Context,
	request *p.GetReplicationTasksFromDLQRequest,
) (bool, error) {
	res, err := m.Db.RangeSelectFromReplicationDLQTasks(ctx, sqlplugin.ReplicationDLQTasksRangeFilter{
		ShardID:            request.ShardID,
		SourceClusterName:  request.SourceClusterName,
		InclusiveMinTaskID: request.InclusiveMinTaskKey.TaskID,
		ExclusiveMaxTaskID: math.MaxInt64,
		PageSize:           1,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// The queue is empty
			return true, nil
		}
		return false, err
	}
	return len(res) == 0, nil
}

func (m *sqlExecutionStore) getVisibilityTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
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
		Tasks: make([]p.InternalHistoryTask, len(rows)),
	}
	if len(rows) == 0 {
		return resp, nil
	}

	for i, row := range rows {
		resp.Tasks[i] = p.InternalHistoryTask{
			Key:  tasks.NewImmediateKey(row.TaskID),
			Blob: p.NewDataBlob(row.Data, row.DataEncoding),
		}
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
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	if _, err := m.Db.DeleteFromVisibilityTasks(ctx, sqlplugin.VisibilityTasksFilter{
		ShardID: request.ShardID,
		TaskID:  request.TaskKey.TaskID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("CompleteVisibilityTask operation failed. Error: %v", err))
	}
	return nil
}

func (m *sqlExecutionStore) rangeCompleteVisibilityTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	if _, err := m.Db.RangeDeleteFromVisibilityTasks(ctx, sqlplugin.VisibilityTasksRangeFilter{
		ShardID:            request.ShardID,
		InclusiveMinTaskID: request.InclusiveMinTaskKey.TaskID,
		ExclusiveMaxTaskID: request.ExclusiveMaxTaskKey.TaskID,
	}); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("RangeCompleteVisibilityTask operation failed. Error: %v", err))
	}
	return nil
}

type scheduledTaskPageToken struct {
	TaskID    int64
	Timestamp time.Time
}

func (t *scheduledTaskPageToken) serialize() ([]byte, error) {
	return json.Marshal(t)
}

func (t *scheduledTaskPageToken) deserialize(payload []byte) error {
	return json.Unmarshal(payload, t)
}
