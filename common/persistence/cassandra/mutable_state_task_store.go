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

package cassandra

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
)

const (
	templateCreateTransferTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, transfer, transfer_encoding, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateCreateReplicationTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, replication, replication_encoding, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateCreateVisibilityTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, visibility_task_data, visibility_task_encoding, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateCreateTimerTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, timer, timer_encoding, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateCreateHistoryTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, task_data, task_encoding, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`

	templateGetHistoryTaskQuery = `SELECT task_data, task_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateGetHistoryImmediateTasksQuery = `SELECT task_data, task_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id > ? ` +
		`and task_id <= ?`

	templateGetHistoryScheduledTasksQuery = `SELECT task_data, task_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ?` +
		`and namespace_id = ? ` +
		`and workflow_id = ?` +
		`and run_id = ?` +
		`and visibility_ts >= ? ` +
		`and visibility_ts < ?`

	templateGetTransferTaskQuery = `SELECT transfer, transfer_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateGetTransferTasksQuery = `SELECT transfer, transfer_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id >= ? ` +
		`and task_id < ?`

	templateGetVisibilityTaskQuery = `SELECT visibility_task_data, visibility_task_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateGetVisibilityTasksQuery = `SELECT visibility_task_data, visibility_task_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id >= ? ` +
		`and task_id < ?`

	templateGetReplicationTaskQuery = `SELECT replication, replication_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateGetReplicationTasksQuery = `SELECT replication, replication_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id >= ? ` +
		`and task_id < ?`

	templateCompleteTransferTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateRangeCompleteTransferTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id >= ? ` +
		`and task_id < ?`

	templateCompleteVisibilityTaskQuery = templateCompleteTransferTaskQuery

	templateRangeCompleteVisibilityTaskQuery = templateRangeCompleteTransferTaskQuery

	templateCompleteReplicationTaskQuery = templateCompleteTransferTaskQuery

	templateRangeCompleteReplicationTaskQuery = templateRangeCompleteTransferTaskQuery

	templateCompleteHistoryTaskQuery = templateCompleteTransferTaskQuery

	templateRangeCompleteHistoryImmediateTasksQuery = templateRangeCompleteTransferTaskQuery

	templateRangeCompleteHistoryScheduledTasksQuery = templateRangeCompleteTimerTaskQuery

	templateGetTimerTaskQuery = `SELECT timer, timer_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateGetTimerTasksQuery = `SELECT timer, timer_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ?` +
		`and namespace_id = ? ` +
		`and workflow_id = ?` +
		`and run_id = ?` +
		`and visibility_ts >= ? ` +
		`and visibility_ts < ?`

	templateCompleteTimerTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ?` +
		`and run_id = ?` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateRangeCompleteTimerTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ?` +
		`and run_id = ?` +
		`and visibility_ts >= ? ` +
		`and visibility_ts < ?`
)

type (
	MutableStateTaskStore struct {
		Session gocql.Session
		Logger  log.Logger
	}
)

func NewMutableStateTaskStore(
	session gocql.Session,
	logger log.Logger,
) *MutableStateTaskStore {
	return &MutableStateTaskStore{
		Session: session,
		Logger:  logger,
	}
}

func (d *MutableStateTaskStore) AddHistoryTasks(
	ctx context.Context,
	request *p.InternalAddHistoryTasksRequest,
) error {
	batch := d.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)

	if err := applyTasks(
		batch,
		request.ShardID,
		request.Tasks,
	); err != nil {
		return err
	}

	batch.Query(templateUpdateLeaseQuery,
		request.RangeID,
		request.ShardID,
		rowTypeShard,
		rowTypeShardNamespaceID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		request.RangeID,
	)

	previous := make(map[string]interface{})
	applied, iter, err := d.Session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		return gocql.ConvertError("AddTasks", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	if !applied {
		if previousRangeID, ok := previous["range_id"].(int64); ok && previousRangeID != request.RangeID {
			// CreateWorkflowExecution failed because rangeID was modified
			return &p.ShardOwnershipLostError{
				ShardID: request.ShardID,
				Msg:     fmt.Sprintf("Failed to add tasks.  Request RangeID: %v, Actual RangeID: %v", request.RangeID, previousRangeID),
			}
		} else {
			return serviceerror.NewUnavailable("AddTasks operation failed: %v")
		}
	}
	return nil
}

func (d *MutableStateTaskStore) GetHistoryTask(
	ctx context.Context,
	request *p.GetHistoryTaskRequest,
) (*p.InternalGetHistoryTaskResponse, error) {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		return d.getTransferTask(ctx, request)
	case tasks.CategoryIDTimer:
		return d.getTimerTask(ctx, request)
	case tasks.CategoryIDVisibility:
		return d.getVisibilityTask(ctx, request)
	case tasks.CategoryIDReplication:
		return d.getReplicationTask(ctx, request)
	default:
		return d.getHistoryTask(ctx, request)
	}
}

func (d *MutableStateTaskStore) GetHistoryTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		return d.getTransferTasks(ctx, request)
	case tasks.CategoryIDTimer:
		return d.getTimerTasks(ctx, request)
	case tasks.CategoryIDVisibility:
		return d.getVisibilityTasks(ctx, request)
	case tasks.CategoryIDReplication:
		return d.getReplicationTasks(ctx, request)
	default:
		return d.getHistoryTasks(ctx, request)
	}
}

func (d *MutableStateTaskStore) CompleteHistoryTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		return d.completeTransferTask(ctx, request)
	case tasks.CategoryIDTimer:
		return d.completeTimerTask(ctx, request)
	case tasks.CategoryIDVisibility:
		return d.completeVisibilityTask(ctx, request)
	case tasks.CategoryIDReplication:
		return d.completeReplicationTask(ctx, request)
	default:
		return d.completeHistoryTask(ctx, request)
	}
}

func (d *MutableStateTaskStore) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		return d.rangeCompleteTransferTasks(ctx, request)
	case tasks.CategoryIDTimer:
		return d.rangeCompleteTimerTasks(ctx, request)
	case tasks.CategoryIDVisibility:
		return d.rangeCompleteVisibilityTasks(ctx, request)
	case tasks.CategoryIDReplication:
		return d.rangeCompleteReplicationTasks(ctx, request)
	default:
		return d.rangeCompleteHistoryTasks(ctx, request)
	}
}

func (d *MutableStateTaskStore) getTransferTask(
	ctx context.Context,
	request *p.GetHistoryTaskRequest,
) (*p.InternalGetHistoryTaskResponse, error) {
	shardID := request.ShardID
	taskID := request.TaskKey.TaskID
	query := d.Session.Query(templateGetTransferTaskQuery,
		shardID,
		rowTypeTransferTask,
		rowTypeTransferNamespaceID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		taskID,
	).WithContext(ctx)

	var data []byte
	var encoding string
	if err := query.Scan(&data, &encoding); err != nil {
		return nil, gocql.ConvertError("GetTransferTask", err)
	}
	return &p.InternalGetHistoryTaskResponse{Task: *p.NewDataBlob(data, encoding)}, nil
}

func (d *MutableStateTaskStore) getTransferTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {

	// Reading transfer tasks need to be quorum level consistent, otherwise we could lose task
	query := d.Session.Query(templateGetTransferTasksQuery,
		request.ShardID,
		rowTypeTransferTask,
		rowTypeTransferNamespaceID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx)
	iter := query.PageSize(request.BatchSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetHistoryTasksResponse{}
	var data []byte
	var encoding string

	for iter.Scan(&data, &encoding) {
		response.Tasks = append(response.Tasks, *p.NewDataBlob(data, encoding))

		data = nil
		encoding = ""
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetTransferTasks", err)
	}

	return response, nil
}

func (d *MutableStateTaskStore) completeTransferTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	query := d.Session.Query(templateCompleteTransferTaskQuery,
		request.ShardID,
		rowTypeTransferTask,
		rowTypeTransferNamespaceID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		request.TaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("CompleteTransferTask", err)
}

func (d *MutableStateTaskStore) rangeCompleteTransferTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	query := d.Session.Query(templateRangeCompleteTransferTaskQuery,
		request.ShardID,
		rowTypeTransferTask,
		rowTypeTransferNamespaceID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteTransferTask", err)
}

func (d *MutableStateTaskStore) getTimerTask(
	ctx context.Context,
	request *p.GetHistoryTaskRequest,
) (*p.InternalGetHistoryTaskResponse, error) {
	shardID := request.ShardID
	taskID := request.TaskKey.TaskID
	visibilityTs := request.TaskKey.FireTime // TODO: do we need to convert the timestamp?
	query := d.Session.Query(templateGetTimerTaskQuery,
		shardID,
		rowTypeTimerTask,
		rowTypeTimerNamespaceID,
		rowTypeTimerWorkflowID,
		rowTypeTimerRunID,
		visibilityTs,
		taskID,
	).WithContext(ctx)

	var data []byte
	var encoding string
	if err := query.Scan(&data, &encoding); err != nil {
		return nil, gocql.ConvertError("GetTimerTask", err)
	}

	return &p.InternalGetHistoryTaskResponse{Task: *p.NewDataBlob(data, encoding)}, nil
}

func (d *MutableStateTaskStore) getTimerTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	// Reading timer tasks need to be quorum level consistent, otherwise we could lose tasks
	minTimestamp := p.UnixMilliseconds(request.InclusiveMinTaskKey.FireTime)
	maxTimestamp := p.UnixMilliseconds(request.ExclusiveMaxTaskKey.FireTime)
	query := d.Session.Query(templateGetTimerTasksQuery,
		request.ShardID,
		rowTypeTimerTask,
		rowTypeTimerNamespaceID,
		rowTypeTimerWorkflowID,
		rowTypeTimerRunID,
		minTimestamp,
		maxTimestamp,
	).WithContext(ctx)
	iter := query.PageSize(request.BatchSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetHistoryTasksResponse{}
	var data []byte
	var encoding string

	for iter.Scan(&data, &encoding) {
		response.Tasks = append(response.Tasks, *p.NewDataBlob(data, encoding))

		data = nil
		encoding = ""
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetTimerTasks", err)
	}

	return response, nil
}

func (d *MutableStateTaskStore) completeTimerTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	ts := p.UnixMilliseconds(request.TaskKey.FireTime)
	query := d.Session.Query(templateCompleteTimerTaskQuery,
		request.ShardID,
		rowTypeTimerTask,
		rowTypeTimerNamespaceID,
		rowTypeTimerWorkflowID,
		rowTypeTimerRunID,
		ts,
		request.TaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("CompleteTimerTask", err)
}

func (d *MutableStateTaskStore) rangeCompleteTimerTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	start := p.UnixMilliseconds(request.InclusiveMinTaskKey.FireTime)
	end := p.UnixMilliseconds(request.ExclusiveMaxTaskKey.FireTime)
	query := d.Session.Query(templateRangeCompleteTimerTaskQuery,
		request.ShardID,
		rowTypeTimerTask,
		rowTypeTimerNamespaceID,
		rowTypeTimerWorkflowID,
		rowTypeTimerRunID,
		start,
		end,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteTimerTask", err)
}

func (d *MutableStateTaskStore) getReplicationTask(
	ctx context.Context,
	request *p.GetHistoryTaskRequest,
) (*p.InternalGetHistoryTaskResponse, error) {
	shardID := request.ShardID
	taskID := request.TaskKey.TaskID
	query := d.Session.Query(templateGetReplicationTaskQuery,
		shardID,
		rowTypeReplicationTask,
		rowTypeReplicationNamespaceID,
		rowTypeReplicationWorkflowID,
		rowTypeReplicationRunID,
		defaultVisibilityTimestamp,
		taskID,
	).WithContext(ctx)

	var data []byte
	var encoding string
	if err := query.Scan(&data, &encoding); err != nil {
		return nil, gocql.ConvertError("GetReplicationTask", err)
	}

	return &p.InternalGetHistoryTaskResponse{Task: *p.NewDataBlob(data, encoding)}, nil
}

func (d *MutableStateTaskStore) getReplicationTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {

	// Reading replication tasks need to be quorum level consistent, otherwise we could lose task
	query := d.Session.Query(templateGetReplicationTasksQuery,
		request.ShardID,
		rowTypeReplicationTask,
		rowTypeReplicationNamespaceID,
		rowTypeReplicationWorkflowID,
		rowTypeReplicationRunID,
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx).PageSize(request.BatchSize).PageState(request.NextPageToken)

	return d.populateGetReplicationTasksResponse(query, "GetReplicationTasks")
}

func (d *MutableStateTaskStore) completeReplicationTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	query := d.Session.Query(templateCompleteReplicationTaskQuery,
		request.ShardID,
		rowTypeReplicationTask,
		rowTypeReplicationNamespaceID,
		rowTypeReplicationWorkflowID,
		rowTypeReplicationRunID,
		defaultVisibilityTimestamp,
		request.TaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("CompleteReplicationTask", err)
}

func (d *MutableStateTaskStore) rangeCompleteReplicationTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	query := d.Session.Query(templateRangeCompleteReplicationTaskQuery,
		request.ShardID,
		rowTypeReplicationTask,
		rowTypeReplicationNamespaceID,
		rowTypeReplicationWorkflowID,
		rowTypeReplicationRunID,
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteReplicationTask", err)
}

func (d *MutableStateTaskStore) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *p.PutReplicationTaskToDLQRequest,
) error {
	task := request.TaskInfo
	datablob, err := serialization.ReplicationTaskInfoToBlob(task)
	if err != nil {
		return gocql.ConvertError("PutReplicationTaskToDLQ", err)
	}

	// Use source cluster name as the workflow id for replication dlq
	query := d.Session.Query(templateCreateReplicationTaskQuery,
		request.ShardID,
		rowTypeDLQ,
		rowTypeDLQNamespaceID,
		request.SourceClusterName,
		rowTypeDLQRunID,
		datablob.Data,
		datablob.EncodingType.String(),
		defaultVisibilityTimestamp,
		task.GetTaskId(),
	).WithContext(ctx)

	err = query.Exec()
	if err != nil {
		return gocql.ConvertError("PutReplicationTaskToDLQ", err)
	}

	return nil
}

func (d *MutableStateTaskStore) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *p.GetReplicationTasksFromDLQRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	// Reading replication tasks need to be quorum level consistent, otherwise we could lose tasks
	query := d.Session.Query(templateGetReplicationTasksQuery,
		request.ShardID,
		rowTypeDLQ,
		rowTypeDLQNamespaceID,
		request.SourceClusterName,
		rowTypeDLQRunID,
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx).PageSize(request.BatchSize).PageState(request.NextPageToken)

	return d.populateGetReplicationTasksResponse(query, "GetReplicationTasksFromDLQ")
}

func (d *MutableStateTaskStore) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *p.DeleteReplicationTaskFromDLQRequest,
) error {

	query := d.Session.Query(templateCompleteReplicationTaskQuery,
		request.ShardID,
		rowTypeDLQ,
		rowTypeDLQNamespaceID,
		request.SourceClusterName,
		rowTypeDLQRunID,
		defaultVisibilityTimestamp,
		request.TaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("DeleteReplicationTaskFromDLQ", err)
}

func (d *MutableStateTaskStore) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *p.RangeDeleteReplicationTaskFromDLQRequest,
) error {

	query := d.Session.Query(templateRangeCompleteReplicationTaskQuery,
		request.ShardID,
		rowTypeDLQ,
		rowTypeDLQNamespaceID,
		request.SourceClusterName,
		rowTypeDLQRunID,
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("RangeDeleteReplicationTaskFromDLQ", err)
}

func (d *MutableStateTaskStore) getVisibilityTask(
	ctx context.Context,
	request *p.GetHistoryTaskRequest,
) (*p.InternalGetHistoryTaskResponse, error) {
	shardID := request.ShardID
	taskID := request.TaskKey.TaskID
	query := d.Session.Query(templateGetVisibilityTaskQuery,
		shardID,
		rowTypeVisibilityTask,
		rowTypeVisibilityTaskNamespaceID,
		rowTypeVisibilityTaskWorkflowID,
		rowTypeVisibilityTaskRunID,
		defaultVisibilityTimestamp,
		taskID,
	).WithContext(ctx)

	var data []byte
	var encoding string
	if err := query.Scan(&data, &encoding); err != nil {
		return nil, gocql.ConvertError("GetVisibilityTask", err)
	}
	return &p.InternalGetHistoryTaskResponse{Task: *p.NewDataBlob(data, encoding)}, nil
}

func (d *MutableStateTaskStore) getVisibilityTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {

	// Reading Visibility tasks need to be quorum level consistent, otherwise we could lose task
	query := d.Session.Query(templateGetVisibilityTasksQuery,
		request.ShardID,
		rowTypeVisibilityTask,
		rowTypeVisibilityTaskNamespaceID,
		rowTypeVisibilityTaskWorkflowID,
		rowTypeVisibilityTaskRunID,
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx)
	iter := query.PageSize(request.BatchSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetHistoryTasksResponse{}
	var data []byte
	var encoding string

	for iter.Scan(&data, &encoding) {
		response.Tasks = append(response.Tasks, *p.NewDataBlob(data, encoding))

		data = nil
		encoding = ""
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetVisibilityTasks", err)
	}

	return response, nil
}

func (d *MutableStateTaskStore) completeVisibilityTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	query := d.Session.Query(templateCompleteVisibilityTaskQuery,
		request.ShardID,
		rowTypeVisibilityTask,
		rowTypeVisibilityTaskNamespaceID,
		rowTypeVisibilityTaskWorkflowID,
		rowTypeVisibilityTaskRunID,
		defaultVisibilityTimestamp,
		request.TaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("CompleteVisibilityTask", err)
}

func (d *MutableStateTaskStore) rangeCompleteVisibilityTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	query := d.Session.Query(templateRangeCompleteVisibilityTaskQuery,
		request.ShardID,
		rowTypeVisibilityTask,
		rowTypeVisibilityTaskNamespaceID,
		rowTypeVisibilityTaskWorkflowID,
		rowTypeVisibilityTaskRunID,
		defaultVisibilityTimestamp,
		request.InclusiveMinTaskKey.TaskID,
		request.ExclusiveMaxTaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteVisibilityTask", err)
}

func (d *MutableStateTaskStore) populateGetReplicationTasksResponse(
	query gocql.Query,
	operation string,
) (*p.InternalGetHistoryTasksResponse, error) {
	iter := query.Iter()

	response := &p.InternalGetHistoryTasksResponse{}
	var data []byte
	var encoding string

	for iter.Scan(&data, &encoding) {
		response.Tasks = append(response.Tasks, *p.NewDataBlob(data, encoding))

		data = nil
		encoding = ""
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError(operation, err)
	}

	return response, nil
}

func (d *MutableStateTaskStore) getHistoryTask(
	ctx context.Context,
	request *p.GetHistoryTaskRequest,
) (*p.InternalGetHistoryTaskResponse, error) {
	shardID := request.ShardID
	ts := defaultVisibilityTimestamp
	if request.TaskCategory.Type() == tasks.CategoryTypeScheduled {
		ts = p.UnixMilliseconds(request.TaskKey.FireTime)
	}
	taskID := request.TaskKey.TaskID
	query := d.Session.Query(templateGetHistoryTaskQuery,
		shardID,
		request.TaskCategory.ID(),
		rowTypeHistoryTaskNamespaceID,
		rowTypeHistoryTaskWorkflowID,
		rowTypeHistoryTaskRunID,
		ts,
		taskID,
	).WithContext(ctx)

	var data []byte
	var encoding string
	if err := query.Scan(&data, &encoding); err != nil {
		return nil, gocql.ConvertError("GetHistoryTask", err)
	}
	return &p.InternalGetHistoryTaskResponse{Task: *p.NewDataBlob(data, encoding)}, nil
}

func (d *MutableStateTaskStore) getHistoryTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	// execution manager should already validated the request
	// Reading history tasks need to be quorum level consistent, otherwise we could lose task

	var query gocql.Query
	if request.TaskCategory.Type() == tasks.CategoryTypeImmediate {
		query = d.Session.Query(templateGetHistoryImmediateTasksQuery,
			request.ShardID,
			request.TaskCategory.ID(),
			rowTypeHistoryTaskNamespaceID,
			rowTypeHistoryTaskWorkflowID,
			rowTypeHistoryTaskRunID,
			defaultVisibilityTimestamp,
			request.InclusiveMinTaskKey.TaskID,
			request.ExclusiveMaxTaskKey.TaskID,
		).WithContext(ctx)
	} else {
		minTimestamp := p.UnixMilliseconds(request.InclusiveMinTaskKey.FireTime)
		maxTimestamp := p.UnixMilliseconds(request.ExclusiveMaxTaskKey.FireTime)
		query = d.Session.Query(templateGetHistoryScheduledTasksQuery,
			request.ShardID,
			request.TaskCategory.ID(),
			rowTypeHistoryTaskNamespaceID,
			rowTypeHistoryTaskWorkflowID,
			rowTypeHistoryTaskRunID,
			minTimestamp,
			maxTimestamp,
		).WithContext(ctx)
	}

	iter := query.PageSize(request.BatchSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetHistoryTasksResponse{}
	var data []byte
	var encoding string

	for iter.Scan(&data, &encoding) {
		response.Tasks = append(response.Tasks, *p.NewDataBlob(data, encoding))

		data = nil
		encoding = ""
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetHistoryTasks", err)
	}

	return response, nil
}

func (d *MutableStateTaskStore) completeHistoryTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	ts := defaultVisibilityTimestamp
	if request.TaskCategory.Type() == tasks.CategoryTypeScheduled {
		ts = p.UnixMilliseconds(request.TaskKey.FireTime)
	}
	query := d.Session.Query(templateCompleteHistoryTaskQuery,
		request.ShardID,
		request.TaskCategory.ID(),
		rowTypeHistoryTaskNamespaceID,
		rowTypeHistoryTaskWorkflowID,
		rowTypeHistoryTaskRunID,
		ts,
		request.TaskKey.TaskID,
	).WithContext(ctx)

	err := query.Exec()
	return gocql.ConvertError("CompleteHistoryTask", err)
}

func (d *MutableStateTaskStore) rangeCompleteHistoryTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	// execution manager should already validated the request
	var query gocql.Query
	if request.TaskCategory.Type() == tasks.CategoryTypeImmediate {
		query = d.Session.Query(templateRangeCompleteHistoryImmediateTasksQuery,
			request.ShardID,
			request.TaskCategory.ID(),
			rowTypeHistoryTaskNamespaceID,
			rowTypeHistoryTaskWorkflowID,
			rowTypeHistoryTaskRunID,
			defaultVisibilityTimestamp,
			request.InclusiveMinTaskKey.TaskID,
			request.ExclusiveMaxTaskKey.TaskID,
		).WithContext(ctx)
	} else {
		minTimestamp := p.UnixMilliseconds(request.InclusiveMinTaskKey.FireTime)
		maxTimestamp := p.UnixMilliseconds(request.ExclusiveMaxTaskKey.FireTime)
		query = d.Session.Query(templateRangeCompleteHistoryScheduledTasksQuery,
			request.ShardID,
			request.TaskCategory.ID(),
			rowTypeHistoryTaskNamespaceID,
			rowTypeHistoryTaskWorkflowID,
			rowTypeHistoryTaskRunID,
			minTimestamp,
			maxTimestamp,
		).WithContext(ctx)
	}

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteHistoryTasks", err)
}
