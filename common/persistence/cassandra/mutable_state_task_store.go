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
	_ context.Context,
	request *p.InternalAddHistoryTasksRequest,
) error {
	batch := d.Session.NewBatch(gocql.LoggedBatch)

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
	_ context.Context,
	request *p.GetHistoryTaskRequest,
) (*p.InternalGetHistoryTaskResponse, error) {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		return d.getTransferTask(request)
	case tasks.CategoryIDTimer:
		return d.getTimerTask(request)
	case tasks.CategoryIDVisibility:
		return d.getVisibilityTask(request)
	case tasks.CategoryIDReplication:
		return d.getReplicationTask(request)
	default:
		return d.getHistoryTask(request)
	}
}

func (d *MutableStateTaskStore) GetHistoryTasks(
	_ context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		return d.getTransferTasks(request)
	case tasks.CategoryIDTimer:
		return d.getTimerTasks(request)
	case tasks.CategoryIDVisibility:
		return d.getVisibilityTasks(request)
	case tasks.CategoryIDReplication:
		return d.getReplicationTasks(request)
	default:
		return d.getHistoryTasks(request)
	}
}

func (d *MutableStateTaskStore) CompleteHistoryTask(
	_ context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		return d.completeTransferTask(request)
	case tasks.CategoryIDTimer:
		return d.completeTimerTask(request)
	case tasks.CategoryIDVisibility:
		return d.completeVisibilityTask(request)
	case tasks.CategoryIDReplication:
		return d.completeReplicationTask(request)
	default:
		return d.completeHistoryTask(request)
	}
}

func (d *MutableStateTaskStore) RangeCompleteHistoryTasks(
	_ context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		return d.rangeCompleteTransferTasks(request)
	case tasks.CategoryIDTimer:
		return d.rangeCompleteTimerTasks(request)
	case tasks.CategoryIDVisibility:
		return d.rangeCompleteVisibilityTasks(request)
	case tasks.CategoryIDReplication:
		return d.rangeCompleteReplicationTasks(request)
	default:
		return d.rangeCompleteHistoryTasks(request)
	}
}

func (d *MutableStateTaskStore) getTransferTask(
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
		taskID)

	var data []byte
	var encoding string
	if err := query.Scan(&data, &encoding); err != nil {
		return nil, gocql.ConvertError("GetTransferTask", err)
	}
	return &p.InternalGetHistoryTaskResponse{Task: *p.NewDataBlob(data, encoding)}, nil
}

func (d *MutableStateTaskStore) getTransferTasks(
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
	)
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
	)

	err := query.Exec()
	return gocql.ConvertError("CompleteTransferTask", err)
}

func (d *MutableStateTaskStore) rangeCompleteTransferTasks(
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
	)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteTransferTask", err)
}

func (d *MutableStateTaskStore) getTimerTask(
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
	)

	var data []byte
	var encoding string
	if err := query.Scan(&data, &encoding); err != nil {
		return nil, gocql.ConvertError("GetTimerTask", err)
	}

	return &p.InternalGetHistoryTaskResponse{Task: *p.NewDataBlob(data, encoding)}, nil
}

func (d *MutableStateTaskStore) getTimerTasks(
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
	)
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
	)

	err := query.Exec()
	return gocql.ConvertError("CompleteTimerTask", err)
}

func (d *MutableStateTaskStore) rangeCompleteTimerTasks(
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
	)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteTimerTask", err)
}

func (d *MutableStateTaskStore) getReplicationTask(
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
		taskID)

	var data []byte
	var encoding string
	if err := query.Scan(&data, &encoding); err != nil {
		return nil, gocql.ConvertError("GetReplicationTask", err)
	}

	return &p.InternalGetHistoryTaskResponse{Task: *p.NewDataBlob(data, encoding)}, nil
}

func (d *MutableStateTaskStore) getReplicationTasks(
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
	).PageSize(request.BatchSize).PageState(request.NextPageToken)

	return d.populateGetReplicationTasksResponse(query, "GetReplicationTasks")
}

func (d *MutableStateTaskStore) completeReplicationTask(
	request *p.CompleteHistoryTaskRequest,
) error {
	query := d.Session.Query(templateCompleteReplicationTaskQuery,
		request.ShardID,
		rowTypeReplicationTask,
		rowTypeReplicationNamespaceID,
		rowTypeReplicationWorkflowID,
		rowTypeReplicationRunID,
		defaultVisibilityTimestamp,
		request.TaskKey.TaskID)

	err := query.Exec()
	return gocql.ConvertError("CompleteReplicationTask", err)
}

func (d *MutableStateTaskStore) rangeCompleteReplicationTasks(
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
	)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteReplicationTask", err)
}

func (d *MutableStateTaskStore) PutReplicationTaskToDLQ(
	_ context.Context,
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
		task.GetTaskId())

	err = query.Exec()
	if err != nil {
		return gocql.ConvertError("PutReplicationTaskToDLQ", err)
	}

	return nil
}

func (d *MutableStateTaskStore) GetReplicationTasksFromDLQ(
	_ context.Context,
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
	).PageSize(request.BatchSize).PageState(request.NextPageToken)

	return d.populateGetReplicationTasksResponse(query, "GetReplicationTasksFromDLQ")
}

func (d *MutableStateTaskStore) DeleteReplicationTaskFromDLQ(
	_ context.Context,
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
	)

	err := query.Exec()
	return gocql.ConvertError("DeleteReplicationTaskFromDLQ", err)
}

func (d *MutableStateTaskStore) RangeDeleteReplicationTaskFromDLQ(
	_ context.Context,
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
	)

	err := query.Exec()
	return gocql.ConvertError("RangeDeleteReplicationTaskFromDLQ", err)
}

func (d *MutableStateTaskStore) getVisibilityTask(
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
		taskID)

	var data []byte
	var encoding string
	if err := query.Scan(&data, &encoding); err != nil {
		return nil, gocql.ConvertError("GetVisibilityTask", err)
	}
	return &p.InternalGetHistoryTaskResponse{Task: *p.NewDataBlob(data, encoding)}, nil
}

func (d *MutableStateTaskStore) getVisibilityTasks(
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
	)
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
	request *p.CompleteHistoryTaskRequest,
) error {
	query := d.Session.Query(templateCompleteVisibilityTaskQuery,
		request.ShardID,
		rowTypeVisibilityTask,
		rowTypeVisibilityTaskNamespaceID,
		rowTypeVisibilityTaskWorkflowID,
		rowTypeVisibilityTaskRunID,
		defaultVisibilityTimestamp,
		request.TaskKey.TaskID)

	err := query.Exec()
	return gocql.ConvertError("CompleteVisibilityTask", err)
}

func (d *MutableStateTaskStore) rangeCompleteVisibilityTasks(
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
	)

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
	)

	var data []byte
	var encoding string
	if err := query.Scan(&data, &encoding); err != nil {
		return nil, gocql.ConvertError("GetHistoryTask", err)
	}
	return &p.InternalGetHistoryTaskResponse{Task: *p.NewDataBlob(data, encoding)}, nil
}

func (d *MutableStateTaskStore) getHistoryTasks(
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
		)
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
		)
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
	)

	err := query.Exec()
	return gocql.ConvertError("CompleteHistoryTask", err)
}

func (d *MutableStateTaskStore) rangeCompleteHistoryTasks(
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
		)
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
		)
	}

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteHistoryTasks", err)
}
