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
	"fmt"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
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
		`and task_id > ? ` +
		`and task_id <= ?`

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
		`and task_id > ? ` +
		`and task_id <= ?`

	templateGetTieredStorageTaskQuery = `SELECT tiered_storage_task_data, tiered_storage_task_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateGetTieredStorageTasksQuery = `SELECT tiered_storage_task_data, tiered_storage_task_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id > ? ` +
		`and task_id <= ?`

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
		`and task_id > ? ` +
		`and task_id <= ?`

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
		`and task_id > ? ` +
		`and task_id <= ?`

	templateCompleteVisibilityTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateCompleteTieredStorageTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateRangeCompleteVisibilityTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id > ? ` +
		`and task_id <= ?`

	templateRangeCompleteTieredStorageTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id > ? ` +
		`and task_id <= ?`

	templateCompleteReplicationTaskBeforeQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id <= ?`

	templateCompleteReplicationTaskQuery = templateCompleteTransferTaskQuery

	templateRangeCompleteReplicationTaskQuery = templateRangeCompleteTransferTaskQuery

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

	templateCreateTaskQuery = `INSERT INTO tasks (` +
		`namespace_id, task_queue_name, task_queue_type, type, task_id, task, task_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?)`

	templateCreateTaskWithTTLQuery = `INSERT INTO tasks (` +
		`namespace_id, task_queue_name, task_queue_type, type, task_id, task, task_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?) USING TTL ?`

	templateGetTasksQuery = `SELECT task_id, task, task_encoding ` +
		`FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and task_id > ? ` +
		`and task_id <= ?`

	templateCompleteTaskQuery = `DELETE FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and task_id = ?`

	templateCompleteTasksLessThanQuery = `DELETE FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`AND task_id <= ? `

	templateGetTaskQueue = `SELECT ` +
		`range_id, ` +
		`task_queue, ` +
		`task_queue_encoding ` +
		`FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and task_id = ?`

	templateInsertTaskQueueQuery = `INSERT INTO tasks (` +
		`namespace_id, ` +
		`task_queue_name, ` +
		`task_queue_type, ` +
		`type, ` +
		`task_id, ` +
		`range_id, ` +
		`task_queue, ` +
		`task_queue_encoding ` +
		`) VALUES (?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateUpdateTaskQueueQuery = `UPDATE tasks SET ` +
		`range_id = ?, ` +
		`task_queue = ?, ` +
		`task_queue_encoding = ? ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateUpdateTaskQueueQueryWithTTLPart1 = `INSERT INTO tasks (` +
		`namespace_id, ` +
		`task_queue_name, ` +
		`task_queue_type, ` +
		`type, ` +
		`task_id ` +
		`) VALUES (?, ?, ?, ?, ?) USING TTL ?`

	templateUpdateTaskQueueQueryWithTTLPart2 = `UPDATE tasks USING TTL ? SET ` +
		`range_id = ?, ` +
		`task_queue = ?, ` +
		`task_queue_encoding = ? ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateDeleteTaskQueueQuery = `DELETE FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`AND task_id = ? ` +
		`IF range_id = ?`
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

func (d *MutableStateTaskStore) AddTasks(
	request *p.InternalAddTasksRequest,
) error {
	batch := d.Session.NewBatch(gocql.LoggedBatch)

	if err := applyTasks(
		batch,
		request.ShardID,
		request.TransferTasks,
		request.TimerTasks,
		request.ReplicationTasks,
		request.VisibilityTasks,
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

func (d *MutableStateTaskStore) GetTransferTask(
	request *p.GetTransferTaskRequest,
) (*p.InternalGetTransferTaskResponse, error) {
	shardID := request.ShardID
	taskID := request.TaskID
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
	return &p.InternalGetTransferTaskResponse{Task: *p.NewDataBlob(data, encoding)}, nil
}

func (d *MutableStateTaskStore) GetTransferTasks(
	request *p.GetTransferTasksRequest,
) (*p.InternalGetTransferTasksResponse, error) {

	// Reading transfer tasks need to be quorum level consistent, otherwise we could lose task
	query := d.Session.Query(templateGetTransferTasksQuery,
		request.ShardID,
		rowTypeTransferTask,
		rowTypeTransferNamespaceID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		request.ReadLevel,
		request.MaxReadLevel,
	)
	iter := query.PageSize(request.BatchSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetTransferTasksResponse{}
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

func (d *MutableStateTaskStore) CompleteTransferTask(
	request *p.CompleteTransferTaskRequest,
) error {
	query := d.Session.Query(templateCompleteTransferTaskQuery,
		request.ShardID,
		rowTypeTransferTask,
		rowTypeTransferNamespaceID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		request.TaskID)

	err := query.Exec()
	return gocql.ConvertError("CompleteTransferTask", err)
}

func (d *MutableStateTaskStore) RangeCompleteTransferTask(
	request *p.RangeCompleteTransferTaskRequest,
) error {
	query := d.Session.Query(templateRangeCompleteTransferTaskQuery,
		request.ShardID,
		rowTypeTransferTask,
		rowTypeTransferNamespaceID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		request.ExclusiveBeginTaskID,
		request.InclusiveEndTaskID,
	)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteTransferTask", err)
}

func (d *MutableStateTaskStore) GetTimerTask(
	request *p.GetTimerTaskRequest,
) (*p.InternalGetTimerTaskResponse, error) {
	shardID := request.ShardID
	taskID := request.TaskID
	visibilityTs := request.VisibilityTimestamp
	query := d.Session.Query(templateGetTimerTaskQuery,
		shardID,
		rowTypeTimerTask,
		rowTypeTimerNamespaceID,
		rowTypeTimerWorkflowID,
		rowTypeTimerRunID,
		visibilityTs,
		taskID)

	var data []byte
	var encoding string
	if err := query.Scan(&data, &encoding); err != nil {
		return nil, gocql.ConvertError("GetTimerTask", err)
	}

	return &p.InternalGetTimerTaskResponse{Task: *p.NewDataBlob(data, encoding)}, nil
}

func (d *MutableStateTaskStore) GetTimerTasks(
	request *p.GetTimerTasksRequest,
) (*p.InternalGetTimerTasksResponse, error) {
	// Reading timer tasks need to be quorum level consistent, otherwise we could lose tasks
	minTimestamp := p.UnixMilliseconds(request.MinTimestamp)
	maxTimestamp := p.UnixMilliseconds(request.MaxTimestamp)
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

	response := &p.InternalGetTimerTasksResponse{}
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

func (d *MutableStateTaskStore) CompleteTimerTask(
	request *p.CompleteTimerTaskRequest,
) error {
	ts := p.UnixMilliseconds(request.VisibilityTimestamp)
	query := d.Session.Query(templateCompleteTimerTaskQuery,
		request.ShardID,
		rowTypeTimerTask,
		rowTypeTimerNamespaceID,
		rowTypeTimerWorkflowID,
		rowTypeTimerRunID,
		ts,
		request.TaskID)

	err := query.Exec()
	return gocql.ConvertError("CompleteTimerTask", err)
}

func (d *MutableStateTaskStore) RangeCompleteTimerTask(
	request *p.RangeCompleteTimerTaskRequest,
) error {
	start := p.UnixMilliseconds(request.InclusiveBeginTimestamp)
	end := p.UnixMilliseconds(request.ExclusiveEndTimestamp)
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

func (d *MutableStateTaskStore) GetReplicationTask(
	request *p.GetReplicationTaskRequest,
) (*p.InternalGetReplicationTaskResponse, error) {
	shardID := request.ShardID
	taskID := request.TaskID
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

	return &p.InternalGetReplicationTaskResponse{Task: *p.NewDataBlob(data, encoding)}, nil
}

func (d *MutableStateTaskStore) GetReplicationTasks(
	request *p.GetReplicationTasksRequest,
) (*p.InternalGetReplicationTasksResponse, error) {

	// Reading replication tasks need to be quorum level consistent, otherwise we could lose task
	query := d.Session.Query(templateGetReplicationTasksQuery,
		request.ShardID,
		rowTypeReplicationTask,
		rowTypeReplicationNamespaceID,
		rowTypeReplicationWorkflowID,
		rowTypeReplicationRunID,
		defaultVisibilityTimestamp,
		request.MinTaskID,
		request.MaxTaskID,
	).PageSize(request.BatchSize).PageState(request.NextPageToken)

	return d.populateGetReplicationTasksResponse(query, "GetReplicationTasks")
}

func (d *MutableStateTaskStore) CompleteReplicationTask(
	request *p.CompleteReplicationTaskRequest,
) error {
	query := d.Session.Query(templateCompleteReplicationTaskQuery,
		request.ShardID,
		rowTypeReplicationTask,
		rowTypeReplicationNamespaceID,
		rowTypeReplicationWorkflowID,
		rowTypeReplicationRunID,
		defaultVisibilityTimestamp,
		request.TaskID)

	err := query.Exec()
	return gocql.ConvertError("CompleteReplicationTask", err)
}

func (d *MutableStateTaskStore) RangeCompleteReplicationTask(
	request *p.RangeCompleteReplicationTaskRequest,
) error {
	query := d.Session.Query(templateCompleteReplicationTaskBeforeQuery,
		request.ShardID,
		rowTypeReplicationTask,
		rowTypeReplicationNamespaceID,
		rowTypeReplicationWorkflowID,
		rowTypeReplicationRunID,
		defaultVisibilityTimestamp,
		request.InclusiveEndTaskID,
	)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteReplicationTask", err)
}

func (d *MutableStateTaskStore) PutReplicationTaskToDLQ(
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
	request *p.GetReplicationTasksFromDLQRequest,
) (*p.InternalGetReplicationTasksResponse, error) {
	// Reading replication tasks need to be quorum level consistent, otherwise we could lose tasks
	query := d.Session.Query(templateGetReplicationTasksQuery,
		request.ShardID,
		rowTypeDLQ,
		rowTypeDLQNamespaceID,
		request.SourceClusterName,
		rowTypeDLQRunID,
		defaultVisibilityTimestamp,
		request.MinTaskID,
		request.MinTaskID+int64(request.BatchSize),
	).PageSize(request.BatchSize).PageState(request.NextPageToken)

	return d.populateGetReplicationTasksResponse(query, "GetReplicationTasksFromDLQ")
}

func (d *MutableStateTaskStore) DeleteReplicationTaskFromDLQ(
	request *p.DeleteReplicationTaskFromDLQRequest,
) error {

	query := d.Session.Query(templateCompleteReplicationTaskQuery,
		request.ShardID,
		rowTypeDLQ,
		rowTypeDLQNamespaceID,
		request.SourceClusterName,
		rowTypeDLQRunID,
		defaultVisibilityTimestamp,
		request.TaskID,
	)

	err := query.Exec()
	return gocql.ConvertError("DeleteReplicationTaskFromDLQ", err)
}

func (d *MutableStateTaskStore) RangeDeleteReplicationTaskFromDLQ(
	request *p.RangeDeleteReplicationTaskFromDLQRequest,
) error {

	query := d.Session.Query(templateRangeCompleteReplicationTaskQuery,
		request.ShardID,
		rowTypeDLQ,
		rowTypeDLQNamespaceID,
		request.SourceClusterName,
		rowTypeDLQRunID,
		defaultVisibilityTimestamp,
		request.ExclusiveBeginTaskID,
		request.InclusiveEndTaskID,
	)

	err := query.Exec()
	return gocql.ConvertError("RangeDeleteReplicationTaskFromDLQ", err)
}

func (d *MutableStateTaskStore) GetVisibilityTask(
	request *p.GetVisibilityTaskRequest,
) (*p.InternalGetVisibilityTaskResponse, error) {
	shardID := request.ShardID
	taskID := request.TaskID
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
	return &p.InternalGetVisibilityTaskResponse{Task: *p.NewDataBlob(data, encoding)}, nil
}

func (d *MutableStateTaskStore) GetVisibilityTasks(
	request *p.GetVisibilityTasksRequest,
) (*p.InternalGetVisibilityTasksResponse, error) {

	// Reading Visibility tasks need to be quorum level consistent, otherwise we could lose task
	query := d.Session.Query(templateGetVisibilityTasksQuery,
		request.ShardID,
		rowTypeVisibilityTask,
		rowTypeVisibilityTaskNamespaceID,
		rowTypeVisibilityTaskWorkflowID,
		rowTypeVisibilityTaskRunID,
		defaultVisibilityTimestamp,
		request.ReadLevel,
		request.MaxReadLevel,
	)
	iter := query.PageSize(request.BatchSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetVisibilityTasksResponse{}
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

func (d *MutableStateTaskStore) CompleteVisibilityTask(
	request *p.CompleteVisibilityTaskRequest,
) error {
	query := d.Session.Query(templateCompleteVisibilityTaskQuery,
		request.ShardID,
		rowTypeVisibilityTask,
		rowTypeVisibilityTaskNamespaceID,
		rowTypeVisibilityTaskWorkflowID,
		rowTypeVisibilityTaskRunID,
		defaultVisibilityTimestamp,
		request.TaskID)

	err := query.Exec()
	return gocql.ConvertError("CompleteVisibilityTask", err)
}

func (d *MutableStateTaskStore) RangeCompleteVisibilityTask(
	request *p.RangeCompleteVisibilityTaskRequest,
) error {
	query := d.Session.Query(templateRangeCompleteVisibilityTaskQuery,
		request.ShardID,
		rowTypeVisibilityTask,
		rowTypeVisibilityTaskNamespaceID,
		rowTypeVisibilityTaskWorkflowID,
		rowTypeVisibilityTaskRunID,
		defaultVisibilityTimestamp,
		request.ExclusiveBeginTaskID,
		request.InclusiveEndTaskID,
	)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteVisibilityTask", err)
}

func (d *MutableStateTaskStore) GetTieredStorageTask(
	request *p.GetTieredStorageTaskRequest,
) (*p.InternalGetTieredStorageTaskResponse, error) {
	shardID := request.ShardID
	taskID := request.TaskID
	query := d.Session.Query(templateGetTieredStorageTaskQuery,
		shardID,
		rowTypeTieredStorageTask,
		rowTypeTieredStorageTaskNamespaceID,
		rowTypeTieredStorageTaskWorkflowID,
		rowTypeTieredStorageTaskRunID,
		defaultTieredStorageTimestamp,
		taskID)

	var data []byte
	var encoding string
	if err := query.Scan(&data, &encoding); err != nil {
		return nil, gocql.ConvertError("GetTieredStorageTask", err)
	}
	return &p.InternalGetTieredStorageTaskResponse{Task: *p.NewDataBlob(data, encoding)}, nil
}

func (d *MutableStateTaskStore) GetTieredStorageTasks(
	request *p.GetTieredStorageTasksRequest,
) (*p.InternalGetTieredStorageTasksResponse, error) {

	// Reading TieredStorage tasks need to be quorum level consistent, otherwise we could lose task
	query := d.Session.Query(templateGetTieredStorageTasksQuery,
		request.ShardID,
		rowTypeTieredStorageTask,
		rowTypeTieredStorageTaskNamespaceID,
		rowTypeTieredStorageTaskWorkflowID,
		rowTypeTieredStorageTaskRunID,
		defaultTieredStorageTimestamp,
		request.MinTaskID,
		request.MaxTaskID,
	)
	iter := query.PageSize(request.BatchSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetTieredStorageTasksResponse{}
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
		return nil, gocql.ConvertError("GetTieredStorageTasks", err)
	}

	return response, nil
}

func (d *MutableStateTaskStore) CompleteTieredStorageTask(
	request *p.CompleteTieredStorageTaskRequest,
) error {
	query := d.Session.Query(templateCompleteTieredStorageTaskQuery,
		request.ShardID,
		rowTypeTieredStorageTask,
		rowTypeTieredStorageTaskNamespaceID,
		rowTypeTieredStorageTaskWorkflowID,
		rowTypeTieredStorageTaskRunID,
		defaultTieredStorageTimestamp,
		request.TaskID)

	err := query.Exec()
	return gocql.ConvertError("CompleteTieredStorageTask", err)
}

func (d *MutableStateTaskStore) RangeCompleteTieredStorageTask(
	request *p.RangeCompleteTieredStorageTaskRequest,
) error {
	query := d.Session.Query(templateRangeCompleteTieredStorageTaskQuery,
		request.ShardID,
		rowTypeTieredStorageTask,
		rowTypeTieredStorageTaskNamespaceID,
		rowTypeTieredStorageTaskWorkflowID,
		rowTypeTieredStorageTaskRunID,
		defaultTieredStorageTimestamp,
		request.ExclusiveBeginTaskID,
		request.InclusiveEndTaskID,
	)

	err := query.Exec()
	return gocql.ConvertError("RangeCompleteTieredStorageTask", err)
}

func (d *MutableStateTaskStore) populateGetReplicationTasksResponse(
	query gocql.Query,
	operation string,
) (*p.InternalGetReplicationTasksResponse, error) {
	iter := query.Iter()

	response := &p.InternalGetReplicationTasksResponse{}
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
