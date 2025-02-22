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

package tidb

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	executionsColumns = `shard_id, namespace_id, workflow_id, run_id, next_event_id, last_write_version, data, data_encoding, state, state_encoding, db_record_version`

	createExecutionQuery = `INSERT INTO executions(` + executionsColumns + `)
 VALUES(:shard_id, :namespace_id, :workflow_id, :run_id, :next_event_id, :last_write_version, :data, :data_encoding, :state, :state_encoding, :db_record_version)`

	updateExecutionQuery = `UPDATE executions SET
 db_record_version = :db_record_version, next_event_id = :next_event_id, last_write_version = :last_write_version, data = :data, data_encoding = :data_encoding, state = :state, state_encoding = :state_encoding
 WHERE shard_id = :shard_id AND namespace_id = :namespace_id AND workflow_id = :workflow_id AND run_id = :run_id`

	getExecutionQuery = `SELECT ` + executionsColumns + ` FROM executions
 WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ?`

	deleteExecutionQuery = `DELETE FROM executions 
 WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ?`

	lockExecutionQueryBase = `SELECT db_record_version, next_event_id FROM executions 
 WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ?`

	writeLockExecutionQuery = lockExecutionQueryBase + ` FOR UPDATE`

	createCurrentExecutionQuery = `INSERT INTO current_executions
(shard_id, namespace_id, workflow_id, run_id, create_request_id, state, status, last_write_version) VALUES
(:shard_id, :namespace_id, :workflow_id, :run_id, :create_request_id, :state, :status, :last_write_version)`

	deleteCurrentExecutionQuery = "DELETE FROM current_executions WHERE shard_id=? AND namespace_id=? AND workflow_id=? AND run_id=?"

	getCurrentExecutionQuery = `SELECT
shard_id, namespace_id, workflow_id, run_id, create_request_id, state, status, last_write_version
FROM current_executions WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ?`

	lockCurrentExecutionJoinExecutionsQuery = `SELECT
ce.shard_id, ce.namespace_id, ce.workflow_id, ce.run_id, ce.create_request_id, ce.state, ce.status, e.last_write_version
FROM current_executions ce
INNER JOIN executions e ON e.shard_id = ce.shard_id AND e.namespace_id = ce.namespace_id AND e.workflow_id = ce.workflow_id AND e.run_id = ce.run_id
WHERE ce.shard_id = ? AND ce.namespace_id = ? AND ce.workflow_id = ? FOR UPDATE`

	lockCurrentExecutionQuery = getCurrentExecutionQuery + ` FOR UPDATE`

	updateCurrentExecutionsQuery = `UPDATE current_executions SET
run_id = :run_id,
create_request_id = :create_request_id,
state = :state,
status = :status,
last_write_version = :last_write_version
WHERE
shard_id = :shard_id AND
namespace_id = :namespace_id AND
workflow_id = :workflow_id
`

	createHistoryImmediateTasksQuery = `INSERT INTO history_immediate_tasks(shard_id, category_id, task_id, data, data_encoding) 
 VALUES(:shard_id, :category_id, :task_id, :data, :data_encoding)`

	getHistoryImmediateTasksQuery = `SELECT task_id, data, data_encoding 
 FROM history_immediate_tasks WHERE shard_id = ? AND category_id = ? AND task_id >= ? AND task_id < ? ORDER BY task_id LIMIT ?`

	deleteHistoryImmediateTaskQuery       = `DELETE FROM history_immediate_tasks WHERE shard_id = ? AND category_id = ? AND task_id = ?`
	rangeDeleteHistoryImmediateTasksQuery = `DELETE FROM history_immediate_tasks WHERE shard_id = ? AND category_id = ? AND task_id >= ? AND task_id < ?`

	createHistoryScheduledTasksQuery = `INSERT INTO history_scheduled_tasks (shard_id, category_id, visibility_timestamp, task_id, data, data_encoding)
  VALUES (:shard_id, :category_id, :visibility_timestamp, :task_id, :data, :data_encoding)`

	getHistoryScheduledTasksQuery = `SELECT visibility_timestamp, task_id, data, data_encoding FROM history_scheduled_tasks 
  WHERE shard_id = ? 
  AND category_id = ? 
  AND ((visibility_timestamp >= ? AND task_id >= ?) OR visibility_timestamp > ?) 
  AND visibility_timestamp < ?
  ORDER BY visibility_timestamp,task_id LIMIT ?`

	deleteHistoryScheduledTaskQuery       = `DELETE FROM history_scheduled_tasks WHERE shard_id = ? AND category_id = ? AND visibility_timestamp = ? AND task_id = ?`
	rangeDeleteHistoryScheduledTasksQuery = `DELETE FROM history_scheduled_tasks WHERE shard_id = ? AND category_id = ? AND visibility_timestamp >= ? AND visibility_timestamp < ?`

	createTransferTasksQuery = `INSERT INTO transfer_tasks(shard_id, task_id, data, data_encoding) 
 VALUES(:shard_id, :task_id, :data, :data_encoding)`

	getTransferTasksQuery = `SELECT task_id, data, data_encoding 
 FROM transfer_tasks WHERE shard_id = ? AND task_id >= ? AND task_id < ? ORDER BY task_id LIMIT ?`

	deleteTransferTaskQuery      = `DELETE FROM transfer_tasks WHERE shard_id = ? AND task_id = ?`
	rangeDeleteTransferTaskQuery = `DELETE FROM transfer_tasks WHERE shard_id = ? AND task_id >= ? AND task_id < ?`

	createTimerTasksQuery = `INSERT INTO timer_tasks (shard_id, visibility_timestamp, task_id, data, data_encoding)
  VALUES (:shard_id, :visibility_timestamp, :task_id, :data, :data_encoding)`

	getTimerTasksQuery = `SELECT visibility_timestamp, task_id, data, data_encoding FROM timer_tasks 
  WHERE shard_id = ? 
  AND ((visibility_timestamp >= ? AND task_id >= ?) OR visibility_timestamp > ?) 
  AND visibility_timestamp < ?
  ORDER BY visibility_timestamp,task_id LIMIT ?`

	deleteTimerTaskQuery      = `DELETE FROM timer_tasks WHERE shard_id = ? AND visibility_timestamp = ? AND task_id = ?`
	rangeDeleteTimerTaskQuery = `DELETE FROM timer_tasks WHERE shard_id = ? AND visibility_timestamp >= ? AND visibility_timestamp < ?`

	createReplicationTasksQuery = `INSERT INTO replication_tasks (shard_id, task_id, data, data_encoding) 
  VALUES(:shard_id, :task_id, :data, :data_encoding)`

	getReplicationTasksQuery = `SELECT task_id, data, data_encoding FROM replication_tasks WHERE 
shard_id = ? AND task_id >= ? AND task_id < ? ORDER BY task_id LIMIT ?`

	deleteReplicationTaskQuery      = `DELETE FROM replication_tasks WHERE shard_id = ? AND task_id = ?`
	rangeDeleteReplicationTaskQuery = `DELETE FROM replication_tasks WHERE shard_id = ? AND task_id >= ? AND task_id < ?`

	getReplicationTasksDLQQuery = `SELECT task_id, data, data_encoding FROM replication_tasks_dlq WHERE 
source_cluster_name = ? AND
shard_id = ? AND
task_id >= ? AND
task_id < ?
ORDER BY task_id LIMIT ?`

	createVisibilityTasksQuery = `INSERT INTO visibility_tasks(shard_id, task_id, data, data_encoding) 
 VALUES(:shard_id, :task_id, :data, :data_encoding)`

	getVisibilityTasksQuery = `SELECT task_id, data, data_encoding 
 FROM visibility_tasks WHERE shard_id = ? AND task_id >= ? AND task_id < ? ORDER BY task_id LIMIT ?`

	deleteVisibilityTaskQuery      = `DELETE FROM visibility_tasks WHERE shard_id = ? AND task_id = ?`
	rangeDeleteVisibilityTaskQuery = `DELETE FROM visibility_tasks WHERE shard_id = ? AND task_id >= ? AND task_id < ?`

	bufferedEventsColumns     = `shard_id, namespace_id, workflow_id, run_id, data, data_encoding`
	createBufferedEventsQuery = `INSERT INTO buffered_events(` + bufferedEventsColumns + `)
VALUES (:shard_id, :namespace_id, :workflow_id, :run_id, :data, :data_encoding)`

	deleteBufferedEventsQuery = `DELETE FROM buffered_events WHERE shard_id=? AND namespace_id=? AND workflow_id=? AND run_id=?`
	getBufferedEventsQuery    = `SELECT data, data_encoding FROM buffered_events WHERE
shard_id=? AND namespace_id=? AND workflow_id=? AND run_id=?`

	insertReplicationTaskDLQQuery = `
INSERT INTO replication_tasks_dlq 
            (source_cluster_name, 
             shard_id, 
             task_id, 
             data, 
             data_encoding) 
VALUES     (:source_cluster_name, 
            :shard_id, 
            :task_id, 
            :data, 
            :data_encoding)
`
	deleteReplicationTaskFromDLQQuery = `
	DELETE FROM replication_tasks_dlq 
		WHERE source_cluster_name = ? 
		AND shard_id = ? 
		AND task_id = ?`

	rangeDeleteReplicationTaskFromDLQQuery = `
	DELETE FROM replication_tasks_dlq 
		WHERE source_cluster_name = ? 
		AND shard_id = ? 
		AND task_id >= ?
		AND task_id < ?`
)

// InsertIntoExecutions inserts a row into executions table
func (tidb *db) InsertIntoExecutions(
	ctx context.Context,
	row *sqlplugin.ExecutionsRow,
) (sql.Result, error) {
	return tidb.conn.NamedExecContext(ctx,
		createExecutionQuery,
		row,
	)
}

// UpdateExecutions updates a single row in executions table
func (tidb *db) UpdateExecutions(
	ctx context.Context,
	row *sqlplugin.ExecutionsRow,
) (sql.Result, error) {
	return tidb.conn.NamedExecContext(ctx,
		updateExecutionQuery,
		row,
	)
}

// SelectFromExecutions reads a single row from executions table
func (tidb *db) SelectFromExecutions(
	ctx context.Context,
	filter sqlplugin.ExecutionsFilter,
) (*sqlplugin.ExecutionsRow, error) {
	var row sqlplugin.ExecutionsRow
	err := tidb.conn.GetContext(ctx,
		&row, getExecutionQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
	if err != nil {
		return nil, err
	}
	return &row, err
}

// DeleteFromExecutions deletes a single row from executions table
func (tidb *db) DeleteFromExecutions(
	ctx context.Context,
	filter sqlplugin.ExecutionsFilter,
) (sql.Result, error) {
	return tidb.conn.ExecContext(ctx,
		deleteExecutionQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
}

// ReadLockExecutions acquires a write lock on a single row in executions table
// TiDB doesn't support the read lock now, so upgrade the lock to write lock
func (tidb *db) ReadLockExecutions(
	ctx context.Context,
	filter sqlplugin.ExecutionsFilter,
) (int64, int64, error) {
	return tidb.WriteLockExecutions(ctx, filter)
}

// WriteLockExecutions acquires a write lock on a single row in executions table
func (tidb *db) WriteLockExecutions(
	ctx context.Context,
	filter sqlplugin.ExecutionsFilter,
) (int64, int64, error) {
	var executionVersion sqlplugin.ExecutionVersion
	err := tidb.conn.GetContext(ctx,
		&executionVersion,
		writeLockExecutionQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
	return executionVersion.DBRecordVersion, executionVersion.NextEventID, err
}

// InsertIntoCurrentExecutions inserts a single row into current_executions table
func (tidb *db) InsertIntoCurrentExecutions(
	ctx context.Context,
	row *sqlplugin.CurrentExecutionsRow,
) (sql.Result, error) {
	return tidb.conn.NamedExecContext(ctx,
		createCurrentExecutionQuery,
		row,
	)
}

// UpdateCurrentExecutions updates a single row in current_executions table
func (tidb *db) UpdateCurrentExecutions(
	ctx context.Context,
	row *sqlplugin.CurrentExecutionsRow,
) (sql.Result, error) {
	return tidb.conn.NamedExecContext(ctx,
		updateCurrentExecutionsQuery,
		row,
	)
}

// SelectFromCurrentExecutions reads one or more rows from current_executions table
func (tidb *db) SelectFromCurrentExecutions(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) (*sqlplugin.CurrentExecutionsRow, error) {
	var row sqlplugin.CurrentExecutionsRow
	err := tidb.conn.GetContext(ctx,
		&row,
		getCurrentExecutionQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
	)
	return &row, err
}

// DeleteFromCurrentExecutions deletes a single row in current_executions table
func (tidb *db) DeleteFromCurrentExecutions(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) (sql.Result, error) {
	return tidb.conn.ExecContext(ctx,
		deleteCurrentExecutionQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
}

// LockCurrentExecutions acquires a write lock on a single row in current_executions table
func (tidb *db) LockCurrentExecutions(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) (*sqlplugin.CurrentExecutionsRow, error) {
	var row sqlplugin.CurrentExecutionsRow
	err := tidb.conn.GetContext(ctx,
		&row,
		lockCurrentExecutionQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
	)
	return &row, err
}

// LockCurrentExecutionsJoinExecutions joins a row in current_executions with executions table and acquires a
// write lock on the result
func (tidb *db) LockCurrentExecutionsJoinExecutions(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) ([]sqlplugin.CurrentExecutionsRow, error) {
	var rows []sqlplugin.CurrentExecutionsRow
	err := tidb.conn.SelectContext(ctx,
		&rows,
		lockCurrentExecutionJoinExecutionsQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
	)
	return rows, err
}

// InsertIntoHistoryImmediateTasks inserts one or more rows into history_immediate_tasks table
func (tidb *db) InsertIntoHistoryImmediateTasks(
	ctx context.Context,
	rows []sqlplugin.HistoryImmediateTasksRow,
) (sql.Result, error) {
	return tidb.conn.NamedExecContext(ctx,
		createHistoryImmediateTasksQuery,
		rows,
	)
}

// RangeSelectFromHistoryImmediateTasks reads one or more rows from transfer_tasks table
func (tidb *db) RangeSelectFromHistoryImmediateTasks(
	ctx context.Context,
	filter sqlplugin.HistoryImmediateTasksRangeFilter,
) ([]sqlplugin.HistoryImmediateTasksRow, error) {
	var rows []sqlplugin.HistoryImmediateTasksRow
	if err := tidb.conn.SelectContext(ctx,
		&rows,
		getHistoryImmediateTasksQuery,
		filter.ShardID,
		filter.CategoryID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
		filter.PageSize,
	); err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteFromHistoryImmediateTasks deletes one or more rows from transfer_tasks table
func (tidb *db) DeleteFromHistoryImmediateTasks(
	ctx context.Context,
	filter sqlplugin.HistoryImmediateTasksFilter,
) (sql.Result, error) {
	return tidb.conn.ExecContext(ctx,
		deleteHistoryImmediateTaskQuery,
		filter.ShardID,
		filter.CategoryID,
		filter.TaskID,
	)
}

// RangeDeleteFromHistoryImmediateTasks deletes one or more rows from transfer_tasks table
func (tidb *db) RangeDeleteFromHistoryImmediateTasks(
	ctx context.Context,
	filter sqlplugin.HistoryImmediateTasksRangeFilter,
) (sql.Result, error) {
	return tidb.conn.ExecContext(ctx,
		rangeDeleteHistoryImmediateTasksQuery,
		filter.ShardID,
		filter.CategoryID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
	)
}

// InsertIntoHistoryScheduledTasks inserts one or more rows into timer_tasks table
func (tidb *db) InsertIntoHistoryScheduledTasks(
	ctx context.Context,
	rows []sqlplugin.HistoryScheduledTasksRow,
) (sql.Result, error) {
	for i := range rows {
		rows[i].VisibilityTimestamp = tidb.converter.ToMySQLDateTime(rows[i].VisibilityTimestamp)
	}
	return tidb.conn.NamedExecContext(
		ctx,
		createHistoryScheduledTasksQuery,
		rows,
	)
}

// RangeSelectFromHistoryScheduledTasks reads one or more rows from timer_tasks table
func (tidb *db) RangeSelectFromHistoryScheduledTasks(
	ctx context.Context,
	filter sqlplugin.HistoryScheduledTasksRangeFilter,
) ([]sqlplugin.HistoryScheduledTasksRow, error) {
	var rows []sqlplugin.HistoryScheduledTasksRow
	filter.InclusiveMinVisibilityTimestamp = tidb.converter.ToMySQLDateTime(filter.InclusiveMinVisibilityTimestamp)
	filter.ExclusiveMaxVisibilityTimestamp = tidb.converter.ToMySQLDateTime(filter.ExclusiveMaxVisibilityTimestamp)
	if err := tidb.conn.SelectContext(ctx,
		&rows,
		getHistoryScheduledTasksQuery,
		filter.ShardID,
		filter.CategoryID,
		filter.InclusiveMinVisibilityTimestamp,
		filter.InclusiveMinTaskID,
		filter.InclusiveMinVisibilityTimestamp,
		filter.ExclusiveMaxVisibilityTimestamp,
		filter.PageSize,
	); err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].VisibilityTimestamp = tidb.converter.FromMySQLDateTime(rows[i].VisibilityTimestamp)
	}
	return rows, nil
}

// DeleteFromHistoryScheduledTasks deletes one or more rows from timer_tasks table
func (tidb *db) DeleteFromHistoryScheduledTasks(
	ctx context.Context,
	filter sqlplugin.HistoryScheduledTasksFilter,
) (sql.Result, error) {
	filter.VisibilityTimestamp = tidb.converter.ToMySQLDateTime(filter.VisibilityTimestamp)
	return tidb.conn.ExecContext(ctx,
		deleteHistoryScheduledTaskQuery,
		filter.ShardID,
		filter.CategoryID,
		filter.VisibilityTimestamp,
		filter.TaskID,
	)
}

// RangeDeleteFromHistoryScheduledTasks deletes one or more rows from timer_tasks table
func (tidb *db) RangeDeleteFromHistoryScheduledTasks(
	ctx context.Context,
	filter sqlplugin.HistoryScheduledTasksRangeFilter,
) (sql.Result, error) {
	filter.InclusiveMinVisibilityTimestamp = tidb.converter.ToMySQLDateTime(filter.InclusiveMinVisibilityTimestamp)
	filter.ExclusiveMaxVisibilityTimestamp = tidb.converter.ToMySQLDateTime(filter.ExclusiveMaxVisibilityTimestamp)
	return tidb.conn.ExecContext(ctx,
		rangeDeleteHistoryScheduledTasksQuery,
		filter.ShardID,
		filter.CategoryID,
		filter.InclusiveMinVisibilityTimestamp,
		filter.ExclusiveMaxVisibilityTimestamp,
	)
}

// InsertIntoTransferTasks inserts one or more rows into transfer_tasks table
func (tidb *db) InsertIntoTransferTasks(
	ctx context.Context,
	rows []sqlplugin.TransferTasksRow,
) (sql.Result, error) {
	return tidb.conn.NamedExecContext(ctx,
		createTransferTasksQuery,
		rows,
	)
}

// RangeSelectFromTransferTasks reads one or more rows from transfer_tasks table
func (tidb *db) RangeSelectFromTransferTasks(
	ctx context.Context,
	filter sqlplugin.TransferTasksRangeFilter,
) ([]sqlplugin.TransferTasksRow, error) {
	var rows []sqlplugin.TransferTasksRow
	if err := tidb.conn.SelectContext(ctx,
		&rows,
		getTransferTasksQuery,
		filter.ShardID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
		filter.PageSize,
	); err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteFromTransferTasks deletes one or more rows from transfer_tasks table
func (tidb *db) DeleteFromTransferTasks(
	ctx context.Context,
	filter sqlplugin.TransferTasksFilter,
) (sql.Result, error) {
	return tidb.conn.ExecContext(ctx,
		deleteTransferTaskQuery,
		filter.ShardID,
		filter.TaskID,
	)
}

// RangeDeleteFromTransferTasks deletes one or more rows from transfer_tasks table
func (tidb *db) RangeDeleteFromTransferTasks(
	ctx context.Context,
	filter sqlplugin.TransferTasksRangeFilter,
) (sql.Result, error) {
	return tidb.conn.ExecContext(ctx,
		rangeDeleteTransferTaskQuery,
		filter.ShardID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
	)
}

// InsertIntoTimerTasks inserts one or more rows into timer_tasks table
func (tidb *db) InsertIntoTimerTasks(
	ctx context.Context,
	rows []sqlplugin.TimerTasksRow,
) (sql.Result, error) {
	for i := range rows {
		rows[i].VisibilityTimestamp = tidb.converter.ToMySQLDateTime(rows[i].VisibilityTimestamp)
	}
	return tidb.conn.NamedExecContext(
		ctx,
		createTimerTasksQuery,
		rows,
	)
}

// RangeSelectFromTimerTasks reads one or more rows from timer_tasks table
func (tidb *db) RangeSelectFromTimerTasks(
	ctx context.Context,
	filter sqlplugin.TimerTasksRangeFilter,
) ([]sqlplugin.TimerTasksRow, error) {
	var rows []sqlplugin.TimerTasksRow
	filter.InclusiveMinVisibilityTimestamp = tidb.converter.ToMySQLDateTime(filter.InclusiveMinVisibilityTimestamp)
	filter.ExclusiveMaxVisibilityTimestamp = tidb.converter.ToMySQLDateTime(filter.ExclusiveMaxVisibilityTimestamp)
	if err := tidb.conn.SelectContext(ctx,
		&rows,
		getTimerTasksQuery,
		filter.ShardID,
		filter.InclusiveMinVisibilityTimestamp,
		filter.InclusiveMinTaskID,
		filter.InclusiveMinVisibilityTimestamp,
		filter.ExclusiveMaxVisibilityTimestamp,
		filter.PageSize,
	); err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].VisibilityTimestamp = tidb.converter.FromMySQLDateTime(rows[i].VisibilityTimestamp)
	}
	return rows, nil
}

// DeleteFromTimerTasks deletes one or more rows from timer_tasks table
func (tidb *db) DeleteFromTimerTasks(
	ctx context.Context,
	filter sqlplugin.TimerTasksFilter,
) (sql.Result, error) {
	filter.VisibilityTimestamp = tidb.converter.ToMySQLDateTime(filter.VisibilityTimestamp)
	return tidb.conn.ExecContext(ctx,
		deleteTimerTaskQuery,
		filter.ShardID,
		filter.VisibilityTimestamp,
		filter.TaskID,
	)
}

// RangeDeleteFromTimerTasks deletes one or more rows from timer_tasks table
func (tidb *db) RangeDeleteFromTimerTasks(
	ctx context.Context,
	filter sqlplugin.TimerTasksRangeFilter,
) (sql.Result, error) {
	filter.InclusiveMinVisibilityTimestamp = tidb.converter.ToMySQLDateTime(filter.InclusiveMinVisibilityTimestamp)
	filter.ExclusiveMaxVisibilityTimestamp = tidb.converter.ToMySQLDateTime(filter.ExclusiveMaxVisibilityTimestamp)
	return tidb.conn.ExecContext(ctx,
		rangeDeleteTimerTaskQuery,
		filter.ShardID,
		filter.InclusiveMinVisibilityTimestamp,
		filter.ExclusiveMaxVisibilityTimestamp,
	)
}

// InsertIntoBufferedEvents inserts one or more rows into buffered_events table
func (tidb *db) InsertIntoBufferedEvents(
	ctx context.Context,
	rows []sqlplugin.BufferedEventsRow,
) (sql.Result, error) {
	return tidb.conn.NamedExecContext(ctx,
		createBufferedEventsQuery,
		rows,
	)
}

// SelectFromBufferedEvents reads one or more rows from buffered_events table
func (tidb *db) SelectFromBufferedEvents(
	ctx context.Context,
	filter sqlplugin.BufferedEventsFilter,
) ([]sqlplugin.BufferedEventsRow, error) {
	var rows []sqlplugin.BufferedEventsRow
	if err := tidb.conn.SelectContext(ctx,
		&rows,
		getBufferedEventsQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	); err != nil {
		return nil, err
	}
	for i := 0; i < len(rows); i++ {
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
		rows[i].ShardID = filter.ShardID
	}
	return rows, nil
}

// DeleteFromBufferedEvents deletes one or more rows from buffered_events table
func (tidb *db) DeleteFromBufferedEvents(
	ctx context.Context,
	filter sqlplugin.BufferedEventsFilter,
) (sql.Result, error) {
	return tidb.conn.ExecContext(ctx,
		deleteBufferedEventsQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
}

// InsertIntoReplicationTasks inserts one or more rows into replication_tasks table
func (tidb *db) InsertIntoReplicationTasks(
	ctx context.Context,
	rows []sqlplugin.ReplicationTasksRow,
) (sql.Result, error) {
	return tidb.conn.NamedExecContext(ctx,
		createReplicationTasksQuery,
		rows,
	)
}

// RangeSelectFromReplicationTasks reads one or more rows from replication_tasks table
func (tidb *db) RangeSelectFromReplicationTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationTasksRangeFilter,
) ([]sqlplugin.ReplicationTasksRow, error) {
	var rows []sqlplugin.ReplicationTasksRow
	err := tidb.conn.SelectContext(ctx,
		&rows,
		getReplicationTasksQuery,
		filter.ShardID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
		filter.PageSize,
	)
	return rows, err
}

// DeleteFromReplicationTasks deletes one row from replication_tasks table
func (tidb *db) DeleteFromReplicationTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationTasksFilter,
) (sql.Result, error) {
	return tidb.conn.ExecContext(ctx,
		deleteReplicationTaskQuery,
		filter.ShardID,
		filter.TaskID,
	)
}

// RangeDeleteFromReplicationTasks deletes multi rows from replication_tasks table
func (tidb *db) RangeDeleteFromReplicationTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationTasksRangeFilter,
) (sql.Result, error) {
	return tidb.conn.ExecContext(ctx,
		rangeDeleteReplicationTaskQuery,
		filter.ShardID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
	)
}

// InsertIntoReplicationDLQTasks inserts one or more rows into replication_tasks_dlq table
func (tidb *db) InsertIntoReplicationDLQTasks(
	ctx context.Context,
	rows []sqlplugin.ReplicationDLQTasksRow,
) (sql.Result, error) {
	return tidb.conn.NamedExecContext(ctx,
		insertReplicationTaskDLQQuery,
		rows,
	)
}

// RangeSelectFromReplicationDLQTasks reads one or more rows from replication_tasks_dlq table
func (tidb *db) RangeSelectFromReplicationDLQTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationDLQTasksRangeFilter,
) ([]sqlplugin.ReplicationDLQTasksRow, error) {
	var rows []sqlplugin.ReplicationDLQTasksRow
	err := tidb.conn.SelectContext(ctx,
		&rows, getReplicationTasksDLQQuery,
		filter.SourceClusterName,
		filter.ShardID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
		filter.PageSize,
	)
	return rows, err
}

// DeleteFromReplicationDLQTasks deletes one row from replication_tasks_dlq table
func (tidb *db) DeleteFromReplicationDLQTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationDLQTasksFilter,
) (sql.Result, error) {

	return tidb.conn.ExecContext(ctx,
		deleteReplicationTaskFromDLQQuery,
		filter.SourceClusterName,
		filter.ShardID,
		filter.TaskID,
	)
}

// RangeDeleteFromReplicationDLQTasks deletes one or more rows from replication_tasks_dlq table
func (tidb *db) RangeDeleteFromReplicationDLQTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationDLQTasksRangeFilter,
) (sql.Result, error) {

	return tidb.conn.ExecContext(ctx,
		rangeDeleteReplicationTaskFromDLQQuery,
		filter.SourceClusterName,
		filter.ShardID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
	)
}

// InsertIntoVisibilityTasks inserts one or more rows into visibility_tasks table
func (tidb *db) InsertIntoVisibilityTasks(
	ctx context.Context,
	rows []sqlplugin.VisibilityTasksRow,
) (sql.Result, error) {
	return tidb.conn.NamedExecContext(ctx,
		createVisibilityTasksQuery,
		rows,
	)
}

// RangeSelectFromVisibilityTasks reads one or more rows from visibility_tasks table
func (tidb *db) RangeSelectFromVisibilityTasks(
	ctx context.Context,
	filter sqlplugin.VisibilityTasksRangeFilter,
) ([]sqlplugin.VisibilityTasksRow, error) {
	var rows []sqlplugin.VisibilityTasksRow
	if err := tidb.conn.SelectContext(ctx,
		&rows,
		getVisibilityTasksQuery,
		filter.ShardID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
		filter.PageSize,
	); err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteFromVisibilityTasks deletes one or more rows from visibility_tasks table
func (tidb *db) DeleteFromVisibilityTasks(
	ctx context.Context,
	filter sqlplugin.VisibilityTasksFilter,
) (sql.Result, error) {
	return tidb.conn.ExecContext(ctx,
		deleteVisibilityTaskQuery,
		filter.ShardID,
		filter.TaskID,
	)
}

// RangeDeleteFromVisibilityTasks deletes one or more rows from visibility_tasks table
func (tidb *db) RangeDeleteFromVisibilityTasks(
	ctx context.Context,
	filter sqlplugin.VisibilityTasksRangeFilter,
) (sql.Result, error) {
	return tidb.conn.ExecContext(ctx,
		rangeDeleteVisibilityTaskQuery,
		filter.ShardID,
		filter.InclusiveMinTaskID,
		filter.ExclusiveMaxTaskID,
	)
}
