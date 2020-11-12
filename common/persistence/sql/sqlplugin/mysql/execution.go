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

package mysql

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	executionsColumns = `shard_id, namespace_id, workflow_id, run_id, next_event_id, last_write_version, data, data_encoding, state, state_encoding`

	createExecutionQuery = `INSERT INTO executions(` + executionsColumns + `)
 VALUES(:shard_id, :namespace_id, :workflow_id, :run_id, :next_event_id, :last_write_version, :data, :data_encoding, :state, :state_encoding)`

	updateExecutionQuery = `UPDATE executions SET
 next_event_id = :next_event_id, last_write_version = :last_write_version, data = :data, data_encoding = :data_encoding, state = :state, state_encoding = :state_encoding
 WHERE shard_id = :shard_id AND namespace_id = :namespace_id AND workflow_id = :workflow_id AND run_id = :run_id`

	getExecutionQuery = `SELECT ` + executionsColumns + ` FROM executions
 WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ?`

	deleteExecutionQuery = `DELETE FROM executions 
 WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ?`

	lockExecutionQueryBase = `SELECT next_event_id FROM executions 
 WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ?`

	writeLockExecutionQuery = lockExecutionQueryBase + ` FOR UPDATE`
	readLockExecutionQuery  = lockExecutionQueryBase + ` LOCK IN SHARE MODE`

	createCurrentExecutionQuery = `INSERT INTO current_executions
(shard_id, namespace_id, workflow_id, run_id, create_request_id, state, status, start_version, last_write_version) VALUES
(:shard_id, :namespace_id, :workflow_id, :run_id, :create_request_id, :state, :status, :start_version, :last_write_version)`

	deleteCurrentExecutionQuery = "DELETE FROM current_executions WHERE shard_id=? AND namespace_id=? AND workflow_id=? AND run_id=?"

	getCurrentExecutionQuery = `SELECT
shard_id, namespace_id, workflow_id, run_id, create_request_id, state, status, start_version, last_write_version
FROM current_executions WHERE shard_id = ? AND namespace_id = ? AND workflow_id = ?`

	lockCurrentExecutionJoinExecutionsQuery = `SELECT
ce.shard_id, ce.namespace_id, ce.workflow_id, ce.run_id, ce.create_request_id, ce.state, ce.status, ce.start_version, e.last_write_version
FROM current_executions ce
INNER JOIN executions e ON e.shard_id = ce.shard_id AND e.namespace_id = ce.namespace_id AND e.workflow_id = ce.workflow_id AND e.run_id = ce.run_id
WHERE ce.shard_id = ? AND ce.namespace_id = ? AND ce.workflow_id = ? FOR UPDATE`

	lockCurrentExecutionQuery = getCurrentExecutionQuery + ` FOR UPDATE`

	updateCurrentExecutionsQuery = `UPDATE current_executions SET
run_id = :run_id,
create_request_id = :create_request_id,
state = :state,
status = :status,
start_version = :start_version,
last_write_version = :last_write_version
WHERE
shard_id = :shard_id AND
namespace_id = :namespace_id AND
workflow_id = :workflow_id
`

	createTransferTasksQuery = `INSERT INTO transfer_tasks(shard_id, task_id, data, data_encoding) 
 VALUES(:shard_id, :task_id, :data, :data_encoding)`

	getTransferTaskQuery = `SELECT task_id, data, data_encoding 
 FROM transfer_tasks WHERE shard_id = ? AND task_id = ?`
	getTransferTasksQuery = `SELECT task_id, data, data_encoding 
 FROM transfer_tasks WHERE shard_id = ? AND task_id > ? AND task_id <= ? ORDER BY shard_id, task_id`

	deleteTransferTaskQuery      = `DELETE FROM transfer_tasks WHERE shard_id = ? AND task_id = ?`
	rangeDeleteTransferTaskQuery = `DELETE FROM transfer_tasks WHERE shard_id = ? AND task_id > ? AND task_id <= ?`

	createTimerTasksQuery = `INSERT INTO timer_tasks (shard_id, visibility_timestamp, task_id, data, data_encoding)
  VALUES (:shard_id, :visibility_timestamp, :task_id, :data, :data_encoding)`

	getTimerTaskQuery = `SELECT visibility_timestamp, task_id, data, data_encoding FROM timer_tasks 
  WHERE shard_id = ? AND visibility_timestamp = ? AND task_id = ?`
	getTimerTasksQuery = `SELECT visibility_timestamp, task_id, data, data_encoding FROM timer_tasks 
  WHERE shard_id = ? 
  AND ((visibility_timestamp >= ? AND task_id >= ?) OR visibility_timestamp > ?) 
  AND visibility_timestamp < ?
  ORDER BY visibility_timestamp,task_id LIMIT ?`

	deleteTimerTaskQuery      = `DELETE FROM timer_tasks WHERE shard_id = ? AND visibility_timestamp = ? AND task_id = ?`
	rangeDeleteTimerTaskQuery = `DELETE FROM timer_tasks WHERE shard_id = ? AND visibility_timestamp >= ? AND visibility_timestamp < ?`

	createReplicationTasksQuery = `INSERT INTO replication_tasks (shard_id, task_id, data, data_encoding) 
  VALUES(:shard_id, :task_id, :data, :data_encoding)`

	getReplicationTaskQuery = `SELECT task_id, data, data_encoding FROM replication_tasks WHERE 
shard_id = ? AND task_id = ?`
	getReplicationTasksQuery = `SELECT task_id, data, data_encoding FROM replication_tasks WHERE 
shard_id = ? AND task_id > ? AND task_id <= ? ORDER BY task_id LIMIT ?`

	deleteReplicationTaskQuery      = `DELETE FROM replication_tasks WHERE shard_id = ? AND task_id = ?`
	rangeDeleteReplicationTaskQuery = `DELETE FROM replication_tasks WHERE shard_id = ? AND task_id > ? AND task_id <= ?`

	getReplicationTaskDLQQuery = `SELECT task_id, data, data_encoding FROM replication_tasks_dlq WHERE 
source_cluster_name = ? AND
shard_id = ? AND
task_id = ?`
	getReplicationTasksDLQQuery = `SELECT task_id, data, data_encoding FROM replication_tasks_dlq WHERE 
source_cluster_name = ? AND
shard_id = ? AND
task_id > ? AND
task_id <= ?
ORDER BY task_id LIMIT ?`

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
		AND task_id > ?
		AND task_id <= ?`
)

// InsertIntoExecutions inserts a row into executions table
func (mdb *db) InsertIntoExecutions(
	ctx context.Context,
	row *sqlplugin.ExecutionsRow,
) (sql.Result, error) {
	return mdb.conn.NamedExecContext(ctx,
		createExecutionQuery,
		row,
	)
}

// UpdateExecutions updates a single row in executions table
func (mdb *db) UpdateExecutions(
	ctx context.Context,
	row *sqlplugin.ExecutionsRow,
) (sql.Result, error) {
	return mdb.conn.NamedExecContext(ctx,
		updateExecutionQuery,
		row,
	)
}

// SelectFromExecutions reads a single row from executions table
func (mdb *db) SelectFromExecutions(
	ctx context.Context,
	filter sqlplugin.ExecutionsFilter,
) (*sqlplugin.ExecutionsRow, error) {
	var row sqlplugin.ExecutionsRow
	err := mdb.conn.GetContext(ctx,
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
func (mdb *db) DeleteFromExecutions(
	ctx context.Context,
	filter sqlplugin.ExecutionsFilter,
) (sql.Result, error) {
	return mdb.conn.ExecContext(ctx,
		deleteExecutionQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
}

// ReadLockExecutions acquires a write lock on a single row in executions table
func (mdb *db) ReadLockExecutions(
	ctx context.Context,
	filter sqlplugin.ExecutionsFilter,
) (int64, error) {
	var nextEventID int64
	err := mdb.conn.GetContext(ctx,
		&nextEventID,
		readLockExecutionQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
	return nextEventID, err
}

// WriteLockExecutions acquires a write lock on a single row in executions table
func (mdb *db) WriteLockExecutions(
	ctx context.Context,
	filter sqlplugin.ExecutionsFilter,
) (int64, error) {
	var nextEventID int64
	err := mdb.conn.GetContext(ctx,
		&nextEventID,
		writeLockExecutionQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
	return nextEventID, err
}

// InsertIntoCurrentExecutions inserts a single row into current_executions table
func (mdb *db) InsertIntoCurrentExecutions(
	ctx context.Context,
	row *sqlplugin.CurrentExecutionsRow,
) (sql.Result, error) {
	return mdb.conn.NamedExecContext(ctx,
		createCurrentExecutionQuery,
		row,
	)
}

// UpdateCurrentExecutions updates a single row in current_executions table
func (mdb *db) UpdateCurrentExecutions(
	ctx context.Context,
	row *sqlplugin.CurrentExecutionsRow,
) (sql.Result, error) {
	return mdb.conn.NamedExecContext(ctx,
		updateCurrentExecutionsQuery,
		row,
	)
}

// SelectFromCurrentExecutions reads one or more rows from current_executions table
func (mdb *db) SelectFromCurrentExecutions(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) (*sqlplugin.CurrentExecutionsRow, error) {
	var row sqlplugin.CurrentExecutionsRow
	err := mdb.conn.GetContext(ctx,
		&row,
		getCurrentExecutionQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
	)
	return &row, err
}

// DeleteFromCurrentExecutions deletes a single row in current_executions table
func (mdb *db) DeleteFromCurrentExecutions(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) (sql.Result, error) {
	return mdb.conn.ExecContext(ctx,
		deleteCurrentExecutionQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
}

// LockCurrentExecutions acquires a write lock on a single row in current_executions table
func (mdb *db) LockCurrentExecutions(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) (*sqlplugin.CurrentExecutionsRow, error) {
	var row sqlplugin.CurrentExecutionsRow
	err := mdb.conn.GetContext(ctx,
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
func (mdb *db) LockCurrentExecutionsJoinExecutions(
	ctx context.Context,
	filter sqlplugin.CurrentExecutionsFilter,
) ([]sqlplugin.CurrentExecutionsRow, error) {
	var rows []sqlplugin.CurrentExecutionsRow
	err := mdb.conn.SelectContext(ctx,
		&rows,
		lockCurrentExecutionJoinExecutionsQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
	)
	return rows, err
}

// InsertIntoTransferTasks inserts one or more rows into transfer_tasks table
func (mdb *db) InsertIntoTransferTasks(
	ctx context.Context,
	rows []sqlplugin.TransferTasksRow,
) (sql.Result, error) {
	return mdb.conn.NamedExecContext(ctx,
		createTransferTasksQuery,
		rows,
	)
}

// SelectFromTransferTasks reads one or more rows from transfer_tasks table
func (mdb *db) SelectFromTransferTasks(
	ctx context.Context,
	filter sqlplugin.TransferTasksFilter,
) ([]sqlplugin.TransferTasksRow, error) {
	var rows []sqlplugin.TransferTasksRow
	if err := mdb.conn.SelectContext(ctx,
		&rows,
		getTransferTaskQuery,
		filter.ShardID,
		filter.TaskID,
	); err != nil {
		return nil, err
	}
	return rows, nil
}

// RangeSelectFromTransferTasks reads one or more rows from transfer_tasks table
func (mdb *db) RangeSelectFromTransferTasks(
	ctx context.Context,
	filter sqlplugin.TransferTasksRangeFilter,
) ([]sqlplugin.TransferTasksRow, error) {
	var rows []sqlplugin.TransferTasksRow
	if err := mdb.conn.SelectContext(ctx,
		&rows,
		getTransferTasksQuery,
		filter.ShardID,
		filter.MinTaskID,
		filter.MaxTaskID,
	); err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteFromTransferTasks deletes one or more rows from transfer_tasks table
func (mdb *db) DeleteFromTransferTasks(
	ctx context.Context,
	filter sqlplugin.TransferTasksFilter,
) (sql.Result, error) {
	return mdb.conn.ExecContext(ctx,
		deleteTransferTaskQuery,
		filter.ShardID,
		filter.TaskID,
	)
}

// RangeDeleteFromTransferTasks deletes one or more rows from transfer_tasks table
func (mdb *db) RangeDeleteFromTransferTasks(
	ctx context.Context,
	filter sqlplugin.TransferTasksRangeFilter,
) (sql.Result, error) {
	return mdb.conn.ExecContext(ctx,
		rangeDeleteTransferTaskQuery,
		filter.ShardID,
		filter.MinTaskID,
		filter.MaxTaskID,
	)
}

// InsertIntoTimerTasks inserts one or more rows into timer_tasks table
func (mdb *db) InsertIntoTimerTasks(
	ctx context.Context,
	rows []sqlplugin.TimerTasksRow,
) (sql.Result, error) {
	for i := range rows {
		rows[i].VisibilityTimestamp = mdb.converter.ToMySQLDateTime(rows[i].VisibilityTimestamp)
	}
	return mdb.conn.NamedExecContext(
		ctx,
		createTimerTasksQuery,
		rows,
	)
}

// SelectFromTimerTasks reads one or more rows from timer_tasks table
func (mdb *db) SelectFromTimerTasks(
	ctx context.Context,
	filter sqlplugin.TimerTasksFilter,
) ([]sqlplugin.TimerTasksRow, error) {
	var rows []sqlplugin.TimerTasksRow
	filter.VisibilityTimestamp = mdb.converter.ToMySQLDateTime(filter.VisibilityTimestamp)
	err := mdb.conn.SelectContext(ctx,
		&rows,
		getTimerTaskQuery,
		filter.ShardID,
		filter.VisibilityTimestamp,
		filter.TaskID,
	)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].VisibilityTimestamp = mdb.converter.FromMySQLDateTime(rows[i].VisibilityTimestamp)
	}
	return rows, err
}

// RangeSelectFromTimerTasks reads one or more rows from timer_tasks table
func (mdb *db) RangeSelectFromTimerTasks(
	ctx context.Context,
	filter sqlplugin.TimerTasksRangeFilter,
) ([]sqlplugin.TimerTasksRow, error) {
	var rows []sqlplugin.TimerTasksRow
	filter.MinVisibilityTimestamp = mdb.converter.ToMySQLDateTime(filter.MinVisibilityTimestamp)
	filter.MaxVisibilityTimestamp = mdb.converter.ToMySQLDateTime(filter.MaxVisibilityTimestamp)
	if err := mdb.conn.SelectContext(ctx,
		&rows,
		getTimerTasksQuery,
		filter.ShardID,
		filter.MinVisibilityTimestamp,
		filter.TaskID,
		filter.MinVisibilityTimestamp,
		filter.MaxVisibilityTimestamp,
		filter.PageSize,
	); err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].VisibilityTimestamp = mdb.converter.FromMySQLDateTime(rows[i].VisibilityTimestamp)
	}
	return rows, nil
}

// DeleteFromTimerTasks deletes one or more rows from timer_tasks table
func (mdb *db) DeleteFromTimerTasks(
	ctx context.Context,
	filter sqlplugin.TimerTasksFilter,
) (sql.Result, error) {
	filter.VisibilityTimestamp = mdb.converter.ToMySQLDateTime(filter.VisibilityTimestamp)
	return mdb.conn.ExecContext(ctx,
		deleteTimerTaskQuery,
		filter.ShardID,
		filter.VisibilityTimestamp,
		filter.TaskID,
	)
}

// RangeDeleteFromTimerTasks deletes one or more rows from timer_tasks table
func (mdb *db) RangeDeleteFromTimerTasks(
	ctx context.Context,
	filter sqlplugin.TimerTasksRangeFilter,
) (sql.Result, error) {
	filter.MinVisibilityTimestamp = mdb.converter.ToMySQLDateTime(filter.MinVisibilityTimestamp)
	filter.MaxVisibilityTimestamp = mdb.converter.ToMySQLDateTime(filter.MaxVisibilityTimestamp)
	return mdb.conn.ExecContext(ctx,
		rangeDeleteTimerTaskQuery,
		filter.ShardID,
		filter.MinVisibilityTimestamp,
		filter.MaxVisibilityTimestamp,
	)
}

// InsertIntoBufferedEvents inserts one or more rows into buffered_events table
func (mdb *db) InsertIntoBufferedEvents(
	ctx context.Context,
	rows []sqlplugin.BufferedEventsRow,
) (sql.Result, error) {
	return mdb.conn.NamedExecContext(ctx,
		createBufferedEventsQuery,
		rows,
	)
}

// SelectFromBufferedEvents reads one or more rows from buffered_events table
func (mdb *db) SelectFromBufferedEvents(
	ctx context.Context,
	filter sqlplugin.BufferedEventsFilter,
) ([]sqlplugin.BufferedEventsRow, error) {
	var rows []sqlplugin.BufferedEventsRow
	if err := mdb.conn.SelectContext(ctx,
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
func (mdb *db) DeleteFromBufferedEvents(
	ctx context.Context,
	filter sqlplugin.BufferedEventsFilter,
) (sql.Result, error) {
	return mdb.conn.ExecContext(ctx,
		deleteBufferedEventsQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
}

// InsertIntoReplicationTasks inserts one or more rows into replication_tasks table
func (mdb *db) InsertIntoReplicationTasks(
	ctx context.Context,
	rows []sqlplugin.ReplicationTasksRow,
) (sql.Result, error) {
	return mdb.conn.NamedExecContext(ctx,
		createReplicationTasksQuery,
		rows,
	)
}

// SelectFromReplicationTasks reads one or more rows from replication_tasks table
func (mdb *db) SelectFromReplicationTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationTasksFilter,
) ([]sqlplugin.ReplicationTasksRow, error) {
	var rows []sqlplugin.ReplicationTasksRow
	err := mdb.conn.SelectContext(ctx,
		&rows,
		getReplicationTaskQuery,
		filter.ShardID,
		filter.TaskID,
	)
	return rows, err
}

// RangeSelectFromReplicationTasks reads one or more rows from replication_tasks table
func (mdb *db) RangeSelectFromReplicationTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationTasksRangeFilter,
) ([]sqlplugin.ReplicationTasksRow, error) {
	var rows []sqlplugin.ReplicationTasksRow
	err := mdb.conn.SelectContext(ctx,
		&rows,
		getReplicationTasksQuery,
		filter.ShardID,
		filter.MinTaskID,
		filter.MaxTaskID,
		filter.PageSize,
	)
	return rows, err
}

// DeleteFromReplicationTasks deletes one row from replication_tasks table
func (mdb *db) DeleteFromReplicationTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationTasksFilter,
) (sql.Result, error) {
	return mdb.conn.ExecContext(ctx,
		deleteReplicationTaskQuery,
		filter.ShardID,
		filter.TaskID,
	)
}

// RangeDeleteFromReplicationTasks deletes multi rows from replication_tasks table
func (mdb *db) RangeDeleteFromReplicationTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationTasksRangeFilter,
) (sql.Result, error) {
	return mdb.conn.ExecContext(ctx,
		rangeDeleteReplicationTaskQuery,
		filter.ShardID,
		filter.MinTaskID,
		filter.MaxTaskID,
	)
}

// InsertIntoReplicationTasksDLQ inserts one or more rows into replication_tasks_dlq table
func (mdb *db) InsertIntoReplicationDLQTasks(
	ctx context.Context,
	rows []sqlplugin.ReplicationDLQTasksRow,
) (sql.Result, error) {
	return mdb.conn.NamedExecContext(ctx,
		insertReplicationTaskDLQQuery,
		rows,
	)
}

// SelectFromReplicationTasksDLQ reads one or more rows from replication_tasks_dlq table
func (mdb *db) SelectFromReplicationDLQTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationDLQTasksFilter,
) ([]sqlplugin.ReplicationDLQTasksRow, error) {
	var rows []sqlplugin.ReplicationDLQTasksRow
	err := mdb.conn.SelectContext(ctx,
		&rows, getReplicationTaskDLQQuery,
		filter.SourceClusterName,
		filter.ShardID,
		filter.TaskID,
	)
	return rows, err
}

// RangeSelectFromReplicationTasksDLQ reads one or more rows from replication_tasks_dlq table
func (mdb *db) RangeSelectFromReplicationDLQTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationDLQTasksRangeFilter,
) ([]sqlplugin.ReplicationDLQTasksRow, error) {
	var rows []sqlplugin.ReplicationDLQTasksRow
	err := mdb.conn.SelectContext(ctx,
		&rows, getReplicationTasksDLQQuery,
		filter.SourceClusterName,
		filter.ShardID,
		filter.MinTaskID,
		filter.MaxTaskID,
		filter.PageSize,
	)
	return rows, err
}

// DeleteMessageFromReplicationTasksDLQ deletes one row from replication_tasks_dlq table
func (mdb *db) DeleteFromReplicationDLQTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationDLQTasksFilter,
) (sql.Result, error) {

	return mdb.conn.ExecContext(ctx,
		deleteReplicationTaskFromDLQQuery,
		filter.SourceClusterName,
		filter.ShardID,
		filter.TaskID,
	)
}

// DeleteMessageFromReplicationTasksDLQ deletes one or more rows from replication_tasks_dlq table
func (mdb *db) RangeDeleteFromReplicationDLQTasks(
	ctx context.Context,
	filter sqlplugin.ReplicationDLQTasksRangeFilter,
) (sql.Result, error) {

	return mdb.conn.ExecContext(ctx,
		rangeDeleteReplicationTaskFromDLQQuery,
		filter.SourceClusterName,
		filter.ShardID,
		filter.MinTaskID,
		filter.MaxTaskID,
	)
}
