// Copyright (c) 2017 Uber Technologies, Inc.
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
	"database/sql"

	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
)

const (
	executionsColumns = `shard_id, domain_id, workflow_id, run_id, next_event_id, last_write_version, data, data_encoding`

	createExecutionQry = `INSERT INTO executions(` + executionsColumns + `)
 VALUES(:shard_id, :domain_id, :workflow_id, :run_id, :next_event_id, :last_write_version, :data, :data_encoding)`

	updateExecutionQry = `UPDATE executions SET
 next_event_id = :next_event_id, last_write_version = :last_write_version, data = :data, data_encoding = :data_encoding
 WHERE shard_id = :shard_id AND domain_id = :domain_id AND workflow_id = :workflow_id AND run_id = :run_id`

	getExecutionQry = `SELECT ` + executionsColumns + ` FROM executions
 WHERE shard_id = ? AND domain_id = ? AND workflow_id = ? AND run_id = ?`

	deleteExecutionQry = `DELETE FROM executions 
 WHERE shard_id = ? AND domain_id = ? AND workflow_id = ? AND run_id = ?`

	lockExecutionQryBase = `SELECT next_event_id FROM executions 
 WHERE shard_id = ? AND domain_id = ? AND workflow_id = ? AND run_id = ?`

	writeLockExecutionQry = lockExecutionQryBase + ` FOR UPDATE`
	readLockExecutionQry  = lockExecutionQryBase + ` LOCK IN SHARE MODE`

	createCurrentExecutionQry = `INSERT INTO current_executions
(shard_id, domain_id, workflow_id, run_id, create_request_id, state, close_status, start_version, last_write_version) VALUES
(:shard_id, :domain_id, :workflow_id, :run_id, :create_request_id, :state, :close_status, :start_version, :last_write_version)`

	deleteCurrentExecutionQry = "DELETE FROM current_executions WHERE shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?"

	getCurrentExecutionQry = `SELECT
shard_id, domain_id, workflow_id, run_id, create_request_id, state, close_status, start_version, last_write_version
FROM current_executions WHERE shard_id = ? AND domain_id = ? AND workflow_id = ?`

	lockCurrentExecutionJoinExecutionsQry = `SELECT
ce.shard_id, ce.domain_id, ce.workflow_id, ce.run_id, ce.create_request_id, ce.state, ce.close_status, ce.start_version, e.last_write_version
FROM current_executions ce
INNER JOIN executions e ON e.shard_id = ce.shard_id AND e.domain_id = ce.domain_id AND e.workflow_id = ce.workflow_id AND e.run_id = ce.run_id
WHERE ce.shard_id = ? AND ce.domain_id = ? AND ce.workflow_id = ? FOR UPDATE`

	lockCurrentExecutionQry = `SELECT run_id FROM current_executions WHERE
shard_id = ? AND
domain_id = ? AND
workflow_id = ?
FOR UPDATE`

	updateCurrentExecutionsQry = `UPDATE current_executions SET
run_id = :run_id,
create_request_id = :create_request_id,
state = :state,
close_status = :close_status,
start_version = :start_version,
last_write_version = :last_write_version
WHERE
shard_id = :shard_id AND
domain_id = :domain_id AND
workflow_id = :workflow_id
`

	getTransferTasksQry = `SELECT task_id, data, data_encoding 
 FROM transfer_tasks WHERE shard_id = ? AND task_id > ? AND task_id <= ?`

	createTransferTasksQry = `INSERT INTO transfer_tasks(shard_id, task_id, data, data_encoding) 
 VALUES(:shard_id, :task_id, :data, :data_encoding)`

	deleteTransferTaskQry      = `DELETE FROM transfer_tasks WHERE shard_id = ? AND task_id = ?`
	rangeDeleteTransferTaskQry = `DELETE FROM transfer_tasks WHERE shard_id = ? AND task_id > ? AND task_id <= ?`

	createTimerTasksQry = `INSERT INTO timer_tasks (shard_id, visibility_timestamp, task_id, data, data_encoding)
  VALUES (:shard_id, :visibility_timestamp, :task_id, :data, :data_encoding)`

	getTimerTasksQry = `SELECT visibility_timestamp, task_id, data, data_encoding FROM timer_tasks 
  WHERE shard_id = ? 
  AND ((visibility_timestamp >= ? AND task_id >= ?) OR visibility_timestamp > ?) 
  AND visibility_timestamp < ?
  ORDER BY visibility_timestamp,task_id LIMIT ?`

	deleteTimerTaskQry      = `DELETE FROM timer_tasks WHERE shard_id = ? AND visibility_timestamp = ? AND task_id = ?`
	rangeDeleteTimerTaskQry = `DELETE FROM timer_tasks WHERE shard_id = ? AND visibility_timestamp >= ? AND visibility_timestamp < ?`

	createReplicationTasksQry = `INSERT INTO replication_tasks (shard_id, task_id, data, data_encoding) 
  VALUES(:shard_id, :task_id, :data, :data_encoding)`

	getReplicationTasksQry = `SELECT task_id, data, data_encoding FROM replication_tasks WHERE 
shard_id = ? AND
task_id > ? AND
task_id <= ? 
ORDER BY task_id LIMIT ?`

	deleteReplicationTaskQry = `DELETE FROM replication_tasks WHERE shard_id = ? AND task_id = ?`

	bufferedEventsColumns    = `shard_id, domain_id, workflow_id, run_id, data, data_encoding`
	createBufferedEventsQury = `INSERT INTO buffered_events(` + bufferedEventsColumns + `)
VALUES (:shard_id, :domain_id, :workflow_id, :run_id, :data, :data_encoding)`

	deleteBufferedEventsQury = `DELETE FROM buffered_events WHERE shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?`
	getBufferedEventsQury    = `SELECT data, data_encoding FROM buffered_events WHERE
shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?`
)

// InsertIntoExecutions inserts a row into executions table
func (mdb *DB) InsertIntoExecutions(row *sqldb.ExecutionsRow) (sql.Result, error) {
	return mdb.conn.NamedExec(createExecutionQry, row)
}

// UpdateExecutions updates a single row in executions table
func (mdb *DB) UpdateExecutions(row *sqldb.ExecutionsRow) (sql.Result, error) {
	return mdb.conn.NamedExec(updateExecutionQry, row)
}

// SelectFromExecutions reads a single row from executions table
func (mdb *DB) SelectFromExecutions(filter *sqldb.ExecutionsFilter) (*sqldb.ExecutionsRow, error) {
	var row sqldb.ExecutionsRow
	err := mdb.conn.Get(&row, getExecutionQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
	if err != nil {
		return nil, err
	}
	return &row, err
}

// DeleteFromExecutions deletes a single row from executions table
func (mdb *DB) DeleteFromExecutions(filter *sqldb.ExecutionsFilter) (sql.Result, error) {
	return mdb.conn.Exec(deleteExecutionQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
}

// ReadLockExecutions acquires a write lock on a single row in executions table
func (mdb *DB) ReadLockExecutions(filter *sqldb.ExecutionsFilter) (int, error) {
	var nextEventID int
	err := mdb.conn.Get(&nextEventID, readLockExecutionQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
	return nextEventID, err
}

// WriteLockExecutions acquires a write lock on a single row in executions table
func (mdb *DB) WriteLockExecutions(filter *sqldb.ExecutionsFilter) (int, error) {
	var nextEventID int
	err := mdb.conn.Get(&nextEventID, writeLockExecutionQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
	return nextEventID, err
}

// InsertIntoCurrentExecutions inserts a single row into current_executions table
func (mdb *DB) InsertIntoCurrentExecutions(row *sqldb.CurrentExecutionsRow) (sql.Result, error) {
	return mdb.conn.NamedExec(createCurrentExecutionQry, row)
}

// UpdateCurrentExecutions updates a single row in current_executions table
func (mdb *DB) UpdateCurrentExecutions(row *sqldb.CurrentExecutionsRow) (sql.Result, error) {
	return mdb.conn.NamedExec(updateCurrentExecutionsQry, row)
}

// SelectFromCurrentExecutions reads one or more rows from current_executions table
func (mdb *DB) SelectFromCurrentExecutions(filter *sqldb.CurrentExecutionsFilter) (*sqldb.CurrentExecutionsRow, error) {
	var row sqldb.CurrentExecutionsRow
	err := mdb.conn.Get(&row, getCurrentExecutionQry, filter.ShardID, filter.DomainID, filter.WorkflowID)
	return &row, err
}

// DeleteFromCurrentExecutions deletes a single row in current_executions table
func (mdb *DB) DeleteFromCurrentExecutions(filter *sqldb.CurrentExecutionsFilter) (sql.Result, error) {
	return mdb.conn.Exec(deleteCurrentExecutionQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
}

// LockCurrentExecutions acquires a write lock on a single row in current_executions table
func (mdb *DB) LockCurrentExecutions(filter *sqldb.CurrentExecutionsFilter) (sqldb.UUID, error) {
	var runID sqldb.UUID
	err := mdb.conn.Get(&runID, lockCurrentExecutionQry, filter.ShardID, filter.DomainID, filter.WorkflowID)
	return runID, err
}

// LockCurrentExecutionsJoinExecutions joins a row in current_executions with executions table and acquires a
// write lock on the result
func (mdb *DB) LockCurrentExecutionsJoinExecutions(filter *sqldb.CurrentExecutionsFilter) ([]sqldb.CurrentExecutionsRow, error) {
	var rows []sqldb.CurrentExecutionsRow
	err := mdb.conn.Select(&rows, lockCurrentExecutionJoinExecutionsQry, filter.ShardID, filter.DomainID, filter.WorkflowID)
	return rows, err
}

// InsertIntoTransferTasks inserts one or more rows into transfer_tasks table
func (mdb *DB) InsertIntoTransferTasks(rows []sqldb.TransferTasksRow) (sql.Result, error) {
	return mdb.conn.NamedExec(createTransferTasksQry, rows)
}

// SelectFromTransferTasks reads one or more rows from transfer_tasks table
func (mdb *DB) SelectFromTransferTasks(filter *sqldb.TransferTasksFilter) ([]sqldb.TransferTasksRow, error) {
	var rows []sqldb.TransferTasksRow
	err := mdb.conn.Select(&rows, getTransferTasksQry, filter.ShardID, *filter.MinTaskID, *filter.MaxTaskID)
	if err != nil {
		return nil, err
	}
	return rows, err
}

// DeleteFromTransferTasks deletes one or more rows from transfer_tasks table
func (mdb *DB) DeleteFromTransferTasks(filter *sqldb.TransferTasksFilter) (sql.Result, error) {
	if filter.MinTaskID != nil {
		return mdb.conn.Exec(rangeDeleteTransferTaskQry, filter.ShardID, *filter.MinTaskID, *filter.MaxTaskID)
	}
	return mdb.conn.Exec(deleteTransferTaskQry, filter.ShardID, *filter.TaskID)
}

// InsertIntoTimerTasks inserts one or more rows into timer_tasks table
func (mdb *DB) InsertIntoTimerTasks(rows []sqldb.TimerTasksRow) (sql.Result, error) {
	for i := range rows {
		rows[i].VisibilityTimestamp = mdb.converter.ToMySQLDateTime(rows[i].VisibilityTimestamp)
	}
	return mdb.conn.NamedExec(createTimerTasksQry, rows)
}

// SelectFromTimerTasks reads one or more rows from timer_tasks table
func (mdb *DB) SelectFromTimerTasks(filter *sqldb.TimerTasksFilter) ([]sqldb.TimerTasksRow, error) {
	var rows []sqldb.TimerTasksRow
	*filter.MinVisibilityTimestamp = mdb.converter.ToMySQLDateTime(*filter.MinVisibilityTimestamp)
	*filter.MaxVisibilityTimestamp = mdb.converter.ToMySQLDateTime(*filter.MaxVisibilityTimestamp)
	err := mdb.conn.Select(&rows, getTimerTasksQry, filter.ShardID, *filter.MinVisibilityTimestamp,
		filter.TaskID, *filter.MinVisibilityTimestamp, *filter.MaxVisibilityTimestamp, *filter.PageSize)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].VisibilityTimestamp = mdb.converter.FromMySQLDateTime(rows[i].VisibilityTimestamp)
	}
	return rows, err
}

// DeleteFromTimerTasks deletes one or more rows from timer_tasks table
func (mdb *DB) DeleteFromTimerTasks(filter *sqldb.TimerTasksFilter) (sql.Result, error) {
	if filter.MinVisibilityTimestamp != nil {
		*filter.MinVisibilityTimestamp = mdb.converter.ToMySQLDateTime(*filter.MinVisibilityTimestamp)
		*filter.MaxVisibilityTimestamp = mdb.converter.ToMySQLDateTime(*filter.MaxVisibilityTimestamp)
		return mdb.conn.Exec(rangeDeleteTimerTaskQry, filter.ShardID, *filter.MinVisibilityTimestamp, *filter.MaxVisibilityTimestamp)
	}
	*filter.VisibilityTimestamp = mdb.converter.ToMySQLDateTime(*filter.VisibilityTimestamp)
	return mdb.conn.Exec(deleteTimerTaskQry, filter.ShardID, *filter.VisibilityTimestamp, filter.TaskID)
}

// InsertIntoBufferedEvents inserts one or more rows into buffered_events table
func (mdb *DB) InsertIntoBufferedEvents(rows []sqldb.BufferedEventsRow) (sql.Result, error) {
	return mdb.conn.NamedExec(createBufferedEventsQury, rows)
}

// SelectFromBufferedEvents reads one or more rows from buffered_events table
func (mdb *DB) SelectFromBufferedEvents(filter *sqldb.BufferedEventsFilter) ([]sqldb.BufferedEventsRow, error) {
	var rows []sqldb.BufferedEventsRow
	err := mdb.conn.Select(&rows, getBufferedEventsQury, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
	for i := 0; i < len(rows); i++ {
		rows[i].DomainID = filter.DomainID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
		rows[i].ShardID = filter.ShardID
	}
	return rows, err
}

// DeleteFromBufferedEvents deletes one or more rows from buffered_events table
func (mdb *DB) DeleteFromBufferedEvents(filter *sqldb.BufferedEventsFilter) (sql.Result, error) {
	return mdb.conn.Exec(deleteBufferedEventsQury, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
}

// InsertIntoReplicationTasks inserts one or more rows into replication_tasks table
func (mdb *DB) InsertIntoReplicationTasks(rows []sqldb.ReplicationTasksRow) (sql.Result, error) {
	return mdb.conn.NamedExec(createReplicationTasksQry, rows)
}

// SelectFromReplicationTasks reads one or more rows from replication_tasks table
func (mdb *DB) SelectFromReplicationTasks(filter *sqldb.ReplicationTasksFilter) ([]sqldb.ReplicationTasksRow, error) {
	var rows []sqldb.ReplicationTasksRow
	err := mdb.conn.Select(&rows, getReplicationTasksQry, filter.ShardID, *filter.MinTaskID, *filter.MaxTaskID, *filter.PageSize)
	return rows, err
}

// DeleteFromReplicationTasks deletes one or more rows from replication_tasks table
func (mdb *DB) DeleteFromReplicationTasks(filter *sqldb.ReplicationTasksFilter) (sql.Result, error) {
	return mdb.conn.Exec(deleteReplicationTaskQry, filter.ShardID, *filter.TaskID)
}
