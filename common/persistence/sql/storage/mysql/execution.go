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
	executionsColumns = `shard_id,
domain_id,
workflow_id,
run_id,
task_list,
workflow_type_name,
workflow_timeout_seconds,
decision_task_timeout_minutes,
state,
close_status,
last_first_event_id,
next_event_id,
last_processed_event,
start_time,
last_updated_time,
create_request_id,
decision_version,
decision_schedule_id,
decision_started_id,
decision_timeout,
decision_attempt,
decision_timestamp,
sticky_task_list,
sticky_schedule_to_start_timeout,
client_library_version,
client_feature_version,
client_impl,
signal_count,
history_size,
completion_event_encoding,
cron_schedule,
has_retry_policy,
attempt,
initial_interval,
backoff_coefficient,
maximum_interval,
maximum_attempts,
expiration_seconds,
expiration_time,
non_retryable_errors
`

	executionsColumnsTags = `:shard_id,
:domain_id,
:workflow_id,
:run_id,
:task_list,
:workflow_type_name,
:workflow_timeout_seconds,
:decision_task_timeout_minutes,
:state,
:close_status,
:last_first_event_id,
:next_event_id,
:last_processed_event,
:start_time,
:last_updated_time,
:create_request_id,
:decision_version,
:decision_schedule_id,
:decision_started_id,
:decision_timeout,
:decision_attempt,
:decision_timestamp,
:sticky_task_list,
:sticky_schedule_to_start_timeout,
:client_library_version,
:client_feature_version,
:client_impl,
:signal_count,
:history_size,
:completion_event_encoding,
:cron_schedule,
:has_retry_policy,
:attempt,
:initial_interval,
:backoff_coefficient,
:maximum_interval,
:maximum_attempts,
:expiration_seconds,
:expiration_time,
:non_retryable_errors`

	executionsBlobColumns = `completion_event,
execution_context`

	// Excluding completion_event
	executionsNonblobParentColumns = `parent_domain_id,
parent_workflow_id,
parent_run_id,
initiated_id,
completion_event_batch_id`

	executionsNonblobParentColumnsTags = `:parent_domain_id,
:parent_workflow_id,
:parent_run_id,
:initiated_id,
:completion_event_batch_id`

	executionsCancelColumns = `cancel_requested,
cancel_request_id`

	executionsReplicationStateColumns     = `start_version, current_version, last_write_version, last_write_event_id, last_replication_info`
	executionsReplicationStateColumnsTags = `:start_version, :current_version, :last_write_version, :last_write_event_id, :last_replication_info`

	createExecutionQry = `INSERT INTO executions
(` + executionsColumns + `,` +
		executionsNonblobParentColumns +
		`,
execution_context,
cancel_requested,
cancel_request_id,` +
		executionsReplicationStateColumns +
		`)
VALUES
(` + executionsColumnsTags + `,` +
		executionsNonblobParentColumnsTags + `,
:execution_context,
:cancel_requested,
:cancel_request_id,` +
		executionsReplicationStateColumnsTags +
		`)
`

	updateExecutionQry = `UPDATE executions SET
domain_id = :domain_id,
workflow_id = :workflow_id,
run_id = :run_id,
parent_domain_id = :parent_domain_id,
parent_workflow_id = :parent_workflow_id,
parent_run_id = :parent_run_id,
initiated_id = :initiated_id,
completion_event = :completion_event,
completion_event_batch_id = :completion_event_batch_id,
completion_event_encoding = :completion_event_encoding,
task_list = :task_list,
workflow_type_name = :workflow_type_name,
workflow_timeout_seconds = :workflow_timeout_seconds,
decision_task_timeout_minutes = :decision_task_timeout_minutes,
execution_context = :execution_context,
state = :state,
close_status = :close_status,
last_first_event_id = :last_first_event_id,
next_event_id = :next_event_id,
last_processed_event = :last_processed_event,
start_time = :start_time,
last_updated_time = :last_updated_time,
create_request_id = :create_request_id,
decision_version = :decision_version,
decision_schedule_id = :decision_schedule_id,
decision_started_id = :decision_started_id,
decision_request_id = :decision_request_id,
decision_timeout = :decision_timeout,
decision_attempt = :decision_attempt,
decision_timestamp = :decision_timestamp,
cancel_requested = :cancel_requested,
cancel_request_id = :cancel_request_id,
sticky_task_list = :sticky_task_list,
sticky_schedule_to_start_timeout = :sticky_schedule_to_start_timeout,
client_library_version = :client_library_version,
client_feature_version = :client_feature_version,
client_impl = :client_impl,
start_version = :start_version,
current_version = :current_version,
last_write_version = :last_write_version,
last_write_event_id = :last_write_event_id,
last_replication_info = :last_replication_info,
signal_count = :signal_count,
history_size = :history_size,
cron_schedule = :cron_schedule,
has_retry_policy = :has_retry_policy,
attempt = :attempt,
initial_interval = :initial_interval,
backoff_coefficient = :backoff_coefficient,
maximum_interval = :maximum_interval,
maximum_attempts = :maximum_attempts,
expiration_seconds = :expiration_seconds,
expiration_time = :expiration_time,
non_retryable_errors = :non_retryable_errors

WHERE
shard_id = :shard_id AND
domain_id = :domain_id AND
workflow_id = :workflow_id AND
run_id = :run_id
`

	getExecutionQry = `SELECT ` +
		executionsColumns + "," +
		executionsBlobColumns + "," +
		executionsNonblobParentColumns + "," +
		executionsCancelColumns + "," +
		executionsReplicationStateColumns +
		` FROM executions WHERE
shard_id = ? AND
domain_id = ? AND
workflow_id = ? AND
run_id = ?`

	deleteExecutionQry = `DELETE FROM executions WHERE
shard_id = ? AND
domain_id = ? AND
workflow_id = ? AND
run_id = ?`

	transferTaskInfoColumns = `task_id,
domain_id,
workflow_id,
run_id,
task_type,
target_domain_id,
target_workflow_id,
target_run_id,
target_child_workflow_only,
task_list,
schedule_id,
visibility_timestamp,
version`

	transferTaskInfoColumnsTags = `:task_id,
:domain_id,
:workflow_id,
:run_id,
:task_type,
:target_domain_id,
:target_workflow_id,
:target_run_id,
:target_child_workflow_only,
:task_list,
:schedule_id,
:visibility_timestamp,
:version`

	transferTasksColumns = `shard_id,` + transferTaskInfoColumns

	transferTasksColumnsTags = `:shard_id,` + transferTaskInfoColumnsTags

	getTransferTasksQry = `SELECT
` + transferTaskInfoColumns +
		`
FROM transfer_tasks WHERE
shard_id = ? AND
task_id > ? AND
task_id <= ?
`

	createCurrentExecutionQry = `INSERT INTO current_executions
(shard_id, domain_id, workflow_id, run_id, create_request_id, state, close_status, start_version, last_write_version) VALUES
(:shard_id, :domain_id, :workflow_id, :run_id, :create_request_id, :state, :close_status, :start_version, :last_write_version)`

	deleteCurrentExecutionQry = "DELETE FROM current_executions WHERE shard_id=? AND domain_id=? AND workflow_id=?"

	getCurrentExecutionQry = `SELECT
ce.shard_id, ce.domain_id, ce.workflow_id, ce.run_id, ce.create_request_id, ce.state, ce.close_status, ce.start_version, e.last_write_version
FROM current_executions ce
INNER JOIN executions e ON e.shard_id = ce.shard_id AND e.domain_id = ce.domain_id AND e.workflow_id = ce.workflow_id AND e.run_id = ce.run_id
WHERE ce.shard_id = ? AND ce.domain_id = ? AND ce.workflow_id = ?
`

	getCurrentExecutionQryForUpdate = getCurrentExecutionQry + " FOR UPDATE"

	lockCurrentExecutionQry = `SELECT run_id FROM current_executions WHERE
shard_id = ? AND
domain_id = ? AND
workflow_id = ?
FOR UPDATE`

	// The workflowIDReuseQry and continueAsNewUpdateCurrentExecutionsQry together comprise workflowIDReuse.
	// The updates must be executed only after locking current_run_id, current_state and current_last_write_version of
	// the current_executions row that we are going to update,
	// and asserting that it is PreviousRunId.
	workflowIDReuseQry = `SELECT run_id, state, last_write_version FROM current_executions WHERE
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

	createTransferTasksQry = `INSERT INTO transfer_tasks
(` + transferTasksColumns + `)
VALUES
(` + transferTasksColumnsTags + `
)
`

	deleteTransferTaskQry      = `DELETE FROM transfer_tasks WHERE shard_id = ? AND task_id = ?`
	rangeDeleteTransferTaskQry = `DELETE FROM transfer_tasks WHERE shard_id = ? AND task_id > ? AND task_id <= ?`

	replicationTaskInfoColumns = `task_id,
domain_id,
workflow_id,
run_id,
task_type,
first_event_id,
next_event_id,
version,
last_replication_info,
scheduled_id`

	replicationTaskInfoColumnsTags = `:task_id,
:domain_id,
:workflow_id,
:run_id,
:task_type,
:first_event_id,
:next_event_id,
:version,
:last_replication_info,
:scheduled_id`

	replicationTasksColumns     = `shard_id, ` + replicationTaskInfoColumns
	replicationTasksColumnsTags = `:shard_id, ` + replicationTaskInfoColumnsTags

	createReplicationTasksQry = `INSERT INTO replication_tasks (` +
		replicationTasksColumns + `) VALUES(` +
		replicationTasksColumnsTags + `)`

	getReplicationTasksQry = `SELECT ` + replicationTaskInfoColumns +
		`
FROM replication_tasks WHERE
shard_id = ? AND
task_id > ? AND
task_id <= ?
LIMIT ?`

	deleteReplicationTaskQry = `DELETE FROM replication_tasks WHERE shard_id = ? AND task_id = ?`

	timerTaskInfoColumns     = `visibility_timestamp, task_id, domain_id, workflow_id, run_id, task_type, timeout_type, event_id, schedule_attempt, version`
	timerTaskInfoColumnsTags = `:visibility_timestamp, :task_id, :domain_id, :workflow_id, :run_id, :task_type, :timeout_type, :event_id, :schedule_attempt, :version`
	timerTasksColumns        = `shard_id,` + timerTaskInfoColumns
	timerTasksColumnsTags    = `:shard_id,` + timerTaskInfoColumnsTags
	createTimerTasksQry      = `INSERT INTO timer_tasks (` +
		timerTasksColumns + `) VALUES (` +
		timerTasksColumnsTags + `)`
	getTimerTasksQry = `SELECT ` + timerTaskInfoColumns +
		`
FROM timer_tasks WHERE
shard_id = ? AND
((visibility_timestamp >= ? AND task_id >= ?) OR visibility_timestamp > ?) AND
visibility_timestamp < ?
ORDER BY visibility_timestamp,task_id LIMIT ?`
	deleteTimerTaskQry         = `DELETE FROM timer_tasks WHERE shard_id = ? AND visibility_timestamp = ? AND task_id = ?`
	rangeDeleteTimerTaskQry    = `DELETE FROM timer_tasks WHERE shard_id = ? AND visibility_timestamp >= ? AND visibility_timestamp < ?`
	lockAndCheckNextEventIDQry = `SELECT next_event_id FROM executions WHERE
shard_id = ? AND
domain_id = ? AND
workflow_id = ? AND
run_id = ?
FOR UPDATE`

	bufferedEventsColumns    = `shard_id, domain_id, workflow_id, run_id, data, data_encoding`
	createBufferedEventsQury = `INSERT INTO buffered_events(` + bufferedEventsColumns + `)
VALUES (:shard_id, :domain_id, :workflow_id, :run_id, :data, :data_encoding)`

	deleteBufferedEventsQury = `DELETE FROM buffered_events WHERE shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?`
	getBufferedEventsQury    = `SELECT data, data_encoding FROM buffered_events WHERE
shard_id=? AND domain_id=? AND workflow_id=? AND run_id=?`
)

// InsertIntoExecutions inserts a row into executions table
func (mdb *DB) InsertIntoExecutions(row *sqldb.ExecutionsRow) (sql.Result, error) {
	row.StartTime = mdb.converter.ToMySQLDateTime(row.StartTime)
	row.LastUpdatedTime = mdb.converter.ToMySQLDateTime(row.LastUpdatedTime)
	row.ExpirationTime = mdb.converter.ToMySQLDateTime(row.ExpirationTime)
	return mdb.conn.NamedExec(createExecutionQry, row)
}

// UpdateExecutions updates a single row in executions table
func (mdb *DB) UpdateExecutions(row *sqldb.ExecutionsRow) (sql.Result, error) {
	row.StartTime = mdb.converter.ToMySQLDateTime(row.StartTime)
	row.LastUpdatedTime = mdb.converter.ToMySQLDateTime(row.LastUpdatedTime)
	row.ExpirationTime = mdb.converter.ToMySQLDateTime(row.ExpirationTime)
	return mdb.conn.NamedExec(updateExecutionQry, row)
}

// SelectFromExecutions reads a single row from executions table
func (mdb *DB) SelectFromExecutions(filter *sqldb.ExecutionsFilter) (*sqldb.ExecutionsRow, error) {
	var row sqldb.ExecutionsRow
	err := mdb.conn.Get(&row, getExecutionQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
	if err != nil {
		return nil, err
	}
	row.StartTime = mdb.converter.FromMySQLDateTime(row.StartTime)
	row.LastUpdatedTime = mdb.converter.FromMySQLDateTime(row.LastUpdatedTime)
	row.ExpirationTime = mdb.converter.FromMySQLDateTime(row.ExpirationTime)
	return &row, err
}

// DeleteFromExecutions deletes a single row from executions table
func (mdb *DB) DeleteFromExecutions(filter *sqldb.ExecutionsFilter) (sql.Result, error) {
	return mdb.conn.Exec(deleteExecutionQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
}

// LockExecutions acquires a write lock on a single row in executions table
func (mdb *DB) LockExecutions(filter *sqldb.ExecutionsFilter) (int, error) {
	var nextEventID int
	err := mdb.conn.Get(&nextEventID, lockAndCheckNextEventIDQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
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
	return mdb.conn.Exec(deleteCurrentExecutionQry, filter.ShardID, filter.DomainID, filter.WorkflowID)
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
	err := mdb.conn.Select(&rows, getCurrentExecutionQryForUpdate, filter.ShardID, filter.DomainID, filter.WorkflowID)
	return rows, err
}

// InsertIntoTransferTasks inserts one or more rows into transfer_tasks table
func (mdb *DB) InsertIntoTransferTasks(rows []sqldb.TransferTasksRow) (sql.Result, error) {
	for i := range rows {
		rows[i].VisibilityTimestamp = mdb.converter.ToMySQLDateTime(rows[i].VisibilityTimestamp)
	}
	return mdb.conn.NamedExec(createTransferTasksQry, rows)
}

// SelectFromTransferTasks reads one or more rows from transfer_tasks table
func (mdb *DB) SelectFromTransferTasks(filter *sqldb.TransferTasksFilter) ([]sqldb.TransferTasksRow, error) {
	var rows []sqldb.TransferTasksRow
	err := mdb.conn.Select(&rows, getTransferTasksQry, filter.ShardID, *filter.MinTaskID, *filter.MaxTaskID)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].VisibilityTimestamp = mdb.converter.FromMySQLDateTime(rows[i].VisibilityTimestamp)
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
