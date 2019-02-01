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
	"fmt"
	"strings"

	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
)

const (
	deleteMapQryTemplate = `DELETE FROM %v
WHERE
shard_id = ? AND
domain_id = ? AND
workflow_id = ? AND
run_id = ?`

	// %[2]v is the columns of the value struct (i.e. no primary key columns), comma separated
	// %[3]v should be %[2]v with colons prepended.
	// i.e. %[3]v = ",".join(":" + s for s in %[2]v)
	// So that this query can be used with BindNamed
	// %[4]v should be the name of the key associated with the map
	// e.g. for ActivityInfo it is "schedule_id"
	setKeyInMapQryTemplate = `REPLACE INTO %[1]v
(shard_id, domain_id, workflow_id, run_id, %[4]v, %[2]v)
VALUES
(:shard_id, :domain_id, :workflow_id, :run_id, :%[4]v, %[3]v)`

	// %[2]v is the name of the key
	deleteKeyInMapQryTemplate = `DELETE FROM %[1]v
WHERE
shard_id = ? AND
domain_id = ? AND
workflow_id = ? AND
run_id = ? AND
%[2]v = ?`

	// %[1]v is the name of the table
	// %[2]v is the name of the key
	// %[3]v is the value columns, separated by commas
	getMapQryTemplate = `SELECT %[2]v, %[3]v FROM %[1]v
WHERE
shard_id = ? AND
domain_id = ? AND
workflow_id = ? AND
run_id = ?`
)

func stringMap(a []string, f func(string) string) []string {
	b := make([]string, len(a))
	for i, v := range a {
		b[i] = f(v)
	}
	return b
}

func makeDeleteMapQry(tableName string) string {
	return fmt.Sprintf(deleteMapQryTemplate, tableName)
}

func makeSetKeyInMapQry(tableName string, nonPrimaryKeyColumns []string, mapKeyName string) string {
	return fmt.Sprintf(setKeyInMapQryTemplate,
		tableName,
		strings.Join(nonPrimaryKeyColumns, ","),
		strings.Join(stringMap(nonPrimaryKeyColumns, func(x string) string {
			return ":" + x
		}), ","),
		mapKeyName)
}

func makeDeleteKeyInMapQry(tableName string, mapKeyName string) string {
	return fmt.Sprintf(deleteKeyInMapQryTemplate,
		tableName,
		mapKeyName)
}

func makeGetMapQryTemplate(tableName string, nonPrimaryKeyColumns []string, mapKeyName string) string {
	return fmt.Sprintf(getMapQryTemplate,
		tableName,
		mapKeyName,
		strings.Join(nonPrimaryKeyColumns, ","))
}

var (
	// Omit shard_id, run_id, domain_id, workflow_id, schedule_id since they're in the primary key
	activityInfoColumns = []string{
		"version",
		"scheduled_event_batch_id",
		"scheduled_event",
		"scheduled_event_encoding",
		"scheduled_time",
		"started_id",
		"started_event",
		"started_event_encoding",
		"started_time",
		"activity_id",
		"request_id",
		"details",
		"schedule_to_start_timeout",
		"schedule_to_close_timeout",
		"start_to_close_timeout",
		"heartbeat_timeout",
		"cancel_requested",
		"cancel_request_id",
		"last_heartbeat_updated_time",
		"timer_task_status",
		"attempt",
		"task_list",
		"started_identity",
		"has_retry_policy",
		"init_interval",
		"backoff_coefficient",
		"max_interval",
		"expiration_time",
		"max_attempts",
		"non_retriable_errors",
	}
	activityInfoTableName = "activity_info_maps"
	activityInfoKey       = "schedule_id"

	deleteActivityInfoMapQry      = makeDeleteMapQry(activityInfoTableName)
	setKeyInActivityInfoMapQry    = makeSetKeyInMapQry(activityInfoTableName, activityInfoColumns, activityInfoKey)
	deleteKeyInActivityInfoMapQry = makeDeleteKeyInMapQry(activityInfoTableName, activityInfoKey)
	getActivityInfoMapQry         = makeGetMapQryTemplate(activityInfoTableName, activityInfoColumns, activityInfoKey)
)

// ReplaceIntoActivityInfoMaps replaces one or more rows in activity_info_maps table
func (mdb *DB) ReplaceIntoActivityInfoMaps(rows []sqldb.ActivityInfoMapsRow) (sql.Result, error) {
	for i := range rows {
		rows[i].ScheduledTime = mdb.converter.ToMySQLDateTime(rows[i].ScheduledTime)
		rows[i].StartedTime = mdb.converter.ToMySQLDateTime(rows[i].StartedTime)
		rows[i].LastHeartbeatUpdatedTime = mdb.converter.ToMySQLDateTime(rows[i].LastHeartbeatUpdatedTime)
		rows[i].ExpirationTime = mdb.converter.ToMySQLDateTime(rows[i].ExpirationTime)
	}
	return mdb.conn.NamedExec(setKeyInActivityInfoMapQry, rows)
}

// SelectFromActivityInfoMaps reads one or more rows from activity_info_maps table
func (mdb *DB) SelectFromActivityInfoMaps(filter *sqldb.ActivityInfoMapsFilter) ([]sqldb.ActivityInfoMapsRow, error) {
	var rows []sqldb.ActivityInfoMapsRow
	err := mdb.conn.Select(&rows, getActivityInfoMapQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = int64(filter.ShardID)
		rows[i].DomainID = filter.DomainID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
		rows[i].ScheduledTime = mdb.converter.FromMySQLDateTime(rows[i].ScheduledTime)
		rows[i].StartedTime = mdb.converter.FromMySQLDateTime(rows[i].StartedTime)
		rows[i].LastHeartbeatUpdatedTime = mdb.converter.FromMySQLDateTime(rows[i].LastHeartbeatUpdatedTime)
		rows[i].ExpirationTime = mdb.converter.FromMySQLDateTime(rows[i].ExpirationTime)
	}
	return rows, err
}

// DeleteFromActivityInfoMaps deletes one or more rows from activity_info_maps table
func (mdb *DB) DeleteFromActivityInfoMaps(filter *sqldb.ActivityInfoMapsFilter) (sql.Result, error) {
	if filter.ScheduleID != nil {
		return mdb.conn.Exec(deleteKeyInActivityInfoMapQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID, *filter.ScheduleID)
	}
	return mdb.conn.Exec(deleteActivityInfoMapQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
}

var (
	timerInfoColumns = []string{
		"version",
		"started_id",
		"expiry_time",
		"task_id",
	}
	timerInfoTableName = "timer_info_maps"
	timerInfoKey       = "timer_id"

	deleteTimerInfoMapSQLQuery      = makeDeleteMapQry(timerInfoTableName)
	setKeyInTimerInfoMapSQLQuery    = makeSetKeyInMapQry(timerInfoTableName, timerInfoColumns, timerInfoKey)
	deleteKeyInTimerInfoMapSQLQuery = makeDeleteKeyInMapQry(timerInfoTableName, timerInfoKey)
	getTimerInfoMapSQLQuery         = makeGetMapQryTemplate(timerInfoTableName, timerInfoColumns, timerInfoKey)
)

// ReplaceIntoTimerInfoMaps replaces one or more rows in timer_info_maps table
func (mdb *DB) ReplaceIntoTimerInfoMaps(rows []sqldb.TimerInfoMapsRow) (sql.Result, error) {
	for i := range rows {
		rows[i].ExpiryTime = mdb.converter.ToMySQLDateTime(rows[i].ExpiryTime)
	}
	return mdb.conn.NamedExec(setKeyInTimerInfoMapSQLQuery, rows)
}

// SelectFromTimerInfoMaps reads one or more rows from timer_info_maps table
func (mdb *DB) SelectFromTimerInfoMaps(filter *sqldb.TimerInfoMapsFilter) ([]sqldb.TimerInfoMapsRow, error) {
	var rows []sqldb.TimerInfoMapsRow
	err := mdb.conn.Select(&rows, getTimerInfoMapSQLQuery, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = int64(filter.ShardID)
		rows[i].DomainID = filter.DomainID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, err
}

// DeleteFromTimerInfoMaps deletes one or more rows from timer_info_maps table
func (mdb *DB) DeleteFromTimerInfoMaps(filter *sqldb.TimerInfoMapsFilter) (sql.Result, error) {
	if filter.TimerID != nil {
		return mdb.conn.Exec(deleteKeyInTimerInfoMapSQLQuery, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID, *filter.TimerID)
	}
	return mdb.conn.Exec(deleteTimerInfoMapSQLQuery, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
}

var (
	childExecutionInfoColumns = []string{
		"version",
		"initiated_event_batch_id",
		"initiated_event",
		"initiated_event_encoding",
		"started_id",
		"started_workflow_id",
		"started_run_id",
		"started_event",
		"started_event_encoding",
		"create_request_id",
		"domain_name",
		"workflow_type_name",
	}
	childExecutionInfoTableName = "child_execution_info_maps"
	childExecutionInfoKey       = "initiated_id"

	deleteChildExecutionInfoMapQry      = makeDeleteMapQry(childExecutionInfoTableName)
	setKeyInChildExecutionInfoMapQry    = makeSetKeyInMapQry(childExecutionInfoTableName, childExecutionInfoColumns, childExecutionInfoKey)
	deleteKeyInChildExecutionInfoMapQry = makeDeleteKeyInMapQry(childExecutionInfoTableName, childExecutionInfoKey)
	getChildExecutionInfoMapQry         = makeGetMapQryTemplate(childExecutionInfoTableName, childExecutionInfoColumns, childExecutionInfoKey)
)

// ReplaceIntoChildExecutionInfoMaps replaces one or more rows in child_execution_info_maps table
func (mdb *DB) ReplaceIntoChildExecutionInfoMaps(rows []sqldb.ChildExecutionInfoMapsRow) (sql.Result, error) {
	return mdb.conn.NamedExec(setKeyInChildExecutionInfoMapQry, rows)
}

// SelectFromChildExecutionInfoMaps reads one or more rows from child_execution_info_maps table
func (mdb *DB) SelectFromChildExecutionInfoMaps(filter *sqldb.ChildExecutionInfoMapsFilter) ([]sqldb.ChildExecutionInfoMapsRow, error) {
	var rows []sqldb.ChildExecutionInfoMapsRow
	err := mdb.conn.Select(&rows, getChildExecutionInfoMapQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = int64(filter.ShardID)
		rows[i].DomainID = filter.DomainID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, err
}

// DeleteFromChildExecutionInfoMaps deletes one or more rows from child_execution_info_maps table
func (mdb *DB) DeleteFromChildExecutionInfoMaps(filter *sqldb.ChildExecutionInfoMapsFilter) (sql.Result, error) {
	if filter.InitiatedID != nil {
		return mdb.conn.Exec(deleteKeyInChildExecutionInfoMapQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID, *filter.InitiatedID)
	}
	return mdb.conn.Exec(deleteChildExecutionInfoMapQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
}

var (
	requestCancelInfoColumns = []string{
		"version",
		"cancel_request_id",
	}
	requestCancelInfoTableName = "request_cancel_info_maps"
	requestCancelInfoKey       = "initiated_id"

	deleteRequestCancelInfoMapQry      = makeDeleteMapQry(requestCancelInfoTableName)
	setKeyInRequestCancelInfoMapQry    = makeSetKeyInMapQry(requestCancelInfoTableName, requestCancelInfoColumns, requestCancelInfoKey)
	deleteKeyInRequestCancelInfoMapQry = makeDeleteKeyInMapQry(requestCancelInfoTableName, requestCancelInfoKey)
	getRequestCancelInfoMapQry         = makeGetMapQryTemplate(requestCancelInfoTableName, requestCancelInfoColumns, requestCancelInfoKey)
)

// ReplaceIntoRequestCancelInfoMaps replaces one or more rows in request_cancel_info_maps table
func (mdb *DB) ReplaceIntoRequestCancelInfoMaps(rows []sqldb.RequestCancelInfoMapsRow) (sql.Result, error) {
	return mdb.conn.NamedExec(setKeyInRequestCancelInfoMapQry, rows)
}

// SelectFromRequestCancelInfoMaps reads one or more rows from request_cancel_info_maps table
func (mdb *DB) SelectFromRequestCancelInfoMaps(filter *sqldb.RequestCancelInfoMapsFilter) ([]sqldb.RequestCancelInfoMapsRow, error) {
	var rows []sqldb.RequestCancelInfoMapsRow
	err := mdb.conn.Select(&rows, getRequestCancelInfoMapQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = int64(filter.ShardID)
		rows[i].DomainID = filter.DomainID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, err
}

// DeleteFromRequestCancelInfoMaps deletes one or more rows from request_cancel_info_maps table
func (mdb *DB) DeleteFromRequestCancelInfoMaps(filter *sqldb.RequestCancelInfoMapsFilter) (sql.Result, error) {
	if filter.InitiatedID != nil {
		return mdb.conn.Exec(deleteKeyInRequestCancelInfoMapQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID, *filter.InitiatedID)
	}
	return mdb.conn.Exec(deleteRequestCancelInfoMapQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
}

var (
	signalInfoColumns = []string{
		"version",
		"signal_request_id",
		"signal_name",
		"input",
		"control",
	}
	signalInfoTableName = "signal_info_maps"
	signalInfoKey       = "initiated_id"

	deleteSignalInfoMapQry      = makeDeleteMapQry(signalInfoTableName)
	setKeyInSignalInfoMapQry    = makeSetKeyInMapQry(signalInfoTableName, signalInfoColumns, signalInfoKey)
	deleteKeyInSignalInfoMapQry = makeDeleteKeyInMapQry(signalInfoTableName, signalInfoKey)
	getSignalInfoMapQry         = makeGetMapQryTemplate(signalInfoTableName, signalInfoColumns, signalInfoKey)
)

// ReplaceIntoSignalInfoMaps replaces one or more rows in signal_info_maps table
func (mdb *DB) ReplaceIntoSignalInfoMaps(rows []sqldb.SignalInfoMapsRow) (sql.Result, error) {
	return mdb.conn.NamedExec(setKeyInSignalInfoMapQry, rows)
}

// SelectFromSignalInfoMaps reads one or more rows from signal_info_maps table
func (mdb *DB) SelectFromSignalInfoMaps(filter *sqldb.SignalInfoMapsFilter) ([]sqldb.SignalInfoMapsRow, error) {
	var rows []sqldb.SignalInfoMapsRow
	err := mdb.conn.Select(&rows, getSignalInfoMapQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = int64(filter.ShardID)
		rows[i].DomainID = filter.DomainID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, err
}

// DeleteFromSignalInfoMaps deletes one or more rows from signal_info_maps table
func (mdb *DB) DeleteFromSignalInfoMaps(filter *sqldb.SignalInfoMapsFilter) (sql.Result, error) {
	if filter.InitiatedID != nil {
		return mdb.conn.Exec(deleteKeyInSignalInfoMapQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID, *filter.InitiatedID)
	}
	return mdb.conn.Exec(deleteSignalInfoMapQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
}

var (
	bufferedReplicationTasksMapColumns = []string{
		"version",
		"next_event_id",
		"history",
		"history_encoding",
		"new_run_history",
		"new_run_history_encoding",
	}
	bufferedReplicationTasksNoNewRunHistoryMapColumns = []string{
		"version",
		"next_event_id",
		"history",
		"history_encoding",
	}
	bufferedReplicationTasksTableName = "buffered_replication_task_maps"
	bufferedReplicationTasksKey       = "first_event_id"

	deleteBufferedReplicationTasksMapQry                  = makeDeleteMapQry(bufferedReplicationTasksTableName)
	setKeyInBufferedReplicationTasksMapQry                = makeSetKeyInMapQry(bufferedReplicationTasksTableName, bufferedReplicationTasksMapColumns, bufferedReplicationTasksKey)
	setKeyInBufferedReplicationTasksNoNewRunHistoryMapQry = makeSetKeyInMapQry(bufferedReplicationTasksTableName, bufferedReplicationTasksNoNewRunHistoryMapColumns, bufferedReplicationTasksKey)
	deleteKeyInBufferedReplicationTasksMapQry             = makeDeleteKeyInMapQry(bufferedReplicationTasksTableName, bufferedReplicationTasksKey)
	getBufferedReplicationTasksMapQry                     = makeGetMapQryTemplate(bufferedReplicationTasksTableName, bufferedReplicationTasksMapColumns, bufferedReplicationTasksKey)
)

// ReplaceIntoBufferedReplicationTasks replaces one or more rows in buffered_replication_task_maps table
func (mdb *DB) ReplaceIntoBufferedReplicationTasks(row *sqldb.BufferedReplicationTaskMapsRow) (sql.Result, error) {
	if row.NewRunHistory != nil {
		return mdb.conn.NamedExec(setKeyInBufferedReplicationTasksMapQry, row)
	}
	return mdb.conn.NamedExec(setKeyInBufferedReplicationTasksNoNewRunHistoryMapQry, row)
}

// SelectFromBufferedReplicationTasks reads one or more rows from buffered_replication_tasks table
func (mdb *DB) SelectFromBufferedReplicationTasks(filter *sqldb.BufferedReplicationTaskMapsFilter) ([]sqldb.BufferedReplicationTaskMapsRow, error) {
	var rows []sqldb.BufferedReplicationTaskMapsRow
	err := mdb.conn.Select(&rows, getBufferedReplicationTasksMapQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = int64(filter.ShardID)
		rows[i].DomainID = filter.DomainID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, err
}

// DeleteFromBufferedReplicationTasks deletes one or more rows from buffered_replication_tasks table
func (mdb *DB) DeleteFromBufferedReplicationTasks(filter *sqldb.BufferedReplicationTaskMapsFilter) (sql.Result, error) {
	if filter.FirstEventID != nil {
		return mdb.conn.Exec(deleteKeyInBufferedReplicationTasksMapQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID, *filter.FirstEventID)
	}
	return mdb.conn.Exec(deleteBufferedReplicationTasksMapQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
}

const (
	deleteAllSignalsRequestedSetQry = `DELETE FROM signals_requested_sets
WHERE
shard_id = ? AND
domain_id = ? AND
workflow_id = ? AND
run_id = ?
`

	createSignalsRequestedSetQry = `INSERT IGNORE INTO signals_requested_sets
(shard_id, domain_id, workflow_id, run_id, signal_id) VALUES
(:shard_id, :domain_id, :workflow_id, :run_id, :signal_id)`

	deleteSignalsRequestedSetQry = `DELETE FROM signals_requested_sets
WHERE 
shard_id = ? AND
domain_id = ? AND
workflow_id = ? AND
run_id = ? AND
signal_id = ?`

	getSignalsRequestedSetQry = `SELECT signal_id FROM signals_requested_sets WHERE
shard_id = ? AND
domain_id = ? AND
workflow_id = ? AND
run_id = ?`
)

// InsertIntoSignalsRequestedSets inserts one or more rows into signals_requested_sets table
func (mdb *DB) InsertIntoSignalsRequestedSets(rows []sqldb.SignalsRequestedSetsRow) (sql.Result, error) {
	return mdb.conn.NamedExec(createSignalsRequestedSetQry, rows)
}

// SelectFromSignalsRequestedSets reads one or more rows from signals_requested_sets table
func (mdb *DB) SelectFromSignalsRequestedSets(filter *sqldb.SignalsRequestedSetsFilter) ([]sqldb.SignalsRequestedSetsRow, error) {
	var rows []sqldb.SignalsRequestedSetsRow
	err := mdb.conn.Select(&rows, getSignalsRequestedSetQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = int64(filter.ShardID)
		rows[i].DomainID = filter.DomainID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, err
}

// DeleteFromSignalsRequestedSets deletes one or more rows from signals_requested_sets table
func (mdb *DB) DeleteFromSignalsRequestedSets(filter *sqldb.SignalsRequestedSetsFilter) (sql.Result, error) {
	if filter.SignalID != nil {
		return mdb.conn.Exec(deleteSignalsRequestedSetQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID, *filter.SignalID)
	}
	return mdb.conn.Exec(deleteAllSignalsRequestedSetQry, filter.ShardID, filter.DomainID, filter.WorkflowID, filter.RunID)
}
