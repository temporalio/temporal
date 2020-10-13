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
	"database/sql"
	"fmt"
	"strings"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	deleteMapQryTemplate = `DELETE FROM %v
WHERE
shard_id = ? AND
namespace_id = ? AND
workflow_id = ? AND
run_id = ?`

	// %[2]v is the columns of the value struct (i.e. no primary key columns), comma separated
	// %[3]v should be %[2]v with colons prepended.
	// i.e. %[3]v = ",".join(":" + s for s in %[2]v)
	// %[4]v should be %[2]v in the format of n=VALUES(n).
	// i.e. %[4]v = ",".join(s + "=VALUES(" + s + ")" for s in %[2]v)
	// So that this query can be used with BindNamed
	// %[5]v should be the name of the key associated with the map
	// e.g. for ActivityInfo it is "schedule_id"
	setKeyInMapQryTemplate = `INSERT INTO %[1]v
(shard_id, namespace_id, workflow_id, run_id, %[5]v, %[2]v)
VALUES
(:shard_id, :namespace_id, :workflow_id, :run_id, :%[5]v, %[3]v) 
ON DUPLICATE KEY UPDATE %[5]v=VALUES(%[5]v), %[4]v;`

	// %[2]v is the name of the key
	deleteKeyInMapQryTemplate = `DELETE FROM %[1]v
WHERE
shard_id = ? AND
namespace_id = ? AND
workflow_id = ? AND
run_id = ? AND
%[2]v = ?`

	// %[1]v is the name of the table
	// %[2]v is the name of the key
	// %[3]v is the value columns, separated by commas
	getMapQryTemplate = `SELECT %[2]v, %[3]v FROM %[1]v
WHERE
shard_id = ? AND
namespace_id = ? AND
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
		strings.Join(stringMap(nonPrimaryKeyColumns, func(x string) string {
			return x + "=VALUES(" + x + ")"
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
	// Omit shard_id, run_id, namespace_id, workflow_id, schedule_id since they're in the primary key
	activityInfoColumns = []string{
		"data",
		"data_encoding",
	}
	activityInfoTableName = "activity_info_maps"
	activityInfoKey       = "schedule_id"

	deleteActivityInfoMapQry      = makeDeleteMapQry(activityInfoTableName)
	setKeyInActivityInfoMapQry    = makeSetKeyInMapQry(activityInfoTableName, activityInfoColumns, activityInfoKey)
	deleteKeyInActivityInfoMapQry = makeDeleteKeyInMapQry(activityInfoTableName, activityInfoKey)
	getActivityInfoMapQry         = makeGetMapQryTemplate(activityInfoTableName, activityInfoColumns, activityInfoKey)
)

// ReplaceIntoActivityInfoMaps replaces one or more rows in activity_info_maps table
func (mdb *db) ReplaceIntoActivityInfoMaps(rows []sqlplugin.ActivityInfoMapsRow) (sql.Result, error) {
	q, args, err := mdb.db.BindNamed(setKeyInActivityInfoMapQry, rows)
	if err != nil {
		return nil, err
	}

	q = expandBatchInsertQuery(q, len(rows))

	return mdb.conn.Exec(q, args...)
}

func expandBatchInsertQuery(q string, rowCount int) string {
	b := strings.Builder{}
	// Inclusive start index
	valStartIdx := strings.Index(q, "(?,")

	// Add 2 to get exclusive end index
	valEndIdx := strings.Index(q, "?)") + 2

	// Write initial half of query
	b.WriteString(q[0:valEndIdx])

	// Create rowCount-1 more (?, ?, ?) placeholders
	for i := 0; i < rowCount-1; i++ {
		b.WriteString(",")
		b.WriteString(q[valStartIdx:valEndIdx])
	}

	// sqlx does not like VALUES() and is generating an artifact
	// into the sql query when we don't use a semicolon when we don't terminate with a `;`.
	// Removing that while writing the second half of the query if it exists.
	lastIdx := strings.LastIndex(q, ",")
	if lastIdx == -1 || lastIdx <= valEndIdx {
		b.WriteString(q[valEndIdx:])
	} else {
		b.WriteString(q[valEndIdx:lastIdx])
	}

	return b.String()
}

// SelectFromActivityInfoMaps reads one or more rows from activity_info_maps table
func (mdb *db) SelectFromActivityInfoMaps(filter sqlplugin.ActivityInfoMapsSelectFilter) ([]sqlplugin.ActivityInfoMapsRow, error) {
	var rows []sqlplugin.ActivityInfoMapsRow
	err := mdb.conn.Select(&rows, getActivityInfoMapQry, filter.ShardID, filter.NamespaceID, filter.WorkflowID, filter.RunID)
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, err
}

// DeleteFromActivityInfoMaps deletes one or more rows from activity_info_maps table
func (mdb *db) DeleteFromActivityInfoMaps(filter sqlplugin.ActivityInfoMapsDeleteFilter) (sql.Result, error) {
	if filter.ScheduleID != nil {
		return mdb.conn.Exec(deleteKeyInActivityInfoMapQry, filter.ShardID, filter.NamespaceID, filter.WorkflowID, filter.RunID, *filter.ScheduleID)
	}
	return mdb.conn.Exec(deleteActivityInfoMapQry, filter.ShardID, filter.NamespaceID, filter.WorkflowID, filter.RunID)
}

var (
	timerInfoColumns = []string{
		"data",
		"data_encoding",
	}
	timerInfoTableName = "timer_info_maps"
	timerInfoKey       = "timer_id"

	deleteTimerInfoMapSQLQuery      = makeDeleteMapQry(timerInfoTableName)
	setKeyInTimerInfoMapSQLQuery    = makeSetKeyInMapQry(timerInfoTableName, timerInfoColumns, timerInfoKey)
	deleteKeyInTimerInfoMapSQLQuery = makeDeleteKeyInMapQry(timerInfoTableName, timerInfoKey)
	getTimerInfoMapSQLQuery         = makeGetMapQryTemplate(timerInfoTableName, timerInfoColumns, timerInfoKey)
)

// ReplaceIntoTimerInfoMaps replaces one or more rows in timer_info_maps table
func (mdb *db) ReplaceIntoTimerInfoMaps(rows []sqlplugin.TimerInfoMapsRow) (sql.Result, error) {
	q, args, err := mdb.db.BindNamed(setKeyInTimerInfoMapSQLQuery, rows)
	if err != nil {
		return nil, err
	}

	q = expandBatchInsertQuery(q, len(rows))

	return mdb.conn.Exec(q, args...)

}

// SelectFromTimerInfoMaps reads one or more rows from timer_info_maps table
func (mdb *db) SelectFromTimerInfoMaps(filter sqlplugin.TimerInfoMapsSelectFilter) ([]sqlplugin.TimerInfoMapsRow, error) {
	var rows []sqlplugin.TimerInfoMapsRow
	err := mdb.conn.Select(&rows, getTimerInfoMapSQLQuery, filter.ShardID, filter.NamespaceID, filter.WorkflowID, filter.RunID)
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, err
}

// DeleteFromTimerInfoMaps deletes one or more rows from timer_info_maps table
func (mdb *db) DeleteFromTimerInfoMaps(filter sqlplugin.TimerInfoMapsDeleteFilter) (sql.Result, error) {
	if filter.TimerID != nil {
		return mdb.conn.Exec(deleteKeyInTimerInfoMapSQLQuery, filter.ShardID, filter.NamespaceID, filter.WorkflowID, filter.RunID, *filter.TimerID)
	}
	return mdb.conn.Exec(deleteTimerInfoMapSQLQuery, filter.ShardID, filter.NamespaceID, filter.WorkflowID, filter.RunID)
}

var (
	childExecutionInfoColumns = []string{
		"data",
		"data_encoding",
	}
	childExecutionInfoTableName = "child_execution_info_maps"
	childExecutionInfoKey       = "initiated_id"

	deleteChildExecutionInfoMapQry      = makeDeleteMapQry(childExecutionInfoTableName)
	setKeyInChildExecutionInfoMapQry    = makeSetKeyInMapQry(childExecutionInfoTableName, childExecutionInfoColumns, childExecutionInfoKey)
	deleteKeyInChildExecutionInfoMapQry = makeDeleteKeyInMapQry(childExecutionInfoTableName, childExecutionInfoKey)
	getChildExecutionInfoMapQry         = makeGetMapQryTemplate(childExecutionInfoTableName, childExecutionInfoColumns, childExecutionInfoKey)
)

// ReplaceIntoChildExecutionInfoMaps replaces one or more rows in child_execution_info_maps table
func (mdb *db) ReplaceIntoChildExecutionInfoMaps(rows []sqlplugin.ChildExecutionInfoMapsRow) (sql.Result, error) {
	q, args, err := mdb.db.BindNamed(setKeyInChildExecutionInfoMapQry, rows)
	if err != nil {
		return nil, err
	}

	q = expandBatchInsertQuery(q, len(rows))

	return mdb.conn.Exec(q, args...)

}

// SelectFromChildExecutionInfoMaps reads one or more rows from child_execution_info_maps table
func (mdb *db) SelectFromChildExecutionInfoMaps(filter sqlplugin.ChildExecutionInfoMapsSelectFilter) ([]sqlplugin.ChildExecutionInfoMapsRow, error) {
	var rows []sqlplugin.ChildExecutionInfoMapsRow
	err := mdb.conn.Select(&rows, getChildExecutionInfoMapQry, filter.ShardID, filter.NamespaceID, filter.WorkflowID, filter.RunID)
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, err
}

// DeleteFromChildExecutionInfoMaps deletes one or more rows from child_execution_info_maps table
func (mdb *db) DeleteFromChildExecutionInfoMaps(filter sqlplugin.ChildExecutionInfoMapsDeleteFilter) (sql.Result, error) {
	if filter.InitiatedID != nil {
		return mdb.conn.Exec(deleteKeyInChildExecutionInfoMapQry, filter.ShardID, filter.NamespaceID, filter.WorkflowID, filter.RunID, *filter.InitiatedID)
	}
	return mdb.conn.Exec(deleteChildExecutionInfoMapQry, filter.ShardID, filter.NamespaceID, filter.WorkflowID, filter.RunID)
}

var (
	requestCancelInfoColumns = []string{
		"data",
		"data_encoding",
	}
	requestCancelInfoTableName = "request_cancel_info_maps"
	requestCancelInfoKey       = "initiated_id"

	deleteRequestCancelInfoMapQry      = makeDeleteMapQry(requestCancelInfoTableName)
	setKeyInRequestCancelInfoMapQry    = makeSetKeyInMapQry(requestCancelInfoTableName, requestCancelInfoColumns, requestCancelInfoKey)
	deleteKeyInRequestCancelInfoMapQry = makeDeleteKeyInMapQry(requestCancelInfoTableName, requestCancelInfoKey)
	getRequestCancelInfoMapQry         = makeGetMapQryTemplate(requestCancelInfoTableName, requestCancelInfoColumns, requestCancelInfoKey)
)

// ReplaceIntoRequestCancelInfoMaps replaces one or more rows in request_cancel_info_maps table
func (mdb *db) ReplaceIntoRequestCancelInfoMaps(rows []sqlplugin.RequestCancelInfoMapsRow) (sql.Result, error) {
	q, args, err := mdb.db.BindNamed(setKeyInRequestCancelInfoMapQry, rows)
	if err != nil {
		return nil, err
	}

	q = expandBatchInsertQuery(q, len(rows))

	return mdb.conn.Exec(q, args...)

}

// SelectFromRequestCancelInfoMaps reads one or more rows from request_cancel_info_maps table
func (mdb *db) SelectFromRequestCancelInfoMaps(filter sqlplugin.RequestCancelInfoMapsSelectFilter) ([]sqlplugin.RequestCancelInfoMapsRow, error) {
	var rows []sqlplugin.RequestCancelInfoMapsRow
	err := mdb.conn.Select(&rows, getRequestCancelInfoMapQry, filter.ShardID, filter.NamespaceID, filter.WorkflowID, filter.RunID)
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, err
}

// DeleteFromRequestCancelInfoMaps deletes one or more rows from request_cancel_info_maps table
func (mdb *db) DeleteFromRequestCancelInfoMaps(filter sqlplugin.RequestCancelInfoMapsDeleteFilter) (sql.Result, error) {
	if filter.InitiatedID != nil {
		return mdb.conn.Exec(deleteKeyInRequestCancelInfoMapQry, filter.ShardID, filter.NamespaceID, filter.WorkflowID, filter.RunID, *filter.InitiatedID)
	}
	return mdb.conn.Exec(deleteRequestCancelInfoMapQry, filter.ShardID, filter.NamespaceID, filter.WorkflowID, filter.RunID)
}

var (
	signalInfoColumns = []string{
		"data",
		"data_encoding",
	}
	signalInfoTableName = "signal_info_maps"
	signalInfoKey       = "initiated_id"

	deleteSignalInfoMapQry      = makeDeleteMapQry(signalInfoTableName)
	setKeyInSignalInfoMapQry    = makeSetKeyInMapQry(signalInfoTableName, signalInfoColumns, signalInfoKey)
	deleteKeyInSignalInfoMapQry = makeDeleteKeyInMapQry(signalInfoTableName, signalInfoKey)
	getSignalInfoMapQry         = makeGetMapQryTemplate(signalInfoTableName, signalInfoColumns, signalInfoKey)
)

// ReplaceIntoSignalInfoMaps replaces one or more rows in signal_info_maps table
func (mdb *db) ReplaceIntoSignalInfoMaps(rows []sqlplugin.SignalInfoMapsRow) (sql.Result, error) {
	q, args, err := mdb.db.BindNamed(setKeyInSignalInfoMapQry, rows)
	if err != nil {
		return nil, err
	}

	q = expandBatchInsertQuery(q, len(rows))

	return mdb.conn.Exec(q, args...)
}

// SelectFromSignalInfoMaps reads one or more rows from signal_info_maps table
func (mdb *db) SelectFromSignalInfoMaps(filter sqlplugin.SignalInfoMapsSelectFilter) ([]sqlplugin.SignalInfoMapsRow, error) {
	var rows []sqlplugin.SignalInfoMapsRow
	err := mdb.conn.Select(&rows, getSignalInfoMapQry, filter.ShardID, filter.NamespaceID, filter.WorkflowID, filter.RunID)
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, err
}

// DeleteFromSignalInfoMaps deletes one or more rows from signal_info_maps table
func (mdb *db) DeleteFromSignalInfoMaps(filter sqlplugin.SignalInfoMapsDeleteFilter) (sql.Result, error) {
	if filter.InitiatedID != nil {
		return mdb.conn.Exec(deleteKeyInSignalInfoMapQry, filter.ShardID, filter.NamespaceID, filter.WorkflowID, filter.RunID, *filter.InitiatedID)
	}
	return mdb.conn.Exec(deleteSignalInfoMapQry, filter.ShardID, filter.NamespaceID, filter.WorkflowID, filter.RunID)
}

const (
	deleteAllSignalsRequestedSetQry = `DELETE FROM signals_requested_sets
WHERE
shard_id = ? AND
namespace_id = ? AND
workflow_id = ? AND
run_id = ?
`

	createSignalsRequestedSetQry = `INSERT INTO signals_requested_sets
(shard_id, namespace_id, workflow_id, run_id, signal_id) VALUES
(:shard_id, :namespace_id, :workflow_id, :run_id, :signal_id)
ON DUPLICATE KEY UPDATE signal_id=VALUES(signal_id);`

	deleteSignalsRequestedSetQry = `DELETE FROM signals_requested_sets
WHERE 
shard_id = ? AND
namespace_id = ? AND
workflow_id = ? AND
run_id = ? AND
signal_id = ?`

	getSignalsRequestedSetQry = `SELECT signal_id FROM signals_requested_sets WHERE
shard_id = ? AND
namespace_id = ? AND
workflow_id = ? AND
run_id = ?`
)

// InsertIntoSignalsRequestedSets inserts one or more rows into signals_requested_sets table
func (mdb *db) ReplaceIntoSignalsRequestedSets(rows []sqlplugin.SignalsRequestedSetsRow) (sql.Result, error) {
	q, args, err := mdb.db.BindNamed(createSignalsRequestedSetQry, rows)
	if err != nil {
		return nil, err
	}

	q = expandBatchInsertQuery(q, len(rows))

	return mdb.conn.Exec(q, args...)
}

// SelectFromSignalsRequestedSets reads one or more rows from signals_requested_sets table
func (mdb *db) SelectFromSignalsRequestedSets(filter sqlplugin.SignalsRequestedSetsSelectFilter) ([]sqlplugin.SignalsRequestedSetsRow, error) {
	var rows []sqlplugin.SignalsRequestedSetsRow
	err := mdb.conn.Select(&rows, getSignalsRequestedSetQry, filter.ShardID, filter.NamespaceID, filter.WorkflowID, filter.RunID)
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, err
}

// DeleteFromSignalsRequestedSets deletes one or more rows from signals_requested_sets table
func (mdb *db) DeleteFromSignalsRequestedSets(filter sqlplugin.SignalsRequestedSetsDeleteFilter) (sql.Result, error) {
	if filter.SignalID != nil {
		return mdb.conn.Exec(deleteSignalsRequestedSetQry, filter.ShardID, filter.NamespaceID, filter.WorkflowID, filter.RunID, *filter.SignalID)
	}
	return mdb.conn.Exec(deleteAllSignalsRequestedSetQry, filter.ShardID, filter.NamespaceID, filter.WorkflowID, filter.RunID)
}
