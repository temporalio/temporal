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

package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	deleteMapQueryTemplate = `DELETE FROM %v
WHERE
shard_id = $1 AND
namespace_id = $2 AND
workflow_id = $3 AND
run_id = $4`

	// %[2]v is the columns of the value struct (i.e. no primary key columns), comma separated
	// %[3]v should be %[2]v with colons prepended.
	// i.e. %[3]v = ",".join(":" + s for s in %[2]v)
	// %[5]v should be %[2]v with "excluded." prepended.
	// i.e. %[5]v = ",".join("excluded." + s for s in %[2]v)
	// So that this query can be used with BindNamed
	// %[4]v should be the name of the key associated with the map
	// e.g. for ActivityInfo it is "schedule_id"
	setKeyInMapQueryTemplate = `INSERT INTO %[1]v
(shard_id, namespace_id, workflow_id, run_id, %[4]v, %[2]v)
VALUES
(:shard_id, :namespace_id, :workflow_id, :run_id, :%[4]v, %[3]v)
ON CONFLICT (shard_id, namespace_id, workflow_id, run_id, %[4]v) DO UPDATE
	SET (shard_id, namespace_id, workflow_id, run_id, %[4]v, %[2]v)
  	  = (excluded.shard_id, excluded.namespace_id, excluded.workflow_id, excluded.run_id, excluded.%[4]v, %[5]v)`

	// %[2]v is the name of the key
	deleteKeyInMapQueryTemplate = `DELETE FROM %[1]v
WHERE
shard_id = $1 AND
namespace_id = $2 AND
workflow_id = $3 AND
run_id = $4 AND
%[2]v = $5`

	// %[1]v is the name of the table
	// %[2]v is the name of the key
	// %[3]v is the value columns, separated by commas
	getMapQueryTemplate = `SELECT %[2]v, %[3]v FROM %[1]v
WHERE
shard_id = $1 AND
namespace_id = $2 AND
workflow_id = $3 AND
run_id = $4`
)

const (
	deleteAllSignalsRequestedSetQuery = `DELETE FROM signals_requested_sets
WHERE
shard_id = $1 AND
namespace_id = $2 AND
workflow_id = $3 AND
run_id = $4
`

	createSignalsRequestedSetQuery = `INSERT INTO signals_requested_sets
(shard_id, namespace_id, workflow_id, run_id, signal_id) VALUES
(:shard_id, :namespace_id, :workflow_id, :run_id, :signal_id)
ON CONFLICT (shard_id, namespace_id, workflow_id, run_id, signal_id) DO NOTHING`

	deleteSignalsRequestedSetQuery = `DELETE FROM signals_requested_sets
WHERE
shard_id = $1 AND
namespace_id = $2 AND
workflow_id = $3 AND
run_id = $4 AND
signal_id = $5`

	getSignalsRequestedSetQuery = `SELECT signal_id FROM signals_requested_sets WHERE
shard_id = $1 AND
namespace_id = $2 AND
workflow_id = $3 AND
run_id = $4`
)

func stringMap(a []string, f func(string) string) []string {
	b := make([]string, len(a))
	for i, v := range a {
		b[i] = f(v)
	}
	return b
}

func makeDeleteMapQry(tableName string) string {
	return fmt.Sprintf(deleteMapQueryTemplate, tableName)
}

func makeSetKeyInMapQry(tableName string, nonPrimaryKeyColumns []string, mapKeyName string) string {
	return fmt.Sprintf(setKeyInMapQueryTemplate,
		tableName,
		strings.Join(nonPrimaryKeyColumns, ","),
		strings.Join(stringMap(nonPrimaryKeyColumns, func(x string) string {
			return ":" + x
		}), ","),
		mapKeyName,
		strings.Join(stringMap(nonPrimaryKeyColumns, func(x string) string {
			return "excluded." + x
		}), ","))
}

func makeDeleteKeyInMapQry(tableName string, mapKeyName string) string {
	return fmt.Sprintf(deleteKeyInMapQueryTemplate,
		tableName,
		mapKeyName)
}

func makeGetMapQryTemplate(tableName string, nonPrimaryKeyColumns []string, mapKeyName string) string {
	return fmt.Sprintf(getMapQueryTemplate,
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
func (pdb *db) ReplaceIntoActivityInfoMaps(
	ctx context.Context,
	rows []sqlplugin.ActivityInfoMapsRow,
) (sql.Result, error) {
	return pdb.conn.NamedExecContext(ctx,
		setKeyInActivityInfoMapQry,
		rows,
	)
}

// SelectFromActivityInfoMaps reads one or more rows from activity_info_maps table
func (pdb *db) SelectFromActivityInfoMaps(
	ctx context.Context,
	filter sqlplugin.ActivityInfoMapsSelectFilter,
) ([]sqlplugin.ActivityInfoMapsRow, error) {
	var rows []sqlplugin.ActivityInfoMapsRow
	if err := pdb.conn.SelectContext(ctx,
		&rows, getActivityInfoMapQry,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	); err != nil {
		return nil, err
	}
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, nil
}

// DeleteFromActivityInfoMaps deletes one or more rows from activity_info_maps table
func (pdb *db) DeleteFromActivityInfoMaps(
	ctx context.Context,
	filter sqlplugin.ActivityInfoMapsDeleteFilter,
) (sql.Result, error) {
	if filter.ScheduleID != nil {
		return pdb.conn.ExecContext(ctx,
			deleteKeyInActivityInfoMapQry,
			filter.ShardID,
			filter.NamespaceID,
			filter.WorkflowID,
			filter.RunID,
			*filter.ScheduleID,
		)
	}
	return pdb.conn.ExecContext(ctx,
		deleteActivityInfoMapQry,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
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
func (pdb *db) ReplaceIntoTimerInfoMaps(
	ctx context.Context,
	rows []sqlplugin.TimerInfoMapsRow,
) (sql.Result, error) {
	return pdb.conn.NamedExecContext(ctx,
		setKeyInTimerInfoMapSQLQuery,
		rows,
	)
}

// SelectFromTimerInfoMaps reads one or more rows from timer_info_maps table
func (pdb *db) SelectFromTimerInfoMaps(
	ctx context.Context,
	filter sqlplugin.TimerInfoMapsSelectFilter,
) ([]sqlplugin.TimerInfoMapsRow, error) {
	var rows []sqlplugin.TimerInfoMapsRow
	if err := pdb.conn.SelectContext(ctx,
		&rows,
		getTimerInfoMapSQLQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	); err != nil {
		return nil, err
	}
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, nil
}

// DeleteFromTimerInfoMaps deletes one or more rows from timer_info_maps table
func (pdb *db) DeleteFromTimerInfoMaps(
	ctx context.Context,
	filter sqlplugin.TimerInfoMapsDeleteFilter,
) (sql.Result, error) {
	if filter.TimerID != nil {
		return pdb.conn.ExecContext(ctx,
			deleteKeyInTimerInfoMapSQLQuery,
			filter.ShardID,
			filter.NamespaceID,
			filter.WorkflowID,
			filter.RunID,
			*filter.TimerID,
		)
	}
	return pdb.conn.ExecContext(ctx,
		deleteTimerInfoMapSQLQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
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
func (pdb *db) ReplaceIntoChildExecutionInfoMaps(
	ctx context.Context,
	rows []sqlplugin.ChildExecutionInfoMapsRow,
) (sql.Result, error) {
	return pdb.conn.NamedExecContext(ctx,
		setKeyInChildExecutionInfoMapQry,
		rows,
	)
}

// SelectFromChildExecutionInfoMaps reads one or more rows from child_execution_info_maps table
func (pdb *db) SelectFromChildExecutionInfoMaps(
	ctx context.Context,
	filter sqlplugin.ChildExecutionInfoMapsSelectFilter,
) ([]sqlplugin.ChildExecutionInfoMapsRow, error) {
	var rows []sqlplugin.ChildExecutionInfoMapsRow
	if err := pdb.conn.SelectContext(ctx,
		&rows,
		getChildExecutionInfoMapQry,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	); err != nil {
		return nil, err
	}
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, nil
}

// DeleteFromChildExecutionInfoMaps deletes one or more rows from child_execution_info_maps table
func (pdb *db) DeleteFromChildExecutionInfoMaps(
	ctx context.Context,
	filter sqlplugin.ChildExecutionInfoMapsDeleteFilter,
) (sql.Result, error) {
	if filter.InitiatedID != nil {
		return pdb.conn.ExecContext(ctx,
			deleteKeyInChildExecutionInfoMapQry,
			filter.ShardID,
			filter.NamespaceID,
			filter.WorkflowID,
			filter.RunID,
			*filter.InitiatedID,
		)
	}
	return pdb.conn.ExecContext(ctx,
		deleteChildExecutionInfoMapQry,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
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
func (pdb *db) ReplaceIntoRequestCancelInfoMaps(
	ctx context.Context,
	rows []sqlplugin.RequestCancelInfoMapsRow,
) (sql.Result, error) {
	return pdb.conn.NamedExecContext(ctx,
		setKeyInRequestCancelInfoMapQry,
		rows,
	)
}

// SelectFromRequestCancelInfoMaps reads one or more rows from request_cancel_info_maps table
func (pdb *db) SelectFromRequestCancelInfoMaps(
	ctx context.Context,
	filter sqlplugin.RequestCancelInfoMapsSelectFilter,
) ([]sqlplugin.RequestCancelInfoMapsRow, error) {
	var rows []sqlplugin.RequestCancelInfoMapsRow
	if err := pdb.conn.SelectContext(ctx,
		&rows,
		getRequestCancelInfoMapQry,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	); err != nil {
		return nil, err
	}
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, nil
}

// DeleteFromRequestCancelInfoMaps deletes one or more rows from request_cancel_info_maps table
func (pdb *db) DeleteFromRequestCancelInfoMaps(
	ctx context.Context,
	filter sqlplugin.RequestCancelInfoMapsDeleteFilter,
) (sql.Result, error) {
	if filter.InitiatedID != nil {
		return pdb.conn.ExecContext(ctx,
			deleteKeyInRequestCancelInfoMapQry,
			filter.ShardID,
			filter.NamespaceID,
			filter.WorkflowID,
			filter.RunID,
			*filter.InitiatedID,
		)
	}
	return pdb.conn.ExecContext(ctx,
		deleteRequestCancelInfoMapQry,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
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
func (pdb *db) ReplaceIntoSignalInfoMaps(
	ctx context.Context,
	rows []sqlplugin.SignalInfoMapsRow,
) (sql.Result, error) {
	return pdb.conn.NamedExecContext(ctx,
		setKeyInSignalInfoMapQry,
		rows,
	)
}

// SelectFromSignalInfoMaps reads one or more rows from signal_info_maps table
func (pdb *db) SelectFromSignalInfoMaps(
	ctx context.Context,
	filter sqlplugin.SignalInfoMapsSelectFilter,
) ([]sqlplugin.SignalInfoMapsRow, error) {
	var rows []sqlplugin.SignalInfoMapsRow
	if err := pdb.conn.SelectContext(ctx,
		&rows,
		getSignalInfoMapQry,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	); err != nil {
		return nil, err
	}
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, nil
}

// DeleteFromSignalInfoMaps deletes one or more rows from signal_info_maps table
func (pdb *db) DeleteFromSignalInfoMaps(
	ctx context.Context,
	filter sqlplugin.SignalInfoMapsDeleteFilter,
) (sql.Result, error) {
	if filter.InitiatedID != nil {
		return pdb.conn.ExecContext(ctx,
			deleteKeyInSignalInfoMapQry,
			filter.ShardID,
			filter.NamespaceID,
			filter.WorkflowID,
			filter.RunID,
			*filter.InitiatedID,
		)
	}
	return pdb.conn.ExecContext(ctx,
		deleteSignalInfoMapQry,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
}

// InsertIntoSignalsRequestedSets inserts one or more rows into signals_requested_sets table
func (pdb *db) ReplaceIntoSignalsRequestedSets(
	ctx context.Context,
	rows []sqlplugin.SignalsRequestedSetsRow,
) (sql.Result, error) {
	return pdb.conn.NamedExecContext(ctx,
		createSignalsRequestedSetQuery,
		rows,
	)
}

// SelectFromSignalsRequestedSets reads one or more rows from signals_requested_sets table
func (pdb *db) SelectFromSignalsRequestedSets(
	ctx context.Context,
	filter sqlplugin.SignalsRequestedSetsSelectFilter,
) ([]sqlplugin.SignalsRequestedSetsRow, error) {
	var rows []sqlplugin.SignalsRequestedSetsRow
	if err := pdb.conn.SelectContext(ctx,
		&rows,
		getSignalsRequestedSetQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	); err != nil {
		return nil, err
	}
	for i := 0; i < len(rows); i++ {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}
	return rows, nil
}

// DeleteFromSignalsRequestedSets deletes one or more rows from signals_requested_sets table
func (pdb *db) DeleteFromSignalsRequestedSets(
	ctx context.Context,
	filter sqlplugin.SignalsRequestedSetsDeleteFilter,
) (sql.Result, error) {
	if filter.SignalID != nil {
		return pdb.conn.ExecContext(ctx,
			deleteSignalsRequestedSetQuery,
			filter.ShardID,
			filter.NamespaceID,
			filter.WorkflowID,
			filter.RunID,
			*filter.SignalID,
		)
	}
	return pdb.conn.ExecContext(ctx,
		deleteAllSignalsRequestedSetQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
}
