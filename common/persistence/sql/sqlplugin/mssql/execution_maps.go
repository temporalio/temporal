package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

const (
	deleteMapQueryTemplate = `DELETE FROM %v
WHERE
shard_id = ? AND
namespace_id = ? AND
workflow_id = ? AND
run_id = ?`

	// %[1]v is the name of the table
	// %[2]v is the columns of the value struct (i.e. no primary key columns), comma separated
	// %[3]v should be %[2]v as ":col AS col" projections, comma separated.
	// i.e. %[3]v = ",".join(":" + s + " AS " + s for s in %[2]v)
	// %[4]v should be the name of the key associated with the map
	// e.g. for ActivityInfo it is "schedule_id"
	// %[5]v should be %[2]v as "col = source.col" assignments, comma separated.
	// i.e. %[5]v = ",".join(s + " = source." + s for s in %[2]v)
	// %[6]v should be %[2]v with "source." prepended, comma separated.
	// i.e. %[6]v = ",".join("source." + s for s in %[2]v)
	// So that this query can be used with BindNamed. sqlx cannot expand a
	// MERGE statement into multi-row form for a slice argument, so this
	// query must be executed via namedExecPerRow.
	setKeyInMapQueryTemplate = `MERGE %[1]v WITH (HOLDLOCK) AS target
USING (
SELECT :shard_id AS shard_id, :namespace_id AS namespace_id, :workflow_id AS workflow_id, :run_id AS run_id, :%[4]v AS %[4]v, %[3]v
) AS source
ON target.shard_id = source.shard_id AND target.namespace_id = source.namespace_id AND target.workflow_id = source.workflow_id AND target.run_id = source.run_id AND target.%[4]v = source.%[4]v
WHEN MATCHED THEN UPDATE SET %[5]v
WHEN NOT MATCHED THEN INSERT (shard_id, namespace_id, workflow_id, run_id, %[4]v, %[2]v)
VALUES (source.shard_id, source.namespace_id, source.workflow_id, source.run_id, source.%[4]v, %[6]v);`

	// %[2]v is the name of the key
	// NOTE: sqlx only support ? when doing `sqlx.In` expanding query
	deleteKeyInMapQueryTemplate = `DELETE FROM %[1]v
WHERE
shard_id = ? AND
namespace_id = ? AND
workflow_id = ? AND
run_id = ? AND
%[2]v IN ( ? )`

	// %[1]v is the name of the table
	// %[2]v is the name of the key
	// %[3]v is the value columns, separated by commas
	getMapQueryTemplate = `SELECT %[2]v, %[3]v FROM %[1]v
WHERE
shard_id = ? AND
namespace_id = ? AND
workflow_id = ? AND
run_id = ?`
)

const (
	deleteAllSignalsRequestedSetQuery = `DELETE FROM signals_requested_sets
WHERE
shard_id = ? AND
namespace_id = ? AND
workflow_id = ? AND
run_id = ?
`

	// DO NOTHING semantics: the WHEN MATCHED clause is omitted. Executed via
	// namedExecPerRow because sqlx cannot expand MERGE over a row slice.
	createSignalsRequestedSetQuery = `MERGE signals_requested_sets WITH (HOLDLOCK) AS target
USING (
SELECT :shard_id AS shard_id, :namespace_id AS namespace_id, :workflow_id AS workflow_id, :run_id AS run_id, :signal_id AS signal_id
) AS source
ON target.shard_id = source.shard_id AND target.namespace_id = source.namespace_id AND target.workflow_id = source.workflow_id AND target.run_id = source.run_id AND target.signal_id = source.signal_id
WHEN NOT MATCHED THEN INSERT (shard_id, namespace_id, workflow_id, run_id, signal_id)
VALUES (source.shard_id, source.namespace_id, source.workflow_id, source.run_id, source.signal_id);`

	// NOTE: sqlx only support ? when doing `sqlx.In` expanding query
	deleteSignalsRequestedSetQuery = `DELETE FROM signals_requested_sets
WHERE
shard_id = ? AND
namespace_id = ? AND
workflow_id = ? AND
run_id = ? AND
signal_id IN ( ? )`

	getSignalsRequestedSetQuery = `SELECT signal_id FROM signals_requested_sets WHERE
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
	return fmt.Sprintf(deleteMapQueryTemplate, tableName)
}

func makeSetKeyInMapQry(tableName string, nonPrimaryKeyColumns []string, mapKeyName string) string {
	return fmt.Sprintf(setKeyInMapQueryTemplate,
		tableName,
		strings.Join(nonPrimaryKeyColumns, ","),
		strings.Join(stringMap(nonPrimaryKeyColumns, func(x string) string {
			return ":" + x + " AS " + x
		}), ","),
		mapKeyName,
		strings.Join(stringMap(nonPrimaryKeyColumns, func(x string) string {
			return x + " = source." + x
		}), ","),
		strings.Join(stringMap(nonPrimaryKeyColumns, func(x string) string {
			return "source." + x
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
func (mdb *db) ReplaceIntoActivityInfoMaps(
	ctx context.Context,
	rows []sqlplugin.ActivityInfoMapsRow,
) (sql.Result, error) {
	return namedExecPerRow(ctx, mdb,
		setKeyInActivityInfoMapQry,
		rows,
	)
}

// SelectAllFromActivityInfoMaps reads all rows from activity_info_maps table
func (mdb *db) SelectAllFromActivityInfoMaps(
	ctx context.Context,
	filter sqlplugin.ActivityInfoMapsAllFilter,
) ([]sqlplugin.ActivityInfoMapsRow, error) {
	var rows []sqlplugin.ActivityInfoMapsRow
	if err := mdb.SelectContext(ctx,
		&rows,
		getActivityInfoMapQry,
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
func (mdb *db) DeleteFromActivityInfoMaps(
	ctx context.Context,
	filter sqlplugin.ActivityInfoMapsFilter,
) (sql.Result, error) {
	query, args, err := sqlx.In(
		deleteKeyInActivityInfoMapQry,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
		filter.ScheduleIDs,
	)
	if err != nil {
		return nil, err
	}
	return mdb.ExecContext(ctx,
		mdb.Rebind(query),
		args...,
	)
}

// DeleteAllFromActivityInfoMaps deletes all rows from activity_info_maps table
func (mdb *db) DeleteAllFromActivityInfoMaps(
	ctx context.Context,
	filter sqlplugin.ActivityInfoMapsAllFilter,
) (sql.Result, error) {
	return mdb.ExecContext(ctx,
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
func (mdb *db) ReplaceIntoTimerInfoMaps(
	ctx context.Context,
	rows []sqlplugin.TimerInfoMapsRow,
) (sql.Result, error) {
	return namedExecPerRow(ctx, mdb,
		setKeyInTimerInfoMapSQLQuery,
		rows,
	)
}

// SelectAllFromTimerInfoMaps reads all rows from timer_info_maps table
func (mdb *db) SelectAllFromTimerInfoMaps(
	ctx context.Context,
	filter sqlplugin.TimerInfoMapsAllFilter,
) ([]sqlplugin.TimerInfoMapsRow, error) {
	var rows []sqlplugin.TimerInfoMapsRow
	if err := mdb.SelectContext(ctx,
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
func (mdb *db) DeleteFromTimerInfoMaps(
	ctx context.Context,
	filter sqlplugin.TimerInfoMapsFilter,
) (sql.Result, error) {
	query, args, err := sqlx.In(
		deleteKeyInTimerInfoMapSQLQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
		filter.TimerIDs,
	)
	if err != nil {
		return nil, err
	}
	return mdb.ExecContext(ctx,
		mdb.Rebind(query),
		args...,
	)
}

// DeleteAllFromTimerInfoMaps deletes all rows from timer_info_maps table
func (mdb *db) DeleteAllFromTimerInfoMaps(
	ctx context.Context,
	filter sqlplugin.TimerInfoMapsAllFilter,
) (sql.Result, error) {
	return mdb.ExecContext(ctx,
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
func (mdb *db) ReplaceIntoChildExecutionInfoMaps(
	ctx context.Context,
	rows []sqlplugin.ChildExecutionInfoMapsRow,
) (sql.Result, error) {
	return namedExecPerRow(ctx, mdb,
		setKeyInChildExecutionInfoMapQry,
		rows,
	)
}

// SelectAllFromChildExecutionInfoMaps reads all rows from child_execution_info_maps table
func (mdb *db) SelectAllFromChildExecutionInfoMaps(
	ctx context.Context,
	filter sqlplugin.ChildExecutionInfoMapsAllFilter,
) ([]sqlplugin.ChildExecutionInfoMapsRow, error) {
	var rows []sqlplugin.ChildExecutionInfoMapsRow
	if err := mdb.SelectContext(ctx,
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
func (mdb *db) DeleteFromChildExecutionInfoMaps(
	ctx context.Context,
	filter sqlplugin.ChildExecutionInfoMapsFilter,
) (sql.Result, error) {
	query, args, err := sqlx.In(
		deleteKeyInChildExecutionInfoMapQry,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
		filter.InitiatedIDs,
	)
	if err != nil {
		return nil, err
	}
	return mdb.ExecContext(ctx,
		mdb.Rebind(query),
		args...,
	)
}

// DeleteAllFromChildExecutionInfoMaps deletes all rows from child_execution_info_maps table
func (mdb *db) DeleteAllFromChildExecutionInfoMaps(
	ctx context.Context,
	filter sqlplugin.ChildExecutionInfoMapsAllFilter,
) (sql.Result, error) {
	return mdb.ExecContext(ctx,
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
func (mdb *db) ReplaceIntoRequestCancelInfoMaps(
	ctx context.Context,
	rows []sqlplugin.RequestCancelInfoMapsRow,
) (sql.Result, error) {
	return namedExecPerRow(ctx, mdb,
		setKeyInRequestCancelInfoMapQry,
		rows,
	)
}

// SelectAllFromRequestCancelInfoMaps reads all rows from request_cancel_info_maps table
func (mdb *db) SelectAllFromRequestCancelInfoMaps(
	ctx context.Context,
	filter sqlplugin.RequestCancelInfoMapsAllFilter,
) ([]sqlplugin.RequestCancelInfoMapsRow, error) {
	var rows []sqlplugin.RequestCancelInfoMapsRow
	if err := mdb.SelectContext(ctx,
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
func (mdb *db) DeleteFromRequestCancelInfoMaps(
	ctx context.Context,
	filter sqlplugin.RequestCancelInfoMapsFilter,
) (sql.Result, error) {
	query, args, err := sqlx.In(
		deleteKeyInRequestCancelInfoMapQry,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
		filter.InitiatedIDs,
	)
	if err != nil {
		return nil, err
	}
	return mdb.ExecContext(ctx,
		mdb.Rebind(query),
		args...,
	)
}

// DeleteAllFromRequestCancelInfoMaps deletes all rows from request_cancel_info_maps table
func (mdb *db) DeleteAllFromRequestCancelInfoMaps(
	ctx context.Context,
	filter sqlplugin.RequestCancelInfoMapsAllFilter,
) (sql.Result, error) {
	return mdb.ExecContext(ctx,
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
func (mdb *db) ReplaceIntoSignalInfoMaps(
	ctx context.Context,
	rows []sqlplugin.SignalInfoMapsRow,
) (sql.Result, error) {
	return namedExecPerRow(ctx, mdb,
		setKeyInSignalInfoMapQry,
		rows,
	)
}

// SelectAllFromSignalInfoMaps reads all rows from signal_info_maps table
func (mdb *db) SelectAllFromSignalInfoMaps(
	ctx context.Context,
	filter sqlplugin.SignalInfoMapsAllFilter,
) ([]sqlplugin.SignalInfoMapsRow, error) {
	var rows []sqlplugin.SignalInfoMapsRow
	if err := mdb.SelectContext(ctx,
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
func (mdb *db) DeleteFromSignalInfoMaps(
	ctx context.Context,
	filter sqlplugin.SignalInfoMapsFilter,
) (sql.Result, error) {
	query, args, err := sqlx.In(
		deleteKeyInSignalInfoMapQry,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
		filter.InitiatedIDs,
	)
	if err != nil {
		return nil, err
	}
	return mdb.ExecContext(ctx,
		mdb.Rebind(query),
		args...,
	)
}

// DeleteAllFromSignalInfoMaps deletes all rows from signal_info_maps table
func (mdb *db) DeleteAllFromSignalInfoMaps(
	ctx context.Context,
	filter sqlplugin.SignalInfoMapsAllFilter,
) (sql.Result, error) {
	return mdb.ExecContext(ctx,
		deleteSignalInfoMapQry,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
}

// InsertIntoSignalsRequestedSets inserts one or more rows into signals_requested_sets table
func (mdb *db) ReplaceIntoSignalsRequestedSets(
	ctx context.Context,
	rows []sqlplugin.SignalsRequestedSetsRow,
) (sql.Result, error) {
	return namedExecPerRow(ctx, mdb,
		createSignalsRequestedSetQuery,
		rows,
	)
}

// SelectAllFromSignalsRequestedSets reads all rows from signals_requested_sets table
func (mdb *db) SelectAllFromSignalsRequestedSets(
	ctx context.Context,
	filter sqlplugin.SignalsRequestedSetsAllFilter,
) ([]sqlplugin.SignalsRequestedSetsRow, error) {
	var rows []sqlplugin.SignalsRequestedSetsRow
	if err := mdb.SelectContext(ctx,
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
func (mdb *db) DeleteFromSignalsRequestedSets(
	ctx context.Context,
	filter sqlplugin.SignalsRequestedSetsFilter,
) (sql.Result, error) {
	query, args, err := sqlx.In(
		deleteSignalsRequestedSetQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
		filter.SignalIDs,
	)
	if err != nil {
		return nil, err
	}
	return mdb.ExecContext(ctx,
		mdb.Rebind(query),
		args...,
	)
}

// DeleteAllFromSignalsRequestedSets deletes all rows from signals_requested_sets table
func (mdb *db) DeleteAllFromSignalsRequestedSets(
	ctx context.Context,
	filter sqlplugin.SignalsRequestedSetsAllFilter,
) (sql.Result, error) {
	return mdb.ExecContext(ctx,
		deleteAllSignalsRequestedSetQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
}

var (
	chasmNodeColumns = []string{
		"metadata",
		"metadata_encoding",
		"data",
		"data_encoding",
	}
	chasmNodeTableName = "chasm_node_maps"
	chasmNodeKey       = "chasm_path"

	deleteChasmNodeMapSQLQuery      = makeDeleteMapQry(chasmNodeTableName)
	setKeyInChasmNodeMapSQLQuery    = makeSetKeyInMapQry(chasmNodeTableName, chasmNodeColumns, chasmNodeKey)
	deleteKeyInChasmNodeMapSQLQuery = makeDeleteKeyInMapQry(chasmNodeTableName, chasmNodeKey)
	getChasmNodeMapSQLQuery         = makeGetMapQryTemplate(chasmNodeTableName, chasmNodeColumns, chasmNodeKey)
)

func (mdb *db) SelectAllFromChasmNodeMaps(
	ctx context.Context,
	filter sqlplugin.ChasmNodeMapsAllFilter,
) ([]sqlplugin.ChasmNodeMapsRow, error) {
	var rows []sqlplugin.ChasmNodeMapsRow

	if err := mdb.SelectContext(ctx,
		&rows,
		getChasmNodeMapSQLQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	); err != nil {
		return nil, err
	}

	for i := range rows {
		rows[i].ShardID = filter.ShardID
		rows[i].NamespaceID = filter.NamespaceID
		rows[i].WorkflowID = filter.WorkflowID
		rows[i].RunID = filter.RunID
	}

	return rows, nil
}

// mssqlChasmNodeMapsRow mirrors sqlplugin.ChasmNodeMapsRow with ChasmPath as
// raw bytes: chasm_path is VARBINARY(1536) in the mssql schema and go-mssqldb
// binds Go strings as NVARCHAR, which SQL Server will not implicitly convert
// to varbinary (error 257). Binding []byte sends varbinary. A SQL-side CAST
// would be wrong here — it would store UTF-16 bytes, doubling the path size
// and corrupting the persisted chasm_path_hash key.
type mssqlChasmNodeMapsRow struct {
	ShardID          int32
	NamespaceID      primitives.UUID
	WorkflowID       string
	RunID            primitives.UUID
	ChasmPath        []byte
	Metadata         []byte
	MetadataEncoding string
	Data             []byte
	DataEncoding     string
}

func (mdb *db) ReplaceIntoChasmNodeMaps(
	ctx context.Context,
	rows []sqlplugin.ChasmNodeMapsRow,
) (sql.Result, error) {
	mssqlRows := make([]mssqlChasmNodeMapsRow, len(rows))
	for i, row := range rows {
		mssqlRows[i] = mssqlChasmNodeMapsRow{
			ShardID:          row.ShardID,
			NamespaceID:      row.NamespaceID,
			WorkflowID:       row.WorkflowID,
			RunID:            row.RunID,
			ChasmPath:        []byte(row.ChasmPath),
			Metadata:         row.Metadata,
			MetadataEncoding: row.MetadataEncoding,
			Data:             row.Data,
			DataEncoding:     row.DataEncoding,
		}
	}
	return namedExecPerRow(ctx, mdb,
		setKeyInChasmNodeMapSQLQuery,
		mssqlRows,
	)
}

func (mdb *db) DeleteFromChasmNodeMaps(ctx context.Context, filter sqlplugin.ChasmNodeMapsFilter) (sql.Result, error) {
	// Bind paths as []byte so they compare as varbinary against the
	// VARBINARY(1536) chasm_path column; see mssqlChasmNodeMapsRow.
	chasmPaths := make([][]byte, len(filter.ChasmPaths))
	for i, p := range filter.ChasmPaths {
		chasmPaths[i] = []byte(p)
	}
	query, args, err := sqlx.In(
		deleteKeyInChasmNodeMapSQLQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
		chasmPaths,
	)
	if err != nil {
		return nil, err
	}
	return mdb.ExecContext(ctx,
		mdb.Rebind(query),
		args...,
	)
}

func (mdb *db) DeleteAllFromChasmNodeMaps(ctx context.Context, filter sqlplugin.ChasmNodeMapsAllFilter) (sql.Result, error) {
	return mdb.ExecContext(ctx,
		deleteChasmNodeMapSQLQuery,
		filter.ShardID,
		filter.NamespaceID,
		filter.WorkflowID,
		filter.RunID,
	)
}
