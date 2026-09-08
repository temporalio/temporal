package mssql

import (
	"context"
	"database/sql"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	// TOP (?) replaces LIMIT ?; the page size binds first in these queries.
	getTaskMinMaxQry = `SELECT TOP (?) task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE range_hash = ? AND task_queue_id = ? AND task_id >= ? AND task_id < ? ` +
		`ORDER BY task_id`

	getTaskMinQry = `SELECT TOP (?) task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE range_hash = ? AND task_queue_id = ? AND task_id >= ? ORDER BY task_id`

	createTaskQry = `INSERT INTO ` +
		`tasks(range_hash, task_queue_id, task_id, data, data_encoding) ` +
		`VALUES(:range_hash, :task_queue_id, :task_id, :data, :data_encoding)`

	// T-SQL has no DELETE ... ORDER BY LIMIT; cap the delete through a
	// TOP (?) subquery like the postgresql plugin does with LIMIT.
	rangeDeleteTaskQry = `DELETE FROM tasks ` +
		`WHERE range_hash = ? AND task_queue_id = ? AND task_id IN (SELECT TOP (?) task_id FROM ` +
		`tasks WHERE range_hash = ? AND task_queue_id = ? AND task_id < ? ` +
		`ORDER BY task_queue_id,task_id)`
)

// InsertIntoTasks inserts one or more rows into tasks table
func (mdb *db) InsertIntoTasks(
	ctx context.Context,
	rows []sqlplugin.TasksRow,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		createTaskQry,
		rows,
	)
}

// SelectFromTasks reads one or more rows from tasks table
func (mdb *db) SelectFromTasks(
	ctx context.Context,
	filter sqlplugin.TasksFilter,
) ([]sqlplugin.TasksRow, error) {
	var err error
	var rows []sqlplugin.TasksRow
	switch {
	case filter.ExclusiveMaxTaskID != nil:
		err = mdb.SelectContext(ctx,
			&rows, getTaskMinMaxQry,
			*filter.PageSize,
			filter.RangeHash,
			filter.TaskQueueID,
			*filter.InclusiveMinTaskID,
			*filter.ExclusiveMaxTaskID,
		)
	default:
		err = mdb.SelectContext(ctx,
			&rows, getTaskMinQry,
			*filter.PageSize,
			filter.RangeHash,
			filter.TaskQueueID,
			*filter.InclusiveMinTaskID,
		)
	}
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteFromTasks deletes one or more rows from tasks table
func (mdb *db) DeleteFromTasks(
	ctx context.Context,
	filter sqlplugin.TasksFilter,
) (sql.Result, error) {
	if filter.ExclusiveMaxTaskID == nil {
		return nil, serviceerror.NewInternal("missing ExclusiveMaxTaskID parameter")
	}
	if filter.Limit == nil || *filter.Limit == 0 {
		return nil, serviceerror.NewInternal("missing limit parameter")
	}
	return mdb.ExecContext(ctx,
		rangeDeleteTaskQry,
		filter.RangeHash,
		filter.TaskQueueID,
		*filter.Limit,
		filter.RangeHash,
		filter.TaskQueueID,
		*filter.ExclusiveMaxTaskID,
	)
}
