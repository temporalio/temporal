package mssql

import (
	"context"
	"database/sql"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	// T-SQL has no row-value tuple comparison; (pass, task_id) >= (?, ?)
	// is expanded to its boolean form with the pass argument bound twice.
	getTaskV2Qry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks_v2 ` +
		`WHERE range_hash = ? ` +
		`AND task_queue_id = ? ` +
		`AND (pass > ? OR (pass = ? AND task_id >= ?)) ` +
		`ORDER BY pass, task_id`

	// TOP (?) replaces LIMIT ?; the page size binds first in this query.
	getTaskV2QryWithLimit = `SELECT TOP (?) task_id, data, data_encoding ` +
		`FROM tasks_v2 ` +
		`WHERE range_hash = ? ` +
		`AND task_queue_id = ? ` +
		`AND (pass > ? OR (pass = ? AND task_id >= ?)) ` +
		`ORDER BY pass, task_id`

	createTaskV2Qry = `INSERT INTO ` +
		`tasks_v2 ( range_hash,  task_queue_id,  task_id,       pass,  data,  data_encoding) ` +
		`VALUES   (:range_hash, :task_queue_id, :task_id, :task_pass, :data, :data_encoding)`

	// T-SQL has no DELETE ... ORDER BY LIMIT; cap the delete through a
	// TOP (?) subquery like the postgresql plugin does with LIMIT, and
	// expand the (pass, task_id) < (?, ?) tuple to its boolean form.
	rangeDeleteTaskV2Qry = `DELETE FROM tasks_v2 ` +
		`WHERE range_hash = ? AND task_queue_id = ? AND task_id IN (SELECT TOP (?) task_id FROM ` +
		`tasks_v2 WHERE range_hash = ? AND task_queue_id = ? AND (pass < ? OR (pass = ? AND task_id < ?)) ` +
		`ORDER BY task_queue_id,pass,task_id)`
)

func (mdb *db) InsertIntoTasksV2(
	ctx context.Context,
	rows []sqlplugin.TasksRowV2,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		createTaskV2Qry,
		rows,
	)
}

// SelectFromTasks reads one or more rows from tasks table
func (mdb *db) SelectFromTasksV2(
	ctx context.Context,
	filter sqlplugin.TasksFilterV2,
) ([]sqlplugin.TasksRowV2, error) {
	if filter.InclusiveMinLevel == nil {
		return nil, serviceerror.NewInternal("missing InclusiveMinLevel")
	}
	var err error
	var rows []sqlplugin.TasksRowV2
	switch {
	case filter.PageSize != nil:
		err = mdb.SelectContext(ctx,
			&rows, getTaskV2QryWithLimit,
			*filter.PageSize,
			filter.RangeHash,
			filter.TaskQueueID,
			filter.InclusiveMinLevel.TaskPass,
			filter.InclusiveMinLevel.TaskPass,
			filter.InclusiveMinLevel.TaskID,
		)
	default:
		err = mdb.SelectContext(ctx,
			&rows, getTaskV2Qry,
			filter.RangeHash,
			filter.TaskQueueID,
			filter.InclusiveMinLevel.TaskPass,
			filter.InclusiveMinLevel.TaskPass,
			filter.InclusiveMinLevel.TaskID,
		)
	}
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteFromTasks deletes one or more rows from tasks table
func (mdb *db) DeleteFromTasksV2(
	ctx context.Context,
	filter sqlplugin.TasksFilterV2,
) (sql.Result, error) {
	if filter.ExclusiveMaxLevel == nil {
		return nil, serviceerror.NewInternal("missing ExclusiveMaxTaskLevel")
	}
	if filter.Limit == nil || *filter.Limit == 0 {
		return nil, serviceerror.NewInternal("missing limit parameter")
	}
	return mdb.ExecContext(ctx,
		rangeDeleteTaskV2Qry,
		filter.RangeHash,
		filter.TaskQueueID,
		*filter.Limit,
		filter.RangeHash,
		filter.TaskQueueID,
		filter.ExclusiveMaxLevel.TaskPass,
		filter.ExclusiveMaxLevel.TaskPass,
		filter.ExclusiveMaxLevel.TaskID,
	)
}
