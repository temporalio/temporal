package mysql

import (
	"context"
	"database/sql"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	getTaskV2Qry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks_v2 ` +
		`WHERE range_hash = ? ` +
		`AND task_queue_id = ? ` +
		`AND (pass, task_id) >= (?, ?) ` +
		`ORDER BY pass, task_id `

	getTaskV2QryWithLimit = `SELECT task_id, data, data_encoding ` +
		`FROM tasks_v2 ` +
		`WHERE range_hash = ? ` +
		`AND task_queue_id = ? ` +
		`AND (pass, task_id) >= (?, ?) ` +
		`ORDER BY pass, task_id ` +
		`LIMIT ?`

	createTaskV2Qry = `INSERT INTO ` +
		`tasks_v2(range_hash, task_queue_id, task_id, pass, data, data_encoding) ` +
		`VALUES(:range_hash, :task_queue_id, :task_id, :pass, :data, :data_encoding)`

	rangeDeleteTaskV2Qry = `DELETE FROM tasks_v2 ` +
		`WHERE range_hash = ? ` +
		`AND task_queue_id = ? ` +
		`AND (pass, task_id) < (?, ?) `
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
	var err error
	var rows []sqlplugin.TasksRowV2
	switch {
	case filter.PageSize != nil:
		err = mdb.SelectContext(ctx,
			&rows, getTaskV2QryWithLimit,
			filter.RangeHash,
			filter.TaskQueueID,
			filter.InclusiveMinLevel.TaskPass,
			filter.InclusiveMinLevel.TaskID,
			*filter.PageSize,
		)
	default:
		err = mdb.SelectContext(ctx,
			&rows, getTaskV2Qry,
			filter.RangeHash,
			filter.TaskQueueID,
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
		return nil, serviceerror.NewInternal("missing ExclusiveMaxTaskID parameter")
	}
	return mdb.ExecContext(ctx,
		rangeDeleteTaskV2Qry,
		filter.RangeHash,
		filter.TaskQueueID,
		filter.ExclusiveMaxLevel.TaskPass,
		filter.ExclusiveMaxLevel.TaskID,
	)
}
