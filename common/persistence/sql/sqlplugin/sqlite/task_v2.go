package sqlite

import (
	"context"
	"database/sql"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	getTaskMinMaxQryV2 = `SELECT task_id, data, data_encoding ` +
		`FROM tasks_v2 ` +
		`WHERE range_hash = ? ` +
		`AND task_queue_id = ? ` +
		`AND (pass, task_id) >= (?, ?) ` +
		`AND task_id < ? ` +
		`ORDER BY pass, task_id ` +
		`LIMIT ?`

	getFairnessTaskQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks_v2 ` +
		`WHERE range_hash = ? ` +
		`AND task_queue_id = ? ` +
		`AND (pass, task_id) >= (?, ?) ` +
		`ORDER BY pass, task_id `

	getFairnessTaskQryWithLimit = `SELECT task_id, data, data_encoding ` +
		`FROM tasks_v2 ` +
		`WHERE range_hash = ? ` +
		`AND task_queue_id = ? ` +
		`AND (pass, task_id) >= (?, ?) ` +
		`ORDER BY pass, task_id ` +
		`LIMIT ?`

	createTaskQryV2 = `INSERT INTO ` +
		`tasks_v2(range_hash, task_queue_id, task_id, pass, data, data_encoding) ` +
		`VALUES(:range_hash, :task_queue_id, :task_id, :pass, :data, :data_encoding)`

	rangeDeleteTaskQryV2 = `DELETE FROM tasks_v2 ` +
		`WHERE range_hash = ? AND task_queue_id = ? AND task_id IN (SELECT task_id FROM
		 tasks_v2 WHERE range_hash = ? AND task_queue_id = ? AND task_id < ? ` +
		`ORDER BY task_queue_id,task_id LIMIT ? ) `

	rangeDeleteFairnessTaskQry = `DELETE FROM tasks_v2 ` +
		`WHERE range_hash = ? ` +
		`AND task_queue_id = ? ` +
		`AND (pass, task_id) < (?, ?) `
)

// InsertIntoTasks inserts one or more rows into tasks_v2 table
func (mdb *db) InsertIntoTasksV2(
	ctx context.Context,
	rows []sqlplugin.TasksRowV2,
) (sql.Result, error) {
	return mdb.conn.NamedExecContext(ctx,
		createTaskQryV2,
		rows,
	)
}

// SelectFromTasks reads one or more rows from tasks_v2 table
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
		err = mdb.conn.SelectContext(ctx,
			&rows, getFairnessTaskQryWithLimit,
			filter.RangeHash,
			filter.TaskQueueID,
			filter.InclusiveMinLevel.TaskPass,
			filter.InclusiveMinLevel.TaskID,
			*filter.PageSize,
		)
	default:
		err = mdb.conn.SelectContext(ctx,
			&rows, getFairnessTaskQry,
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

// DeleteFromTasks deletes one or more rows from tasks_v2 table
func (mdb *db) DeleteFromTasksV2(
	ctx context.Context,
	filter sqlplugin.TasksFilterV2,
) (sql.Result, error) {
	if filter.ExclusiveMaxLevel == nil {
		return nil, serviceerror.NewInternal("missing ExclusiveMaxTaskLevel")
	}
	return mdb.conn.ExecContext(ctx,
		rangeDeleteFairnessTaskQry,
		filter.RangeHash,
		filter.TaskQueueID,
		filter.ExclusiveMaxLevel.TaskPass,
		filter.ExclusiveMaxLevel.TaskID,
	)
}
