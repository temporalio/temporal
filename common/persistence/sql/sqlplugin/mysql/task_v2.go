package mysql

import (
	"context"
	"database/sql"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	taskQueueCreatePartV2 = `INTO task_queues_v2(range_hash, task_queue_id, range_id, data, data_encoding) ` +
		`VALUES (:range_hash, :task_queue_id, :range_id, :data, :data_encoding)`

	// (default range ID: initialRangeID == 1)
	createTaskQueueQryV2 = `INSERT ` + taskQueueCreatePartV2

	updateTaskQueueQryV2 = `UPDATE task_queues_v2 SET
		range_id = :range_id,
		data = :data,
		data_encoding = :data_encoding
		WHERE
		range_hash = :range_hash AND
		task_queue_id = :task_queue_id
	`

	listTaskQueueRowSelectV2 = `SELECT range_hash, task_queue_id, range_id, data, data_encoding from task_queues_v2 `

	listTaskQueueWithHashRangeQryV2 = listTaskQueueRowSelectV2 +
		`WHERE range_hash >= ? AND range_hash <= ? AND task_queue_id > ? ORDER BY task_queue_id ASC LIMIT ?`

	listTaskQueueQryV2 = listTaskQueueRowSelect +
		`WHERE range_hash = ? AND task_queue_id > ? ORDER BY task_queue_id ASC LIMIT ?`

	getTaskQueueQryV2 = listTaskQueueRowSelect +
		`WHERE range_hash = ? AND task_queue_id = ?`

	deleteTaskQueueQryV2 = `DELETE FROM task_queues_v2 WHERE range_hash=? AND task_queue_id=? AND range_id=?`

	lockTaskQueueQryV2 = `SELECT range_id FROM task_queues_v2 ` +
		`WHERE range_hash = ? AND task_queue_id = ? FOR UPDATE`
	// *** Task_Queues Table Above ***

	// *** Tasks Below ***
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

	rangeDeleteFairnessTaskQry = `DELETE FROM tasks_v2 ` +
		`WHERE range_hash = ? ` +
		`AND task_queue_id = ? ` +
		`AND (pass, task_id) < (?, ?) `
)

func (mdb *db) InsertIntoTasksV2(
	ctx context.Context,
	rows []sqlplugin.TasksRowV2,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		createTaskQryV2,
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
			&rows, getFairnessTaskQryWithLimit,
			filter.RangeHash,
			filter.TaskQueueID,
			filter.InclusiveMinPass,
			*filter.InclusiveMinTaskID,
			*filter.PageSize,
		)
	default:
		err = mdb.SelectContext(ctx,
			&rows, getFairnessTaskQry,
			filter.RangeHash,
			filter.TaskQueueID,
			filter.InclusiveMinPass,
			*filter.InclusiveMinTaskID,
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
	if filter.ExclusiveMaxTaskID == nil {
		return nil, serviceerror.NewInternal("missing ExclusiveMaxTaskID parameter")
	}
	return mdb.ExecContext(ctx,
		rangeDeleteFairnessTaskQry,
		filter.RangeHash,
		filter.TaskQueueID,
		filter.InclusiveMinPass,
		filter.ExclusiveMaxTaskID,
	)
}

// InsertIntoTaskQueues inserts one or more rows into task_queues table
func (mdb *db) InsertIntoTaskQueuesV2(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRowV2,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		createTaskQueueQryV2,
		row,
	)
}

// UpdateTaskQueues updates a row in task_queues table
func (mdb *db) UpdateTaskQueuesV2(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRowV2,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		updateTaskQueueQryV2,
		row,
	)
}

// SelectFromTaskQueues reads one or more rows from task_queues table
func (mdb *db) SelectFromTaskQueuesV2(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilterV2,
) ([]sqlplugin.TaskQueuesRowV2, error) {
	switch {
	case filter.TaskQueueID != nil:
		if filter.RangeHashLessThanEqualTo != 0 || filter.RangeHashGreaterThanEqualTo != 0 {
			return nil, serviceerror.NewInternal("range of hashes not supported for specific selection")
		}
		return mdb.selectFromTaskQueuesV2(ctx, filter)
	case filter.RangeHashLessThanEqualTo != 0 && filter.PageSize != nil:
		if filter.RangeHashLessThanEqualTo < filter.RangeHashGreaterThanEqualTo {
			return nil, serviceerror.NewInternal("range of hashes bound is invalid")
		}
		return mdb.rangeSelectFromTaskQueuesV2(ctx, filter)
	case filter.TaskQueueIDGreaterThan != nil && filter.PageSize != nil:
		return mdb.rangeSelectFromTaskQueuesV2(ctx, filter)
	default:
		return nil, serviceerror.NewInternal("invalid set of query filter params")
	}
}

func (mdb *db) selectFromTaskQueuesV2(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilterV2,
) ([]sqlplugin.TaskQueuesRowV2, error) {
	var err error
	var row sqlplugin.TaskQueuesRowV2
	err = mdb.GetContext(ctx,
		&row,
		getTaskQueueQryV2,
		filter.RangeHash,
		filter.TaskQueueID,
	)
	if err != nil {
		return nil, err
	}
	return []sqlplugin.TaskQueuesRowV2{row}, nil
}

func (mdb *db) rangeSelectFromTaskQueuesV2(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilterV2,
) ([]sqlplugin.TaskQueuesRowV2, error) {
	var err error
	var rows []sqlplugin.TaskQueuesRowV2

	if filter.RangeHashLessThanEqualTo != 0 {
		err = mdb.SelectContext(ctx,
			&rows,
			listTaskQueueWithHashRangeQryV2,
			filter.RangeHashGreaterThanEqualTo,
			filter.RangeHashLessThanEqualTo,
			filter.TaskQueueIDGreaterThan,
			*filter.PageSize,
		)
	} else {
		err = mdb.SelectContext(ctx,
			&rows,
			listTaskQueueQryV2,
			filter.RangeHash,
			filter.TaskQueueIDGreaterThan,
			*filter.PageSize,
		)
	}
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteFromTaskQueues deletes a row from task_queues table
func (mdb *db) DeleteFromTaskQueuesV2(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilterV2,
) (sql.Result, error) {
	return mdb.ExecContext(ctx,
		deleteTaskQueueQryV2,
		filter.RangeHash,
		filter.TaskQueueID,
		*filter.RangeID,
	)
}

// LockTaskQueues locks a row in task_queues table
func (mdb *db) LockTaskQueuesV2(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilterV2,
) (int64, error) {
	var rangeID int64
	err := mdb.GetContext(ctx,
		&rangeID,
		lockTaskQueueQryV2,
		filter.RangeHash,
		filter.TaskQueueID,
	)
	return rangeID, err
}
