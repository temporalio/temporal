package mssql

import (
	"context"
	"database/sql"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	taskQueueCreatePart = `INTO task_queues_v2 (range_hash, task_queue_id, range_id, data, data_encoding) ` +
		`VALUES (:range_hash, :task_queue_id, :range_id, :data, :data_encoding)`

	// (default range ID: initialRangeID == 1)
	createTaskQueueQry = `INSERT ` + taskQueueCreatePart

	updateTaskQueueQry = `UPDATE task_queues_v2 SET
	range_id = :range_id,
	data = :data,
	data_encoding = :data_encoding
	WHERE
	range_hash = :range_hash AND
	task_queue_id = :task_queue_id
	`

	listTaskQueueRowSelect = `SELECT range_hash, task_queue_id, range_id, data, data_encoding FROM task_queues_v2 `

	// TOP (?) replaces LIMIT ?; the page size binds first in these queries.
	listTaskQueueRowSelectTop = `SELECT TOP (?) range_hash, task_queue_id, range_id, data, data_encoding FROM task_queues_v2 `

	listTaskQueueWithHashRangeQry = listTaskQueueRowSelectTop +
		`WHERE range_hash >= ? AND range_hash <= ? AND task_queue_id > ? ORDER BY task_queue_id ASC`

	listTaskQueueQry = listTaskQueueRowSelectTop +
		`WHERE range_hash = ? AND task_queue_id > ? ORDER BY task_queue_id ASC`

	getTaskQueueQry = listTaskQueueRowSelect +
		`WHERE range_hash = ? AND task_queue_id = ?`

	deleteTaskQueueQry = `DELETE FROM task_queues_v2 WHERE range_hash=? AND task_queue_id=? AND range_id=?`

	lockTaskQueueQry = `SELECT range_id FROM task_queues_v2 WITH (UPDLOCK, ROWLOCK) ` +
		`WHERE range_hash = ? AND task_queue_id = ?`
)

// InsertIntoTaskQueues inserts one or more rows into task_queues[_v2] table
func (mdb *db) InsertIntoTaskQueues(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
	v sqlplugin.MatchingTaskVersion,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		sqlplugin.SwitchTaskQueuesTable(createTaskQueueQry, v),
		row,
	)
}

// UpdateTaskQueues updates a row in task_queues[_v2] table
func (mdb *db) UpdateTaskQueues(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
	v sqlplugin.MatchingTaskVersion,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		sqlplugin.SwitchTaskQueuesTable(updateTaskQueueQry, v),
		row,
	)
}

// SelectFromTaskQueues reads one or more rows from task_queues[_v2] table
func (mdb *db) SelectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) ([]sqlplugin.TaskQueuesRow, error) {
	switch {
	case filter.TaskQueueID != nil:
		if filter.RangeHashLessThanEqualTo != 0 || filter.RangeHashGreaterThanEqualTo != 0 {
			return nil, serviceerror.NewInternal("range of hashes not supported for specific selection")
		}
		return mdb.selectFromTaskQueues(ctx, filter, v)
	case filter.RangeHashLessThanEqualTo != 0 && filter.PageSize != nil:
		if filter.RangeHashLessThanEqualTo < filter.RangeHashGreaterThanEqualTo {
			return nil, serviceerror.NewInternal("range of hashes bound is invalid")
		}
		return mdb.rangeSelectFromTaskQueues(ctx, filter, v)
	case filter.TaskQueueIDGreaterThan != nil && filter.PageSize != nil:
		return mdb.rangeSelectFromTaskQueues(ctx, filter, v)
	default:
		return nil, serviceerror.NewInternal("invalid set of query filter params")
	}
}

func (mdb *db) selectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) ([]sqlplugin.TaskQueuesRow, error) {
	var err error
	var row sqlplugin.TaskQueuesRow
	err = mdb.GetContext(ctx,
		&row,
		sqlplugin.SwitchTaskQueuesTable(getTaskQueueQry, v),
		filter.RangeHash,
		filter.TaskQueueID,
	)
	if err != nil {
		return nil, err
	}
	return []sqlplugin.TaskQueuesRow{row}, nil
}

func (mdb *db) rangeSelectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) ([]sqlplugin.TaskQueuesRow, error) {
	var err error
	var rows []sqlplugin.TaskQueuesRow

	if filter.RangeHashLessThanEqualTo != 0 {
		err = mdb.SelectContext(ctx,
			&rows,
			sqlplugin.SwitchTaskQueuesTable(listTaskQueueWithHashRangeQry, v),
			*filter.PageSize,
			filter.RangeHashGreaterThanEqualTo,
			filter.RangeHashLessThanEqualTo,
			filter.TaskQueueIDGreaterThan,
		)
	} else {
		err = mdb.SelectContext(ctx,
			&rows,
			sqlplugin.SwitchTaskQueuesTable(listTaskQueueQry, v),
			*filter.PageSize,
			filter.RangeHash,
			filter.TaskQueueIDGreaterThan,
		)
	}
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteFromTaskQueues deletes a row from task_queues[_v2] table
func (mdb *db) DeleteFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) (sql.Result, error) {
	return mdb.ExecContext(ctx,
		sqlplugin.SwitchTaskQueuesTable(deleteTaskQueueQry, v),
		filter.RangeHash,
		filter.TaskQueueID,
		*filter.RangeID,
	)
}

// LockTaskQueues locks a row in task_queues[_v2] table
func (mdb *db) LockTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) (int64, error) {
	var rangeID int64
	err := mdb.GetContext(ctx,
		&rangeID,
		sqlplugin.SwitchTaskQueuesTable(lockTaskQueueQry, v),
		filter.RangeHash,
		filter.TaskQueueID,
	)
	return rangeID, err
}
