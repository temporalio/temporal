package postgresql

import (
	"context"
	"database/sql"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	taskQueueCreatePart = `INTO task_queues (range_hash, task_queue_id, range_id, data, data_encoding) ` +
		`VALUES (:range_hash, :task_queue_id, :range_id, :data, :data_encoding)`

	// (default range ID: initialRangeID == 1)
	createTaskQueueQry = `INSERT ` + taskQueueCreatePart

	updateTaskQueueQry = `UPDATE task_queues SET
	range_id = :range_id,
	data = :data,
	data_encoding = :data_encoding
	WHERE
	range_hash = :range_hash AND
	task_queue_id = :task_queue_id
	`

	listTaskQueueRowSelect = `SELECT range_hash, task_queue_id, range_id, data, data_encoding FROM task_queues `

	listTaskQueueWithHashRangeQry = listTaskQueueRowSelect +
		`WHERE range_hash >= $1 AND range_hash <= $2 AND task_queue_id > $3 ORDER BY task_queue_id ASC LIMIT $4`

	listTaskQueueQry = listTaskQueueRowSelect +
		`WHERE range_hash = $1 AND task_queue_id > $2 ORDER BY task_queue_id ASC LIMIT $3`

	getTaskQueueQry = listTaskQueueRowSelect +
		`WHERE range_hash = $1 AND task_queue_id=$2`

	deleteTaskQueueQry = `DELETE FROM task_queues WHERE range_hash=$1 AND task_queue_id=$2 AND range_id=$3`

	lockTaskQueueQry = `SELECT range_id FROM task_queues ` +
		`WHERE range_hash=$1 AND task_queue_id=$2 FOR UPDATE`
)

// InsertIntoTaskQueues inserts one or more rows into task_queues table
func (pdb *db) InsertIntoTaskQueues(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
	v sqlplugin.MatchingTaskVersion,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		sqlplugin.SwitchTaskQueuesTable(createTaskQueueQry, v),
		row,
	)
}

// UpdateTaskQueues updates a row in task_queues table
func (pdb *db) UpdateTaskQueues(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
	v sqlplugin.MatchingTaskVersion,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		sqlplugin.SwitchTaskQueuesTable(updateTaskQueueQry, v),
		row,
	)
}

// SelectFromTaskQueues reads one or more rows from task_queues table
func (pdb *db) SelectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) ([]sqlplugin.TaskQueuesRow, error) {
	switch {
	case filter.TaskQueueID != nil:
		if filter.RangeHashLessThanEqualTo != 0 || filter.RangeHashGreaterThanEqualTo != 0 {
			return nil, serviceerror.NewInternal("shardID range not supported for specific selection")
		}
		return pdb.selectFromTaskQueues(ctx, filter, v)
	case filter.RangeHashLessThanEqualTo != 0 && filter.PageSize != nil:
		if filter.RangeHashLessThanEqualTo < filter.RangeHashGreaterThanEqualTo {
			return nil, serviceerror.NewInternal("range of hashes bound is invalid")
		}
		return pdb.rangeSelectFromTaskQueues(ctx, filter, v)
	case filter.TaskQueueIDGreaterThan != nil && filter.PageSize != nil:
		return pdb.rangeSelectFromTaskQueues(ctx, filter, v)
	default:
		return nil, serviceerror.NewInternal("invalid set of query filter params")
	}
}

func (pdb *db) selectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) ([]sqlplugin.TaskQueuesRow, error) {
	var err error
	var row sqlplugin.TaskQueuesRow
	err = pdb.GetContext(ctx,
		&row,
		sqlplugin.SwitchTaskQueuesTable(getTaskQueueQry, v),
		filter.RangeHash,
		filter.TaskQueueID,
	)
	if err != nil {
		return nil, err
	}
	return []sqlplugin.TaskQueuesRow{row}, err
}

func (pdb *db) rangeSelectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) ([]sqlplugin.TaskQueuesRow, error) {
	var err error
	var rows []sqlplugin.TaskQueuesRow
	if filter.RangeHashLessThanEqualTo > 0 {
		err = pdb.SelectContext(ctx,
			&rows,
			sqlplugin.SwitchTaskQueuesTable(listTaskQueueWithHashRangeQry, v),
			filter.RangeHashGreaterThanEqualTo,
			filter.RangeHashLessThanEqualTo,
			filter.TaskQueueIDGreaterThan,
			*filter.PageSize,
		)
	} else {
		err = pdb.SelectContext(ctx,
			&rows,
			sqlplugin.SwitchTaskQueuesTable(listTaskQueueQry, v),
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
func (pdb *db) DeleteFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		sqlplugin.SwitchTaskQueuesTable(deleteTaskQueueQry, v),
		filter.RangeHash,
		filter.TaskQueueID,
		*filter.RangeID,
	)
}

// LockTaskQueues locks a row in task_queues table
func (pdb *db) LockTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) (int64, error) {
	var rangeID int64
	err := pdb.GetContext(ctx,
		&rangeID,
		sqlplugin.SwitchTaskQueuesTable(lockTaskQueueQry, v),
		filter.RangeHash,
		filter.TaskQueueID,
	)
	return rangeID, err
}
