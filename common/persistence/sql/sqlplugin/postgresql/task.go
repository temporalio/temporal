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
	// *** Task_Queues Table Above ***

	// *** Tasks Below ***
	getTaskMinMaxQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE range_hash = $1 AND task_queue_id=$2 AND task_id >= $3 AND task_id < $4 ` +
		`ORDER BY task_id LIMIT $5`

	getTaskMinQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE range_hash = $1 AND task_queue_id = $2 AND task_id >= $3 ORDER BY task_id LIMIT $4`

	createTaskQry = `INSERT INTO ` +
		`tasks(range_hash, task_queue_id, task_id, data, data_encoding) ` +
		`VALUES(:range_hash, :task_queue_id, :task_id, :data, :data_encoding)`

	rangeDeleteTaskQry = `DELETE FROM tasks ` +
		`WHERE range_hash = $1 AND task_queue_id = $2 AND task_id IN (SELECT task_id FROM
		 tasks WHERE range_hash = $1 AND task_queue_id = $2 AND task_id < $3 ` +
		`ORDER BY task_queue_id,task_id LIMIT $4 )`
)

// InsertIntoTasks inserts one or more rows into tasks table
func (pdb *db) InsertIntoTasks(
	ctx context.Context,
	rows []sqlplugin.TasksRow,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		createTaskQry,
		rows,
	)
}

// SelectFromTasks reads one or more rows from tasks table
func (pdb *db) SelectFromTasks(
	ctx context.Context,
	filter sqlplugin.TasksFilter,
) ([]sqlplugin.TasksRow, error) {
	var err error
	var rows []sqlplugin.TasksRow
	switch {
	case filter.ExclusiveMaxTaskID != nil:
		err = pdb.SelectContext(ctx,
			&rows,
			getTaskMinMaxQry,
			filter.RangeHash,
			filter.TaskQueueID,
			*filter.InclusiveMinTaskID,
			*filter.ExclusiveMaxTaskID,
			*filter.PageSize,
		)
	default:
		err = pdb.SelectContext(ctx,
			&rows,
			getTaskMinQry,
			filter.RangeHash,
			filter.TaskQueueID,
			*filter.InclusiveMinTaskID,
			*filter.PageSize,
		)
	}
	return rows, err
}

// DeleteFromTasks deletes multiple rows from tasks table
func (pdb *db) DeleteFromTasks(
	ctx context.Context,
	filter sqlplugin.TasksFilter,
) (sql.Result, error) {
	if filter.ExclusiveMaxTaskID == nil {
		return nil, serviceerror.NewInternal("missing ExclusiveMaxTaskID parameter")
	}
	if filter.Limit == nil || *filter.Limit == 0 {
		return nil, serviceerror.NewInternal("missing limit parameter")
	}
	return pdb.ExecContext(ctx,
		rangeDeleteTaskQry,
		filter.RangeHash,
		filter.TaskQueueID,
		*filter.ExclusiveMaxTaskID,
		*filter.Limit,
	)
}

// InsertIntoTaskQueues inserts one or more rows into task_queues table
func (pdb *db) InsertIntoTaskQueues(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		createTaskQueueQry,
		row,
	)
}

// UpdateTaskQueues updates a row in task_queues table
func (pdb *db) UpdateTaskQueues(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		updateTaskQueueQry,
		row,
	)
}

// SelectFromTaskQueues reads one or more rows from task_queues table
func (pdb *db) SelectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
) ([]sqlplugin.TaskQueuesRow, error) {
	switch {
	case filter.TaskQueueID != nil:
		if filter.RangeHashLessThanEqualTo != 0 || filter.RangeHashGreaterThanEqualTo != 0 {
			return nil, serviceerror.NewInternal("shardID range not supported for specific selection")
		}
		return pdb.selectFromTaskQueues(ctx, filter)
	case filter.RangeHashLessThanEqualTo != 0 && filter.PageSize != nil:
		if filter.RangeHashLessThanEqualTo < filter.RangeHashGreaterThanEqualTo {
			return nil, serviceerror.NewInternal("range of hashes bound is invalid")
		}
		return pdb.rangeSelectFromTaskQueues(ctx, filter)
	case filter.TaskQueueIDGreaterThan != nil && filter.PageSize != nil:
		return pdb.rangeSelectFromTaskQueues(ctx, filter)
	default:
		return nil, serviceerror.NewInternal("invalid set of query filter params")
	}
}

func (pdb *db) selectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
) ([]sqlplugin.TaskQueuesRow, error) {
	var err error
	var row sqlplugin.TaskQueuesRow
	err = pdb.GetContext(ctx,
		&row,
		getTaskQueueQry,
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
) ([]sqlplugin.TaskQueuesRow, error) {
	var err error
	var rows []sqlplugin.TaskQueuesRow
	if filter.RangeHashLessThanEqualTo > 0 {
		err = pdb.SelectContext(ctx,
			&rows,
			listTaskQueueWithHashRangeQry,
			filter.RangeHashGreaterThanEqualTo,
			filter.RangeHashLessThanEqualTo,
			filter.TaskQueueIDGreaterThan,
			*filter.PageSize,
		)
	} else {
		err = pdb.SelectContext(ctx,
			&rows,
			listTaskQueueQry,
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
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		deleteTaskQueueQry,
		filter.RangeHash,
		filter.TaskQueueID,
		*filter.RangeID,
	)
}

// LockTaskQueues locks a row in task_queues table
func (pdb *db) LockTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
) (int64, error) {
	var rangeID int64
	err := pdb.GetContext(ctx,
		&rangeID,
		lockTaskQueueQry,
		filter.RangeHash,
		filter.TaskQueueID,
	)
	return rangeID, err
}
