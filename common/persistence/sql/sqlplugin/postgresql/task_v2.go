package postgresql

import (
	"context"
	"database/sql"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	// (default range ID: initialRangeID == 1)
	createTaskQueueQryV2 = `INSERT INTO task_queues_v2(range_hash, task_queue_id, range_id, data, data_encoding) ` +
		`VALUES (:range_hash, :task_queue_id, :range_id, :data, :data_encoding)`

	updateTaskQueueQryV2 = `UPDATE task_queues_v2 SET
		range_id = :range_id,
		data = :data,
		data_encoding = :data_encoding
		WHERE
		range_hash = :range_hash AND
		task_queue_id = :task_queue_id
		`

	listTaskQueueV2RowSelect = `SELECT range_hash, task_queue_id, range_id, data, data_encoding from task_queues_v2 `

	listTaskQueueWithHashRangeQryV2 = listTaskQueueV2RowSelect +
		`WHERE range_hash >= $1 AND range_hash <= $2 AND task_queue_id > $3 ORDER BY task_queue_id ASC LIMIT $4`

	listTaskQueueQryV2 = listTaskQueueV2RowSelect +
		`WHERE range_hash = $1 AND task_queue_id > $2 ORDER BY task_queue_id ASC LIMIT $3`

	getTaskQueueQryV2 = listTaskQueueV2RowSelect +
		`WHERE range_hash = $1 AND task_queue_id=$2`

	deleteTaskQueueQryV2 = `DELETE FROM task_queues_v2 WHERE range_hash=$1 AND task_queue_id=$2 AND range_id=$3`

	lockTaskQueueQryV2 = `SELECT range_id FROM task_queues_v2 ` +
		`WHERE range_hash=$1 AND task_queue_id=$2 FOR UPDATE`
	// *** Task_Queues Table Above ***

	// *** Tasks Below ***
	getFairnessTaskQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks_v2 ` +
		`WHERE range_hash = $1 ` +
		`AND task_queue_id = $2 ` +
		`AND (pass, task_id) >= ($3, $4) ` +
		`ORDER BY pass, task_id `

	getFairnessTaskQryWithLimit = `SELECT task_id, data, data_encoding ` +
		`FROM tasks_v2 ` +
		`WHERE range_hash = $1 ` +
		`AND task_queue_id = $2 ` +
		`AND (pass, task_id) >= ($3, $4) ` +
		`ORDER BY pass, task_id ` +
		`LIMIT $6`

	createTaskQryV2 = `INSERT INTO ` +
		`tasks_v2(range_hash, task_queue_id, task_id, pass, data, data_encoding) ` +
		`VALUES(:range_hash, :task_queue_id, :task_id, :pass, :data, :data_encoding)`

	rangeDeleteFairnessTaskQry = `DELETE FROM tasks_v2 ` +
		`WHERE range_hash = $1 ` +
		`AND task_queue_id = $2 ` +
		`AND (pass, task_id) < ($3, $4)`
)

// InsertIntoTasks inserts one or more rows into tasks table
func (pdb *db) InsertIntoTasksV2(
	ctx context.Context,
	rows []sqlplugin.TasksRowV2,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		createTaskQryV2,
		rows,
	)
}

// SelectFromTasks reads one or more rows from tasks table
func (pdb *db) SelectFromTasksV2(
	ctx context.Context,
	filter sqlplugin.TasksFilterV2,
) ([]sqlplugin.TasksRowV2, error) {
	var err error
	var rows []sqlplugin.TasksRowV2
	switch {
	case filter.PageSize != nil:
		err = pdb.SelectContext(ctx,
			&rows,
			getFairnessTaskQryWithLimit,
			filter.RangeHash,
			filter.TaskQueueID,
			filter.InclusiveMinPass,
			*filter.InclusiveMinTaskID,
			*filter.PageSize,
		)
	default:
		err = pdb.SelectContext(ctx,
			&rows,
			getFairnessTaskQry,
			filter.RangeHash,
			filter.TaskQueueID,
			filter.InclusiveMinPass,
			*filter.InclusiveMinTaskID,
		)
	}
	return rows, err
}

// DeleteFromTasks deletes multiple rows from tasks table
func (pdb *db) DeleteFromTasksV2(
	ctx context.Context,
	filter sqlplugin.TasksFilterV2,
) (sql.Result, error) {
	if filter.ExclusiveMaxTaskID == nil {
		return nil, serviceerror.NewInternal("missing ExclusiveMaxTaskID parameter")
	}
	return pdb.ExecContext(ctx,
		rangeDeleteFairnessTaskQry,
		filter.RangeHash,
		filter.TaskQueueID,
		filter.InclusiveMinPass,
		*filter.ExclusiveMaxTaskID,
	)
}

// InsertIntoTaskQueues inserts one or more rows into task_queues table
func (pdb *db) InsertIntoTaskQueuesV2(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRowV2,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		createTaskQueueQryV2,
		row,
	)
}

// UpdateTaskQueues updates a row in task_queues table
func (pdb *db) UpdateTaskQueuesV2(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRowV2,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		updateTaskQueueQryV2,
		row,
	)
}

// SelectFromTaskQueues reads one or more rows from task_queues table
func (pdb *db) SelectFromTaskQueuesV2(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilterV2,
) ([]sqlplugin.TaskQueuesRowV2, error) {
	switch {
	case filter.TaskQueueID != nil:
		if filter.RangeHashLessThanEqualTo != 0 || filter.RangeHashGreaterThanEqualTo != 0 {
			return nil, serviceerror.NewInternal("shardID range not supported for specific selection")
		}
		return pdb.selectFromTaskQueuesV2(ctx, filter)
	case filter.RangeHashLessThanEqualTo != 0 && filter.PageSize != nil:
		if filter.RangeHashLessThanEqualTo < filter.RangeHashGreaterThanEqualTo {
			return nil, serviceerror.NewInternal("range of hashes bound is invalid")
		}
		return pdb.rangeSelectFromTaskQueuesV2(ctx, filter)
	case filter.TaskQueueIDGreaterThan != nil && filter.PageSize != nil:
		return pdb.rangeSelectFromTaskQueuesV2(ctx, filter)
	default:
		return nil, serviceerror.NewInternal("invalid set of query filter params")
	}
}

func (pdb *db) selectFromTaskQueuesV2(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilterV2,
) ([]sqlplugin.TaskQueuesRowV2, error) {
	var err error
	var row sqlplugin.TaskQueuesRowV2
	err = pdb.GetContext(ctx,
		&row,
		getTaskQueueQryV2,
		filter.RangeHash,
		filter.TaskQueueID,
	)
	if err != nil {
		return nil, err
	}
	return []sqlplugin.TaskQueuesRowV2{row}, err
}

func (pdb *db) rangeSelectFromTaskQueuesV2(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilterV2,
) ([]sqlplugin.TaskQueuesRowV2, error) {
	var err error
	var rows []sqlplugin.TaskQueuesRowV2
	if filter.RangeHashLessThanEqualTo > 0 {
		err = pdb.SelectContext(ctx,
			&rows,
			listTaskQueueWithHashRangeQryV2,
			filter.RangeHashGreaterThanEqualTo,
			filter.RangeHashLessThanEqualTo,
			filter.TaskQueueIDGreaterThan,
			*filter.PageSize,
		)
	} else {
		err = pdb.SelectContext(ctx,
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
func (pdb *db) DeleteFromTaskQueuesV2(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilterV2,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		deleteTaskQueueQryV2,
		filter.RangeHash,
		filter.TaskQueueID,
		*filter.RangeID,
	)
}

// LockTaskQueues locks a row in task_queues table
func (pdb *db) LockTaskQueuesV2(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilterV2,
) (int64, error) {
	var rangeID int64
	err := pdb.GetContext(ctx,
		&rangeID,
		lockTaskQueueQryV2,
		filter.RangeHash,
		filter.TaskQueueID,
	)
	return rangeID, err
}
