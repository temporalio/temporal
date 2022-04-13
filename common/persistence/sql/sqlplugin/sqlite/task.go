// The MIT License
//
// Copyright (c) 2021 Datadog, Inc.
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	taskQueueCreatePart = `INTO task_queues(range_hash, task_queue_id, range_id, data, data_encoding) ` +
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

	listTaskQueueRowSelect = `SELECT range_hash, task_queue_id, range_id, data, data_encoding from task_queues `

	listTaskQueueWithHashRangeQry = listTaskQueueRowSelect +
		`WHERE range_hash >= ? AND range_hash <= ? AND task_queue_id > ? ORDER BY task_queue_id ASC LIMIT ?`

	listTaskQueueQry = listTaskQueueRowSelect +
		`WHERE range_hash = ? AND task_queue_id > ? ORDER BY task_queue_id ASC LIMIT ?`

	getTaskQueueQry = listTaskQueueRowSelect +
		`WHERE range_hash = ? AND task_queue_id = ?`

	deleteTaskQueueQry = `DELETE FROM task_queues WHERE range_hash=? AND task_queue_id=? AND range_id=?`

	lockTaskQueueQry = `SELECT range_id FROM task_queues ` +
		`WHERE range_hash = ? AND task_queue_id = ?`
	// *** Task_Queues Table Above ***

	// *** Tasks Below ***
	getTaskMinMaxQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE range_hash = ? AND task_queue_id = ? AND task_id >= ? AND task_id < ? ` +
		` ORDER BY task_id LIMIT ?`

	getTaskMinQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE range_hash = ? AND task_queue_id = ? AND task_id >= ? ORDER BY task_id LIMIT ?`

	createTaskQry = `INSERT INTO ` +
		`tasks(range_hash, task_queue_id, task_id, data, data_encoding) ` +
		`VALUES(:range_hash, :task_queue_id, :task_id, :data, :data_encoding)`

	deleteTaskQry = `DELETE FROM tasks ` +
		`WHERE range_hash = ? AND task_queue_id = ? AND task_id = ?`

	rangeDeleteTaskQry = `DELETE FROM tasks ` +
		`WHERE range_hash = ? AND task_queue_id = ? AND task_id IN (SELECT task_id FROM
		 tasks WHERE range_hash = ? AND task_queue_id = ? AND task_id < ? ` +
		`ORDER BY task_queue_id,task_id LIMIT ? ) `
)

// InsertIntoTasks inserts one or more rows into tasks table
func (mdb *db) InsertIntoTasks(
	ctx context.Context,
	rows []sqlplugin.TasksRow,
) (sql.Result, error) {
	return mdb.conn.NamedExecContext(ctx,
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
		err = mdb.conn.SelectContext(ctx,
			&rows, getTaskMinMaxQry,
			filter.RangeHash,
			filter.TaskQueueID,
			*filter.InclusiveMinTaskID,
			*filter.ExclusiveMaxTaskID,
			*filter.PageSize,
		)
	default:
		err = mdb.conn.SelectContext(ctx,
			&rows, getTaskMinQry,
			filter.RangeHash,
			filter.TaskQueueID,
			*filter.ExclusiveMaxTaskID,
			*filter.PageSize,
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
	if filter.ExclusiveMaxTaskID != nil {
		if filter.Limit == nil || *filter.Limit == 0 {
			return nil, fmt.Errorf("missing limit parameter")
		}
		return mdb.conn.ExecContext(ctx,
			rangeDeleteTaskQry,
			filter.RangeHash,
			filter.TaskQueueID,
			filter.RangeHash,
			filter.TaskQueueID,
			*filter.ExclusiveMaxTaskID,
			*filter.Limit,
		)
	}
	return mdb.conn.ExecContext(ctx,
		deleteTaskQry,
		filter.RangeHash,
		filter.TaskQueueID,
		*filter.TaskID,
	)
}

// InsertIntoTaskQueues inserts one or more rows into task_queues table
func (mdb *db) InsertIntoTaskQueues(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
) (sql.Result, error) {
	return mdb.conn.NamedExecContext(ctx,
		createTaskQueueQry,
		row,
	)
}

// UpdateTaskQueues updates a row in task_queues table
func (mdb *db) UpdateTaskQueues(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
) (sql.Result, error) {
	return mdb.conn.NamedExecContext(ctx,
		updateTaskQueueQry,
		row,
	)
}

// SelectFromTaskQueues reads one or more rows from task_queues table
func (mdb *db) SelectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
) ([]sqlplugin.TaskQueuesRow, error) {
	switch {
	case filter.TaskQueueID != nil:
		if filter.RangeHashLessThanEqualTo != 0 || filter.RangeHashGreaterThanEqualTo != 0 {
			return nil, serviceerror.NewInternal("range of hashes not supported for specific selection")
		}
		return mdb.selectFromTaskQueues(ctx, filter)
	case filter.RangeHashLessThanEqualTo != 0 && filter.PageSize != nil:
		if filter.RangeHashLessThanEqualTo < filter.RangeHashGreaterThanEqualTo {
			return nil, serviceerror.NewInternal("range of hashes bound is invalid")
		}
		return mdb.rangeSelectFromTaskQueues(ctx, filter)
	case filter.TaskQueueIDGreaterThan != nil && filter.PageSize != nil:
		return mdb.rangeSelectFromTaskQueues(ctx, filter)
	default:
		return nil, serviceerror.NewInternal("invalid set of query filter params")
	}
}

func (mdb *db) selectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
) ([]sqlplugin.TaskQueuesRow, error) {
	var err error
	var row sqlplugin.TaskQueuesRow
	err = mdb.conn.GetContext(ctx,
		&row,
		getTaskQueueQry,
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
) ([]sqlplugin.TaskQueuesRow, error) {
	var err error
	var rows []sqlplugin.TaskQueuesRow

	if filter.RangeHashLessThanEqualTo != 0 {
		err = mdb.conn.SelectContext(ctx,
			&rows,
			listTaskQueueWithHashRangeQry,
			filter.RangeHashGreaterThanEqualTo,
			filter.RangeHashLessThanEqualTo,
			filter.TaskQueueIDGreaterThan,
			*filter.PageSize,
		)
	} else {
		err = mdb.conn.SelectContext(ctx,
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
func (mdb *db) DeleteFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
) (sql.Result, error) {
	return mdb.conn.ExecContext(ctx,
		deleteTaskQueueQry,
		filter.RangeHash,
		filter.TaskQueueID,
		*filter.RangeID,
	)
}

// LockTaskQueues locks a row in task_queues table
func (mdb *db) LockTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
) (int64, error) {
	var rangeID int64
	err := mdb.conn.GetContext(ctx,
		&rangeID,
		lockTaskQueueQry,
		filter.RangeHash,
		filter.TaskQueueID,
	)
	return rangeID, err
}
