// The MIT License
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

package postgresql

import (
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

	replaceTaskQueueQry = `INSERT ` + taskQueueCreatePart +
		`ON CONFLICT (range_hash, task_queue_id) DO UPDATE
SET range_id = excluded.range_id,
data = excluded.data,
data_encoding = excluded.data_encoding`

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
		`WHERE range_hash = $1 AND task_queue_id=$2 AND task_id > $3 AND task_id <= $4 ` +
		`ORDER BY task_id LIMIT $5`

	getTaskMinQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE range_hash = $1 AND task_queue_id = $2 AND task_id > $3 ORDER BY task_id LIMIT $4`

	createTaskQry = `INSERT INTO ` +
		`tasks(range_hash, task_queue_id, task_id, data, data_encoding) ` +
		`VALUES(:range_hash, :task_queue_id, :task_id, :data, :data_encoding)`

	deleteTaskQry = `DELETE FROM tasks ` +
		`WHERE range_hash = $1 AND task_queue_id = $2 AND task_id = $3`

	rangeDeleteTaskQry = `DELETE FROM tasks ` +
		`WHERE range_hash = $1 AND task_queue_id = $2 AND task_id IN (SELECT task_id FROM
		 tasks WHERE range_hash = $1 AND task_queue_id = $2 AND task_id <= $3 ` +
		`ORDER BY task_queue_id,task_id LIMIT $4 )`
)

// InsertIntoTasks inserts one or more rows into tasks table
func (pdb *db) InsertIntoTasks(rows []sqlplugin.TasksRow) (sql.Result, error) {
	return pdb.conn.NamedExec(createTaskQry, rows)
}

// SelectFromTasks reads one or more rows from tasks table
func (pdb *db) SelectFromTasks(filter *sqlplugin.TasksFilter) ([]sqlplugin.TasksRow, error) {
	var err error
	var rows []sqlplugin.TasksRow
	switch {
	case filter.MaxTaskID != nil:
		err = pdb.conn.Select(&rows, getTaskMinMaxQry,
			filter.RangeHash, filter.TaskQueueID, *filter.MinTaskID, *filter.MaxTaskID, *filter.PageSize)
	default:
		err = pdb.conn.Select(&rows, getTaskMinQry,
			filter.RangeHash, filter.TaskQueueID, *filter.MinTaskID, *filter.PageSize)
	}
	if err != nil {
		return nil, err
	}
	return rows, err
}

// DeleteFromTasks deletes one or more rows from tasks table
func (pdb *db) DeleteFromTasks(filter *sqlplugin.TasksFilter) (sql.Result, error) {
	if filter.TaskIDLessThanEquals != nil {
		if filter.Limit == nil || *filter.Limit == 0 {
			return nil, fmt.Errorf("missing limit parameter")
		}
		return pdb.conn.Exec(rangeDeleteTaskQry,
			filter.RangeHash, filter.TaskQueueID, *filter.TaskIDLessThanEquals, *filter.Limit)
	}
	return pdb.conn.Exec(deleteTaskQry, filter.RangeHash, filter.TaskQueueID, *filter.TaskID)
}

// InsertIntoTaskQueues inserts one or more rows into task_queues table
func (pdb *db) InsertIntoTaskQueues(row *sqlplugin.TaskQueuesRow) (sql.Result, error) {
	return pdb.conn.NamedExec(createTaskQueueQry, row)
}

// ReplaceIntoTaskQueues replaces one or more rows in task_queues table
func (pdb *db) ReplaceIntoTaskQueues(row *sqlplugin.TaskQueuesRow) (sql.Result, error) {
	return pdb.conn.NamedExec(replaceTaskQueueQry, row)
}

// UpdateTaskQueues updates a row in task_queues table
func (pdb *db) UpdateTaskQueues(row *sqlplugin.TaskQueuesRow) (sql.Result, error) {
	return pdb.conn.NamedExec(updateTaskQueueQry, row)
}

// SelectFromTaskQueues reads one or more rows from task_queues table
func (pdb *db) SelectFromTaskQueues(filter *sqlplugin.TaskQueuesFilter) ([]sqlplugin.TaskQueuesRow, error) {
	switch {
	case filter.TaskQueueID != nil:
		if filter.RangeHashLessThanEqualTo != 0 || filter.RangeHashGreaterThanEqualTo != 0 {
			return nil, serviceerror.NewInternal("shardID range not supported for specific selection")
		}
		return pdb.selectFromTaskQueues(filter)
	case filter.RangeHashLessThanEqualTo != 0 && filter.PageSize != nil:
		if filter.RangeHashLessThanEqualTo < filter.RangeHashGreaterThanEqualTo {
			return nil, serviceerror.NewInternal("range of hashes bound is invalid")
		}
		return pdb.rangeSelectFromTaskQueues(filter)
	case filter.TaskQueueIDGreaterThan != nil && filter.PageSize != nil:
		return pdb.rangeSelectFromTaskQueues(filter)
	default:
		return nil, serviceerror.NewInternal("invalid set of query filter params")
	}
}

func (pdb *db) selectFromTaskQueues(filter *sqlplugin.TaskQueuesFilter) ([]sqlplugin.TaskQueuesRow, error) {
	var err error
	var row sqlplugin.TaskQueuesRow
	err = pdb.conn.Get(&row, getTaskQueueQry, filter.RangeHash, filter.TaskQueueID)
	if err != nil {
		return nil, err
	}
	return []sqlplugin.TaskQueuesRow{row}, err
}

func (pdb *db) rangeSelectFromTaskQueues(filter *sqlplugin.TaskQueuesFilter) ([]sqlplugin.TaskQueuesRow, error) {
	var err error
	var rows []sqlplugin.TaskQueuesRow
	if filter.RangeHashLessThanEqualTo > 0 {
		err = pdb.conn.Select(&rows, listTaskQueueWithHashRangeQry,
			filter.RangeHashGreaterThanEqualTo, filter.RangeHashLessThanEqualTo, filter.TaskQueueIDGreaterThan, *filter.PageSize)
	} else {
		err = pdb.conn.Select(&rows, listTaskQueueQry,
			filter.RangeHash, filter.TaskQueueIDGreaterThan, *filter.PageSize)
	}
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// DeleteFromTaskQueues deletes a row from task_queues table
func (pdb *db) DeleteFromTaskQueues(filter *sqlplugin.TaskQueuesFilter) (sql.Result, error) {
	return pdb.conn.Exec(deleteTaskQueueQry, filter.RangeHash, filter.TaskQueueID, *filter.RangeID)
}

// LockTaskQueues locks a row in task_queues table
func (pdb *db) LockTaskQueues(filter *sqlplugin.TaskQueuesFilter) (int64, error) {
	var rangeID int64
	err := pdb.conn.Get(&rangeID, lockTaskQueueQry, filter.RangeHash, filter.TaskQueueID)
	return rangeID, err
}
