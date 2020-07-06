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

package postgres

import (
	"database/sql"
	"fmt"

	"github.com/temporalio/temporal/common/persistence/sql/sqlplugin"
)

const (
	taskQueueCreatePart = `INTO task_queues(shard_id, namespace_id, name, task_type, range_id, data, data_encoding) ` +
		`VALUES (:shard_id, :namespace_id, :name, :task_type, :range_id, :data, :data_encoding)`

	// (default range ID: initialRangeID == 1)
	createTaskQueueQry = `INSERT ` + taskQueueCreatePart

	replaceTaskQueueQry = `INSERT ` + taskQueueCreatePart +
		`ON CONFLICT (shard_id, namespace_id, name, task_type) DO UPDATE
SET range_id = excluded.range_id,
data = excluded.data,
data_encoding = excluded.data_encoding`

	updateTaskQueueQry = `UPDATE task_queues SET
range_id = :range_id,
data = :data,
data_encoding = :data_encoding
WHERE
shard_id = :shard_id AND
namespace_id = :namespace_id AND
name = :name AND
task_type = :task_type
`
	listTaskQueueWithShardRangeQry = `SELECT shard_id, namespace_id, range_id, name, task_type, data, data_encoding ` +
		`FROM task_queues ` +
		`WHERE shard_id >= $1 AND shard_id <= $2 AND namespace_id > $3 AND name > $4 AND task_type > $5 ORDER BY shard_id, namespace_id,name,task_type LIMIT $6`

	listTaskQueueQry = `SELECT shard_id, namespace_id, range_id, name, task_type, data, data_encoding ` +
		`FROM task_queues ` +
		`WHERE shard_id = $1 AND namespace_id > $2 AND name > $3 AND task_type > $4 ORDER BY shard_id, namespace_id,name,task_type LIMIT $5`

	getTaskQueueQry = `SELECT namespace_id, range_id, name, task_type, data, data_encoding ` +
		`FROM task_queues ` +
		`WHERE shard_id = $1 AND namespace_id = $2 AND name = $3 AND task_type = $4`

	deleteTaskQueueQry = `DELETE FROM task_queues WHERE shard_id=$1 AND namespace_id=$2 AND name=$3 AND task_type=$4 AND range_id=$5`

	lockTaskQueueQry = `SELECT range_id FROM task_queues ` +
		`WHERE shard_id = $1 AND namespace_id = $2 AND name = $3 AND task_type = $4 FOR UPDATE`

	getTaskMinMaxQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE namespace_id = $1 AND task_queue_name = $2 AND task_type = $3 AND task_id > $4 AND task_id <= $5 ` +
		` ORDER BY task_id LIMIT $6`

	getTaskMinQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE namespace_id = $1 AND task_queue_name = $2 AND task_type = $3 AND task_id > $4 ORDER BY task_id LIMIT $5`

	createTaskQry = `INSERT INTO ` +
		`tasks(namespace_id, task_queue_name, task_type, task_id, data, data_encoding) ` +
		`VALUES(:namespace_id, :task_queue_name, :task_type, :task_id, :data, :data_encoding)`

	deleteTaskQry = `DELETE FROM tasks ` +
		`WHERE namespace_id = $1 AND task_queue_name = $2 AND task_type = $3 AND task_id = $4`

	rangeDeleteTaskQry = `DELETE FROM tasks ` +
		`WHERE namespace_id = $1 AND task_queue_name = $2 AND task_type = $3 AND task_id IN (SELECT task_id FROM
		 tasks WHERE namespace_id = $1 AND task_queue_name = $2 AND task_type = $3 AND task_id <= $4 ` +
		`ORDER BY namespace_id,task_queue_name,task_type,task_id LIMIT $5 )`
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
		err = pdb.conn.Select(&rows, getTaskMinMaxQry, filter.NamespaceID,
			filter.TaskQueueName, filter.TaskType, *filter.MinTaskID, *filter.MaxTaskID, *filter.PageSize)
	default:
		err = pdb.conn.Select(&rows, getTaskMinQry, filter.NamespaceID,
			filter.TaskQueueName, filter.TaskType, *filter.MinTaskID, *filter.PageSize)
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
			filter.NamespaceID, filter.TaskQueueName, filter.TaskType, *filter.TaskIDLessThanEquals, *filter.Limit)
	}
	return pdb.conn.Exec(deleteTaskQry, filter.NamespaceID, filter.TaskQueueName, filter.TaskType, *filter.TaskID)
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
	case filter.NamespaceID != nil && filter.Name != nil && filter.TaskType != nil:
		if filter.ShardIDLessThanEqualTo != 0 || filter.ShardIDGreaterThanEqualTo != 0 {
			return nil, fmt.Errorf("shardID range not supported for specific selection")
		}
		return pdb.selectFromTaskQueues(filter)
	case filter.NamespaceIDGreaterThan != nil && filter.NameGreaterThan != nil && filter.TaskTypeGreaterThan != nil && filter.PageSize != nil:
		return pdb.rangeSelectFromTaskQueues(filter)
	default:
		return nil, fmt.Errorf("invalid set of query filter params")
	}
}

func (pdb *db) selectFromTaskQueues(filter *sqlplugin.TaskQueuesFilter) ([]sqlplugin.TaskQueuesRow, error) {
	var err error
	var row sqlplugin.TaskQueuesRow
	err = pdb.conn.Get(&row, getTaskQueueQry, filter.ShardID, *filter.NamespaceID, *filter.Name, *filter.TaskType)
	if err != nil {
		return nil, err
	}
	return []sqlplugin.TaskQueuesRow{row}, err
}

func (pdb *db) rangeSelectFromTaskQueues(filter *sqlplugin.TaskQueuesFilter) ([]sqlplugin.TaskQueuesRow, error) {
	var err error
	var rows []sqlplugin.TaskQueuesRow
	if filter.ShardIDLessThanEqualTo > 0 {
		err = pdb.conn.Select(&rows, listTaskQueueWithShardRangeQry,
			filter.ShardIDGreaterThanEqualTo, filter.ShardIDLessThanEqualTo, *filter.NamespaceIDGreaterThan, *filter.NameGreaterThan, *filter.TaskTypeGreaterThan, *filter.PageSize)
	} else {
		err = pdb.conn.Select(&rows, listTaskQueueQry,
			filter.ShardID, *filter.NamespaceIDGreaterThan, *filter.NameGreaterThan, *filter.TaskTypeGreaterThan, *filter.PageSize)
	}
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// DeleteFromTaskQueues deletes a row from task_queues table
func (pdb *db) DeleteFromTaskQueues(filter *sqlplugin.TaskQueuesFilter) (sql.Result, error) {
	return pdb.conn.Exec(deleteTaskQueueQry, filter.ShardID, *filter.NamespaceID, *filter.Name, *filter.TaskType, *filter.RangeID)
}

// LockTaskQueues locks a row in task_queues table
func (pdb *db) LockTaskQueues(filter *sqlplugin.TaskQueuesFilter) (int64, error) {
	var rangeID int64
	err := pdb.conn.Get(&rangeID, lockTaskQueueQry, filter.ShardID, *filter.NamespaceID, *filter.Name, *filter.TaskType)
	return rangeID, err
}
