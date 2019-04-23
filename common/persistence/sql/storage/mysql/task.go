// Copyright (c) 2017 Uber Technologies, Inc.
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

package mysql

import (
	"database/sql"

	"fmt"

	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
)

const (
	taskListCreatePart = `INTO task_lists(shard_id, domain_id, name, task_type, range_id, data, data_encoding) ` +
		`VALUES (:shard_id, :domain_id, :name, :task_type, :range_id, :data, :data_encoding)`

	// (default range ID: initialRangeID == 1)
	createTaskListQry = `INSERT ` + taskListCreatePart

	replaceTaskListQry = `REPLACE ` + taskListCreatePart

	updateTaskListQry = `UPDATE task_lists SET
range_id = :range_id,
data = :data,
data_encoding = :data_encoding
WHERE
shard_id = :shard_id AND
domain_id = :domain_id AND
name = :name AND
task_type = :task_type
`

	listTaskListQry = `SELECT domain_id, range_id, name, task_type, data, data_encoding ` +
		`FROM task_lists ` +
		`WHERE shard_id = ? AND domain_id > ? AND name > ? AND task_type > ? ORDER BY domain_id,name,task_type LIMIT ?`

	getTaskListQry = `SELECT domain_id, range_id, name, task_type, data, data_encoding ` +
		`FROM task_lists ` +
		`WHERE shard_id = ? AND domain_id = ? AND name = ? AND task_type = ?`

	deleteTaskListQry = `DELETE FROM task_lists WHERE shard_id=? AND domain_id=? AND name=? AND task_type=? AND range_id=?`

	lockTaskListQry = `SELECT range_id FROM task_lists ` +
		`WHERE shard_id = ? AND domain_id = ? AND name = ? AND task_type = ? FOR UPDATE`

	getTaskMinMaxQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE domain_id = ? AND task_list_name = ? AND task_type = ? AND task_id > ? AND task_id <= ? ` +
		` ORDER BY task_id LIMIT ?`

	getTaskMinQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE domain_id = ? AND task_list_name = ? AND task_type = ? AND task_id > ? ORDER BY task_id LIMIT ?`

	createTaskQry = `INSERT INTO ` +
		`tasks(domain_id, task_list_name, task_type, task_id, data, data_encoding) ` +
		`VALUES(:domain_id, :task_list_name, :task_type, :task_id, :data, :data_encoding)`

	deleteTaskQry = `DELETE FROM tasks ` +
		`WHERE domain_id = ? AND task_list_name = ? AND task_type = ? AND task_id = ?`

	rangeDeleteTaskQry = `DELETE FROM tasks ` +
		`WHERE domain_id = ? AND task_list_name = ? AND task_type = ? AND task_id <= ? ` +
		`ORDER BY domain_id,task_list_name,task_type,task_id LIMIT ?`
)

// InsertIntoTasks inserts one or more rows into tasks table
func (mdb *DB) InsertIntoTasks(rows []sqldb.TasksRow) (sql.Result, error) {
	return mdb.conn.NamedExec(createTaskQry, rows)
}

// SelectFromTasks reads one or more rows from tasks table
func (mdb *DB) SelectFromTasks(filter *sqldb.TasksFilter) ([]sqldb.TasksRow, error) {
	var err error
	var rows []sqldb.TasksRow
	switch {
	case filter.MaxTaskID != nil:
		err = mdb.conn.Select(&rows, getTaskMinMaxQry, filter.DomainID,
			filter.TaskListName, filter.TaskType, *filter.MinTaskID, *filter.MaxTaskID, *filter.PageSize)
	default:
		err = mdb.conn.Select(&rows, getTaskMinQry, filter.DomainID,
			filter.TaskListName, filter.TaskType, *filter.MinTaskID, *filter.PageSize)
	}
	if err != nil {
		return nil, err
	}
	return rows, err
}

// DeleteFromTasks deletes one or more rows from tasks table
func (mdb *DB) DeleteFromTasks(filter *sqldb.TasksFilter) (sql.Result, error) {
	if filter.TaskIDLessThanEquals != nil {
		if filter.Limit == nil || *filter.Limit == 0 {
			return nil, fmt.Errorf("missing limit parameter")
		}
		return mdb.conn.Exec(rangeDeleteTaskQry,
			filter.DomainID, filter.TaskListName, filter.TaskType, *filter.TaskIDLessThanEquals, *filter.Limit)
	}
	return mdb.conn.Exec(deleteTaskQry, filter.DomainID, filter.TaskListName, filter.TaskType, *filter.TaskID)
}

// InsertIntoTaskLists inserts one or more rows into task_lists table
func (mdb *DB) InsertIntoTaskLists(row *sqldb.TaskListsRow) (sql.Result, error) {
	return mdb.conn.NamedExec(createTaskListQry, row)
}

// ReplaceIntoTaskLists replaces one or more rows in task_lists table
func (mdb *DB) ReplaceIntoTaskLists(row *sqldb.TaskListsRow) (sql.Result, error) {
	return mdb.conn.NamedExec(replaceTaskListQry, row)
}

// UpdateTaskLists updates a row in task_lists table
func (mdb *DB) UpdateTaskLists(row *sqldb.TaskListsRow) (sql.Result, error) {
	return mdb.conn.NamedExec(updateTaskListQry, row)
}

// SelectFromTaskLists reads one or more rows from task_lists table
func (mdb *DB) SelectFromTaskLists(filter *sqldb.TaskListsFilter) ([]sqldb.TaskListsRow, error) {
	switch {
	case filter.DomainID != nil && filter.Name != nil && filter.TaskType != nil:
		return mdb.selectFromTaskLists(filter)
	case filter.DomainIDGreaterThan != nil && filter.NameGreaterThan != nil && filter.TaskTypeGreaterThan != nil && filter.PageSize != nil:
		return mdb.rangeSelectFromTaskLists(filter)
	default:
		return nil, fmt.Errorf("invalid set of query filter params")
	}
}

func (mdb *DB) selectFromTaskLists(filter *sqldb.TaskListsFilter) ([]sqldb.TaskListsRow, error) {
	var err error
	var row sqldb.TaskListsRow
	err = mdb.conn.Get(&row, getTaskListQry, filter.ShardID, *filter.DomainID, *filter.Name, *filter.TaskType)
	if err != nil {
		return nil, err
	}
	return []sqldb.TaskListsRow{row}, err
}

func (mdb *DB) rangeSelectFromTaskLists(filter *sqldb.TaskListsFilter) ([]sqldb.TaskListsRow, error) {
	var err error
	var rows []sqldb.TaskListsRow
	err = mdb.conn.Select(&rows, listTaskListQry,
		filter.ShardID, *filter.DomainIDGreaterThan, *filter.NameGreaterThan, *filter.TaskTypeGreaterThan, *filter.PageSize)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].ShardID = filter.ShardID
	}
	return rows, nil
}

// DeleteFromTaskLists deletes a row from task_lists table
func (mdb *DB) DeleteFromTaskLists(filter *sqldb.TaskListsFilter) (sql.Result, error) {
	return mdb.conn.Exec(deleteTaskListQry, filter.ShardID, *filter.DomainID, *filter.Name, *filter.TaskType, *filter.RangeID)
}

// LockTaskLists locks a row in task_lists table
func (mdb *DB) LockTaskLists(filter *sqldb.TaskListsFilter) (int64, error) {
	var rangeID int64
	err := mdb.conn.Get(&rangeID, lockTaskListQry, filter.ShardID, *filter.DomainID, *filter.Name, *filter.TaskType)
	return rangeID, err
}
