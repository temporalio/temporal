// Copyright (c) 2019 Uber Technologies, Inc.
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

	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

const (
	taskListCreatePart = `INTO task_lists(shard_id, domain_id, name, task_type, range_id, data, data_encoding) ` +
		`VALUES (:shard_id, :domain_id, :name, :task_type, :range_id, :data, :data_encoding)`

	// (default range ID: initialRangeID == 1)
	createTaskListQry = `INSERT ` + taskListCreatePart

	replaceTaskListQry = `INSERT ` + taskListCreatePart +
		`ON CONFLICT (shard_id, domain_id, name, task_type) DO UPDATE
SET range_id = excluded.range_id,
data = excluded.data,
data_encoding = excluded.data_encoding`

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
		`WHERE shard_id = $1 AND domain_id > $2 AND name > $3 AND task_type > $4 ORDER BY domain_id,name,task_type LIMIT $5`

	getTaskListQry = `SELECT domain_id, range_id, name, task_type, data, data_encoding ` +
		`FROM task_lists ` +
		`WHERE shard_id = $1 AND domain_id = $2 AND name = $3 AND task_type = $4`

	deleteTaskListQry = `DELETE FROM task_lists WHERE shard_id=$1 AND domain_id=$2 AND name=$3 AND task_type=$4 AND range_id=$5`

	lockTaskListQry = `SELECT range_id FROM task_lists ` +
		`WHERE shard_id = $1 AND domain_id = $2 AND name = $3 AND task_type = $4 FOR UPDATE`

	getTaskMinMaxQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE domain_id = $1 AND task_list_name = $2 AND task_type = $3 AND task_id > $4 AND task_id <= $5 ` +
		` ORDER BY task_id LIMIT $6`

	getTaskMinQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE domain_id = $1 AND task_list_name = $2 AND task_type = $3 AND task_id > $4 ORDER BY task_id LIMIT $5`

	createTaskQry = `INSERT INTO ` +
		`tasks(domain_id, task_list_name, task_type, task_id, data, data_encoding) ` +
		`VALUES(:domain_id, :task_list_name, :task_type, :task_id, :data, :data_encoding)`

	deleteTaskQry = `DELETE FROM tasks ` +
		`WHERE domain_id = $1 AND task_list_name = $2 AND task_type = $3 AND task_id = $4`

	rangeDeleteTaskQry = `DELETE FROM tasks ` +
		`WHERE domain_id = $1 AND task_list_name = $2 AND task_type = $3 AND task_id IN (SELECT task_id FROM
		 tasks WHERE domain_id = $1 AND task_list_name = $2 AND task_type = $3 AND task_id <= $4 ` +
		`ORDER BY domain_id,task_list_name,task_type,task_id LIMIT $5 )`
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
		err = pdb.conn.Select(&rows, getTaskMinMaxQry, filter.DomainID,
			filter.TaskListName, filter.TaskType, *filter.MinTaskID, *filter.MaxTaskID, *filter.PageSize)
	default:
		err = pdb.conn.Select(&rows, getTaskMinQry, filter.DomainID,
			filter.TaskListName, filter.TaskType, *filter.MinTaskID, *filter.PageSize)
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
			filter.DomainID, filter.TaskListName, filter.TaskType, *filter.TaskIDLessThanEquals, *filter.Limit)
	}
	return pdb.conn.Exec(deleteTaskQry, filter.DomainID, filter.TaskListName, filter.TaskType, *filter.TaskID)
}

// InsertIntoTaskLists inserts one or more rows into task_lists table
func (pdb *db) InsertIntoTaskLists(row *sqlplugin.TaskListsRow) (sql.Result, error) {
	return pdb.conn.NamedExec(createTaskListQry, row)
}

// ReplaceIntoTaskLists replaces one or more rows in task_lists table
func (pdb *db) ReplaceIntoTaskLists(row *sqlplugin.TaskListsRow) (sql.Result, error) {
	return pdb.conn.NamedExec(replaceTaskListQry, row)
}

// UpdateTaskLists updates a row in task_lists table
func (pdb *db) UpdateTaskLists(row *sqlplugin.TaskListsRow) (sql.Result, error) {
	return pdb.conn.NamedExec(updateTaskListQry, row)
}

// SelectFromTaskLists reads one or more rows from task_lists table
func (pdb *db) SelectFromTaskLists(filter *sqlplugin.TaskListsFilter) ([]sqlplugin.TaskListsRow, error) {
	switch {
	case filter.DomainID != nil && filter.Name != nil && filter.TaskType != nil:
		return pdb.selectFromTaskLists(filter)
	case filter.DomainIDGreaterThan != nil && filter.NameGreaterThan != nil && filter.TaskTypeGreaterThan != nil && filter.PageSize != nil:
		return pdb.rangeSelectFromTaskLists(filter)
	default:
		return nil, fmt.Errorf("invalid set of query filter params")
	}
}

func (pdb *db) selectFromTaskLists(filter *sqlplugin.TaskListsFilter) ([]sqlplugin.TaskListsRow, error) {
	var err error
	var row sqlplugin.TaskListsRow
	err = pdb.conn.Get(&row, getTaskListQry, filter.ShardID, *filter.DomainID, *filter.Name, *filter.TaskType)
	if err != nil {
		return nil, err
	}
	return []sqlplugin.TaskListsRow{row}, err
}

func (pdb *db) rangeSelectFromTaskLists(filter *sqlplugin.TaskListsFilter) ([]sqlplugin.TaskListsRow, error) {
	var err error
	var rows []sqlplugin.TaskListsRow
	err = pdb.conn.Select(&rows, listTaskListQry,
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
func (pdb *db) DeleteFromTaskLists(filter *sqlplugin.TaskListsFilter) (sql.Result, error) {
	return pdb.conn.Exec(deleteTaskListQry, filter.ShardID, *filter.DomainID, *filter.Name, *filter.TaskType, *filter.RangeID)
}

// LockTaskLists locks a row in task_lists table
func (pdb *db) LockTaskLists(filter *sqlplugin.TaskListsFilter) (int64, error) {
	var rangeID int64
	err := pdb.conn.Get(&rangeID, lockTaskListQry, filter.ShardID, *filter.DomainID, *filter.Name, *filter.TaskType)
	return rangeID, err
}
