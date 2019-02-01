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

	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
)

const (
	taskListCreatePart = `INTO task_lists(domain_id, range_id, name, task_type, ack_level, kind, expiry_ts) ` +
		`VALUES (:domain_id, :range_id, :name, :task_type, :ack_level, :kind, :expiry_ts)`

	// (default range ID: initialRangeID == 1)
	createTaskListQry = `INSERT ` + taskListCreatePart

	replaceTaskListQry = `REPLACE ` + taskListCreatePart

	updateTaskListQry = `UPDATE task_lists SET
domain_id = :domain_id,
range_id = :range_id,
name = :name,
task_type = :task_type,
ack_level = :ack_level,
kind = :kind,
expiry_ts = :expiry_ts
WHERE
domain_id = :domain_id AND
name = :name AND
task_type = :task_type
`

	getTaskListQry = `SELECT domain_id, range_id, name, task_type, ack_level, kind, expiry_ts ` +
		`FROM task_lists ` +
		`WHERE domain_id = ? AND name = ? AND task_type = ?`

	lockTaskListQry = `SELECT range_id FROM task_lists ` +
		`WHERE domain_id = ? AND name = ? AND task_type = ? FOR UPDATE`

	getTaskQry = `SELECT workflow_id, run_id, schedule_id, task_id ` +
		`FROM tasks ` +
		`WHERE domain_id = ? AND task_list_name = ? AND task_type = ? AND task_id > ? AND task_id <= ?`

	createTaskQry = `INSERT INTO ` +
		`tasks(domain_id, workflow_id, run_id, schedule_id, task_list_name, task_type, task_id, expiry_ts) ` +
		`VALUES(:domain_id, :workflow_id, :run_id, :schedule_id, :task_list_name, :task_type, :task_id, :expiry_ts)`

	deleteTaskQry = `DELETE FROM tasks ` +
		`WHERE domain_id = ? AND task_list_name = ? AND task_type = ? AND task_id = ?`
)

// InsertIntoTasks inserts one or more rows into tasks table
func (mdb *DB) InsertIntoTasks(rows []sqldb.TasksRow) (sql.Result, error) {
	for i := range rows {
		rows[i].ExpiryTs = mdb.converter.ToMySQLDateTime(rows[i].ExpiryTs)
	}
	return mdb.conn.NamedExec(createTaskQry, rows)
}

// SelectFromTasks reads one or more rows from tasks table
func (mdb *DB) SelectFromTasks(filter *sqldb.TasksFilter) ([]sqldb.TasksRow, error) {
	var err error
	var rows []sqldb.TasksRow
	err = mdb.conn.Select(&rows, getTaskQry, filter.DomainID,
		filter.TaskListName, filter.TaskType, *filter.MinTaskID, *filter.MaxTaskID)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].ExpiryTs = mdb.converter.FromMySQLDateTime(rows[i].ExpiryTs)
	}
	return rows, err
}

// DeleteFromTasks deletes one or more rows from tasks table
func (mdb *DB) DeleteFromTasks(filter *sqldb.TasksFilter) (sql.Result, error) {
	return mdb.conn.Exec(deleteTaskQry, filter.DomainID, filter.TaskListName, filter.TaskType, *filter.TaskID)
}

// InsertIntoTaskLists inserts one or more rows into task_lists table
func (mdb *DB) InsertIntoTaskLists(row *sqldb.TaskListsRow) (sql.Result, error) {
	row.ExpiryTs = mdb.converter.ToMySQLDateTime(row.ExpiryTs)
	return mdb.conn.NamedExec(createTaskListQry, row)
}

// ReplaceIntoTaskLists replaces one or more rows in task_lists table
func (mdb *DB) ReplaceIntoTaskLists(row *sqldb.TaskListsRow) (sql.Result, error) {
	row.ExpiryTs = mdb.converter.ToMySQLDateTime(row.ExpiryTs)
	return mdb.conn.NamedExec(replaceTaskListQry, row)
}

// UpdateTaskLists updates a row in task_lists table
func (mdb *DB) UpdateTaskLists(row *sqldb.TaskListsRow) (sql.Result, error) {
	row.ExpiryTs = mdb.converter.ToMySQLDateTime(row.ExpiryTs)
	return mdb.conn.NamedExec(updateTaskListQry, row)
}

// SelectFromTaskLists reads one or more rows from task_lists table
func (mdb *DB) SelectFromTaskLists(filter *sqldb.TaskListsFilter) (*sqldb.TaskListsRow, error) {
	var err error
	var row sqldb.TaskListsRow
	err = mdb.conn.Get(&row, getTaskListQry, filter.DomainID, filter.Name, filter.TaskType)
	if err != nil {
		return nil, err
	}
	row.ExpiryTs = mdb.converter.FromMySQLDateTime(row.ExpiryTs)
	return &row, err
}

// DeleteFromTaskLists deletes a row from task_lists table
func (mdb *DB) DeleteFromTaskLists(filter *sqldb.TaskListsFilter) (sql.Result, error) {
	return mdb.conn.Exec(deleteTaskQry, filter.DomainID, filter.Name, filter.TaskType)
}

// LockTaskLists locks a row in task_lists table
func (mdb *DB) LockTaskLists(filter *sqldb.TaskListsFilter) (int64, error) {
	var rangeID int64
	err := mdb.conn.Get(&rangeID, lockTaskListQry, filter.DomainID, filter.Name, filter.TaskType)
	return rangeID, err
}
