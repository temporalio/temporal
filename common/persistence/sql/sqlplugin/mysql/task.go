package mysql

import (
	"database/sql"
	"fmt"

	"github.com/temporalio/temporal/common/persistence/sql/sqlplugin"
)

const (
	taskListCreatePart = `INTO task_lists(shard_id, namespace_id, name, task_type, range_id, data, data_encoding) ` +
		`VALUES (:shard_id, :namespace_id, :name, :task_type, :range_id, :data, :data_encoding)`

	// (default range ID: initialRangeID == 1)
	createTaskListQry = `INSERT ` + taskListCreatePart

	replaceTaskListQry = `REPLACE ` + taskListCreatePart

	updateTaskListQry = `UPDATE task_lists SET
range_id = :range_id,
data = :data,
data_encoding = :data_encoding
WHERE
shard_id = :shard_id AND
namespace_id = :namespace_id AND
name = :name AND
task_type = :task_type
`

	listTaskListQry = `SELECT namespace_id, range_id, name, task_type, data, data_encoding ` +
		`FROM task_lists ` +
		`WHERE shard_id = ? AND namespace_id > ? AND name > ? AND task_type > ? ORDER BY namespace_id,name,task_type LIMIT ?`

	getTaskListQry = `SELECT namespace_id, range_id, name, task_type, data, data_encoding ` +
		`FROM task_lists ` +
		`WHERE shard_id = ? AND namespace_id = ? AND name = ? AND task_type = ?`

	deleteTaskListQry = `DELETE FROM task_lists WHERE shard_id=? AND namespace_id=? AND name=? AND task_type=? AND range_id=?`

	lockTaskListQry = `SELECT range_id FROM task_lists ` +
		`WHERE shard_id = ? AND namespace_id = ? AND name = ? AND task_type = ? FOR UPDATE`

	getTaskMinMaxQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE namespace_id = ? AND task_list_name = ? AND task_type = ? AND task_id > ? AND task_id <= ? ` +
		` ORDER BY task_id LIMIT ?`

	getTaskMinQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE namespace_id = ? AND task_list_name = ? AND task_type = ? AND task_id > ? ORDER BY task_id LIMIT ?`

	createTaskQry = `INSERT INTO ` +
		`tasks(namespace_id, task_list_name, task_type, task_id, data, data_encoding) ` +
		`VALUES(:namespace_id, :task_list_name, :task_type, :task_id, :data, :data_encoding)`

	deleteTaskQry = `DELETE FROM tasks ` +
		`WHERE namespace_id = ? AND task_list_name = ? AND task_type = ? AND task_id = ?`

	rangeDeleteTaskQry = `DELETE FROM tasks ` +
		`WHERE namespace_id = ? AND task_list_name = ? AND task_type = ? AND task_id <= ? ` +
		`ORDER BY namespace_id,task_list_name,task_type,task_id LIMIT ?`
)

// InsertIntoTasks inserts one or more rows into tasks table
func (mdb *db) InsertIntoTasks(rows []sqlplugin.TasksRow) (sql.Result, error) {
	return mdb.conn.NamedExec(createTaskQry, rows)
}

// SelectFromTasks reads one or more rows from tasks table
func (mdb *db) SelectFromTasks(filter *sqlplugin.TasksFilter) ([]sqlplugin.TasksRow, error) {
	var err error
	var rows []sqlplugin.TasksRow
	switch {
	case filter.MaxTaskID != nil:
		err = mdb.conn.Select(&rows, getTaskMinMaxQry, filter.NamespaceID,
			filter.TaskListName, filter.TaskType, *filter.MinTaskID, *filter.MaxTaskID, *filter.PageSize)
	default:
		err = mdb.conn.Select(&rows, getTaskMinQry, filter.NamespaceID,
			filter.TaskListName, filter.TaskType, *filter.MinTaskID, *filter.PageSize)
	}
	if err != nil {
		return nil, err
	}
	return rows, err
}

// DeleteFromTasks deletes one or more rows from tasks table
func (mdb *db) DeleteFromTasks(filter *sqlplugin.TasksFilter) (sql.Result, error) {
	if filter.TaskIDLessThanEquals != nil {
		if filter.Limit == nil || *filter.Limit == 0 {
			return nil, fmt.Errorf("missing limit parameter")
		}
		return mdb.conn.Exec(rangeDeleteTaskQry,
			filter.NamespaceID, filter.TaskListName, filter.TaskType, *filter.TaskIDLessThanEquals, *filter.Limit)
	}
	return mdb.conn.Exec(deleteTaskQry, filter.NamespaceID, filter.TaskListName, filter.TaskType, *filter.TaskID)
}

// InsertIntoTaskLists inserts one or more rows into task_lists table
func (mdb *db) InsertIntoTaskLists(row *sqlplugin.TaskListsRow) (sql.Result, error) {
	return mdb.conn.NamedExec(createTaskListQry, row)
}

// ReplaceIntoTaskLists replaces one or more rows in task_lists table
func (mdb *db) ReplaceIntoTaskLists(row *sqlplugin.TaskListsRow) (sql.Result, error) {
	return mdb.conn.NamedExec(replaceTaskListQry, row)
}

// UpdateTaskLists updates a row in task_lists table
func (mdb *db) UpdateTaskLists(row *sqlplugin.TaskListsRow) (sql.Result, error) {
	return mdb.conn.NamedExec(updateTaskListQry, row)
}

// SelectFromTaskLists reads one or more rows from task_lists table
func (mdb *db) SelectFromTaskLists(filter *sqlplugin.TaskListsFilter) ([]sqlplugin.TaskListsRow, error) {
	switch {
	case filter.NamespaceID != nil && filter.Name != nil && filter.TaskType != nil:
		return mdb.selectFromTaskLists(filter)
	case filter.NamespaceIDGreaterThan != nil && filter.NameGreaterThan != nil && filter.TaskTypeGreaterThan != nil && filter.PageSize != nil:
		return mdb.rangeSelectFromTaskLists(filter)
	default:
		return nil, fmt.Errorf("invalid set of query filter params")
	}
}

func (mdb *db) selectFromTaskLists(filter *sqlplugin.TaskListsFilter) ([]sqlplugin.TaskListsRow, error) {
	var err error
	var row sqlplugin.TaskListsRow
	err = mdb.conn.Get(&row, getTaskListQry, filter.ShardID, *filter.NamespaceID, *filter.Name, *filter.TaskType)
	if err != nil {
		return nil, err
	}
	return []sqlplugin.TaskListsRow{row}, err
}

func (mdb *db) rangeSelectFromTaskLists(filter *sqlplugin.TaskListsFilter) ([]sqlplugin.TaskListsRow, error) {
	var err error
	var rows []sqlplugin.TaskListsRow
	err = mdb.conn.Select(&rows, listTaskListQry,
		filter.ShardID, *filter.NamespaceIDGreaterThan, *filter.NameGreaterThan, *filter.TaskTypeGreaterThan, *filter.PageSize)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].ShardID = filter.ShardID
	}
	return rows, nil
}

// DeleteFromTaskLists deletes a row from task_lists table
func (mdb *db) DeleteFromTaskLists(filter *sqlplugin.TaskListsFilter) (sql.Result, error) {
	return mdb.conn.Exec(deleteTaskListQry, filter.ShardID, *filter.NamespaceID, *filter.Name, *filter.TaskType, *filter.RangeID)
}

// LockTaskLists locks a row in task_lists table
func (mdb *db) LockTaskLists(filter *sqlplugin.TaskListsFilter) (int64, error) {
	var rangeID int64
	err := mdb.conn.Get(&rangeID, lockTaskListQry, filter.ShardID, *filter.NamespaceID, *filter.Name, *filter.TaskType)
	return rangeID, err
}
