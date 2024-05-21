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
	"context"
	"database/sql"
	"fmt"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/persistence"
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

	getTaskQueueUserDataQry = `SELECT data, data_encoding, version FROM task_queue_user_data ` +
		`WHERE namespace_id = $1 AND task_queue_name = $2`

	updateTaskQueueUserDataQry = `UPDATE task_queue_user_data SET ` +
		`data = $1, ` +
		`data_encoding = $2, ` +
		`version = $3 ` +
		`WHERE namespace_id = $4 ` +
		`AND task_queue_name = $5 ` +
		`AND version = $6`

	insertTaskQueueUserDataQry = `INSERT INTO task_queue_user_data` +
		`(namespace_id, task_queue_name, data, data_encoding, version) ` +
		`VALUES ($1, $2, $3, $4, 1)`

	listTaskQueueUserDataQry = `SELECT task_queue_name, data, data_encoding, version FROM task_queue_user_data WHERE namespace_id = $1 AND task_queue_name > $2 LIMIT $3`

	addBuildIdToTaskQueueMappingQry    = `INSERT INTO build_id_to_task_queue (namespace_id, build_id, task_queue_name) VALUES `
	removeBuildIdToTaskQueueMappingQry = `DELETE FROM build_id_to_task_queue WHERE namespace_id = $1 AND task_queue_name = $2 AND build_id IN (`
	listTaskQueuesByBuildIdQry         = `SELECT task_queue_name FROM build_id_to_task_queue WHERE namespace_id = $1 AND build_id = $2`
	countTaskQueuesByBuildIdQry        = `SELECT COUNT(*) FROM build_id_to_task_queue WHERE namespace_id = $1 AND build_id = $2`
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

func (pdb *db) GetTaskQueueUserData(ctx context.Context, request *sqlplugin.GetTaskQueueUserDataRequest) (*sqlplugin.VersionedBlob, error) {
	var row sqlplugin.VersionedBlob
	err := pdb.GetContext(ctx, &row, getTaskQueueUserDataQry, request.NamespaceID, request.TaskQueueName)
	return &row, err
}

func (pdb *db) UpdateTaskQueueUserData(ctx context.Context, request *sqlplugin.UpdateTaskQueueDataRequest) error {
	if request.Version == 0 {
		_, err := pdb.ExecContext(
			ctx,
			insertTaskQueueUserDataQry,
			request.NamespaceID,
			request.TaskQueueName,
			request.Data,
			request.DataEncoding)
		return err
	}
	result, err := pdb.ExecContext(
		ctx,
		updateTaskQueueUserDataQry,
		request.Data,
		request.DataEncoding,
		request.Version+1,
		request.NamespaceID,
		request.TaskQueueName,
		request.Version)
	if err != nil {
		return err
	}
	numRows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if numRows != 1 {
		return &persistence.ConditionFailedError{Msg: "Expected exactly one row to be updated"}
	}
	return nil
}

func (pdb *db) AddToBuildIdToTaskQueueMapping(ctx context.Context, request sqlplugin.AddToBuildIdToTaskQueueMapping) error {
	query := addBuildIdToTaskQueueMappingQry
	var params []any
	for idx, buildId := range request.BuildIds {
		query += fmt.Sprintf("($%d, $%d, $%d)", idx*3+1, idx*3+2, idx*3+3)
		if idx < len(request.BuildIds)-1 {
			query += ", "
		}
		params = append(params, request.NamespaceID, buildId, request.TaskQueueName)
	}

	_, err := pdb.ExecContext(ctx, query, params...)
	return err
}

func (pdb *db) RemoveFromBuildIdToTaskQueueMapping(ctx context.Context, request sqlplugin.RemoveFromBuildIdToTaskQueueMapping) error {
	query := removeBuildIdToTaskQueueMappingQry
	// Golang doesn't support appending a string slice to an any slice which is essentially what we're doing here.
	params := make([]any, len(request.BuildIds)+2)
	params[0] = request.NamespaceID
	params[1] = request.TaskQueueName
	for idx, buildId := range request.BuildIds {
		sep := ", "
		if idx == len(request.BuildIds)-1 {
			sep = ")"
		}
		query += fmt.Sprintf("$%d%s", idx+3, sep)
		params[idx+2] = buildId
	}

	_, err := pdb.ExecContext(ctx, query, params...)
	return err
}

func (pdb *db) ListTaskQueueUserDataEntries(ctx context.Context, request *sqlplugin.ListTaskQueueUserDataEntriesRequest) ([]sqlplugin.TaskQueueUserDataEntry, error) {
	var rows []sqlplugin.TaskQueueUserDataEntry
	err := pdb.SelectContext(ctx, &rows, listTaskQueueUserDataQry, request.NamespaceID, request.LastTaskQueueName, request.Limit)
	return rows, err
}

func (pdb *db) GetTaskQueuesByBuildId(ctx context.Context, request *sqlplugin.GetTaskQueuesByBuildIdRequest) ([]string, error) {
	var rows []struct {
		TaskQueueName string
	}

	err := pdb.SelectContext(ctx, &rows, listTaskQueuesByBuildIdQry, request.NamespaceID, request.BuildID)
	taskQueues := make([]string, len(rows))
	for i, row := range rows {
		taskQueues[i] = row.TaskQueueName
	}
	return taskQueues, err
}

func (pdb *db) CountTaskQueuesByBuildId(ctx context.Context, request *sqlplugin.CountTaskQueuesByBuildIdRequest) (int, error) {
	var count int
	err := pdb.GetContext(ctx, &count, countTaskQueuesByBuildIdQry, request.NamespaceID, request.BuildID)
	return count, err
}
