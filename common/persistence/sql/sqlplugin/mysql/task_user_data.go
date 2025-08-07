package mysql

import (
	"context"
	"strings"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	getTaskQueueUserDataQry = `SELECT data, data_encoding, version FROM task_queue_user_data ` +
		`WHERE namespace_id = ? AND task_queue_name = ?`

	updateTaskQueueUserDataQry = `UPDATE task_queue_user_data SET ` +
		`data = ?, ` +
		`data_encoding = ?, ` +
		`version = ? ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND version = ?`

	insertTaskQueueUserDataQry = `INSERT INTO task_queue_user_data` +
		`(namespace_id, task_queue_name, data, data_encoding, version) ` +
		`VALUES (?, ?, ?, ?, 1)`

	listTaskQueueUserDataQry = `SELECT task_queue_name, data, data_encoding, version FROM task_queue_user_data WHERE namespace_id = ? AND task_queue_name > ? LIMIT ?`

	addBuildIdToTaskQueueMappingQry    = `INSERT INTO build_id_to_task_queue (namespace_id, build_id, task_queue_name) VALUES `
	removeBuildIdToTaskQueueMappingQry = `DELETE FROM build_id_to_task_queue WHERE namespace_id = ? AND task_queue_name = ? AND build_id IN (`
	listTaskQueuesByBuildIdQry         = `SELECT task_queue_name FROM build_id_to_task_queue WHERE namespace_id = ? AND build_id = ?`
	countTaskQueuesByBuildIdQry        = `SELECT COUNT(*) FROM build_id_to_task_queue WHERE namespace_id = ? AND build_id = ?`
)

func (mdb *db) GetTaskQueueUserData(ctx context.Context, request *sqlplugin.GetTaskQueueUserDataRequest) (*sqlplugin.VersionedBlob, error) {
	var row sqlplugin.VersionedBlob
	err := mdb.GetContext(ctx, &row, getTaskQueueUserDataQry, request.NamespaceID, request.TaskQueueName)
	return &row, err
}

func (mdb *db) UpdateTaskQueueUserData(ctx context.Context, request *sqlplugin.UpdateTaskQueueDataRequest) error {
	if request.Version == 0 {
		_, err := mdb.ExecContext(
			ctx,
			insertTaskQueueUserDataQry,
			request.NamespaceID,
			request.TaskQueueName,
			request.Data,
			request.DataEncoding)
		return err
	}
	result, err := mdb.ExecContext(
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

func (mdb *db) AddToBuildIdToTaskQueueMapping(ctx context.Context, request sqlplugin.AddToBuildIdToTaskQueueMapping) error {
	query := addBuildIdToTaskQueueMappingQry
	var params []any
	for idx, buildId := range request.BuildIds {
		if idx == len(request.BuildIds)-1 {
			query += "(?, ?, ?)"
		} else {
			query += "(?, ?, ?), "
		}
		params = append(params, request.NamespaceID, buildId, request.TaskQueueName)
	}

	_, err := mdb.ExecContext(ctx, query, params...)
	return err
}

func (mdb *db) RemoveFromBuildIdToTaskQueueMapping(ctx context.Context, request sqlplugin.RemoveFromBuildIdToTaskQueueMapping) error {
	query := removeBuildIdToTaskQueueMappingQry + strings.Repeat("?, ", len(request.BuildIds)-1) + "?)"
	// Golang doesn't support appending a string slice to an any slice which is essentially what we're doing here.
	params := make([]any, len(request.BuildIds)+2)
	params[0] = request.NamespaceID
	params[1] = request.TaskQueueName
	for i, buildId := range request.BuildIds {
		params[i+2] = buildId
	}

	_, err := mdb.ExecContext(ctx, query, params...)
	return err
}

func (mdb *db) ListTaskQueueUserDataEntries(ctx context.Context, request *sqlplugin.ListTaskQueueUserDataEntriesRequest) ([]sqlplugin.TaskQueueUserDataEntry, error) {
	var rows []sqlplugin.TaskQueueUserDataEntry
	err := mdb.SelectContext(ctx, &rows, listTaskQueueUserDataQry, request.NamespaceID, request.LastTaskQueueName, request.Limit)
	return rows, err
}

func (mdb *db) GetTaskQueuesByBuildId(ctx context.Context, request *sqlplugin.GetTaskQueuesByBuildIdRequest) ([]string, error) {
	var rows []struct {
		TaskQueueName string
	}

	err := mdb.SelectContext(ctx, &rows, listTaskQueuesByBuildIdQry, request.NamespaceID, request.BuildID)
	taskQueues := make([]string, len(rows))
	for i, row := range rows {
		taskQueues[i] = row.TaskQueueName
	}
	return taskQueues, err
}

func (mdb *db) CountTaskQueuesByBuildId(ctx context.Context, request *sqlplugin.CountTaskQueuesByBuildIdRequest) (int, error) {
	var count int
	err := mdb.GetContext(ctx, &count, countTaskQueuesByBuildIdQry, request.NamespaceID, request.BuildID)
	return count, err
}
