package postgresql

import (
	"context"
	"fmt"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
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

func (pdb *db) ListTaskQueueUserDataEntries(ctx context.Context, request *sqlplugin.ListTaskQueueUserDataEntriesRequest) ([]sqlplugin.TaskQueueUserDataEntry, error) {
	var rows []sqlplugin.TaskQueueUserDataEntry
	err := pdb.SelectContext(ctx, &rows, listTaskQueueUserDataQry, request.NamespaceID, request.LastTaskQueueName, request.Limit)
	return rows, err
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
