package cassandra

import (
	"context"
	"fmt"

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	// Not much of a need to make this configurable, we're just reading some strings
	listTaskQueueNamesByBuildIdPageSize = 100

	templateUpdateTaskQueueUserDataQuery = `UPDATE task_queue_user_data SET
		data = ?,
		data_encoding = ?,
		version = ?
		WHERE namespace_id = ?
		AND build_id = ''
		AND task_queue_name = ?
		IF version = ?`
	templateGetTaskQueueUserDataQuery = `SELECT data, data_encoding, version
	    FROM task_queue_user_data
		WHERE namespace_id = ? AND build_id = ''
		AND task_queue_name = ?`
	templateListTaskQueueUserDataQuery       = `SELECT task_queue_name, data, data_encoding, version FROM task_queue_user_data WHERE namespace_id = ? AND build_id = ''`
	templateListTaskQueueNamesByBuildIdQuery = `SELECT task_queue_name FROM task_queue_user_data WHERE namespace_id = ? AND build_id = ?`
	templateInsertTaskQueueUserDataQuery     = `INSERT INTO task_queue_user_data
		(namespace_id, build_id, task_queue_name, data, data_encoding, version) VALUES
		(?           , ''      , ?              , ?   , ?            , 1      ) IF NOT EXISTS`
	templateInsertBuildIdTaskQueueMappingQuery = `INSERT INTO task_queue_user_data
	(namespace_id, build_id, task_queue_name) VALUES
	(?           , ?       , ?)`
	templateDeleteBuildIdTaskQueueMappingQuery = `DELETE FROM task_queue_user_data
	WHERE namespace_id = ? AND build_id = ? AND task_queue_name = ?`
	templateCountTaskQueueByBuildIdQuery = `SELECT COUNT(*) FROM task_queue_user_data WHERE namespace_id = ? AND build_id = ?`
)

type userDataStore struct {
	Session gocql.Session
}

func (d *userDataStore) GetTaskQueueUserData(
	ctx context.Context,
	request *p.GetTaskQueueUserDataRequest,
) (*p.InternalGetTaskQueueUserDataResponse, error) {
	query := d.Session.Query(templateGetTaskQueueUserDataQuery,
		request.NamespaceID,
		request.TaskQueue,
	).WithContext(ctx)
	var version int64
	var userDataBytes []byte
	var encoding string
	if err := query.Scan(&userDataBytes, &encoding, &version); err != nil {
		return nil, gocql.ConvertError("GetTaskQueueData", err)
	}

	return &p.InternalGetTaskQueueUserDataResponse{
		Version:  version,
		UserData: p.NewDataBlob(userDataBytes, encoding),
	}, nil
}

func (d *userDataStore) UpdateTaskQueueUserData(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueUserDataRequest,
) error {
	batch := d.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)

	for taskQueue, update := range request.Updates {
		if update.Version == 0 {
			batch.Query(templateInsertTaskQueueUserDataQuery,
				request.NamespaceID,
				taskQueue,
				update.UserData.Data,
				update.UserData.EncodingType.String(),
			)
		} else {
			batch.Query(templateUpdateTaskQueueUserDataQuery,
				update.UserData.Data,
				update.UserData.EncodingType.String(),
				update.Version+1,
				request.NamespaceID,
				taskQueue,
				update.Version,
			)
		}
		for _, buildId := range update.BuildIdsAdded {
			batch.Query(templateInsertBuildIdTaskQueueMappingQuery, request.NamespaceID, buildId, taskQueue)
		}
		for _, buildId := range update.BuildIdsRemoved {
			batch.Query(templateDeleteBuildIdTaskQueueMappingQuery, request.NamespaceID, buildId, taskQueue)
		}
	}

	previous := make(map[string]any)
	applied, iter, err := d.Session.MapExecuteBatchCAS(batch, previous)
	for _, update := range request.Updates {
		if update.Applied != nil {
			*update.Applied = applied
		}
	}
	if err != nil {
		return gocql.ConvertError("UpdateTaskQueueUserData", err)
	}
	defer iter.Close()

	if !applied {
		// No error, but not applied. That means we had a conflict.
		// Iterate through results to identify first conflicting row.
		for {
			name, nameErr := getTypedFieldFromRow[string]("task_queue_name", previous)
			previousVersion, verErr := getTypedFieldFromRow[int64]("version", previous)
			update, hasUpdate := request.Updates[name]
			if nameErr == nil && verErr == nil && hasUpdate && update.Version != previousVersion {
				if update.Conflicting != nil {
					*update.Conflicting = true
				}
				return &p.ConditionFailedError{
					Msg: fmt.Sprintf("Failed to update task queues: task queue %q version %d != %d",
						name, update.Version, previousVersion),
				}
			}
			clear(previous)
			if !iter.MapScan(previous) {
				break
			}
		}
		return &p.ConditionFailedError{Msg: "Failed to update task queues: unknown conflict"}
	}

	return nil
}

func (d *userDataStore) ListTaskQueueUserDataEntries(ctx context.Context, request *p.ListTaskQueueUserDataEntriesRequest) (*p.InternalListTaskQueueUserDataEntriesResponse, error) {
	query := d.Session.Query(templateListTaskQueueUserDataQuery, request.NamespaceID).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalListTaskQueueUserDataEntriesResponse{}
	var taskQueue string
	var data []byte
	var dataEncoding string
	var version int64
	// Column order must match templateListTaskQueueUserDataQuery SELECT clause.
	for iter.Scan(&taskQueue, &data, &dataEncoding, &version) {
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)
		response.Entries = append(response.Entries, p.InternalTaskQueueUserDataEntry{TaskQueue: taskQueue, Data: p.NewDataBlob(dataCopy, dataEncoding), Version: version})
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ListTaskQueueUserDataEntries", err)
	}
	return response, nil
}

func (d *userDataStore) GetTaskQueuesByBuildId(ctx context.Context, request *p.GetTaskQueuesByBuildIdRequest) ([]string, error) {
	query := d.Session.Query(templateListTaskQueueNamesByBuildIdQuery, request.NamespaceID, request.BuildID).WithContext(ctx)
	iter := query.PageSize(listTaskQueueNamesByBuildIdPageSize).Iter()

	var taskQueues []string
	var taskQueue string

	for {
		// Column order must match templateListTaskQueueNamesByBuildIdQuery SELECT clause.
		for iter.Scan(&taskQueue) {
			taskQueues = append(taskQueues, taskQueue)
		}
		if len(iter.PageState()) == 0 {
			break
		}
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("GetTaskQueuesByBuildId", err)
	}
	return taskQueues, nil
}

func (d *userDataStore) CountTaskQueuesByBuildId(ctx context.Context, request *p.CountTaskQueuesByBuildIdRequest) (int, error) {
	var count int
	query := d.Session.Query(templateCountTaskQueueByBuildIdQuery, request.NamespaceID, request.BuildID).WithContext(ctx)
	err := query.Scan(&count)
	return count, err
}
