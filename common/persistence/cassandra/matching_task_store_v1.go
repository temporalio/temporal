package cassandra

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	templateCreateTaskQuery = `INSERT INTO tasks (` +
		`namespace_id, task_queue_name, task_queue_type, type, task_id, task, task_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?)`

	templateCreateTaskWithTTLQuery = `INSERT INTO tasks (` +
		`namespace_id, task_queue_name, task_queue_type, type, task_id, task, task_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?) USING TTL ?`

	templateGetTasksQuery = `SELECT task_id, task, task_encoding ` +
		`FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and task_id >= ? ` +
		`and task_id < ?`

	templateCompleteTasksLessThanQuery = `DELETE FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`AND task_id < ? `
)

type matchingTaskStoreV1 struct {
	Session gocql.Session
	userDataStore
	taskQueueStore
}

func newMatchingTaskStoreV1(
	session gocql.Session,
) *matchingTaskStoreV1 {
	return &matchingTaskStoreV1{
		Session:        session,
		userDataStore:  userDataStore{Session: session},
		taskQueueStore: taskQueueStore{Session: session, version: matchingTaskVersion1},
	}
}

// CreateTasks add tasks
func (d *matchingTaskStoreV1) CreateTasks(
	ctx context.Context,
	request *p.InternalCreateTasksRequest,
) (*p.CreateTasksResponse, error) {
	batch := d.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	namespaceID := request.NamespaceID
	taskQueue := request.TaskQueue
	taskQueueType := request.TaskType

	for _, task := range request.Tasks {
		if task.TaskPass != 0 {
			return nil, serviceerror.NewInternal("invalid non-fair queue task with pass number")
		}

		ttl := getTaskTTL(task.ExpiryTime)

		if ttl <= 0 || ttl > maxCassandraTTL {
			batch.Query(templateCreateTaskQuery,
				namespaceID,
				taskQueue,
				taskQueueType,
				rowTypeTaskInSubqueue(task.Subqueue),
				task.TaskId,
				task.Task.Data,
				task.Task.EncodingType.String())
		} else {
			batch.Query(templateCreateTaskWithTTLQuery,
				namespaceID,
				taskQueue,
				taskQueueType,
				rowTypeTaskInSubqueue(task.Subqueue),
				task.TaskId,
				task.Task.Data,
				task.Task.EncodingType.String(),
				ttl)
		}
	}

	// The following query is used to ensure that range_id didn't change
	batch.Query(switchTasksTable(templateUpdateTaskQueueQuery, matchingTaskVersion1),
		request.RangeID,
		request.TaskQueueInfo.Data,
		request.TaskQueueInfo.EncodingType.String(),
		namespaceID,
		taskQueue,
		taskQueueType,
		rowTypeTaskQueue,
		taskQueueTaskID,
		request.RangeID,
	)

	previous := make(map[string]interface{})
	applied, _, err := d.Session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		return nil, gocql.ConvertError("CreateTasks", err)
	}
	if !applied {
		rangeID := previous["range_id"]
		return nil, &p.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to create task. TaskQueue: %v, taskQueueType: %v, rangeID: %v, db rangeID: %v",
				taskQueue, taskQueueType, request.RangeID, rangeID),
		}
	}

	return &p.CreateTasksResponse{UpdatedMetadata: true}, nil
}

// GetTasks get a task
func (d *matchingTaskStoreV1) GetTasks(
	ctx context.Context,
	request *p.GetTasksRequest,
) (*p.InternalGetTasksResponse, error) {
	if request.InclusiveMinPass != 0 {
		return nil, serviceerror.NewInternal("invalid GetTasks request on queue: InclusiveMinPass is not supported")
	}

	// Reading taskqueue tasks need to be quorum level consistent, otherwise we could lose tasks
	query := d.Session.Query(templateGetTasksQuery,
		request.NamespaceID,
		request.TaskQueue,
		request.TaskType,
		rowTypeTaskInSubqueue(request.Subqueue),
		request.InclusiveMinTaskID,
		request.ExclusiveMaxTaskID,
	).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalGetTasksResponse{}
	task := make(map[string]interface{})
	for iter.MapScan(task) {
		_, ok := task["task_id"]
		if !ok { // no tasks, but static column record returned
			continue
		}

		rawTask, ok := task["task"]
		if !ok {
			return nil, newFieldNotFoundError("task", task)
		}
		taskVal, ok := rawTask.([]byte)
		if !ok {
			var byteSliceType []byte
			return nil, newPersistedTypeMismatchError("task", byteSliceType, rawTask, task)
		}

		rawEncoding, ok := task["task_encoding"]
		if !ok {
			return nil, newFieldNotFoundError("task_encoding", task)
		}
		encodingVal, ok := rawEncoding.(string)
		if !ok {
			var byteSliceType []byte
			return nil, newPersistedTypeMismatchError("task_encoding", byteSliceType, rawEncoding, task)
		}
		response.Tasks = append(response.Tasks, p.NewDataBlob(taskVal, encodingVal))

		task = make(map[string]interface{}) // Reinitialize map as initialized fails on unmarshalling
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewUnavailablef("GetTasks operation failed. Error: %v", err)
	}
	return response, nil
}

// CompleteTasksLessThan deletes all tasks less than the given task id. This API ignores the
// Limit request parameter i.e. either all tasks leq the task_id will be deleted or an error will
// be returned to the caller
func (d *matchingTaskStoreV1) CompleteTasksLessThan(
	ctx context.Context,
	request *p.CompleteTasksLessThanRequest,
) (int, error) {
	if request.ExclusiveMaxPass != 0 {
		return 0, serviceerror.NewInternal("invalid CompleteTasksLessThan request on queue")
	}

	query := d.Session.Query(
		templateCompleteTasksLessThanQuery,
		request.NamespaceID,
		request.TaskQueueName,
		request.TaskType,
		rowTypeTaskInSubqueue(request.Subqueue),
		request.ExclusiveMaxTaskID,
	).WithContext(ctx)
	err := query.Exec()
	if err != nil {
		return 0, gocql.ConvertError("CompleteTasksLessThan", err)
	}
	return p.UnknownNumRowsAffected, nil
}

func (d *matchingTaskStoreV1) GetName() string {
	return cassandraPersistenceName
}

func (d *matchingTaskStoreV1) Close() {
	if d.Session != nil {
		d.Session.Close()
	}
}
