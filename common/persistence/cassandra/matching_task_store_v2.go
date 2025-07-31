package cassandra

import (
	"context"
	"fmt"
	"math"

	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	templateCreateTaskQuery_v2 = `INSERT INTO tasks_v2 (` +
		`namespace_id, task_queue_name, task_queue_type, type, pass, task_id, task, task_encoding) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?)`

	templateGetTasksQuery_v2 = `SELECT task_id, task, task_encoding ` +
		`FROM tasks_v2 ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and (pass, task_id) >= (?, ?)`

	templateGetTasksQuery_v2_limit = `SELECT task_id, task, task_encoding ` +
		`FROM tasks_v2 ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and (pass, task_id) >= (?, ?) ` +
		`LIMIT ?`

	templateCompleteTasksLessThanQuery_v2 = `DELETE FROM tasks_v2 ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`and (pass, task_id) < (?, ?)`
)

// matchingTaskStoreV2 is a fork of matchingTaskStoreV1 that uses a new task schema.
// Task queue operations are now unified with V1 through query rewriting.
type matchingTaskStoreV2 struct {
	Session gocql.Session
	userDataStore
	taskQueueStore
}

func newMatchingTaskStoreV2(
	session gocql.Session,
) *matchingTaskStoreV2 {
	return &matchingTaskStoreV2{
		Session:        session,
		userDataStore:  userDataStore{Session: session},
		taskQueueStore: taskQueueStore{Session: session, version: matchingTaskVersion2},
	}
}

// CreateTasks add tasks
func (d *matchingTaskStoreV2) CreateTasks(
	ctx context.Context,
	request *p.InternalCreateTasksRequest,
) (*p.CreateTasksResponse, error) {
	batch := d.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	namespaceID := request.NamespaceID
	taskQueue := request.TaskQueue
	taskQueueType := request.TaskType

	for _, task := range request.Tasks {
		if task.TaskPass == 0 {
			return nil, serviceerror.NewInternal("invalid fair queue task missing pass number")
		}

		batch.Query(templateCreateTaskQuery_v2,
			namespaceID,
			taskQueue,
			taskQueueType,
			rowTypeTaskInSubqueue(task.Subqueue),
			task.TaskPass,
			task.TaskId,
			task.Task.Data,
			task.Task.EncodingType.String())
	}

	// The following query is used to ensure that range_id didn't change
	batch.Query(switchTasksTable(templateUpdateTaskQueueQuery, matchingTaskVersion2),
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
func (d *matchingTaskStoreV2) GetTasks(
	ctx context.Context,
	request *p.GetTasksRequest,
) (*p.InternalGetTasksResponse, error) {
	if request.InclusiveMinPass < 1 {
		return nil, serviceerror.NewInternal("invalid GetTasks request on fair queue: InclusiveMinPass must be >= 1")
	}
	if request.ExclusiveMaxTaskID != math.MaxInt64 {
		// ExclusiveMaxTaskID is not supported in fair queue.
		return nil, serviceerror.NewInternal("invalid GetTasks request on fair queue: ExclusiveMaxTaskID is not supported")
	}

	// Reading taskqueue tasks need to be quorum level consistent, otherwise we could lose tasks
	var query gocql.Query
	if request.UseLimit {
		query = d.Session.Query(templateGetTasksQuery_v2_limit,
			request.NamespaceID,
			request.TaskQueue,
			request.TaskType,
			rowTypeTaskInSubqueue(request.Subqueue),
			request.InclusiveMinPass,
			request.InclusiveMinTaskID,
			request.PageSize,
		)
	} else {
		query = d.Session.Query(templateGetTasksQuery_v2,
			request.NamespaceID,
			request.TaskQueue,
			request.TaskType,
			rowTypeTaskInSubqueue(request.Subqueue),
			request.InclusiveMinPass,
			request.InclusiveMinTaskID,
		)
	}
	iter := query.WithContext(ctx).PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

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
func (d *matchingTaskStoreV2) CompleteTasksLessThan(
	ctx context.Context,
	request *p.CompleteTasksLessThanRequest,
) (int, error) {
	// Require starting from pass 1.
	if request.ExclusiveMaxPass < 1 {
		return 0, serviceerror.NewInternal("invalid CompleteTasksLessThan request on fair queue")
	}

	query := d.Session.Query(
		templateCompleteTasksLessThanQuery_v2,
		request.NamespaceID,
		request.TaskQueueName,
		request.TaskType,
		rowTypeTaskInSubqueue(request.Subqueue),
		request.ExclusiveMaxPass,
		request.ExclusiveMaxTaskID,
	).WithContext(ctx)
	err := query.Exec()
	if err != nil {
		return 0, gocql.ConvertError("CompleteTasksLessThan", err)
	}
	return p.UnknownNumRowsAffected, nil
}

func (d *matchingTaskStoreV2) GetName() string {
	return cassandraPersistenceName
}

func (d *matchingTaskStoreV2) Close() {
	if d.Session != nil {
		d.Session.Close()
	}
}
