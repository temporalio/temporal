package cassandra

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/convert"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/primitives/timestamp"
)

// matchingTaskVersion represents the task schema version
type matchingTaskVersion int

const (
	matchingTaskVersion1 matchingTaskVersion = 1
	matchingTaskVersion2 matchingTaskVersion = 2
)

var switchTasksTableV1Cache sync.Map

// switchTasksTable switches table names from tasks to tasks_v2 and modifies queries for v2 schema
func switchTasksTable(baseQuery string, v matchingTaskVersion) string {
	if v == matchingTaskVersion2 {
		return baseQuery
	} else if v != matchingTaskVersion1 {
		panic("invalid task schema version") // nolint:forbidigo // hardcoded constants
	}

	if v1query, ok := switchTasksTableV1Cache.Load(baseQuery); ok {
		return v1query.(string) // nolint:revive
	}

	v1query := strings.ReplaceAll(baseQuery, " tasks_v2 ", " tasks ")
	v1query = strings.ReplaceAll(v1query, " AND pass = 0 ", " ")
	v1query = strings.ReplaceAll(v1query, "type, pass, task_id", "type, task_id")
	v1query = strings.ReplaceAll(v1query, "?, 0, ?", "?, ?")

	switchTasksTableV1Cache.Store(baseQuery, v1query)
	return v1query
}

// Task queue management queries, written for v2 (rewritten for v1 by switchTasksTable)
const (
	templateGetTaskQueueQuery = `SELECT ` +
		`range_id, ` +
		`task_queue, ` +
		`task_queue_encoding ` +
		`FROM tasks_v2 ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`AND pass = 0 ` +
		`AND task_id = ?`

	templateInsertTaskQueueQuery = `INSERT INTO tasks_v2 ` +
		`(namespace_id, task_queue_name, task_queue_type, type, pass, task_id, range_id, task_queue, task_queue_encoding) ` +
		`VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?) IF NOT EXISTS`

	templateUpdateTaskQueueQuery = `UPDATE tasks_v2 SET ` +
		`range_id = ?, ` +
		`task_queue = ?, ` +
		`task_queue_encoding = ? ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`AND pass = 0 ` +
		`AND task_id = ? ` +
		`IF range_id = ?`

	templateUpdateTaskQueueQueryWithTTLPart1 = `INSERT INTO tasks_v2 ` +
		`(namespace_id, task_queue_name, task_queue_type, type, pass, task_id) ` +
		`VALUES (?, ?, ?, ?, 0, ?) USING TTL ?`

	templateUpdateTaskQueueQueryWithTTLPart2 = `UPDATE tasks_v2 USING TTL ? SET ` +
		`range_id = ?, ` +
		`task_queue = ?, ` +
		`task_queue_encoding = ? ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`AND pass = 0 ` +
		`AND task_id = ? ` +
		`IF range_id = ?`

	templateDeleteTaskQueueQuery = `DELETE FROM tasks_v2 ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`AND pass = 0 ` +
		`AND task_id = ? ` +
		`IF range_id = ?`
)

// taskQueueStore handles unified task queue operations for both v1 and v2
type taskQueueStore struct {
	Session gocql.Session
	version matchingTaskVersion
}

func (d *taskQueueStore) CreateTaskQueue(
	ctx context.Context,
	request *p.InternalCreateTaskQueueRequest,
) error {
	query := d.Session.Query(switchTasksTable(templateInsertTaskQueueQuery, d.version),
		request.NamespaceID,
		request.TaskQueue,
		request.TaskType,
		rowTypeTaskQueue,
		taskQueueTaskID,
		request.RangeID,
		request.TaskQueueInfo.Data,
		request.TaskQueueInfo.EncodingType.String(),
	).WithContext(ctx)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return gocql.ConvertError("CreateTaskQueue", err)
	}

	if !applied {
		previousRangeID := previous["range_id"]
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("CreateTaskQueue: TaskQueue:%v, TaskQueueType:%v, PreviousRangeID:%v",
				request.TaskQueue, request.TaskType, previousRangeID),
		}
	}

	return nil
}

func (d *taskQueueStore) GetTaskQueue(
	ctx context.Context,
	request *p.InternalGetTaskQueueRequest,
) (*p.InternalGetTaskQueueResponse, error) {
	query := d.Session.Query(switchTasksTable(templateGetTaskQueueQuery, d.version),
		request.NamespaceID,
		request.TaskQueue,
		request.TaskType,
		rowTypeTaskQueue,
		taskQueueTaskID,
	).WithContext(ctx)

	var rangeID int64
	var tlBytes []byte
	var tlEncoding string
	if err := query.Scan(&rangeID, &tlBytes, &tlEncoding); err != nil {
		return nil, gocql.ConvertError("GetTaskQueue", err)
	}

	return &p.InternalGetTaskQueueResponse{
		RangeID:       rangeID,
		TaskQueueInfo: p.NewDataBlob(tlBytes, tlEncoding),
	}, nil
}

func (d *taskQueueStore) UpdateTaskQueue(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueRequest,
) (*p.UpdateTaskQueueResponse, error) {
	var err error
	var applied bool
	previous := make(map[string]interface{})

	if d.version == matchingTaskVersion1 && request.TaskQueueKind == enumspb.TASK_QUEUE_KIND_STICKY {
		// V1 TTL logic - only applies to V1
		if request.ExpiryTime == nil {
			return nil, serviceerror.NewInternal("ExpiryTime cannot be nil for sticky task queue")
		}
		expiryTTL := convert.Int64Ceil(time.Until(timestamp.TimeValue(request.ExpiryTime)).Seconds())
		if expiryTTL >= maxCassandraTTL {
			expiryTTL = maxCassandraTTL
		}
		batch := d.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)

		batch.Query(switchTasksTable(templateUpdateTaskQueueQueryWithTTLPart1, d.version),
			request.NamespaceID,
			request.TaskQueue,
			request.TaskType,
			rowTypeTaskQueue,
			taskQueueTaskID,
			expiryTTL,
		)

		batch.Query(switchTasksTable(templateUpdateTaskQueueQueryWithTTLPart2, d.version),
			expiryTTL,
			request.RangeID,
			request.TaskQueueInfo.Data,
			request.TaskQueueInfo.EncodingType.String(),
			request.NamespaceID,
			request.TaskQueue,
			request.TaskType,
			rowTypeTaskQueue,
			taskQueueTaskID,
			request.PrevRangeID,
		)
		applied, _, err = d.Session.MapExecuteBatchCAS(batch, previous)
	} else {
		// Regular update logic for both V1 and V2
		query := d.Session.Query(switchTasksTable(templateUpdateTaskQueueQuery, d.version),
			request.RangeID,
			request.TaskQueueInfo.Data,
			request.TaskQueueInfo.EncodingType.String(),
			request.NamespaceID,
			request.TaskQueue,
			request.TaskType,
			rowTypeTaskQueue,
			taskQueueTaskID,
			request.PrevRangeID,
		).WithContext(ctx)
		applied, err = query.MapScanCAS(previous)
	}

	if err != nil {
		return nil, gocql.ConvertError("UpdateTaskQueue", err)
	}

	if !applied {
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}

		return nil, &p.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to update task queue. name: %v, type: %v, rangeID: %v, columns: (%v)",
				request.TaskQueue, request.TaskType, request.RangeID, strings.Join(columns, ",")),
		}
	}

	return &p.UpdateTaskQueueResponse{}, nil
}

func (d *taskQueueStore) ListTaskQueue(
	_ context.Context,
	_ *p.ListTaskQueueRequest,
) (*p.InternalListTaskQueueResponse, error) {
	return nil, serviceerror.NewUnavailable("unsupported operation")
}

func (d *taskQueueStore) DeleteTaskQueue(
	ctx context.Context,
	request *p.DeleteTaskQueueRequest,
) error {
	query := d.Session.Query(switchTasksTable(templateDeleteTaskQueueQuery, d.version),
		request.TaskQueue.NamespaceID,
		request.TaskQueue.TaskQueueName,
		request.TaskQueue.TaskQueueType,
		rowTypeTaskQueue,
		taskQueueTaskID,
		request.RangeID,
	).WithContext(ctx)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return gocql.ConvertError("DeleteTaskQueue", err)
	}
	if !applied {
		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("DeleteTaskQueue operation failed: expected_range_id=%v but found %+v", request.RangeID, previous),
		}
	}
	return nil
}
