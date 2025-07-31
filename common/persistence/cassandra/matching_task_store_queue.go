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

// CassandraTaskVersion represents the task schema version
type CassandraTaskVersion int

const (
	CassandraTaskVersion1 CassandraTaskVersion = 1
	CassandraTaskVersion2 CassandraTaskVersion = 2
)

var switchTasksTableV2Cache sync.Map

// SwitchTasksTable switches table names from tasks to tasks_v2 and modifies queries for v2 schema
func SwitchTasksTable(baseQuery string, v CassandraTaskVersion) string {
	if v == CassandraTaskVersion1 {
		return baseQuery
	} else if v != CassandraTaskVersion2 {
		return "_invalid_version_"
	}

	if v2query, ok := switchTasksTableV2Cache.Load(baseQuery); ok {
		return v2query.(string)
	}

	// Replace table name
	v2query := strings.ReplaceAll(baseQuery, " tasks ", " tasks_v2 ")

	// Handle INSERT queries - need to add pass column
	if strings.Contains(v2query, "INSERT INTO tasks_v2") {
		// For task queue inserts, add pass column with value 0
		if strings.Contains(v2query, "type, task_id") {
			v2query = strings.ReplaceAll(v2query, "type, task_id", "type, pass, task_id")
			// Add pass = 0 parameter in VALUES
			v2query = strings.ReplaceAll(v2query, "?, ?, ?, ?, ?, ?, ?, ?", "?, ?, ?, ?, 0, ?, ?, ?, ?")
		}
	}

	// Handle WHERE clauses - add pass = 0 condition for task queue operations
	if strings.Contains(v2query, "type = ?") && !strings.Contains(v2query, "pass") {
		// For task queue operations, we need to add "and pass = 0" after "type = ?"
		v2query = strings.ReplaceAll(v2query, "and type = ?", "and type = ? and pass = 0")
		// Handle UPDATE WHERE clauses that have different ordering
		if strings.Contains(v2query, "WHERE") && !strings.Contains(v2query, "and pass = 0") {
			// Look for patterns like "and type = ? and task_id"
			v2query = strings.ReplaceAll(v2query, "and type = ? and task_id", "and type = ? and pass = 0 and task_id")
			// Handle end of WHERE clause
			if strings.Contains(v2query, "type = ? IF") {
				v2query = strings.ReplaceAll(v2query, "type = ? IF", "type = ? and pass = 0 IF")
			}
		}
	}

	switchTasksTableV2Cache.Store(baseQuery, v2query)
	return v2query
}

// Task queue management queries (these will be unified)
const (
	templateGetTaskQueueQuery = `SELECT ` +
		`range_id, ` +
		`task_queue, ` +
		`task_queue_encoding ` +
		`FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and task_id = ?`

	templateInsertTaskQueueQuery = `INSERT INTO tasks (` +
		`namespace_id, ` +
		`task_queue_name, ` +
		`task_queue_type, ` +
		`type, ` +
		`task_id, ` +
		`range_id, ` +
		`task_queue, ` +
		`task_queue_encoding ` +
		`) VALUES (?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateUpdateTaskQueueQuery = `UPDATE tasks SET ` +
		`range_id = ?, ` +
		`task_queue = ?, ` +
		`task_queue_encoding = ? ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateUpdateTaskQueueQueryWithTTLPart1 = `INSERT INTO tasks (` +
		`namespace_id, ` +
		`task_queue_name, ` +
		`task_queue_type, ` +
		`type, ` +
		`task_id ` +
		`) VALUES (?, ?, ?, ?, ?) USING TTL ?`

	templateUpdateTaskQueueQueryWithTTLPart2 = `UPDATE tasks USING TTL ? SET ` +
		`range_id = ?, ` +
		`task_queue = ?, ` +
		`task_queue_encoding = ? ` +
		`WHERE namespace_id = ? ` +
		`and task_queue_name = ? ` +
		`and task_queue_type = ? ` +
		`and type = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateDeleteTaskQueueQuery = `DELETE FROM tasks ` +
		`WHERE namespace_id = ? ` +
		`AND task_queue_name = ? ` +
		`AND task_queue_type = ? ` +
		`AND type = ? ` +
		`AND task_id = ? ` +
		`IF range_id = ?`
)

// taskQueueStore handles unified task queue operations for both v1 and v2
type taskQueueStore struct {
	userDataStore
	version CassandraTaskVersion
}

func newTaskQueueStore(
	userDataStore userDataStore,
	version CassandraTaskVersion,
) *taskQueueStore {
	return &taskQueueStore{
		userDataStore: userDataStore,
		version:       version,
	}
}

func (d *taskQueueStore) CreateTaskQueue(
	ctx context.Context,
	request *p.InternalCreateTaskQueueRequest,
) error {
	queryStr := SwitchTasksTable(templateInsertTaskQueueQuery, d.version)

	var query gocql.Query
	if d.version == CassandraTaskVersion1 {
		query = d.Session.Query(queryStr,
			request.NamespaceID,
			request.TaskQueue,
			request.TaskType,
			rowTypeTaskQueue,
			taskQueueTaskID,
			request.RangeID,
			request.TaskQueueInfo.Data,
			request.TaskQueueInfo.EncodingType.String(),
		).WithContext(ctx)
	} else {
		// For v2, the query rewriting adds pass column, so we need the extra parameter
		query = d.Session.Query(queryStr,
			request.NamespaceID,
			request.TaskQueue,
			request.TaskType,
			rowTypeTaskQueue,
			0, // pass = 0 for task queue metadata
			taskQueueTaskID,
			request.RangeID,
			request.TaskQueueInfo.Data,
			request.TaskQueueInfo.EncodingType.String(),
		).WithContext(ctx)
	}

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
	queryStr := SwitchTasksTable(templateGetTaskQueueQuery, d.version)
	query := d.Session.Query(queryStr,
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

	if d.version == CassandraTaskVersion1 && request.TaskQueueKind == enumspb.TASK_QUEUE_KIND_STICKY {
		// V1 TTL logic - only applies to V1
		if request.ExpiryTime == nil {
			return nil, serviceerror.NewInternal("ExpiryTime cannot be nil for sticky task queue")
		}
		expiryTTL := convert.Int64Ceil(time.Until(timestamp.TimeValue(request.ExpiryTime)).Seconds())
		if expiryTTL >= maxCassandraTTL {
			expiryTTL = maxCassandraTTL
		}
		batch := d.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)

		queryStr1 := SwitchTasksTable(templateUpdateTaskQueueQueryWithTTLPart1, d.version)
		batch.Query(queryStr1,
			request.NamespaceID,
			request.TaskQueue,
			request.TaskType,
			rowTypeTaskQueue,
			taskQueueTaskID,
			expiryTTL,
		)

		queryStr2 := SwitchTasksTable(templateUpdateTaskQueueQueryWithTTLPart2, d.version)
		batch.Query(queryStr2,
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
		queryStr := SwitchTasksTable(templateUpdateTaskQueueQuery, d.version)
		query := d.Session.Query(queryStr,
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
	queryStr := SwitchTasksTable(templateDeleteTaskQueueQuery, d.version)
	query := d.Session.Query(queryStr,
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
