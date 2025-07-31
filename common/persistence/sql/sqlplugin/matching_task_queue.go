package sqlplugin

import (
	"context"
	"database/sql"
	"strings"
	"sync"
)

type (
	MatchingTaskVersion int

	// TaskQueuesRow represents a row in task_queues table
	TaskQueuesRow struct {
		RangeHash    uint32
		TaskQueueID  []byte
		RangeID      int64
		Data         []byte
		DataEncoding string
	}

	// TaskQueuesFilter contains the column names within task_queues table that
	// can be used to filter results through a WHERE clause
	TaskQueuesFilter struct {
		RangeHash                   uint32
		RangeHashGreaterThanEqualTo uint32
		RangeHashLessThanEqualTo    uint32
		TaskQueueID                 []byte
		TaskQueueIDGreaterThan      []byte
		RangeID                     *int64
		PageSize                    *int
	}

	UpdateTaskQueueDataRequest struct {
		NamespaceID   []byte
		TaskQueueName string
		Version       int64
		Data          []byte
		DataEncoding  string
	}

	// MatchingTaskQueue is the SQL persistence interface for matching task queues.
	// This handles both "v1" and "v2" tables so that we don't have to duplicate as much code.
	MatchingTaskQueue interface {
		InsertIntoTaskQueues(ctx context.Context, row *TaskQueuesRow, v MatchingTaskVersion) (sql.Result, error)
		UpdateTaskQueues(ctx context.Context, row *TaskQueuesRow, v MatchingTaskVersion) (sql.Result, error)
		// SelectFromTaskQueues returns one or more rows from task_queues table
		// Required Filter params:
		//  to read a single row: {shardID, namespaceID, name, taskType}
		//  to range read multiple rows: {shardID, namespaceIDGreaterThan, nameGreaterThan, taskTypeGreaterThan, pageSize}
		SelectFromTaskQueues(ctx context.Context, filter TaskQueuesFilter, v MatchingTaskVersion) ([]TaskQueuesRow, error)
		DeleteFromTaskQueues(ctx context.Context, filter TaskQueuesFilter, v MatchingTaskVersion) (sql.Result, error)
		LockTaskQueues(ctx context.Context, filter TaskQueuesFilter, v MatchingTaskVersion) (int64, error)
	}
)

const (
	MatchingTaskVersion1 MatchingTaskVersion = 1
	MatchingTaskVersion2 MatchingTaskVersion = 2
)

var switchTaskQueuesTableV1Cache sync.Map

func SwitchTaskQueuesTable(baseQuery string, v MatchingTaskVersion) string {
	if v == MatchingTaskVersion2 {
		return baseQuery
	} else if v != MatchingTaskVersion1 {
		panic("invalid task schema version") // nolint:forbidigo // hardcoded constants
	}
	if v1query, ok := switchTaskQueuesTableV1Cache.Load(baseQuery); ok {
		return v1query.(string) // nolint:revive
	}
	v1query := strings.ReplaceAll(baseQuery, " task_queues_v2 ", " task_queues ")
	switchTaskQueuesTableV1Cache.Store(baseQuery, v1query)
	return v1query
}
