package sqlplugin

import (
	"context"
	"database/sql"
)

type (
	// TaskQueuesRow represents a row in task_queues table
	TaskQueuesRowV2 struct {
		RangeHash    uint32
		TaskQueueID  []byte
		RangeID      int64
		Data         []byte
		DataEncoding string
	}

	// TaskQueuesFilter contains the column names within task_queues table that
	// can be used to filter results through a WHERE clause
	TaskQueuesFilterV2 struct {
		RangeHash                   uint32
		RangeHashGreaterThanEqualTo uint32
		RangeHashLessThanEqualTo    uint32
		TaskQueueID                 []byte
		TaskQueueIDGreaterThan      []byte
		RangeID                     *int64
		PageSize                    *int
	}

	UpdateTaskQueueDataRequestV2 struct {
		NamespaceID   []byte
		TaskQueueName string
		Version       int64
		Data          []byte
		DataEncoding  string
	}

	// MatchingTaskQueue is the SQL persistence interface for matching task queues
	MatchingTaskQueueV2 interface {
		InsertIntoTaskQueuesV2(ctx context.Context, row *TaskQueuesRowV2) (sql.Result, error)
		UpdateTaskQueuesV2(ctx context.Context, row *TaskQueuesRowV2) (sql.Result, error)
		// SelectFromTaskQueues returns one or more rows from task_queues table
		// Required Filter params:
		//  to read a single row: {shardID, namespaceID, name, taskType}
		//  to range read multiple rows: {shardID, namespaceIDGreaterThan, nameGreaterThan, taskTypeGreaterThan, pageSize}
		SelectFromTaskQueuesV2(ctx context.Context, filter TaskQueuesFilterV2) ([]TaskQueuesRowV2, error)
		DeleteFromTaskQueuesV2(ctx context.Context, filter TaskQueuesFilterV2) (sql.Result, error)
		LockTaskQueuesV2(ctx context.Context, filter TaskQueuesFilterV2) (int64, error)
	}
)
