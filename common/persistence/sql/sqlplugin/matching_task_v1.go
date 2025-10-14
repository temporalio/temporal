package sqlplugin

import (
	"context"
	"database/sql"
)

type (
	// TasksRow represents a row in tasks table
	TasksRow struct {
		RangeHash    uint32
		TaskQueueID  []byte
		TaskID       int64
		Data         []byte
		DataEncoding string
	}

	// TasksFilter contains the column names within tasks table that
	// can be used to filter results through a WHERE clause
	TasksFilter struct {
		RangeHash          uint32
		TaskQueueID        []byte
		InclusiveMinTaskID *int64
		ExclusiveMaxTaskID *int64
		Limit              *int
		PageSize           *int
	}

	// MatchingTask is the SQL persistence interface for v1 matching tasks
	MatchingTask interface {
		InsertIntoTasks(ctx context.Context, rows []TasksRow) (sql.Result, error)
		// SelectFromTasks retrieves one or more rows from the tasks table
		// Required filter params - {namespaceID, taskqueueName, taskType, inclusiveMinTaskID, exclusiveMaxTaskID, pageSize}
		SelectFromTasks(ctx context.Context, filter TasksFilter) ([]TasksRow, error)
		// DeleteFromTasks deletes multiple rows from tasks table
		// Required filter params:
		//    - {namespaceID, taskqueueName, taskType, exclusiveMaxTaskID, limit }
		//    - this will delete upto limit number of tasks less than the given max task id
		DeleteFromTasks(ctx context.Context, filter TasksFilter) (sql.Result, error)
	}
)
