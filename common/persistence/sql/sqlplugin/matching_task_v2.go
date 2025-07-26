package sqlplugin

import (
	"context"
	"database/sql"
)

type (
	FairLevel struct {
		TaskPass int64
		TaskID   int64
	}

	// TasksRow represents a row in tasks table
	TasksRowV2 struct {
		RangeHash   uint32
		TaskQueueID []byte
		FairLevel
		Data         []byte
		DataEncoding string
	}

	// TasksFilter contains the column names within tasks table that
	// can be used to filter results through a WHERE clause
	TasksFilterV2 struct {
		RangeHash         uint32
		TaskQueueID       []byte
		InclusiveMinLevel *FairLevel
		ExclusiveMaxLevel *FairLevel
		Limit             *int
		PageSize          *int
	}

	// MatchingTaskV2 is the SQL persistence interface for v2 matching tasks, which support fairness.
	MatchingTaskV2 interface {
		// InsertIntoTasksV2 inserts one or more rows into tasks_v2 table for matching fairness
		InsertIntoTasksV2(ctx context.Context, rows []TasksRowV2) (sql.Result, error)
		// SelectFromTasks retrieves one or more rows from the tasks_v2 table
		// Required filter params - {RangeHash, TaskQueueID, InclusiveMinLevel, PageSize}
		// Returns tasks where the (pass, task_id) tuple is greater than or equal to the provided values,
		// effectively filtering tasks that are at or beyond a specific fairness pass and task ID boundary.
		SelectFromTasksV2(ctx context.Context, filter TasksFilterV2) ([]TasksRowV2, error)
		// DeleteFromTasks deletes multiple rows from tasks table
		// Required filter params:
		//    - {RangeHash, TaskQueueID, ExclusiveMaxLevel, Limit}
		//    - this will delete upto limit number of tasks less than the given max task id
		DeleteFromTasksV2(ctx context.Context, filter TasksFilterV2) (sql.Result, error)
	}
)
