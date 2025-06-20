package sqlplugin

import (
	"context"
	"database/sql"
)

type (
	// VisibilityTasksRow represents a row in visibility_tasks table
	VisibilityTasksRow struct {
		ShardID      int32
		TaskID       int64
		Data         []byte
		DataEncoding string
	}

	// VisibilityTasksFilter contains the column names within visibility_tasks table that
	// can be used to filter results through a WHERE clause
	VisibilityTasksFilter struct {
		ShardID int32
		TaskID  int64
	}

	// VisibilityTasksRangeFilter contains the column names within visibility_tasks table that
	// can be used to filter results through a WHERE clause
	VisibilityTasksRangeFilter struct {
		ShardID            int32
		InclusiveMinTaskID int64
		ExclusiveMaxTaskID int64
		PageSize           int
	}

	// HistoryVisibilityTask is the SQL persistence interface for history visibility tasks
	HistoryVisibilityTask interface {
		// InsertIntoVisibilityTasks inserts rows that into visibility_tasks table.
		InsertIntoVisibilityTasks(ctx context.Context, rows []VisibilityTasksRow) (sql.Result, error)
		// RangeSelectFromVisibilityTasks returns rows that match filter criteria from visibility_tasks table.
		RangeSelectFromVisibilityTasks(ctx context.Context, filter VisibilityTasksRangeFilter) ([]VisibilityTasksRow, error)
		// DeleteFromVisibilityTasks deletes one rows from visibility_tasks table.
		DeleteFromVisibilityTasks(ctx context.Context, filter VisibilityTasksFilter) (sql.Result, error)
		// RangeDeleteFromVisibilityTasks deletes one or more rows from visibility_tasks table.
		//  VisibilityTasksRangeFilter - {PageSize} will be ignored
		RangeDeleteFromVisibilityTasks(ctx context.Context, filter VisibilityTasksRangeFilter) (sql.Result, error)
	}
)
