package sqlplugin

import (
	"context"
	"database/sql"
	"time"
)

type (
	// HistoryScheduledTasksRow represents a row in history_scheduled_tasks table
	HistoryScheduledTasksRow struct {
		ShardID             int32
		CategoryID          int32
		VisibilityTimestamp time.Time
		TaskID              int64
		Data                []byte
		DataEncoding        string
	}

	// HistoryScheduledTasksFilter contains the column names within history_scheduled_tasks table that
	// can be used to filter results through a WHERE clause
	HistoryScheduledTasksFilter struct {
		ShardID             int32
		CategoryID          int32
		TaskID              int64
		VisibilityTimestamp time.Time
	}

	// HistoryScheduledTasksFilter contains the column names within history_scheduled_tasks table that
	// can be used to filter results through a WHERE clause
	HistoryScheduledTasksRangeFilter struct {
		ShardID                         int32
		CategoryID                      int32
		InclusiveMinTaskID              int64
		InclusiveMinVisibilityTimestamp time.Time
		ExclusiveMaxVisibilityTimestamp time.Time
		PageSize                        int
	}

	// HistoryScheduledTask is the SQL persistence interface for history scheduled tasks
	HistoryScheduledTask interface {
		// InsertIntoHistoryScheduledTasks inserts rows that into history_scheduled_tasks table.
		InsertIntoHistoryScheduledTasks(ctx context.Context, rows []HistoryScheduledTasksRow) (sql.Result, error)
		// RangeSelectFromScheduledTasks returns one or more rows from history_scheduled_tasks table
		RangeSelectFromHistoryScheduledTasks(ctx context.Context, filter HistoryScheduledTasksRangeFilter) ([]HistoryScheduledTasksRow, error)
		// DeleteFromScheduledTasks deletes one or more rows from history_scheduled_tasks table
		DeleteFromHistoryScheduledTasks(ctx context.Context, filter HistoryScheduledTasksFilter) (sql.Result, error)
		// RangeDeleteFromScheduledTasks deletes one or more rows from history_scheduled_tasks table
		//  ScheduledTasksRangeFilter - {TaskID, PageSize} will be ignored
		RangeDeleteFromHistoryScheduledTasks(ctx context.Context, filter HistoryScheduledTasksRangeFilter) (sql.Result, error)
	}
)
