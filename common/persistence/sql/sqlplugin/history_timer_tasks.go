package sqlplugin

import (
	"context"
	"database/sql"
	"time"
)

type (
	// TimerTasksRow represents a row in timer_tasks table
	TimerTasksRow struct {
		ShardID             int32
		VisibilityTimestamp time.Time
		TaskID              int64
		Data                []byte
		DataEncoding        string
	}

	// TimerTasksFilter contains the column names within timer_tasks table that
	// can be used to filter results through a WHERE clause
	TimerTasksFilter struct {
		ShardID             int32
		TaskID              int64
		VisibilityTimestamp time.Time
	}

	// TimerTasksFilter contains the column names within timer_tasks table that
	// can be used to filter results through a WHERE clause
	TimerTasksRangeFilter struct {
		ShardID                         int32
		InclusiveMinTaskID              int64
		InclusiveMinVisibilityTimestamp time.Time
		ExclusiveMaxVisibilityTimestamp time.Time
		PageSize                        int
	}

	// HistoryTimerTask is the SQL persistence interface for history timer tasks
	HistoryTimerTask interface {
		// InsertIntoTimerTasks inserts rows that into timer_tasks table.
		InsertIntoTimerTasks(ctx context.Context, rows []TimerTasksRow) (sql.Result, error)
		// RangeSelectFromTimerTasks returns one or more rows from timer_tasks table
		RangeSelectFromTimerTasks(ctx context.Context, filter TimerTasksRangeFilter) ([]TimerTasksRow, error)
		// DeleteFromTimerTasks deletes one or more rows from timer_tasks table
		DeleteFromTimerTasks(ctx context.Context, filter TimerTasksFilter) (sql.Result, error)
		// RangeDeleteFromTimerTasks deletes one or more rows from timer_tasks table
		//  TimerTasksRangeFilter - {TaskID, PageSize} will be ignored
		RangeDeleteFromTimerTasks(ctx context.Context, filter TimerTasksRangeFilter) (sql.Result, error)
	}
)
