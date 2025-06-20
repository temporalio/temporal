package sqlplugin

import (
	"context"
	"database/sql"
)

type (
	// HistoryImmediateTasksRow represents a row in history_immediate_tasks table
	HistoryImmediateTasksRow struct {
		ShardID      int32
		CategoryID   int32
		TaskID       int64
		Data         []byte
		DataEncoding string
	}

	// HistoryImmediateTasksFilter contains the column names within history_immediate_tasks table that
	// can be used to filter results through a WHERE clause
	HistoryImmediateTasksFilter struct {
		ShardID    int32
		CategoryID int32
		TaskID     int64
	}

	// HistoryImmediateTasksRangeFilter contains the column names within history_immediate_tasks table that
	// can be used to filter results through a WHERE clause
	HistoryImmediateTasksRangeFilter struct {
		ShardID            int32
		CategoryID         int32
		InclusiveMinTaskID int64
		ExclusiveMaxTaskID int64
		PageSize           int
	}

	// HistoryImmediateTask is the SQL persistence interface for history immediate tasks
	HistoryImmediateTask interface {
		// InsertIntoHistoryImmediateTasks inserts rows that into history_immediate_tasks table.
		InsertIntoHistoryImmediateTasks(ctx context.Context, rows []HistoryImmediateTasksRow) (sql.Result, error)
		// RangeSelectFromHistoryImmediateTasks returns rows that match filter criteria from history_immediate_tasks table.
		RangeSelectFromHistoryImmediateTasks(ctx context.Context, filter HistoryImmediateTasksRangeFilter) ([]HistoryImmediateTasksRow, error)
		// DeleteFromHistoryImmediateTasks deletes one rows from history_immediate_tasks table.
		DeleteFromHistoryImmediateTasks(ctx context.Context, filter HistoryImmediateTasksFilter) (sql.Result, error)
		// RangeDeleteFromHistoryImmediateTasks deletes one or more rows from history_immediate_tasks table.
		//  HistoryImmediateTasksRangeFilter - {PageSize} will be ignored
		RangeDeleteFromHistoryImmediateTasks(ctx context.Context, filter HistoryImmediateTasksRangeFilter) (sql.Result, error)
	}
)
