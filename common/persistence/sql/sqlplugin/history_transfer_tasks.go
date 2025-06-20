package sqlplugin

import (
	"context"
	"database/sql"
)

type (
	// TransferTasksRow represents a row in transfer_tasks table
	TransferTasksRow struct {
		ShardID      int32
		TaskID       int64
		Data         []byte
		DataEncoding string
	}

	// TransferTasksFilter contains the column names within transfer_tasks table that
	// can be used to filter results through a WHERE clause
	TransferTasksFilter struct {
		ShardID int32
		TaskID  int64
	}

	// TransferTasksRangeFilter contains the column names within transfer_tasks table that
	// can be used to filter results through a WHERE clause
	TransferTasksRangeFilter struct {
		ShardID            int32
		InclusiveMinTaskID int64
		ExclusiveMaxTaskID int64
		PageSize           int
	}

	// HistoryTransferTask is the SQL persistence interface for history transfer tasks
	HistoryTransferTask interface {
		// InsertIntoTransferTasks inserts rows that into transfer_tasks table.
		InsertIntoTransferTasks(ctx context.Context, rows []TransferTasksRow) (sql.Result, error)
		// RangeSelectFromTransferTasks returns rows that match filter criteria from transfer_tasks table.
		RangeSelectFromTransferTasks(ctx context.Context, filter TransferTasksRangeFilter) ([]TransferTasksRow, error)
		// DeleteFromTransferTasks deletes one rows from transfer_tasks table.
		DeleteFromTransferTasks(ctx context.Context, filter TransferTasksFilter) (sql.Result, error)
		// RangeDeleteFromTransferTasks deletes one or more rows from transfer_tasks table.
		//  TransferTasksRangeFilter - {PageSize} will be ignored
		RangeDeleteFromTransferTasks(ctx context.Context, filter TransferTasksRangeFilter) (sql.Result, error)
	}
)
