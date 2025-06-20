package sqlplugin

import (
	"context"
	"database/sql"
)

type (
	// ReplicationTasksRow represents a row in replication_tasks table
	ReplicationTasksRow struct {
		ShardID      int32
		TaskID       int64
		Data         []byte
		DataEncoding string
	}

	// ReplicationTasksFilter contains the column names within replication_tasks table that
	// can be used to filter results through a WHERE clause
	ReplicationTasksFilter struct {
		ShardID int32
		TaskID  int64
	}

	// ReplicationTasksFilter contains the column names within replication_tasks table that
	// can be used to filter results through a WHERE clause
	ReplicationTasksRangeFilter struct {
		ShardID            int32
		InclusiveMinTaskID int64
		ExclusiveMaxTaskID int64
		PageSize           int
	}

	// HistoryReplicationTask is the SQL persistence interface for history replication tasks
	HistoryReplicationTask interface {
		// InsertIntoReplicationTasks inserts rows that into replication_tasks table.
		InsertIntoReplicationTasks(ctx context.Context, rows []ReplicationTasksRow) (sql.Result, error)
		// RangeSelectFromReplicationTasks returns one or more rows from replication_tasks table
		RangeSelectFromReplicationTasks(ctx context.Context, filter ReplicationTasksRangeFilter) ([]ReplicationTasksRow, error)
		// DeleteFromReplicationTasks deletes a row from replication_tasks table
		DeleteFromReplicationTasks(ctx context.Context, filter ReplicationTasksFilter) (sql.Result, error)
		// DeleteFromReplicationTasks deletes multi rows from replication_tasks table
		//  ReplicationTasksRangeFilter - {PageSize} will be ignored
		RangeDeleteFromReplicationTasks(ctx context.Context, filter ReplicationTasksRangeFilter) (sql.Result, error)
	}
)
