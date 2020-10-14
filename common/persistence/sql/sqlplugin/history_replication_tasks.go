package sqlplugin

import "database/sql"

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
		ShardID   int32
		MinTaskID int64
		MaxTaskID int64
		PageSize  int
	}

	// HistoryReplicationTask is the SQL persistence interface for history replication tasks
	HistoryReplicationTask interface {
		InsertIntoReplicationTasks(rows []ReplicationTasksRow) (sql.Result, error)
		// SelectFromReplicationTasks returns one or more rows from replication_tasks table
		SelectFromReplicationTasks(filter ReplicationTasksFilter) ([]ReplicationTasksRow, error)
		// RangeSelectFromReplicationTasks returns one or more rows from replication_tasks table
		RangeSelectFromReplicationTasks(filter ReplicationTasksRangeFilter) ([]ReplicationTasksRow, error)
		// DeleteFromReplicationTasks deletes a row from replication_tasks table
		DeleteFromReplicationTasks(filter ReplicationTasksFilter) (sql.Result, error)
		// DeleteFromReplicationTasks deletes multi rows from replication_tasks table
		//  ReplicationTasksRangeFilter - {PageSize} will be ignored
		RangeDeleteFromReplicationTasks(filter ReplicationTasksRangeFilter) (sql.Result, error)
	}
)
