package sqlplugin

import "database/sql"

type (
	// ReplicationDLQTasksRow represents a row in replication_tasks_dlq table
	ReplicationDLQTasksRow struct {
		SourceClusterName string
		ShardID           int32
		TaskID            int64
		Data              []byte
		DataEncoding      string
	}

	// ReplicationDLQTasksFilter contains the column names within replication_tasks_dlq table that
	// can be used to filter results through a WHERE clause
	ReplicationDLQTasksFilter struct {
		ShardID           int32
		SourceClusterName string
		TaskID            int64
	}

	// ReplicationDLQTasksRangeFilter
	ReplicationDLQTasksRangeFilter struct {
		ShardID           int32
		SourceClusterName string
		MinTaskID         int64
		MaxTaskID         int64
		PageSize          int
	}

	// HistoryReplicationDLQTask is the SQL persistence interface for history replication tasks DLQ
	HistoryReplicationDLQTask interface {
		// InsertIntoReplicationDLQTasks puts the replication task into DLQ
		InsertIntoReplicationDLQTasks(row []ReplicationDLQTasksRow) (sql.Result, error)
		// SelectFromReplicationDLQTasks returns one or more rows from replication_tasks_dlq table
		SelectFromReplicationDLQTasks(filter ReplicationDLQTasksFilter) ([]ReplicationDLQTasksRow, error)
		// RangeSelectFromReplicationDLQTasks returns one or more rows from replication_tasks_dlq table
		RangeSelectFromReplicationDLQTasks(filter ReplicationDLQTasksRangeFilter) ([]ReplicationDLQTasksRow, error)
		// DeleteFromReplicationDLQTasks deletes one row from replication_tasks_dlq table
		DeleteFromReplicationDLQTasks(filter ReplicationDLQTasksFilter) (sql.Result, error)
		// RangeDeleteFromReplicationDLQTasks deletes one or more rows from replication_tasks_dlq table
		//  ReplicationDLQTasksRangeFilter - {PageSize} will be ignored
		RangeDeleteFromReplicationDLQTasks(filter ReplicationDLQTasksRangeFilter) (sql.Result, error)
	}
)
