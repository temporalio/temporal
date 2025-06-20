package sqlplugin

import (
	"context"
	"database/sql"
)

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
		ShardID            int32
		SourceClusterName  string
		InclusiveMinTaskID int64
		ExclusiveMaxTaskID int64
		PageSize           int
	}

	// HistoryReplicationDLQTask is the SQL persistence interface for history replication tasks DLQ
	HistoryReplicationDLQTask interface {
		// InsertIntoReplicationDLQTasks puts the replication task into DLQ
		InsertIntoReplicationDLQTasks(ctx context.Context, row []ReplicationDLQTasksRow) (sql.Result, error)
		// RangeSelectFromReplicationDLQTasks returns one or more rows from replication_tasks_dlq table
		RangeSelectFromReplicationDLQTasks(ctx context.Context, filter ReplicationDLQTasksRangeFilter) ([]ReplicationDLQTasksRow, error)
		// DeleteFromReplicationDLQTasks deletes one row from replication_tasks_dlq table
		DeleteFromReplicationDLQTasks(ctx context.Context, filter ReplicationDLQTasksFilter) (sql.Result, error)
		// RangeDeleteFromReplicationDLQTasks deletes one or more rows from replication_tasks_dlq table
		//  ReplicationDLQTasksRangeFilter - {PageSize} will be ignored
		RangeDeleteFromReplicationDLQTasks(ctx context.Context, filter ReplicationDLQTasksRangeFilter) (sql.Result, error)
	}
)
