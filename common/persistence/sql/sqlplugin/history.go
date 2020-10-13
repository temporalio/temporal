// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package sqlplugin

import "database/sql"

type (
	// HistoryShard is the SQL persistence interface for history shards
	HistoryShard interface {
		InsertIntoShards(rows *ShardsRow) (sql.Result, error)
		UpdateShards(row *ShardsRow) (sql.Result, error)
		SelectFromShards(filter *ShardsFilter) (*ShardsRow, error)
		ReadLockShards(filter *ShardsFilter) (int64, error)
		WriteLockShards(filter *ShardsFilter) (int64, error)
	}

	// HistoryExecutionBuffer is the SQL persistence interface for history nodes and history execution buffer events
	HistoryExecutionBuffer interface {
		InsertIntoBufferedEvents(rows []BufferedEventsRow) (sql.Result, error)
		SelectFromBufferedEvents(filter *BufferedEventsFilter) ([]BufferedEventsRow, error)
		DeleteFromBufferedEvents(filter *BufferedEventsFilter) (sql.Result, error)
	}

	// HistoryReplicationTask is the SQL persistence interface for history nodes and history replication tasks
	HistoryReplicationTask interface {
		InsertIntoReplicationTasks(rows []ReplicationTasksRow) (sql.Result, error)
		// SelectFromReplicationTasks returns one or more rows from replication_tasks table
		// Required filter params - {shardID, minTaskID, maxTaskID, pageSize}
		SelectFromReplicationTasks(filter *ReplicationTasksFilter) ([]ReplicationTasksRow, error)
		// DeleteFromReplicationTasks deletes a row from replication_tasks table
		// Required filter params - {shardID, inclusiveEndTaskID}
		DeleteFromReplicationTasks(filter *ReplicationTasksFilter) (sql.Result, error)
		// DeleteFromReplicationTasks deletes multi rows from replication_tasks table
		// Required filter params - {shardID, inclusiveEndTaskID}
		RangeDeleteFromReplicationTasks(filter *ReplicationTasksFilter) (sql.Result, error)

		// InsertIntoReplicationTasksDLQ puts the replication task into DLQ
		InsertIntoReplicationTasksDLQ(row *ReplicationTaskDLQRow) (sql.Result, error)
		// SelectFromReplicationTasksDLQ returns one or more rows from replication_tasks_dlq table
		// Required filter params - {sourceClusterName, shardID, minTaskID, pageSize}
		SelectFromReplicationTasksDLQ(filter *ReplicationTasksDLQFilter) ([]ReplicationTasksRow, error)
		// DeleteMessageFromReplicationTasksDLQ deletes one row from replication_tasks_dlq table
		// Required filter params - {sourceClusterName, shardID, taskID}
		DeleteMessageFromReplicationTasksDLQ(filter *ReplicationTasksDLQFilter) (sql.Result, error)
		// RangeDeleteMessageFromReplicationTasksDLQ deletes one or more rows from replication_tasks_dlq table
		// Required filter params - {sourceClusterName, shardID, taskID, inclusiveTaskID}
		RangeDeleteMessageFromReplicationTasksDLQ(filter *ReplicationTasksDLQFilter) (sql.Result, error)
	}
)
