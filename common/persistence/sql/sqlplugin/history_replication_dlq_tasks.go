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
		// SelectFromReplicationDLQTasks returns one or more rows from replication_tasks_dlq table
		SelectFromReplicationDLQTasks(ctx context.Context, filter ReplicationDLQTasksFilter) ([]ReplicationDLQTasksRow, error)
		// RangeSelectFromReplicationDLQTasks returns one or more rows from replication_tasks_dlq table
		RangeSelectFromReplicationDLQTasks(ctx context.Context, filter ReplicationDLQTasksRangeFilter) ([]ReplicationDLQTasksRow, error)
		// DeleteFromReplicationDLQTasks deletes one row from replication_tasks_dlq table
		DeleteFromReplicationDLQTasks(ctx context.Context, filter ReplicationDLQTasksFilter) (sql.Result, error)
		// RangeDeleteFromReplicationDLQTasks deletes one or more rows from replication_tasks_dlq table
		//  ReplicationDLQTasksRangeFilter - {PageSize} will be ignored
		RangeDeleteFromReplicationDLQTasks(ctx context.Context, filter ReplicationDLQTasksRangeFilter) (sql.Result, error)
	}
)
