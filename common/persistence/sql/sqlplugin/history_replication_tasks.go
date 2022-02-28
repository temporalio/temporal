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
		InsertIntoReplicationTasks(ctx context.Context, rows []ReplicationTasksRow) (sql.Result, error)
		// SelectFromReplicationTasks returns one or more rows from replication_tasks table
		SelectFromReplicationTasks(ctx context.Context, filter ReplicationTasksFilter) ([]ReplicationTasksRow, error)
		// RangeSelectFromReplicationTasks returns one or more rows from replication_tasks table
		RangeSelectFromReplicationTasks(ctx context.Context, filter ReplicationTasksRangeFilter) ([]ReplicationTasksRow, error)
		// DeleteFromReplicationTasks deletes a row from replication_tasks table
		DeleteFromReplicationTasks(ctx context.Context, filter ReplicationTasksFilter) (sql.Result, error)
		// DeleteFromReplicationTasks deletes multi rows from replication_tasks table
		//  ReplicationTasksRangeFilter - {PageSize} will be ignored
		RangeDeleteFromReplicationTasks(ctx context.Context, filter ReplicationTasksRangeFilter) (sql.Result, error)
	}
)
