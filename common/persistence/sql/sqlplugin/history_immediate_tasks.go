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
