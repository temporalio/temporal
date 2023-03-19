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
	"time"
)

type (
	// HistoryScheduledTasksRow represents a row in history_scheduled_tasks table
	HistoryScheduledTasksRow struct {
		ShardID             int32
		CategoryID          int32
		VisibilityTimestamp time.Time
		TaskID              int64
		Data                []byte
		DataEncoding        string
	}

	// HistoryScheduledTasksFilter contains the column names within history_scheduled_tasks table that
	// can be used to filter results through a WHERE clause
	HistoryScheduledTasksFilter struct {
		ShardID             int32
		CategoryID          int32
		TaskID              int64
		VisibilityTimestamp time.Time
	}

	// HistoryScheduledTasksFilter contains the column names within history_scheduled_tasks table that
	// can be used to filter results through a WHERE clause
	HistoryScheduledTasksRangeFilter struct {
		ShardID                         int32
		CategoryID                      int32
		InclusiveMinTaskID              int64
		InclusiveMinVisibilityTimestamp time.Time
		ExclusiveMaxVisibilityTimestamp time.Time
		PageSize                        int
	}

	// HistoryScheduledTask is the SQL persistence interface for history scheduled tasks
	HistoryScheduledTask interface {
		// InsertIntoHistoryScheduledTasks inserts rows that into history_scheduled_tasks table.
		InsertIntoHistoryScheduledTasks(ctx context.Context, rows []HistoryScheduledTasksRow) (sql.Result, error)
		// RangeSelectFromScheduledTasks returns one or more rows from history_scheduled_tasks table
		RangeSelectFromHistoryScheduledTasks(ctx context.Context, filter HistoryScheduledTasksRangeFilter) ([]HistoryScheduledTasksRow, error)
		// DeleteFromScheduledTasks deletes one or more rows from history_scheduled_tasks table
		DeleteFromHistoryScheduledTasks(ctx context.Context, filter HistoryScheduledTasksFilter) (sql.Result, error)
		// RangeDeleteFromScheduledTasks deletes one or more rows from history_scheduled_tasks table
		//  ScheduledTasksRangeFilter - {TaskID, PageSize} will be ignored
		RangeDeleteFromHistoryScheduledTasks(ctx context.Context, filter HistoryScheduledTasksRangeFilter) (sql.Result, error)
	}
)
