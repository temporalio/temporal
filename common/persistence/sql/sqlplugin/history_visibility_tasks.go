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
	// VisibilityTasksRow represents a row in visibility_tasks table
	VisibilityTasksRow struct {
		ShardID      int32
		TaskID       int64
		Data         []byte
		DataEncoding string
	}

	// VisibilityTasksFilter contains the column names within visibility_tasks table that
	// can be used to filter results through a WHERE clause
	VisibilityTasksFilter struct {
		ShardID int32
		TaskID  int64
	}

	// VisibilityTasksRangeFilter contains the column names within visibility_tasks table that
	// can be used to filter results through a WHERE clause
	VisibilityTasksRangeFilter struct {
		ShardID            int32
		InclusiveMinTaskID int64
		ExclusiveMaxTaskID int64
		PageSize           int
	}

	// HistoryVisibilityTask is the SQL persistence interface for history visibility tasks
	HistoryVisibilityTask interface {
		InsertIntoVisibilityTasks(ctx context.Context, rows []VisibilityTasksRow) (sql.Result, error)
		// SelectFromVisibilityTasks returns rows that match filter criteria from visibility_tasks table.
		SelectFromVisibilityTasks(ctx context.Context, filter VisibilityTasksFilter) ([]VisibilityTasksRow, error)
		// RangeSelectFromVisibilityTasks returns rows that match filter criteria from visibility_tasks table.
		RangeSelectFromVisibilityTasks(ctx context.Context, filter VisibilityTasksRangeFilter) ([]VisibilityTasksRow, error)
		// DeleteFromVisibilityTasks deletes one rows from visibility_tasks table.
		DeleteFromVisibilityTasks(ctx context.Context, filter VisibilityTasksFilter) (sql.Result, error)
		// RangeDeleteFromVisibilityTasks deletes one or more rows from visibility_tasks table.
		//  VisibilityTasksRangeFilter - {PageSize} will be ignored
		RangeDeleteFromVisibilityTasks(ctx context.Context, filter VisibilityTasksRangeFilter) (sql.Result, error)
	}
)
