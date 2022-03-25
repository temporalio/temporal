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
	// TimerTasksRow represents a row in timer_tasks table
	TimerTasksRow struct {
		ShardID             int32
		VisibilityTimestamp time.Time
		TaskID              int64
		Data                []byte
		DataEncoding        string
	}

	// TimerTasksFilter contains the column names within timer_tasks table that
	// can be used to filter results through a WHERE clause
	TimerTasksFilter struct {
		ShardID             int32
		TaskID              int64
		VisibilityTimestamp time.Time
	}

	// TimerTasksFilter contains the column names within timer_tasks table that
	// can be used to filter results through a WHERE clause
	TimerTasksRangeFilter struct {
		ShardID                         int32
		InclusiveMinTaskID              int64
		InclusiveMinVisibilityTimestamp time.Time
		ExclusiveMaxVisibilityTimestamp time.Time
		PageSize                        int
	}

	// HistoryTimerTask is the SQL persistence interface for history timer tasks
	HistoryTimerTask interface {
		InsertIntoTimerTasks(ctx context.Context, rows []TimerTasksRow) (sql.Result, error)
		// SelectFromTimerTasks returns one or more rows from timer_tasks table
		SelectFromTimerTasks(ctx context.Context, filter TimerTasksFilter) ([]TimerTasksRow, error)
		// RangeSelectFromTimerTasks returns one or more rows from timer_tasks table
		RangeSelectFromTimerTasks(ctx context.Context, filter TimerTasksRangeFilter) ([]TimerTasksRow, error)
		// DeleteFromTimerTasks deletes one or more rows from timer_tasks table
		DeleteFromTimerTasks(ctx context.Context, filter TimerTasksFilter) (sql.Result, error)
		// RangeDeleteFromTimerTasks deletes one or more rows from timer_tasks table
		//  TimerTasksRangeFilter - {TaskID, PageSize} will be ignored
		RangeDeleteFromTimerTasks(ctx context.Context, filter TimerTasksRangeFilter) (sql.Result, error)
	}
)
