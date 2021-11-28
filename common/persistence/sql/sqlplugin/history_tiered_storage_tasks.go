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
	// TieredStorageTasksRow represents a row in TieredStorage_tasks table
	TieredStorageTasksRow struct {
		ShardID      int32
		TaskID       int64
		Data         []byte
		DataEncoding string
	}

	// TieredStorageTasksFilter contains the column names within TieredStorage_tasks table that
	// can be used to filter results through a WHERE clause
	TieredStorageTasksFilter struct {
		ShardID int32
		TaskID  int64
	}

	// TieredStorageTasksRangeFilter contains the column names within TieredStorage_tasks table that
	// can be used to filter results through a WHERE clause
	TieredStorageTasksRangeFilter struct {
		ShardID   int32
		MinTaskID int64
		MaxTaskID int64
	}

	// HistoryTieredStorageTask is the SQL persistence interface for history TieredStorage tasks
	HistoryTieredStorageTask interface {
		InsertIntoTieredStorageTasks(ctx context.Context, rows []TieredStorageTasksRow) (sql.Result, error)
		// SelectFromTieredStorageTasks returns rows that match filter criteria from tiered_storage_tasks table.
		SelectFromTieredStorageTasks(ctx context.Context, filter TieredStorageTasksFilter) ([]TieredStorageTasksRow, error)
		// RangeSelectFromTieredStorageTasks returns rows that match filter criteria from tiered_storage_tasks table.
		RangeSelectFromTieredStorageTasks(ctx context.Context, filter TieredStorageTasksRangeFilter) ([]TieredStorageTasksRow, error)
		// DeleteFromTieredStorageTasks deletes one rows from tiered_storage_tasks table.
		DeleteFromTieredStorageTasks(ctx context.Context, filter TieredStorageTasksFilter) (sql.Result, error)
		// RangeDeleteFromTieredStorageTasks deletes one or more rows from tiered_storage_tasks table.
		RangeDeleteFromTieredStorageTasks(ctx context.Context, filter TieredStorageTasksRangeFilter) (sql.Result, error)
	}
)
