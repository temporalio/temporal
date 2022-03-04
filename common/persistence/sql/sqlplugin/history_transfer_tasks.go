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
	// TransferTasksRow represents a row in transfer_tasks table
	TransferTasksRow struct {
		ShardID      int32
		TaskID       int64
		Data         []byte
		DataEncoding string
	}

	// TransferTasksFilter contains the column names within transfer_tasks table that
	// can be used to filter results through a WHERE clause
	TransferTasksFilter struct {
		ShardID int32
		TaskID  int64
	}

	// TransferTasksRangeFilter contains the column names within transfer_tasks table that
	// can be used to filter results through a WHERE clause
	TransferTasksRangeFilter struct {
		ShardID            int32
		InclusiveMinTaskID int64
		ExclusiveMaxTaskID int64
		PageSize           int
	}

	// HistoryTransferTask is the SQL persistence interface for history transfer tasks
	HistoryTransferTask interface {
		InsertIntoTransferTasks(ctx context.Context, rows []TransferTasksRow) (sql.Result, error)
		// SelectFromTransferTasks returns rows that match filter criteria from transfer_tasks table.
		SelectFromTransferTasks(ctx context.Context, filter TransferTasksFilter) ([]TransferTasksRow, error)
		// RangeSelectFromTransferTasks returns rows that match filter criteria from transfer_tasks table.
		RangeSelectFromTransferTasks(ctx context.Context, filter TransferTasksRangeFilter) ([]TransferTasksRow, error)
		// DeleteFromTransferTasks deletes one rows from transfer_tasks table.
		DeleteFromTransferTasks(ctx context.Context, filter TransferTasksFilter) (sql.Result, error)
		// RangeDeleteFromTransferTasks deletes one or more rows from transfer_tasks table.
		//  TransferTasksRangeFilter - {PageSize} will be ignored
		RangeDeleteFromTransferTasks(ctx context.Context, filter TransferTasksRangeFilter) (sql.Result, error)
	}
)
