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

	"go.temporal.io/server/common/primitives"
)

type (
	// HistoryTreeRow represents a row in history_tree table
	HistoryTreeRow struct {
		ShardID      int32
		TreeID       primitives.UUID
		BranchID     primitives.UUID
		Data         []byte
		DataEncoding string
	}

	// HistoryTreeSelectFilter contains the column names within history_tree table that
	// can be used to filter results through a WHERE clause
	HistoryTreeSelectFilter struct {
		ShardID int32
		TreeID  primitives.UUID
	}

	// HistoryTreeDeleteFilter contains the column names within history_tree table that
	// can be used to filter results through a WHERE clause
	HistoryTreeDeleteFilter struct {
		ShardID  int32
		TreeID   primitives.UUID
		BranchID primitives.UUID
	}

	// HistoryNode is the SQL persistence interface for history trees
	HistoryTree interface {
		InsertIntoHistoryTree(ctx context.Context, row *HistoryTreeRow) (sql.Result, error)
		SelectFromHistoryTree(ctx context.Context, filter HistoryTreeSelectFilter) ([]HistoryTreeRow, error)
		DeleteFromHistoryTree(ctx context.Context, filter HistoryTreeDeleteFilter) (sql.Result, error)
	}
)
