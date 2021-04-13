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
	// HistoryNodeRow represents a row in history_node table
	HistoryNodeRow struct {
		ShardID      int32
		TreeID       primitives.UUID
		BranchID     primitives.UUID
		NodeID       int64
		PrevTxnID    int64
		TxnID        int64
		Data         []byte
		DataEncoding string
	}

	// HistoryNodeSelectFilter contains the column names within history_node table that
	// can be used to filter results through a WHERE clause
	HistoryNodeSelectFilter struct {
		ShardID      int32
		TreeID       primitives.UUID
		BranchID     primitives.UUID
		MinNodeID    int64
		MinTxnID     int64
		MaxNodeID    int64
		PageSize     int
		MetadataOnly bool
	}

	// HistoryNodeDeleteFilter contains the column names within history_node table that
	// can be used to filter results through a WHERE clause
	HistoryNodeDeleteFilter struct {
		ShardID   int32
		TreeID    primitives.UUID
		BranchID  primitives.UUID
		MinNodeID int64
	}

	// HistoryNode is the SQL persistence interface for history nodes
	HistoryNode interface {
		InsertIntoHistoryNode(ctx context.Context, row *HistoryNodeRow) (sql.Result, error)
		DeleteFromHistoryNode(ctx context.Context, row *HistoryNodeRow) (sql.Result, error)
		RangeSelectFromHistoryNode(ctx context.Context, filter HistoryNodeSelectFilter) ([]HistoryNodeRow, error)
		RangeDeleteFromHistoryNode(ctx context.Context, filter HistoryNodeDeleteFilter) (sql.Result, error)
	}
)
