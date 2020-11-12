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
	// ShardsRow represents a row in shards table
	ShardsRow struct {
		ShardID      int32
		RangeID      int64
		Data         []byte
		DataEncoding string
	}

	// ShardsFilter contains the column names within shards table that
	// can be used to filter results through a WHERE clause
	ShardsFilter struct {
		ShardID int32
	}

	// HistoryShard is the SQL persistence interface for history shards
	HistoryShard interface {
		InsertIntoShards(ctx context.Context, rows *ShardsRow) (sql.Result, error)
		UpdateShards(ctx context.Context, row *ShardsRow) (sql.Result, error)
		SelectFromShards(ctx context.Context, filter ShardsFilter) (*ShardsRow, error)
		ReadLockShards(ctx context.Context, filter ShardsFilter) (int64, error)
		WriteLockShards(ctx context.Context, filter ShardsFilter) (int64, error)
	}
)
