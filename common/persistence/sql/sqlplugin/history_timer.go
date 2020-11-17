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
	// TimerInfoMapsRow represents a row in timer_info_maps table
	TimerInfoMapsRow struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		TimerID      string
		Data         []byte
		DataEncoding string
	}

	TimerInfoMapsFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		TimerIDs    []string
	}

	TimerInfoMapsAllFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// HistoryExecutionTimer is the SQL persistence interface for history execution timers
	HistoryExecutionTimer interface {
		// ReplaceIntoTimerInfoMaps replace one or more rows into timer_info_maps table
		ReplaceIntoTimerInfoMaps(ctx context.Context, rows []TimerInfoMapsRow) (sql.Result, error)
		// SelectAllFromTimerInfoMaps returns all rows from timer_info_maps table
		SelectAllFromTimerInfoMaps(ctx context.Context, filter TimerInfoMapsAllFilter) ([]TimerInfoMapsRow, error)
		// DeleteFromTimerInfoMaps deletes one or more rows from timer_info_maps table
		DeleteFromTimerInfoMaps(ctx context.Context, filter TimerInfoMapsFilter) (sql.Result, error)
		// DeleteAllFromTimerInfoMaps deletes all rows from timer_info_maps table
		DeleteAllFromTimerInfoMaps(ctx context.Context, filter TimerInfoMapsAllFilter) (sql.Result, error)
	}
)
