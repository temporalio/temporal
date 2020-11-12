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
	// ActivityInfoMapsRow represents a row in activity_info_maps table
	ActivityInfoMapsRow struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		ScheduleID   int64
		Data         []byte
		DataEncoding string
	}

	ActivityInfoMapsFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		ScheduleIDs []int64
	}

	ActivityInfoMapsAllFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// HistoryExecutionActivity is the SQL persistence interface for history nodes and history execution activities
	HistoryExecutionActivity interface {
		// ReplaceIntoActivityInfoMaps replace one or more into activity_info_maps table
		ReplaceIntoActivityInfoMaps(ctx context.Context, rows []ActivityInfoMapsRow) (sql.Result, error)
		// SelectAllFromActivityInfoMaps returns all rows from activity_info_maps table
		SelectAllFromActivityInfoMaps(ctx context.Context, filter ActivityInfoMapsAllFilter) ([]ActivityInfoMapsRow, error)
		// DeleteFromActivityInfoMaps deletes one or more row from activity_info_maps table
		DeleteFromActivityInfoMaps(ctx context.Context, filter ActivityInfoMapsFilter) (sql.Result, error)
		// DeleteAllFromActivityInfoMaps deletes all from activity_info_maps table
		DeleteAllFromActivityInfoMaps(ctx context.Context, filter ActivityInfoMapsAllFilter) (sql.Result, error)
	}
)
