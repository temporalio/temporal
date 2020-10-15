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

	ActivityInfoMapsSelectFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	ActivityInfoMapsDeleteFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		ScheduleID  *int64
	}

	// HistoryExecutionActivity is the SQL persistence interface for history nodes and history execution activities
	HistoryExecutionActivity interface {
		ReplaceIntoActivityInfoMaps(rows []ActivityInfoMapsRow) (sql.Result, error)
		// SelectFromActivityInfoMaps returns one or more rows from activity_info_maps
		SelectFromActivityInfoMaps(filter ActivityInfoMapsSelectFilter) ([]ActivityInfoMapsRow, error)
		// DeleteFromActivityInfoMaps deletes a row from activity_info_maps table
		// Required filter params
		// - single row delete - {shardID, namespaceID, workflowID, runID, scheduleID}
		// - range delete - {shardID, namespaceID, workflowID, runID}
		DeleteFromActivityInfoMaps(filter ActivityInfoMapsDeleteFilter) (sql.Result, error)
	}
)
