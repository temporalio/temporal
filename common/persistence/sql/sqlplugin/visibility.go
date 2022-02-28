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
	// VisibilityRow represents a row in executions_visibility table
	VisibilityRow struct {
		NamespaceID      string
		RunID            string
		WorkflowTypeName string
		WorkflowID       string
		StartTime        time.Time
		ExecutionTime    time.Time
		Status           int32
		CloseTime        *time.Time
		HistoryLength    *int64
		Memo             []byte
		Encoding         string
		TaskQueue        string
	}

	// VisibilitySelectFilter contains the column names within executions_visibility table that
	// can be used to filter results through a WHERE clause
	VisibilitySelectFilter struct {
		NamespaceID      string
		RunID            *string
		WorkflowID       *string
		WorkflowTypeName *string
		Status           int32
		MinTime          *time.Time
		MaxTime          *time.Time
		PageSize         *int
	}

	VisibilityDeleteFilter struct {
		NamespaceID string
		RunID       string
	}

	Visibility interface {
		// InsertIntoVisibility inserts a row into visibility table. If a row already exist,
		// no changes will be made by this API
		InsertIntoVisibility(ctx context.Context, row *VisibilityRow) (sql.Result, error)
		// ReplaceIntoVisibility deletes old row (if it exist) and inserts new row into visibility table
		ReplaceIntoVisibility(ctx context.Context, row *VisibilityRow) (sql.Result, error)
		// SelectFromVisibility returns one or more rows from visibility table
		// Required filter params:
		// - getClosedWorkflowExecution - retrieves single row - {namespaceID, runID, closed=true}
		// - All other queries retrieve multiple rows (range):
		//   - MUST specify following required params:
		//     - namespaceID, minStartTime, maxStartTime, runID and pageSize where some or all of these may come from previous page token
		//   - OPTIONALLY specify one of following params
		//     - workflowID, workflowTypeName, status (along with closed=true)
		SelectFromVisibility(ctx context.Context, filter VisibilitySelectFilter) ([]VisibilityRow, error)
		DeleteFromVisibility(ctx context.Context, filter VisibilityDeleteFilter) (sql.Result, error)
	}
)
