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

	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/primitives"
)

type (
	// ExecutionsRow represents a row in executions table
	ExecutionsRow struct {
		ShardID          int32
		NamespaceID      primitives.UUID
		WorkflowID       string
		RunID            primitives.UUID
		NextEventID      int64
		LastWriteVersion int64
		Data             []byte
		DataEncoding     string
		State            []byte
		StateEncoding    string
	}

	// ExecutionsFilter contains the column names within executions table that
	// can be used to filter results through a WHERE clause
	ExecutionsFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// CurrentExecutionsRow represents a row in current_executions table
	CurrentExecutionsRow struct {
		ShardID          int32
		NamespaceID      primitives.UUID
		WorkflowID       string
		RunID            primitives.UUID
		CreateRequestID  string
		StartVersion     int64
		LastWriteVersion int64
		State            enumsspb.WorkflowExecutionState
		Status           enumspb.WorkflowExecutionStatus
	}

	// CurrentExecutionsFilter contains the column names within current_executions table that
	// can be used to filter results through a WHERE clause
	CurrentExecutionsFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// HistoryExecution is the SQL persistence interface for history nodes and history executions
	HistoryExecution interface {
		InsertIntoExecutions(row *ExecutionsRow) (sql.Result, error)
		UpdateExecutions(row *ExecutionsRow) (sql.Result, error)
		SelectFromExecutions(filter ExecutionsFilter) (*ExecutionsRow, error)
		DeleteFromExecutions(filter ExecutionsFilter) (sql.Result, error)
		ReadLockExecutions(filter ExecutionsFilter) (int64, error)
		WriteLockExecutions(filter ExecutionsFilter) (int64, error)

		LockCurrentExecutionsJoinExecutions(filter CurrentExecutionsFilter) ([]CurrentExecutionsRow, error)

		InsertIntoCurrentExecutions(row *CurrentExecutionsRow) (sql.Result, error)
		UpdateCurrentExecutions(row *CurrentExecutionsRow) (sql.Result, error)
		// SelectFromCurrentExecutions returns one or more rows from current_executions table
		// Required params - {shardID, namespaceID, workflowID}
		SelectFromCurrentExecutions(filter CurrentExecutionsFilter) (*CurrentExecutionsRow, error)
		// DeleteFromCurrentExecutions deletes a single row that matches the filter criteria
		// If a row exist, that row will be deleted and this method will return success
		// If there is no row matching the filter criteria, this method will still return success
		// Callers can check the output of Result.RowsAffected() to see if a row was deleted or not
		// Required params - {shardID, namespaceID, workflowID, runID}
		DeleteFromCurrentExecutions(filter CurrentExecutionsFilter) (sql.Result, error)
		LockCurrentExecutions(filter CurrentExecutionsFilter) (*CurrentExecutionsRow, error)
	}
)
