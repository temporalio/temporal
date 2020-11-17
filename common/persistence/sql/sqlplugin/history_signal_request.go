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
	// SignalsRequestedSetsRow represents a row in signals_requested_sets table
	SignalsRequestedSetsRow struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		SignalID    string
	}

	SignalsRequestedSetsFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		SignalIDs   []string
	}

	SignalsRequestedSetsAllFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// HistoryExecutionSignalRequest is the SQL persistence interface for history execution signal request
	HistoryExecutionSignalRequest interface {
		// ReplaceIntoSignalsRequestedSets replace one or more rows into signals_requested_sets table
		ReplaceIntoSignalsRequestedSets(ctx context.Context, rows []SignalsRequestedSetsRow) (sql.Result, error)
		// SelectAllFromSignalsRequestedSets returns all rows from signals_requested_sets table
		SelectAllFromSignalsRequestedSets(ctx context.Context, filter SignalsRequestedSetsAllFilter) ([]SignalsRequestedSetsRow, error)
		// DeleteFromSignalsRequestedSets deletes one or more rows from signals_requested_sets table
		DeleteFromSignalsRequestedSets(ctx context.Context, filter SignalsRequestedSetsFilter) (sql.Result, error)
		// DeleteAllFromSignalsRequestedSets deletes all rows from signals_requested_sets table
		DeleteAllFromSignalsRequestedSets(ctx context.Context, filter SignalsRequestedSetsAllFilter) (sql.Result, error)
	}
)
