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
	// SignalInfoMapsRow represents a row in signal_info_maps table
	SignalInfoMapsRow struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		InitiatedID  int64
		Data         []byte
		DataEncoding string
	}

	SignalInfoMapsFilter struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		InitiatedIDs []int64
	}

	SignalInfoMapsAllFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// HistoryExecutionSignal is the SQL persistence interface for history execution signals
	HistoryExecutionSignal interface {
		// ReplaceIntoSignalInfoMaps replace one or more rows into signal_info_maps table
		ReplaceIntoSignalInfoMaps(ctx context.Context, rows []SignalInfoMapsRow) (sql.Result, error)
		// SelectAllFromSignalInfoMaps returns one or more rows from signal_info_maps table
		SelectAllFromSignalInfoMaps(ctx context.Context, filter SignalInfoMapsAllFilter) ([]SignalInfoMapsRow, error)
		// DeleteFromSignalInfoMaps deletes one or more rows from signal_info_maps table
		DeleteFromSignalInfoMaps(ctx context.Context, filter SignalInfoMapsFilter) (sql.Result, error)
		// DeleteFromSignalInfoMaps deletes one or more rows from signal_info_maps table
		DeleteAllFromSignalInfoMaps(ctx context.Context, filter SignalInfoMapsAllFilter) (sql.Result, error)
	}
)
