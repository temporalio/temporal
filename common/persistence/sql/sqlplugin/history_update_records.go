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
	// UpdateRecordMapsRow represents a row in update_record_maps table
	UpdateRecordMapsRow struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		UpdateID     string
		Data         []byte
		DataEncoding string
	}

	UpdateRecordMapsFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		UpdateIDs   []string
	}

	UpdateRecordMapsAllFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// HistoryExecutionUpdateRecord is the SQL persistence interface for history execution updates
	HistoryExecutionUpdateRecord interface {
		// ReplaceIntoUpdateRecordMaps replace one or more rows into update_record_maps table
		ReplaceIntoUpdateRecordMaps(ctx context.Context, rows []UpdateRecordMapsRow) (sql.Result, error)
		// SelectAllFromUpdateRecordMaps returns all rows from update_record_maps table
		SelectAllFromUpdateRecordMaps(ctx context.Context, filter UpdateRecordMapsAllFilter) ([]UpdateRecordMapsRow, error)
		// DeleteFromUpdateRecordMaps deletes one or more rows from update_record_maps table
		DeleteFromUpdateRecordMaps(ctx context.Context, filter UpdateRecordMapsFilter) (sql.Result, error)
		// DeleteAllFromUpdateRecordMaps deletes all rows from update_record_maps table
		DeleteAllFromUpdateRecordMaps(ctx context.Context, filter UpdateRecordMapsAllFilter) (sql.Result, error)
	}
)
