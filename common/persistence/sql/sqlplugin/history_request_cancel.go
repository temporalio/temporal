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
	// RequestCancelInfoMapsRow represents a row in request_cancel_info_maps table
	RequestCancelInfoMapsRow struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		InitiatedID  int64
		Data         []byte
		DataEncoding string
	}

	RequestCancelInfoMapsFilter struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		InitiatedIDs []int64
	}

	RequestCancelInfoMapsAllFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// HistoryExecutionRequestCancel is the SQL persistence interface for history execution request cancels
	HistoryExecutionRequestCancel interface {
		// ReplaceIntoRequestCancelInfoMaps replace one or more rows into request_cancel_info_maps table
		ReplaceIntoRequestCancelInfoMaps(ctx context.Context, rows []RequestCancelInfoMapsRow) (sql.Result, error)
		// SelectAllFromRequestCancelInfoMaps returns all rows from request_cancel_info_maps table
		SelectAllFromRequestCancelInfoMaps(ctx context.Context, filter RequestCancelInfoMapsAllFilter) ([]RequestCancelInfoMapsRow, error)
		// DeleteFromRequestCancelInfoMaps deletes one or more rows from request_cancel_info_maps table
		DeleteFromRequestCancelInfoMaps(ctx context.Context, filter RequestCancelInfoMapsFilter) (sql.Result, error)
		// DeleteAllFromRequestCancelInfoMaps deletes all rows from request_cancel_info_maps table
		DeleteAllFromRequestCancelInfoMaps(ctx context.Context, filter RequestCancelInfoMapsAllFilter) (sql.Result, error)
	}
)
