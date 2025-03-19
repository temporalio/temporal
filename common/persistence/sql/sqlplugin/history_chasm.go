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
	// ChasmNodeMapsRow represents a row in the chasm_node_maps table.
	ChasmNodeMapsRow struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		ChasmPath    string
		Data         []byte
		DataEncoding string
	}

	// ChasmNodeMapsFilter represents parameters to selecting a particular subset of
	// a workflow's CHASM nodes.
	ChasmNodeMapsFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		ChasmPaths  []string
	}

	// ChasmNodeMapsAllFilter represents parameters to selecting all of a workflow's
	// CHASM nodes.
	ChasmNodeMapsAllFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	HistoryExecutionChasm interface {
		// SelectAllFromChasmNodeMaps returns all rows related to a particular workflow from the chasm_node_maps table.
		SelectAllFromChasmNodeMaps(ctx context.Context, filter ChasmNodeMapsAllFilter) ([]ChasmNodeMapsRow, error)

		// ReplaceIntoChasmNodeMaps replaces one or more rows in the chasm_node_maps table.
		ReplaceIntoChasmNodeMaps(ctx context.Context, rows []ChasmNodeMapsRow) (sql.Result, error)

		// DeleteFromChasmNodeMaps deletes one or more rows in the chasm_node_maps table.
		DeleteFromChasmNodeMaps(ctx context.Context, filter ChasmNodeMapsFilter) (sql.Result, error)

		// DeleteAllFromChasmNodeMaps deletes all rows related to a particular workflow in the chasm_node_maps table.
		DeleteAllFromChasmNodeMaps(ctx context.Context, filter ChasmNodeMapsAllFilter) (sql.Result, error)
	}
)
