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
	// NamespaceRow represents a row in namespace table
	NamespaceRow struct {
		ID                  primitives.UUID
		Name                string
		Data                []byte
		DataEncoding        string
		IsGlobal            bool
		NotificationVersion int64
	}

	// NamespaceFilter contains the column names within namespace table that
	// can be used to filter results through a WHERE clause. When ID is not
	// nil, it will be used for WHERE condition. If ID is nil and Name is non-nil,
	// Name will be used for WHERE condition. When both ID and Name are nil,
	// no WHERE clause will be used
	NamespaceFilter struct {
		ID            *primitives.UUID
		Name          *string
		GreaterThanID *primitives.UUID
		PageSize      *int
	}

	// NamespaceMetadataRow represents a row in namespace_metadata table
	NamespaceMetadataRow struct {
		NotificationVersion int64
	}

	// Namespace is the SQL persistence interface for namespaces
	Namespace interface {
		InsertIntoNamespace(ctx context.Context, rows *NamespaceRow) (sql.Result, error)
		UpdateNamespace(ctx context.Context, row *NamespaceRow) (sql.Result, error)
		// SelectFromNamespace returns namespaces that match filter criteria. Either ID or
		// Name can be specified to filter results. If both are not specified, all rows
		// will be returned
		SelectFromNamespace(ctx context.Context, filter NamespaceFilter) ([]NamespaceRow, error)
		// DeleteNamespace deletes a single row. One of ID or Name MUST be specified
		DeleteFromNamespace(ctx context.Context, filter NamespaceFilter) (sql.Result, error)

		LockNamespaceMetadata(ctx context.Context) (*NamespaceMetadataRow, error)
		UpdateNamespaceMetadata(ctx context.Context, row *NamespaceMetadataRow) (sql.Result, error)
		SelectFromNamespaceMetadata(ctx context.Context) (*NamespaceMetadataRow, error)
	}
)
