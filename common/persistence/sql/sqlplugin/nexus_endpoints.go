// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
)

type (
	NexusEndpointsRow struct {
		ID           []byte
		Version      int64
		Data         []byte
		DataEncoding string
	}

	ListNexusEndpointsRequest struct {
		LastID []byte
		Limit  int
	}

	// NexusEndpoints is the SQL persistence interface for Nexus endpoints
	NexusEndpoints interface {
		InitializeNexusEndpointsTableVersion(ctx context.Context) (sql.Result, error)
		IncrementNexusEndpointsTableVersion(ctx context.Context, lastKnownTableVersion int64) (sql.Result, error)
		GetNexusEndpointsTableVersion(ctx context.Context) (int64, error)

		InsertIntoNexusEndpoints(ctx context.Context, row *NexusEndpointsRow) (sql.Result, error)
		UpdateNexusEndpoint(ctx context.Context, row *NexusEndpointsRow) (sql.Result, error)
		GetNexusEndpointByID(ctx context.Context, serviceID []byte) (*NexusEndpointsRow, error)
		ListNexusEndpoints(ctx context.Context, request *ListNexusEndpointsRequest) ([]NexusEndpointsRow, error)
		DeleteFromNexusEndpoints(ctx context.Context, id []byte) (sql.Result, error)
	}
)
