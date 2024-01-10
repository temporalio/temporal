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
)

const (
	IncomingTableType = iota
	OutgoingTableType
)

type (
	NexusIncomingServicesRow struct {
		ServiceID    []byte
		Version      int64
		Data         []byte
		DataEncoding string
	}

	ListNexusIncomingServicesRequest struct {
		LastKnownTableVersion int64
		LastServiceID         string
		Limit                 int
	}

	ListNexusIncomingServicesResponse struct {
		CurrentTableVersion int64
		Entries             []NexusIncomingServicesEntry
	}

	NexusIncomingServicesEntry struct {
		ServiceID string
		VersionedBlob
	}

	// NexusServicesTableVersions is the SQL persistence interface for creating and updating rows
	// containing the versions of other Nexus service tables
	NexusServicesTableVersions interface {
		InitializeNexusTableVersion(ctx context.Context, tableType int) error
		IncrementNexusTableVersion(ctx context.Context, tableType int, lastKnownTableVersion int64) error
	}

	// NexusIncomingServices is the SQL persistence interface for incoming Nexus services
	NexusIncomingServices interface {
		InsertIntoNexusIncomingServices(ctx context.Context, row *NexusIncomingServicesRow) error
		UpdateNexusIncomingService(ctx context.Context, row *NexusIncomingServicesRow) error
		ListNexusIncomingServices(ctx context.Context, request *ListNexusIncomingServicesRequest) (*ListNexusIncomingServicesResponse, error)
		DeleteFromNexusIncomingServices(ctx context.Context, serviceID []byte) (sql.Result, error)
	}
)
