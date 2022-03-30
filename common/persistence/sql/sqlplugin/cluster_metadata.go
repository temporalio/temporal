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

	"go.temporal.io/server/common/persistence"
)

type (
	// ClusterMetadataRow represents a row in the cluster_metadata table
	ClusterMetadataRow struct {
		ClusterName  string
		Data         []byte
		DataEncoding string
		Version      int64
	}

	// ClusterMembershipRow represents a row in the cluster_membership table
	ClusterMembershipRow struct {
		Role           persistence.ServiceType
		HostID         []byte
		RPCAddress     string
		RPCPort        uint16
		SessionStart   time.Time
		LastHeartbeat  time.Time
		RecordExpiry   time.Time
		InsertionOrder uint64
	}

	ClusterMetadataFilter struct {
		ClusterName string
		PageSize    *int
	}

	// ClusterMembershipFilter is used for GetClusterMembership queries
	ClusterMembershipFilter struct {
		RPCAddressEquals    string
		HostIDEquals        []byte
		HostIDGreaterThan   []byte
		RoleEquals          persistence.ServiceType
		LastHeartbeatAfter  time.Time
		RecordExpiryAfter   time.Time
		SessionStartedAfter time.Time
		MaxRecordCount      int
	}

	// PruneClusterMembershipFilter is used for PruneClusterMembership queries
	PruneClusterMembershipFilter struct {
		PruneRecordsBefore time.Time
	}

	// ClusterMetadata is the SQL persistence interface for cluster metadata
	ClusterMetadata interface {
		SaveClusterMetadata(ctx context.Context, row *ClusterMetadataRow) (sql.Result, error)
		GetClusterMetadata(ctx context.Context, filter *ClusterMetadataFilter) (*ClusterMetadataRow, error)
		ListClusterMetadata(ctx context.Context, filter *ClusterMetadataFilter) ([]ClusterMetadataRow, error)
		DeleteClusterMetadata(ctx context.Context, filter *ClusterMetadataFilter) (sql.Result, error)
		WriteLockGetClusterMetadata(ctx context.Context, filter *ClusterMetadataFilter) (*ClusterMetadataRow, error)
		GetClusterMembers(ctx context.Context, filter *ClusterMembershipFilter) ([]ClusterMembershipRow, error)
		UpsertClusterMembership(ctx context.Context, row *ClusterMembershipRow) (sql.Result, error)
		PruneClusterMembership(ctx context.Context, filter *PruneClusterMembershipFilter) (sql.Result, error)
	}
)
