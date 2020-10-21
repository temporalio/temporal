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
	"time"

	"go.temporal.io/server/common/persistence"
)

type (
	// ClusterMetadataRow represents a row in the cluster_metadata table
	ClusterMetadataRow struct {
		Data         []byte
		DataEncoding string
		Version      int64
		// TODO(vitarb): immutable metadata is needed for backward compatibility only, remove after 1.1 release.
		ImmutableData         []byte
		ImmutableDataEncoding string
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
		MaxRecordsAffected int
	}

	// ClusterMetadata is the SQL persistence interface for cluster metadata
	ClusterMetadata interface {
		SaveClusterMetadata(row *ClusterMetadataRow) (sql.Result, error)
		GetClusterMetadata() (*ClusterMetadataRow, error)
		WriteLockGetClusterMetadata() (*ClusterMetadataRow, error)
		GetClusterMembers(filter *ClusterMembershipFilter) ([]ClusterMembershipRow, error)
		UpsertClusterMembership(row *ClusterMembershipRow) (sql.Result, error)
		PruneClusterMembership(filter *PruneClusterMembershipFilter) (sql.Result, error)
	}
)
