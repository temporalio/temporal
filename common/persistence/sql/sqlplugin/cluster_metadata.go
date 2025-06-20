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
