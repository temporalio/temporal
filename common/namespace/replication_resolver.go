package namespace

import (
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

type ReplicationResolver interface {
	ActiveClusterName(entityId string) string
	ClusterNames(entityId string) []string
	WorkflowReplicationState(entityId string) enumspb.ReplicationState
	NamespaceReplicationState() enumspb.ReplicationState
	IsGlobalNamespace() bool
	FailoverVersion() int64
	FailoverNotificationVersion() int64
	// GetReplicationConfig returns the replication config for cloning purposes
	GetReplicationConfig() *persistencespb.NamespaceReplicationConfig
}

type ReplicationResolverFactory func(*persistencespb.NamespaceDetail) ReplicationResolver
type defaultReplicationResolver struct {
	replicationConfig           *persistencespb.NamespaceReplicationConfig
	isGlobalNamespace           bool
	failoverVersion             int64
	failoverNotificationVersion int64
}

func NewDefaultReplicationResolverFactory() ReplicationResolverFactory {
	return func(detail *persistencespb.NamespaceDetail) ReplicationResolver {
		// By convention, a namespace with non-zero failover version is a global namespace
		// This can be overridden by WithGlobalFlag mutation if needed
		isGlobal := detail.FailoverVersion != 0
		return &defaultReplicationResolver{
			replicationConfig:           detail.ReplicationConfig,
			isGlobalNamespace:           isGlobal,
			failoverVersion:             detail.FailoverVersion,
			failoverNotificationVersion: detail.FailoverNotificationVersion,
		}
	}
}

func (r *defaultReplicationResolver) ActiveClusterName(entityId string) string {
	if r.replicationConfig == nil {
		return ""
	}
	return r.replicationConfig.ActiveClusterName
}

func (r *defaultReplicationResolver) ClusterNames(entityId string) []string {
	if r.replicationConfig == nil {
		return nil
	}
	// copy slice to preserve immutability
	out := make([]string, len(r.replicationConfig.Clusters))
	copy(out, r.replicationConfig.Clusters)
	return out
}

func (r *defaultReplicationResolver) WorkflowReplicationState(entityId string) enumspb.ReplicationState {
	if r.replicationConfig == nil {
		return enumspb.REPLICATION_STATE_UNSPECIFIED
	}
	return r.replicationConfig.State
}

func (r *defaultReplicationResolver) NamespaceReplicationState() enumspb.ReplicationState {
	if r.replicationConfig == nil {
		return enumspb.REPLICATION_STATE_UNSPECIFIED
	}
	return r.replicationConfig.State
}

func (r *defaultReplicationResolver) IsGlobalNamespace() bool {
	return r.isGlobalNamespace
}

func (r *defaultReplicationResolver) FailoverVersion() int64 {
	return r.failoverVersion
}

func (r *defaultReplicationResolver) FailoverNotificationVersion() int64 {
	return r.failoverNotificationVersion
}

func (r *defaultReplicationResolver) GetReplicationConfig() *persistencespb.NamespaceReplicationConfig {
	return r.replicationConfig
}
