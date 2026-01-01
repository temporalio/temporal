package namespace

import (
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

type ReplicationResolver interface {
	ActiveClusterName(businessID string) string
	ClusterNames(businessID string) []string
	ReplicationState() enumspb.ReplicationState
	IsGlobalNamespace() bool
	FailoverVersion(businessID string) int64
	FailoverNotificationVersion() int64

	// Mutation methods for modifying resolver state
	SetGlobalFlag(isGlobal bool)
	SetActiveCluster(clusterName string)
	PretendLocalNamespace(localClusterName string)

	// Clone returns a deep copy of the resolver
	Clone() ReplicationResolver
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

func (r *defaultReplicationResolver) ActiveClusterName(businessID string) string {
	if r.replicationConfig == nil {
		return ""
	}
	return r.replicationConfig.ActiveClusterName
}

func (r *defaultReplicationResolver) ClusterNames(businessID string) []string {
	if r.replicationConfig == nil {
		return nil
	}
	// copy slice to preserve immutability
	out := make([]string, len(r.replicationConfig.Clusters))
	copy(out, r.replicationConfig.Clusters)
	return out
}

func (r *defaultReplicationResolver) ReplicationState() enumspb.ReplicationState {
	if r.replicationConfig == nil {
		return enumspb.REPLICATION_STATE_UNSPECIFIED
	}
	return r.replicationConfig.State
}

func (r *defaultReplicationResolver) IsGlobalNamespace() bool {
	return r.isGlobalNamespace
}

func (r *defaultReplicationResolver) FailoverVersion(businessID string) int64 {
	return r.failoverVersion
}

func (r *defaultReplicationResolver) FailoverNotificationVersion() int64 {
	return r.failoverNotificationVersion
}

func (r *defaultReplicationResolver) SetGlobalFlag(isGlobal bool) {
	r.isGlobalNamespace = isGlobal
}

func (r *defaultReplicationResolver) SetActiveCluster(clusterName string) {
	if r.replicationConfig != nil {
		r.replicationConfig.ActiveClusterName = clusterName
	}
}

func (r *defaultReplicationResolver) PretendLocalNamespace(localClusterName string) {
	r.isGlobalNamespace = false
	r.replicationConfig = &persistencespb.NamespaceReplicationConfig{
		ActiveClusterName: localClusterName,
		Clusters:          []string{localClusterName},
	}
	r.failoverVersion = 0
}

func (r *defaultReplicationResolver) Clone() ReplicationResolver {
	var clonedConfig *persistencespb.NamespaceReplicationConfig
	if r.replicationConfig != nil {
		clonedConfig = &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: r.replicationConfig.ActiveClusterName,
			State:             r.replicationConfig.State,
		}
		// Deep copy the clusters slice
		if r.replicationConfig.Clusters != nil {
			clonedConfig.Clusters = make([]string, len(r.replicationConfig.Clusters))
			copy(clonedConfig.Clusters, r.replicationConfig.Clusters)
		}
	}
	return &defaultReplicationResolver{
		replicationConfig:           clonedConfig,
		isGlobalNamespace:           r.isGlobalNamespace,
		failoverVersion:             r.failoverVersion,
		failoverNotificationVersion: r.failoverNotificationVersion,
	}
}
