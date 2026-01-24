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
		isGlobal := detail.GetFailoverVersion() != 0
		return &defaultReplicationResolver{
			replicationConfig:           detail.GetReplicationConfig(),
			isGlobalNamespace:           isGlobal,
			failoverVersion:             detail.GetFailoverVersion(),
			failoverNotificationVersion: detail.GetFailoverNotificationVersion(),
		}
	}
}

func (r *defaultReplicationResolver) ActiveClusterName(businessID string) string {
	if r.replicationConfig == nil {
		return ""
	}
	return r.replicationConfig.GetActiveClusterName()
}

func (r *defaultReplicationResolver) ClusterNames(businessID string) []string {
	if r.replicationConfig == nil {
		return nil
	}
	// copy slice to preserve immutability
	out := make([]string, len(r.replicationConfig.GetClusters()))
	copy(out, r.replicationConfig.GetClusters())
	return out
}

func (r *defaultReplicationResolver) ReplicationState() enumspb.ReplicationState {
	if r.replicationConfig == nil {
		return enumspb.REPLICATION_STATE_UNSPECIFIED
	}
	return r.replicationConfig.GetState()
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
		r.replicationConfig.SetActiveClusterName(clusterName)
	}
}

func (r *defaultReplicationResolver) PretendLocalNamespace(localClusterName string) {
	r.isGlobalNamespace = false
	r.replicationConfig = persistencespb.NamespaceReplicationConfig_builder{
		ActiveClusterName: localClusterName,
		Clusters:          []string{localClusterName},
	}.Build()
	r.failoverVersion = 0
}

func (r *defaultReplicationResolver) Clone() ReplicationResolver {
	var clonedConfig *persistencespb.NamespaceReplicationConfig
	if r.replicationConfig != nil {
		clonedConfig = persistencespb.NamespaceReplicationConfig_builder{
			ActiveClusterName: r.replicationConfig.GetActiveClusterName(),
			State:             r.replicationConfig.GetState(),
		}.Build()
		// Deep copy the clusters slice
		if r.replicationConfig.GetClusters() != nil {
			clonedConfig.SetClusters(make([]string, len(r.replicationConfig.GetClusters())))
			copy(clonedConfig.GetClusters(), r.replicationConfig.GetClusters())
		}
	}
	return &defaultReplicationResolver{
		replicationConfig:           clonedConfig,
		isGlobalNamespace:           r.isGlobalNamespace,
		failoverVersion:             r.failoverVersion,
		failoverNotificationVersion: r.failoverNotificationVersion,
	}
}
