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
}

type ReplicationResolverFactory func(*persistencespb.NamespaceDetail) ReplicationResolver
type defaultReplicationResolver struct {
	replicationConfig *persistencespb.NamespaceReplicationConfig
}

func NewDefaultReplicationResolverFactory() ReplicationResolverFactory {
	return func(detail *persistencespb.NamespaceDetail) ReplicationResolver {
		return &defaultReplicationResolver{
			replicationConfig: detail.ReplicationConfig,
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
