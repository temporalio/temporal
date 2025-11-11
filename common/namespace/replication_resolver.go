package namespace

import (
	enumspb "go.temporal.io/api/enums/v1"
)

type ReplicationResolver interface {
	ActiveClusterName(ns *Namespace, workflowID string) string
	ClusterNames(ns *Namespace, workflowID string) []string
	WorkflowReplicationState(ns *Namespace, workflowID string) enumspb.ReplicationState
	NamespaceReplicationState(ns *Namespace) enumspb.ReplicationState
}
type defaultReplicationResolver struct{}

var (
	DefaultReplicationResolver ReplicationResolver = &defaultReplicationResolver{}
)

func NewDefaultReplicationResolver() ReplicationResolver {
	return DefaultReplicationResolver
}

func (r *defaultReplicationResolver) ActiveClusterName(ns *Namespace, workflowID string) string {
	if ns.replicationConfig == nil {
		return ""
	}
	return ns.replicationConfig.ActiveClusterName
}

func (r *defaultReplicationResolver) ClusterNames(ns *Namespace, workflowID string) []string {
	// copy slice to preserve immutability
	out := make([]string, len(ns.replicationConfig.Clusters))
	copy(out, ns.replicationConfig.Clusters)
	return out
}

func (r *defaultReplicationResolver) WorkflowReplicationState(ns *Namespace, workflowID string) enumspb.ReplicationState {
	if ns.replicationConfig == nil {
		return enumspb.REPLICATION_STATE_UNSPECIFIED
	}
	return ns.replicationConfig.State
}

func (r *defaultReplicationResolver) NamespaceReplicationState(ns *Namespace) enumspb.ReplicationState {
	if ns.replicationConfig == nil {
		return enumspb.REPLICATION_STATE_UNSPECIFIED
	}
	return ns.replicationConfig.State
}
