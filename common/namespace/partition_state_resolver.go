package namespace

import (
	enumspb "go.temporal.io/api/enums/v1"
)

// PartitionStateResolver provides partition-aware replication state resolution for workflows.
// This interface encapsulates all access to namespace replication configuration fields
// (ActiveClusterName, Clusters, State) and provides an abstraction layer that will support
// future partitioning.
//
// The resolver takes namespaceID and workflowID as input to make partition-aware decisions.
// The workflowID is used as the partition key to determine which partition the workflow
// belongs to, enabling partition-level replication state management.
//
// Current implementation operates at the namespace level and ignores workflowID, but the
// interface is designed to allow partition-level replication decisions in the future:
//   - During partition rebalancing, some partitions may be in HANDOVER while others are ACTIVE
//   - Different partitions may have different active clusters during migration
//   - Task routing will be partition-aware based on workflowID
//
// This interface is composed into the Registry interface, so callers typically access these
// methods through their Registry instance rather than creating a separate resolver.
//
// Usage via Registry:
//   isActive, err := registry.IsActiveInCluster(namespaceID, workflowID, currentCluster)
//
// IMPORTANT: External packages should use these methods via Registry and NOT access Namespace
// replication fields (ActiveClusterName, Clusters, State) directly.
type PartitionStateResolver interface {
	// ===== Active Cluster Determination =====

	// IsActiveInCluster returns true if the workflow's partition is active in the specified cluster.
	// This is the primary method for determining whether the current cluster should process
	// write operations for a given workflow.
	//
	// Current behavior: Ignores workflowID and returns true if:
	//   - Namespace is not global (local namespaces are always "active"), OR
	//   - clusterName matches the namespace's ActiveClusterName
	//
	// Future behavior: Will use workflowID to determine the partition, then check if that
	// partition is active in clusterName.
	//
	// Parameters:
	//   - namespaceID: The namespace ID
	//   - workflowID: The workflow ID used as partition key (empty string for namespace-level checks)
	//   - clusterName: The cluster to check
	//
	// Returns error if namespace not found.
	IsActiveInCluster(namespaceID ID, workflowID string, clusterName string) (bool, error)

	// GetActiveClusterName returns the name of the cluster that is currently active for the
	// workflow's partition.
	//
	// Current behavior: Ignores workflowID and returns the namespace's ActiveClusterName field.
	//
	// Future behavior: Will use workflowID to determine the partition, then return that
	// partition's active cluster.
	//
	// Parameters:
	//   - namespaceID: The namespace ID
	//   - workflowID: The workflow ID used as partition key (empty string for namespace-level default)
	//
	// Returns error if namespace not found.
	//
	// Note: For API responses, metrics, and logging where a single cluster name is needed
	// for backward compatibility, pass empty string to get the namespace-level default.
	GetActiveClusterName(namespaceID ID, workflowID string) (string, error)

	// GetForwardingTarget returns the cluster name to which requests should be forwarded,
	// and whether forwarding is needed for a given workflow.
	//
	// Current behavior: Ignores workflowID and returns:
	//   - (ActiveClusterName, true) if namespace is global and not active in current cluster
	//   - ("", false) if namespace is local or already active
	//
	// Future behavior: Will use workflowID to determine the partition's active cluster,
	// then return that cluster if forwarding is needed.
	//
	// Parameters:
	//   - namespaceID: The namespace ID
	//   - workflowID: The workflow ID used as partition key
	//   - currentCluster: The current cluster evaluating whether to forward
	//
	// Returns error if namespace not found.
	GetForwardingTarget(namespaceID ID, workflowID string, currentCluster string) (targetCluster string, shouldForward bool, err error)

	// ===== Cluster Membership & Replication =====

	// ShouldReplicateToCluster returns true if replication tasks for the given workflow
	// should be sent to the specified cluster.
	//
	// Current behavior: Ignores workflowID and returns true if clusterName is in the Clusters list.
	//
	// Future behavior: Will use workflowID to determine the partition, then check if that
	// partition should replicate to targetCluster. During rebalancing, some partitions may
	// be moving between clusters and require special handling.
	//
	// Parameters:
	//   - namespaceID: The namespace ID
	//   - workflowID: The workflow ID used as partition key
	//   - targetCluster: The cluster to check
	//
	// Returns error if namespace not found.
	ShouldReplicateToCluster(namespaceID ID, workflowID string, targetCluster string) (bool, error)

	// GetReplicationTargets returns the list of cluster names to which the workflow's partition
	// should replicate.
	//
	// Current behavior: Ignores workflowID and returns a copy of the namespace's Clusters slice.
	//
	// Future behavior: Will use workflowID to determine the partition, then return that
	// partition's replication targets. During rebalancing, this may differ from the
	// namespace-level cluster list.
	//
	// Parameters:
	//   - namespaceID: The namespace ID
	//   - workflowID: The workflow ID used as partition key (empty string for namespace-level list)
	//
	// Returns error if namespace not found.
	GetReplicationTargets(namespaceID ID, workflowID string) ([]string, error)

	// GetReplicationPolicy returns the replication policy for the workflow's partition.
	//
	// Current behavior: Ignores workflowID and returns:
	//   - ReplicationPolicyMultiCluster if global namespace with len(Clusters) > 1
	//   - ReplicationPolicyOneCluster otherwise
	//
	// Future behavior: Will use workflowID to determine the partition's replication policy.
	//
	// Parameters:
	//   - namespaceID: The namespace ID
	//   - workflowID: The workflow ID used as partition key (empty string for namespace-level policy)
	//
	// Returns error if namespace not found.
	GetReplicationPolicy(namespaceID ID, workflowID string) (ReplicationPolicy, error)

	// ===== Handover State Management =====

	// IsInHandover returns true if the workflow's partition is currently in HANDOVER state.
	// During handover, most operations are blocked to ensure safe migration.
	//
	// Current behavior: Ignores workflowID and returns true if State == REPLICATION_STATE_HANDOVER.
	//
	// Future behavior: Will use workflowID to determine if that partition is in handover.
	// During partition rebalancing, only the partitions being moved will be in handover,
	// while other partitions remain active and accept traffic.
	//
	// Parameters:
	//   - namespaceID: The namespace ID
	//   - workflowID: The workflow ID used as partition key
	//
	// Returns error if namespace not found.
	IsInHandover(namespaceID ID, workflowID string) (bool, error)

	// CanProcessTasks returns true if the workflow's partition can currently process queue tasks
	// (transfer, timer, visibility, outbound). Returns false during handover for that partition.
	//
	// Current behavior: Ignores workflowID and returns !IsInHandover().
	//
	// Future behavior: Will check if the specific partition is in handover state.
	//
	// Parameters:
	//   - namespaceID: The namespace ID
	//   - workflowID: The workflow ID used as partition key
	//
	// Returns error if namespace not found.
	CanProcessTasks(namespaceID ID, workflowID string) (bool, error)

	// CanAcceptWrites returns true if the workflow's partition can accept write operations.
	// Returns false during handover to prevent split-brain scenarios.
	//
	// Current behavior: Ignores workflowID and returns !IsInHandover().
	//
	// Future behavior: Will check if the specific partition can accept writes.
	//
	// Parameters:
	//   - namespaceID: The namespace ID
	//   - workflowID: The workflow ID used as partition key
	//
	// Returns error if namespace not found.
	CanAcceptWrites(namespaceID ID, workflowID string) (bool, error)

	// GetReplicationState returns the replication state for the workflow's partition.
	//
	// Current behavior: Ignores workflowID and returns the namespace's State field.
	//
	// Future behavior: Will return the partition-specific replication state.
	//
	// Parameters:
	//   - namespaceID: The namespace ID
	//   - workflowID: The workflow ID used as partition key (empty string for namespace-level state)
	//
	// Returns error if namespace not found.
	//
	// Deprecated: Prefer IsInHandover/CanProcessTasks/CanAcceptWrites for decision-making.
	GetReplicationState(namespaceID ID, workflowID string) (enumspb.ReplicationState, error)
}
