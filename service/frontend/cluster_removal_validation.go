package frontend

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
)

// validateClusterNotInUseByNamespaces guards RemoveRemoteCluster, which runs on
// currentClusterName and deletes clusterName from that cluster's cluster-metadata
// registry (i.e. "disconnect currentClusterName from clusterName"). It returns an
// error only when a single active multi-cluster namespace lists BOTH
// currentClusterName and clusterName in its replication cluster set, because that
// is the only situation where the removal would sever a replication link the
// current cluster is actually relying on and leave the namespace pointing at a
// cluster the registry no longer knows about.
//
// Both membership checks are required, and neither alone is sufficient:
//
//   - Requiring clusterName membership alone (the original check) over-blocks on
//     stale/orphaned records. The local registry can hold copies of global
//     namespaces that currentClusterName was once a member of but is no longer —
//     e.g. after a namespace migration re-homes a namespace onto a different
//     cluster, the replicated update that drops currentClusterName from the
//     namespace's cluster set is applied to the local record but the record
//     itself is never deleted. Such an orphan still lists clusterName, yet
//     removing clusterName cannot strand any replication currentClusterName
//     performs, because currentClusterName does not participate in that namespace.
//
//   - Requiring currentClusterName membership alone over-blocks on unrelated
//     namespaces. currentClusterName may participate in many multi-cluster
//     namespaces that have nothing to do with clusterName; removing clusterName
//     only endangers the ones that clusterName also belongs to.
//
// The dangerous case is the conjunction within one namespace, hence the &&.
//
// Namespaces are skipped when: they are single-cluster (local, or global with
// only one cluster) and therefore do not replicate; or they are in
// NAMESPACE_STATE_DELETED, because the delete worker is tearing them down and
// cluster connectivity is not required for that teardown.
//
// The check reads from the in-memory namespace registry, which may lag
// persistence by up to the configured refresh interval.
func validateClusterNotInUseByNamespaces(
	namespaceRegistry namespace.Registry,
	currentClusterName string,
	clusterName string,
) error {
	for _, ns := range namespaceRegistry.GetAllNamespaces() {
		if ns.ReplicationPolicy() != namespace.ReplicationPolicyMultiCluster {
			continue
		}
		if ns.State() == enumspb.NAMESPACE_STATE_DELETED {
			continue
		}
		// Only block when the current cluster and the cluster being removed both
		// participate in this namespace (see the doc comment for why both terms
		// are required). This ignores orphaned records the current cluster is no
		// longer a member of, and namespaces that do not involve clusterName.
		if ns.IsOnCluster(currentClusterName) && ns.IsOnCluster(clusterName) {
			return serviceerror.NewFailedPrecondition(fmt.Sprintf(
				"cannot remove cluster %q: still referenced by namespace %q",
				clusterName, ns.Name().String(),
			))
		}
	}
	return nil
}
