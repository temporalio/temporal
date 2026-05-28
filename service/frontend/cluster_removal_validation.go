package frontend

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
)

// validateClusterNotInUseByNamespaces returns an error if any active
// multi-cluster namespace still references clusterName in its replication
// cluster list. Single-cluster namespaces (local, or global with only one
// cluster) are ignored because they do not replicate. Namespaces in
// NAMESPACE_STATE_DELETED are ignored because the delete worker is tearing
// them down and cluster connectivity is not required for that teardown. The
// check reads from the in-memory namespace registry, which may lag persistence
// by up to the configured refresh interval.
func validateClusterNotInUseByNamespaces(
	namespaceRegistry namespace.Registry,
	clusterName string,
) error {
	for _, ns := range namespaceRegistry.GetAllNamespaces() {
		if ns.ReplicationPolicy() != namespace.ReplicationPolicyMultiCluster {
			continue
		}
		if ns.State() == enumspb.NAMESPACE_STATE_DELETED {
			continue
		}
		if ns.IsOnCluster(clusterName) {
			return serviceerror.NewFailedPrecondition(fmt.Sprintf(
				"cannot remove cluster %q: still referenced by namespace %q",
				clusterName, ns.Name().String(),
			))
		}
	}
	return nil
}
