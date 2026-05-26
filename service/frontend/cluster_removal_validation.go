package frontend

import (
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
)

// validateClusterNotInUseByNamespaces returns an error if any global namespace
// still references clusterName in its replication cluster list. Local
// (non-global) namespaces are ignored because they do not replicate. The check
// reads from the in-memory namespace registry, which may lag persistence by up
// to the configured refresh interval.
func validateClusterNotInUseByNamespaces(
	namespaceRegistry namespace.Registry,
	clusterName string,
) error {
	for _, ns := range namespaceRegistry.GetAllNamespaces() {
		if !ns.IsGlobalNamespace() {
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
