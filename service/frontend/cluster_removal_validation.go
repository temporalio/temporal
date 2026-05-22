package frontend

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
)

const (
	listNamespacesPageSizeForClusterValidation = 100
)

// validateClusterNotInUseByNamespaces returns an error if any global namespace
// still references clusterName in its replication cluster list. Local
// (non-global) namespaces are ignored because they do not replicate. The scan
// stops at the first offending namespace.
func validateClusterNotInUseByNamespaces(
	ctx context.Context,
	metadataMgr persistence.MetadataManager,
	clusterName string,
) error {
	var nextPageToken []byte
	for {
		resp, err := metadataMgr.ListNamespaces(ctx, &persistence.ListNamespacesRequest{
			PageSize:      listNamespacesPageSizeForClusterValidation,
			NextPageToken: nextPageToken,
		})
		if err != nil {
			return serviceerror.NewUnavailablef("unable to list namespaces while validating cluster removal: %v", err)
		}
		for _, ns := range resp.Namespaces {
			if !ns.IsGlobalNamespace {
				continue
			}
			for _, c := range ns.Namespace.GetReplicationConfig().GetClusters() {
				if c == clusterName {
					return serviceerror.NewFailedPrecondition(fmt.Sprintf(
						"cannot remove cluster %q: still referenced by namespace %q",
						clusterName, ns.Namespace.GetInfo().GetName(),
					))
				}
			}
		}
		if len(resp.NextPageToken) == 0 {
			return nil
		}
		nextPageToken = resp.NextPageToken
	}
}
