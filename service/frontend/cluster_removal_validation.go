package frontend

import (
	"context"
	"fmt"
	"strings"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
)

const (
	listNamespacesPageSizeForClusterValidation  = 100
	maxNamespacesReportedInClusterValidationErr = 5
)

// validateClusterNotInUseByNamespaces returns an error if any global namespace
// still references clusterName in its replication cluster list. Local
// (non-global) namespaces are ignored because they do not replicate.
func validateClusterNotInUseByNamespaces(
	ctx context.Context,
	metadataMgr persistence.MetadataManager,
	clusterName string,
) error {
	var offending []string
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
					offending = append(offending, ns.Namespace.GetInfo().GetName())
					break
				}
			}
		}
		if len(resp.NextPageToken) == 0 {
			break
		}
		nextPageToken = resp.NextPageToken
	}
	if len(offending) == 0 {
		return nil
	}
	return serviceerror.NewFailedPrecondition(formatClusterInUseError(clusterName, offending))
}

func formatClusterInUseError(clusterName string, offending []string) string {
	shown := offending
	suffix := ""
	if len(shown) > maxNamespacesReportedInClusterValidationErr {
		suffix = fmt.Sprintf(" (and %d more)", len(shown)-maxNamespacesReportedInClusterValidationErr)
		shown = shown[:maxNamespacesReportedInClusterValidationErr]
	}
	return fmt.Sprintf(
		"cannot remove cluster %q: still referenced by namespace(s): %s%s",
		clusterName,
		strings.Join(shown, ", "),
		suffix,
	)
}
