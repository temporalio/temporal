package frontend

import (
	"context"

	"go.temporal.io/server/common/persistence"
)

func resetNamespaceReplicationAckForFreshConnect(
	ctx context.Context,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	persistedBefore *persistence.GetClusterMetadataResponse,
	clusterName string,
	enabled bool,
) error {
	if namespaceReplicationQueue == nil || !enabled {
		return nil
	}
	if persistedBefore != nil && persistedBefore.GetIsConnectionEnabled() {
		return nil
	}
	return namespaceReplicationQueue.DeleteAckLevel(ctx, clusterName)
}
