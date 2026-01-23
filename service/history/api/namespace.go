package api

import (
	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func GetActiveNamespace(
	shard historyi.ShardContext,
	namespaceUUID namespace.ID,
	businessID string,
) (*namespace.Namespace, error) {

	err := ValidateNamespaceUUID(namespaceUUID)
	if err != nil {
		return nil, err
	}

	namespaceEntry, err := shard.GetNamespaceRegistry().GetNamespaceByID(namespaceUUID)
	if err != nil {
		return nil, err
	}

	activeCluster := namespaceEntry.ActiveClusterName(businessID)
	currentCluster := shard.GetClusterMetadata().GetCurrentClusterName()

	shard.GetLogger().Info("History GetActiveNamespace check",
		tag.WorkflowNamespace(namespaceEntry.Name().String()),
		tag.NewStringTag("businessID", businessID),
		tag.NewStringTag("activeCluster", activeCluster),
		tag.NewStringTag("currentCluster", currentCluster),
		tag.NewBoolTag("isGlobalNamespace", namespaceEntry.IsGlobalNamespace()))

	if activeCluster != currentCluster {
		shard.GetLogger().Info("History GetActiveNamespace: namespace not active in current cluster",
			tag.WorkflowNamespace(namespaceEntry.Name().String()),
			tag.NewStringTag("businessID", businessID),
			tag.NewStringTag("activeCluster", activeCluster),
			tag.NewStringTag("currentCluster", currentCluster))
		return nil, serviceerror.NewNamespaceNotActive(
			namespaceEntry.Name().String(),
			currentCluster,
			activeCluster)
	}
	return namespaceEntry, nil
}

func GetNamespace(
	shard historyi.ShardContext,
	namespaceUUID namespace.ID,
) (*namespace.Namespace, error) {

	err := ValidateNamespaceUUID(namespaceUUID)
	if err != nil {
		return nil, err
	}

	namespaceEntry, err := shard.GetNamespaceRegistry().GetNamespaceByID(namespaceUUID)
	if err != nil {
		return nil, err
	}

	return namespaceEntry, nil
}

func ValidateNamespaceUUID(
	namespaceUUID namespace.ID,
) error {
	if namespaceUUID == "" {
		return serviceerror.NewInvalidArgument("Missing namespace UUID.")
	} else if uuid.Validate(namespaceUUID.String()) != nil {
		return serviceerror.NewInvalidArgument("Invalid namespace UUID.")
	}
	return nil
}
