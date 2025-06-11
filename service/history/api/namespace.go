package api

import (
	"github.com/pborman/uuid"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func GetActiveNamespace(
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
	if !namespaceEntry.ActiveInCluster(shard.GetClusterMetadata().GetCurrentClusterName()) {
		return nil, serviceerror.NewNamespaceNotActive(
			namespaceEntry.Name().String(),
			shard.GetClusterMetadata().GetCurrentClusterName(),
			namespaceEntry.ActiveClusterName())
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
	} else if uuid.Parse(namespaceUUID.String()) == nil {
		return serviceerror.NewInvalidArgument("Invalid namespace UUID.")
	}
	return nil
}
