package deletenamespace

import (
	"context"
	stderrors "errors"
	"fmt"
	"slices"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/worker/deletenamespace/errors"
)

type (
	localActivities struct {
		metadataManager      persistence.MetadataManager
		clusterMetadata      cluster.Metadata
		nexusEndpointManager persistence.NexusEndpointManager
		logger               log.Logger

		protectedNamespaces                       dynamicconfig.TypedPropertyFn[[]string]
		allowDeleteNamespaceIfNexusEndpointTarget dynamicconfig.BoolPropertyFn
		nexusEndpointListDefaultPageSize          dynamicconfig.IntPropertyFn
	}

	getNamespaceInfoResult struct {
		NamespaceID    namespace.ID
		Namespace      namespace.Name
		Clusters       []string
		ActiveCluster  string
		CurrentCluster string
	}
)

func newLocalActivities(
	metadataManager persistence.MetadataManager,
	clusterMetadata cluster.Metadata,
	nexusEndpointManager persistence.NexusEndpointManager,
	logger log.Logger,
	protectedNamespaces dynamicconfig.TypedPropertyFn[[]string],
	allowDeleteNamespaceIfNexusEndpointTarget dynamicconfig.BoolPropertyFn,
	nexusEndpointListDefaultPageSize dynamicconfig.IntPropertyFn,
) *localActivities {
	return &localActivities{
		metadataManager:      metadataManager,
		clusterMetadata:      clusterMetadata,
		nexusEndpointManager: nexusEndpointManager,
		logger:               logger,
		protectedNamespaces:  protectedNamespaces,
		allowDeleteNamespaceIfNexusEndpointTarget: allowDeleteNamespaceIfNexusEndpointTarget,
		nexusEndpointListDefaultPageSize:          nexusEndpointListDefaultPageSize,
	}
}

func (a *localActivities) GetNamespaceInfoActivity(ctx context.Context, nsID namespace.ID, nsName namespace.Name) (getNamespaceInfoResult, error) {
	ctx = headers.SetCallerName(ctx, nsName.String())

	getNamespaceRequest := &persistence.GetNamespaceRequest{
		Name: nsName.String(),
		ID:   nsID.String(),
	}

	getNamespaceResponse, err := a.metadataManager.GetNamespace(ctx, getNamespaceRequest)
	if err != nil {
		var nsNotFoundErr *serviceerror.NamespaceNotFound
		if stderrors.As(err, &nsNotFoundErr) {
			ns := nsName.String()
			if ns == "" {
				ns = nsID.String()
			}
			return getNamespaceInfoResult{}, errors.NewInvalidArgument(fmt.Sprintf("namespace %s is not found", ns), err)
		}
		return getNamespaceInfoResult{}, err
	}

	if getNamespaceResponse.Namespace == nil || getNamespaceResponse.Namespace.Info == nil || getNamespaceResponse.Namespace.Info.Id == "" {
		return getNamespaceInfoResult{}, stderrors.New("namespace info is corrupted")
	}

	return getNamespaceInfoResult{
		NamespaceID:   namespace.ID(getNamespaceResponse.Namespace.Info.Id),
		Namespace:     namespace.Name(getNamespaceResponse.Namespace.Info.Name),
		Clusters:      getNamespaceResponse.Namespace.ReplicationConfig.Clusters,
		ActiveCluster: getNamespaceResponse.Namespace.ReplicationConfig.ActiveClusterName,
		// CurrentCluster is not technically a "namespace info", but since all cluster data is here,
		// it is convenient to have the current cluster name here too.
		CurrentCluster: a.clusterMetadata.GetCurrentClusterName(),
	}, nil
}

func (a *localActivities) ValidateProtectedNamespacesActivity(_ context.Context, nsName namespace.Name) error {
	if slices.Contains(a.protectedNamespaces(), nsName.String()) {
		return errors.NewFailedPrecondition(fmt.Sprintf("namespace %s is protected from deletion", nsName), nil)
	}
	return nil
}

func (a *localActivities) ValidateNexusEndpointsActivity(ctx context.Context, nsID namespace.ID, nsName namespace.Name) error {
	if a.allowDeleteNamespaceIfNexusEndpointTarget() {
		return nil
	}
	// Prevent deletion of a namespace that is targeted by a Nexus endpoint.
	var nextPageToken []byte
	for {
		resp, err := a.nexusEndpointManager.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{
			LastKnownTableVersion: 0,
			NextPageToken:         nextPageToken,
			PageSize:              a.nexusEndpointListDefaultPageSize(),
		})
		if err != nil {
			a.logger.Error("Unable to list Nexus endpoints from persistence.", tag.WorkflowNamespace(nsName.String()), tag.WorkflowNamespaceID(nsID.String()), tag.Error(err))
			return fmt.Errorf("unable to list Nexus endpoints for namespace %s: %w", nsName, err)
		}

		for _, entry := range resp.Entries {
			if endpointNsID := entry.GetEndpoint().GetSpec().GetTarget().GetWorker().GetNamespaceId(); endpointNsID == nsID.String() {
				return errors.NewFailedPrecondition(fmt.Sprintf("cannot delete a namespace that is a target of a Nexus endpoint %s", entry.GetEndpoint().GetSpec().GetName()), nil)
			}
		}
		nextPageToken = resp.NextPageToken
		if len(nextPageToken) == 0 {
			break
		}
	}
	return nil
}

func (a *localActivities) MarkNamespaceDeletedActivity(ctx context.Context, nsName namespace.Name) error {
	ctx = headers.SetCallerName(ctx, nsName.String())

	getNamespaceRequest := &persistence.GetNamespaceRequest{
		Name: nsName.String(),
	}

	metadata, err := a.metadataManager.GetMetadata(ctx)
	if err != nil {
		a.logger.Error("Unable to get cluster metadata.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
		return err
	}

	ns, err := a.metadataManager.GetNamespace(ctx, getNamespaceRequest)
	if err != nil {
		a.logger.Error("Unable to get namespace details.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
		return err
	}

	ns.Namespace.Info.State = enumspb.NAMESPACE_STATE_DELETED

	updateRequest := &persistence.UpdateNamespaceRequest{
		Namespace:           ns.Namespace,
		IsGlobalNamespace:   ns.IsGlobalNamespace,
		NotificationVersion: metadata.NotificationVersion,
	}

	err = a.metadataManager.UpdateNamespace(ctx, updateRequest)
	if err != nil {
		a.logger.Error("Unable to update namespace state to Deleted.", tag.WorkflowNamespace(nsName.String()), tag.Error(err))
		return err
	}
	return nil
}

func (a *localActivities) GenerateDeletedNamespaceNameActivity(ctx context.Context, nsID namespace.ID, nsName namespace.Name) (namespace.Name, error) {
	ctx = headers.SetCallerName(ctx, nsName.String())

	logger := log.With(a.logger,
		tag.WorkflowNamespace(nsName.String()),
		tag.WorkflowNamespaceID(nsID.String()))

	const initialSuffixLength = 5

	for suffixLength := initialSuffixLength; suffixLength < len(nsID.String()); suffixLength++ { // Just in case. 5 chars from ID should be good enough.
		suffix := fmt.Sprintf("-deleted-%s", nsID.String()[:suffixLength])
		if strings.HasSuffix(nsName.String(), suffix) {
			logger.Info("Namespace is already renamed for deletion")
			return nsName, nil
		}
		newName := fmt.Sprintf("%s%s", nsName, suffix)

		_, err := a.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{
			Name: newName,
		})
		switch err.(type) {
		case nil:
			logger.Warn("Regenerate namespace name due to collision.", tag.NewStringTag("wf-new-namespace", newName))
		case *serviceerror.NamespaceNotFound:
			logger.Info("Generated new name for deleted namespace.", tag.NewStringTag("wf-new-namespace", newName))
			return namespace.Name(newName), nil
		default:
			logger.Error("Unable to get namespace details.", tag.Error(err))
			return namespace.EmptyName, fmt.Errorf("unable to get namespace details: %w", err)
		}
	}
	// Should never get here because namespace ID is guaranteed to be unique.
	return namespace.EmptyName, fmt.Errorf("unable to generate new name for deleted namespace %s. ID %q is not unique", nsName, nsID)

}

func (a *localActivities) RenameNamespaceActivity(ctx context.Context, previousName namespace.Name, newName namespace.Name) error {
	if newName == previousName {
		return nil
	}

	ctx = headers.SetCallerName(ctx, previousName.String())

	renameNamespaceRequest := &persistence.RenameNamespaceRequest{
		PreviousName: previousName.String(),
		NewName:      newName.String(),
	}

	err := a.metadataManager.RenameNamespace(ctx, renameNamespaceRequest)
	if err != nil {
		a.logger.Error("Unable to rename namespace.", tag.WorkflowNamespace(previousName.String()), tag.Error(err))
		return err
	}

	a.logger.Info("Namespace renamed successfully.", tag.WorkflowNamespace(previousName.String()), tag.WorkflowNamespace(newName.String()))
	return nil
}
