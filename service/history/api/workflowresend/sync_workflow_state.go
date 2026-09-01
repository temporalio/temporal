package workflowresend

import (
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// SyncWorkflowStateResult describes the outcome of pulling and applying workflow state.
type SyncWorkflowStateResult int

const (
	// Keep the zero value fail-safe for callers that do not initialize a result.
	SyncWorkflowStateResultSkipped SyncWorkflowStateResult = iota
	SyncWorkflowStateResultApplied
	SyncWorkflowStateResultSourceNotFound
)

// SyncWorkflowStateFromSource pulls workflow state from the namespace's active cluster and applies
// it locally if the namespace routing state remains unchanged for the duration of the RPC.
func SyncWorkflowStateFromSource(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	versionedTransition *persistencespb.VersionedTransition,
	versionHistories *historyspb.VersionHistories,
	onSourceResolved func(string),
) (SyncWorkflowStateResult, error) {
	clusterMetadata := shardContext.GetClusterMetadata()
	currentClusterName := clusterMetadata.GetCurrentClusterName()
	namespaceRegistry := shardContext.GetNamespaceRegistry()
	namespaceEntry, err := namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return SyncWorkflowStateResultSkipped, err
	}
	if !namespaceEntry.IsOnCluster(currentClusterName) {
		return SyncWorkflowStateResultSkipped, nil
	}

	routingKey := namespace.RoutingKey{ID: execution.GetWorkflowId()}
	activeClusterName := namespaceEntry.ActiveClusterName(routingKey)
	if activeClusterName == currentClusterName {
		return SyncWorkflowStateResultSkipped, nil
	}

	targetClusterInfo, ok := clusterMetadata.GetAllClusterInfo()[currentClusterName]
	if !ok {
		return SyncWorkflowStateResultSkipped, fmt.Errorf("current cluster %q is missing from cluster metadata", currentClusterName)
	}
	remoteAdminClient, err := shardContext.GetRemoteAdminClient(activeClusterName)
	if err != nil {
		return SyncWorkflowStateResultSkipped, err
	}
	if onSourceResolved != nil {
		onSourceResolved(activeClusterName)
	}

	resp, err := remoteAdminClient.SyncWorkflowState(ctx, &adminservice.SyncWorkflowStateRequest{
		NamespaceId:         namespaceID.String(),
		Execution:           execution,
		ArchetypeId:         chasm.WorkflowArchetypeID,
		VersionedTransition: versionedTransition,
		VersionHistories:    versionHistories,
		TargetClusterId:     int32(targetClusterInfo.InitialFailoverVersion),
	})
	if err != nil {
		if common.IsNotFoundError(err) {
			return SyncWorkflowStateResultSourceNotFound, nil
		}
		var failedPreconditionErr *serviceerror.FailedPrecondition
		if errors.As(err, &failedPreconditionErr) {
			return SyncWorkflowStateResultSkipped, nil
		}
		return SyncWorkflowStateResultSkipped, err
	}
	if resp == nil || resp.VersionedTransitionArtifact == nil {
		return SyncWorkflowStateResultSkipped, serviceerror.NewInternal("SyncWorkflowState returned an empty artifact")
	}

	namespaceEntry, err = namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		var namespaceNotFoundErr *serviceerror.NamespaceNotFound
		if errors.As(err, &namespaceNotFoundErr) {
			return SyncWorkflowStateResultSkipped, nil
		}
		return SyncWorkflowStateResultSkipped, err
	}
	if !namespaceEntry.IsOnCluster(currentClusterName) ||
		namespaceEntry.ActiveClusterName(routingKey) != activeClusterName {
		return SyncWorkflowStateResultSkipped, nil
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return SyncWorkflowStateResultSkipped, err
	}
	if err := engine.ReplicateVersionedTransition(
		ctx,
		chasm.WorkflowArchetypeID,
		resp.VersionedTransitionArtifact,
		activeClusterName,
	); err != nil && !errors.Is(err, consts.ErrDuplicate) {
		return SyncWorkflowStateResultSkipped, err
	}

	return SyncWorkflowStateResultApplied, nil
}
