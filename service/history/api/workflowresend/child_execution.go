package workflowresend

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// ResolveCurrentChildExecutionOnSource resolves a retained successor of the child first run.
// Callers still verify locally; source existence is not proof that replication has converged.
func ResolveCurrentChildExecutionOnSource(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	child *commonpb.WorkflowExecution,
) (*commonpb.WorkflowExecution, error) {
	registry := shardContext.GetNamespaceRegistry()
	entry, err := registry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}
	currentCluster := shardContext.GetClusterMetadata().GetCurrentClusterName()
	routingKey := namespace.RoutingKey{ID: child.GetWorkflowId()}
	sourceCluster := entry.ActiveClusterName(routingKey)
	if !entry.IsOnCluster(currentCluster) || sourceCluster == currentCluster {
		return nil, consts.ErrWorkflowNotReady
	}
	remoteClient, err := shardContext.GetRemoteAdminClient(sourceCluster)
	if err != nil {
		return nil, err
	}
	// Omitting RunId resolves the source's current execution, not the missing first
	// run. Its first-run identity must match before it can be used for child recovery.
	resp, describeErr := remoteClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace:       entry.Name().String(),
		Execution:       &commonpb.WorkflowExecution{WorkflowId: child.GetWorkflowId()},
		Archetype:       chasm.WorkflowArchetype,
		SkipForceReload: true,
	})
	entry, err = registry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}
	if !entry.IsOnCluster(currentCluster) || entry.ActiveClusterName(routingKey) != sourceCluster {
		return nil, consts.ErrWorkflowNotReady
	}
	if describeErr != nil {
		return nil, describeErr
	}
	state := resp.GetDatabaseMutableState()
	firstRunID := state.GetExecutionState().GetFirstExecutionRunId()
	if firstRunID == "" {
		firstRunID = state.GetExecutionInfo().GetFirstExecutionRunId()
	}
	if firstRunID != child.GetRunId() {
		// Workflow ID reuse does not establish that this cell received the previous
		// chain's completion. Keep verification pending rather than accepting that run.
		return nil, consts.ErrWorkflowNotReady
	}
	runID := state.GetExecutionState().GetRunId()
	if runID == "" {
		return nil, serviceerror.NewInternal("source child mutable state has no run ID")
	}
	return &commonpb.WorkflowExecution{WorkflowId: child.GetWorkflowId(), RunId: runID}, nil
}
