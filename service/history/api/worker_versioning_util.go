package api

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/worker_versioning"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// ReactivateVersionSignalName is the signal name for reactivating a version workflow.
// This duplicates the constant from workerdeployment package to avoid import cycles.
const ReactivateVersionSignalName = "reactivate-version"

// ReactivateVersionWorkflowIfPinned sends a reactivation signal to the version workflow
// when workflows are pinned to a potentially DRAINED/INACTIVE version.
// This is a fire-and-forget operation - errors are logged but don't fail the operation.
// TODO: Add caching to prevent duplicate signals within a time window
func ReactivateVersionWorkflowIfPinned(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	override *workflowpb.VersioningOverride,
) {
	// Only process if we're pinning to a specific version
	if !worker_versioning.OverrideIsPinned(override) {
		return
	}

	pinnedVersion := worker_versioning.GetOverridePinnedVersion(override)
	if pinnedVersion == nil {
		return
	}

	shardContext.GetLogger().Debug("Sending reactivation signal to version workflow",
		tag.WorkflowNamespaceID(namespaceID.String()),
		tag.NewStringTag("deployment", pinnedVersion.GetDeploymentName()),
		tag.NewStringTag("build_id", pinnedVersion.GetBuildId()))

	// Get namespace entry from registry
	nsEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		shardContext.GetLogger().Warn("Failed to get namespace for version workflow signal",
			tag.WorkflowNamespaceID(namespaceID.String()),
			tag.Error(err))
		return
	}

	// Generate the workflow ID for the version workflow
	workflowID := worker_versioning.GenerateVersionWorkflowID(pinnedVersion.GetDeploymentName(), pinnedVersion.GetBuildId())

	// Build the signal request for the version workflow
	// Version workflows run in the same namespace as user workflows
	signalRequest := &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: nsEntry.ID().String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace: nsEntry.Name().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
			},
			SignalName: ReactivateVersionSignalName,
			Input:      nil, // No payload needed
			Identity:   "system-versioning-override-handler",
		},
	}

	// Use history client to route signal to the correct shard (version workflow may be on a different shard)
	historyClient := shardContext.GetHistoryClient()

	_, err = historyClient.SignalWorkflowExecution(ctx, signalRequest)
	if err != nil {
		shardContext.GetLogger().Warn("Failed to signal version workflow for reactivation",
			tag.WorkflowNamespace(nsEntry.Name().String()),
			tag.WorkflowID(workflowID),
			tag.Error(err))
		return
	}
}
