package api

import (
	"context"
	"time"

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

	shardContext.GetLogger().Error("REACTIVATION DEBUG: ReactivateVersionWorkflowIfPinned called",
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

	shardContext.GetLogger().Error("REACTIVATION DEBUG: Attempting to send signal",
		tag.WorkflowNamespaceID(namespaceID.String()),
		tag.WorkflowID(workflowID),
		tag.NewStringTag("deployment_name", pinnedVersion.GetDeploymentName()),
		tag.NewStringTag("build_id", pinnedVersion.GetBuildId()),
		tag.NewStringTag("signal_time", time.Now().Format("15:04:05.000")),
		tag.NewStringTag("namespace_name", nsEntry.Name().String()),
		tag.NewStringTag("request_namespace_id", signalRequest.NamespaceId))

	// Get the engine and send signal synchronously - fire and forget
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		shardContext.GetLogger().Warn("Failed to get engine for version workflow signal",
			tag.WorkflowNamespaceID(namespaceID.String()),
			tag.WorkflowID(workflowID),
			tag.Error(err))
		return
	}

	// Try to signal with retry in case workflow is doing ContinueAsNew
	maxRetries := 3
	var signalErr error
	for i := 0; i < maxRetries; i++ {
		_, signalErr = engine.SignalWorkflowExecution(ctx, signalRequest)
		if signalErr == nil {
			shardContext.GetLogger().Error("REACTIVATION DEBUG: Successfully sent signal",
				tag.WorkflowNamespace(nsEntry.Name().String()),
				tag.WorkflowID(workflowID),
				tag.NewInt("retry_attempt", i))
			return
		}

		// Check if it's a "workflow not found" error
		if signalErr.Error() == "workflow not found for ID: "+workflowID {
			if i < maxRetries-1 {
				// Wait a bit in case workflow is doing ContinueAsNew
				time.Sleep(100 * time.Millisecond)
				shardContext.GetLogger().Error("REACTIVATION DEBUG: Retrying signal",
					tag.WorkflowID(workflowID),
					tag.NewInt("retry_attempt", i+1))
				continue
			}
		}
		// For other errors, don't retry
		break
	}

	// If we get here, all retries failed
	shardContext.GetLogger().Error("REACTIVATION DEBUG: Failed to signal version workflow after retries",
		tag.WorkflowNamespace(nsEntry.Name().String()),
		tag.WorkflowID(workflowID),
		tag.NewStringTag("full_workflow_id", signalRequest.SignalRequest.WorkflowExecution.WorkflowId),
		tag.NewStringTag("deployment_name", pinnedVersion.GetDeploymentName()),
		tag.NewStringTag("build_id", pinnedVersion.GetBuildId()),
		tag.Error(signalErr))
}
