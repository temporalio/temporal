package api

import (
	"context"

	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/worker_versioning"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// VersionReactivationSignalerFn is a function type for sending reactivation signals to version workflows.
// This abstraction allows the history API layer to use the deployment client without importing it directly,
// avoiding import cycles between history/api and worker/workerdeployment packages.
type VersionReactivationSignalerFn func(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName, buildID string,
) error

// ReactivateVersionWorkflowIfPinned sends a reactivation signal to the version workflow
// when workflows are pinned to a potentially DRAINED/INACTIVE version. It also deduplicates
// signals within the cache TTL window.
// This is a fire-and-forget operation - errors are logged but don't fail the calling operation.
func ReactivateVersionWorkflowIfPinned(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	override *workflowpb.VersioningOverride,
	signalCache worker_versioning.ReactivationSignalCache,
	signaler VersionReactivationSignalerFn,
	enabled bool,
) {
	// Check if signals are enabled globally
	if !enabled {
		return
	}

	// Only process if we're pinning to a specific version
	if !worker_versioning.OverrideIsPinned(override) {
		return
	}

	pinnedVersion := worker_versioning.GetOverridePinnedVersion(override)
	if pinnedVersion == nil {
		return
	}

	// Check cache - skip if signal was recently sent
	if signalCache != nil && !signalCache.ShouldSendSignal(
		namespaceID.String(),
		pinnedVersion.GetDeploymentName(),
		pinnedVersion.GetBuildId(),
	) {
		return
	}

	// Get namespace entry from registry
	nsEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		shardContext.GetLogger().Warn("Failed to get namespace for version workflow signal",
			tag.WorkflowNamespaceID(namespaceID.String()),
			tag.Error(err))
		return
	}

	// Use signaler function to send the signal
	err = signaler(ctx, nsEntry, pinnedVersion.GetDeploymentName(), pinnedVersion.GetBuildId())
	if err != nil {
		shardContext.GetLogger().Warn("Failed to signal version workflow for reactivation",
			tag.WorkflowNamespace(nsEntry.Name().String()),
			tag.WorkflowID(worker_versioning.GenerateVersionWorkflowID(pinnedVersion.GetDeploymentName(), pinnedVersion.GetBuildId())),
			tag.Error(err))
	}
}
