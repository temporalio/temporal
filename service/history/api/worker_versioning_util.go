package api

import (
	"context"

	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/worker_versioning"
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
// This is a fire-and-forget operation - the signal is sent asynchronously and errors are
// logged by the signaler implementation.
//
//nolint:revive,errcheck
func ReactivateVersionWorkflowIfPinned(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
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
		namespaceEntry.ID().String(),
		pinnedVersion.GetDeploymentName(),
		pinnedVersion.GetBuildId(),
	) {
		return
	}

	// Send the signal asynchronously to avoid adding latency to the caller's request.
	// Errors are logged by the signaler implementation (e.g. via convertAndRecordError). However,
	// errors are not propagated to the caller as this is a fire-and-forget operation.
	go func() {
		signaler(context.Background(), namespaceEntry, pinnedVersion.GetDeploymentName(), pinnedVersion.GetBuildId()) //nolint:errcheck
	}()
}
