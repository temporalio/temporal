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
// revisionNumber is the version's current revision per matching's view and is used by the signaler
// to compose a cluster-wide-deterministic RequestId on the signal for receiver-side dedup.
type VersionReactivationSignalerFn func(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	deploymentName, buildID string,
	revisionNumber int64,
) error

// ReactivateVersionWorkflowIfPinned sends a reactivation signal to the version workflow
// when workflows are pinned to a potentially DRAINED/INACTIVE version.
// This is a fire-and-forget operation - the signal is sent asynchronously and errors are
// logged by the signaler implementation. The signaler itself is responsible for per-pod
// dedup by revision number; cross-pod duplicates fold at the receiver via a deterministic
// UUID RequestId.
//
//nolint:revive,errcheck
func ReactivateVersionWorkflowIfPinned(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	override *workflowpb.VersioningOverride,
	signaler VersionReactivationSignalerFn,
	enabled bool,
	shouldSkipReactivation bool,
	revisionNumber int64,
) {
	// Check if signals are enabled globally
	if !enabled {
		return
	}

	// Skip signal if matching confirmed the version is active or still draining.
	if shouldSkipReactivation {
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

	// Send the signal asynchronously to avoid adding latency to the caller's request.
	// Errors are logged by the signaler implementation (e.g. via convertAndRecordError). However,
	// errors are not propagated to the caller as this is a fire-and-forget operation.
	go func() {
		signaler(context.Background(), namespaceEntry, pinnedVersion.GetDeploymentName(), pinnedVersion.GetBuildId(), revisionNumber) //nolint:errcheck
	}()
}
