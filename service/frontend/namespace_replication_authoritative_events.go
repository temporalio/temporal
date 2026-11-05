package frontend

import (
	"context"
	"time"

	"go.temporal.io/server/api/adminservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/wideevents"
)

// observeAuthoritativeNamespaceMutation reports the final result of one
// authoritative ApplyNamespaceMutation RPC.
func (adh *AdminHandler) observeAuthoritativeNamespaceMutation(
	request *adminservice.ApplyNamespaceMutationRequest,
	task *replicationspb.NamespaceTaskAttributes,
	outcome string,
	applyErr error,
	startTime time.Time,
) {
	var telemetryPanic error
	defer log.CapturePanic(adh.logger, &telemetryPanic)
	observation := nsreplication.CHASMAuthoritativeApplyObservation{
		Task:                task,
		Stage:               nsreplication.CHASMApplyStageReceive,
		Outcome:             outcome,
		SourceCluster:       request.GetSourceCluster(),
		TargetCluster:       adh.clusterMetadata.GetCurrentClusterName(),
		ComponentBusinessID: request.GetComponentBusinessId(),
		ComponentRunID:      request.GetComponentRunId(),
		AttemptCount:        int(request.GetAttemptCount()),
		Duration:            time.Since(startTime),
		Error:               applyErr,
	}
	nsreplication.RecordCHASMAuthoritativeApply(adh.metricsHandler, observation)
	// The executor emits successful receiver outcomes, including no-ops,
	// with pre-mutation state and persistence requests when applicable.
	// The RPC handler emits failures and records the final RPC metric.
	if applyErr != nil &&
		adh.config != nil &&
		adh.config.EmitNamespaceLifecycleEvents != nil &&
		adh.config.EmitNamespaceLifecycleEvents() {
		nsreplication.EmitCHASMAuthoritativeApply(adh.eventLogger, observation)
	}
}

func (adh *AdminHandler) withAuthoritativeNamespaceMutationEvent(
	ctx context.Context,
	request *adminservice.ApplyNamespaceMutationRequest,
	task *replicationspb.NamespaceTaskAttributes,
) (observationCtx context.Context) {
	observationCtx = ctx
	var telemetryPanic error
	defer log.CapturePanic(adh.logger, &telemetryPanic)
	eventData, ok := nsreplication.CHASMAuthoritativeApplyEventData(
		nsreplication.CHASMAuthoritativeApplyObservation{
			Task:                task,
			Stage:               nsreplication.CHASMApplyStageReceive,
			ComponentBusinessID: request.GetComponentBusinessId(),
			ComponentRunID:      request.GetComponentRunId(),
		},
	)
	if !ok {
		return ctx
	}
	return wideevents.SetNamespaceReplicationTaskContext(ctx, wideevents.NamespaceReplicationTaskContext{
		SourceCluster: request.GetSourceCluster(),
		TargetCluster: adh.clusterMetadata.GetCurrentClusterName(),
		AttemptCount:  int(request.GetAttemptCount()),
		EventData:     eventData,
	})
}

func authoritativeReceiveOutcome(outcome nsreplication.ApplyOutcome) string {
	switch outcome {
	case nsreplication.ApplyOutcomeCreated:
		return nsreplication.CHASMApplyOutcomeCreated
	case nsreplication.ApplyOutcomeApplied:
		return nsreplication.CHASMApplyOutcomeApplied
	case nsreplication.ApplyOutcomeNoOpStale:
		return nsreplication.CHASMApplyOutcomeNoChange
	case nsreplication.ApplyOutcomeDuplicate:
		return nsreplication.CHASMApplyOutcomeDuplicate
	case nsreplication.ApplyOutcomeNotAdmitted:
		return nsreplication.CHASMApplyOutcomeNotAdmitted
	default:
		return nsreplication.CHASMApplyOutcomeError
	}
}
