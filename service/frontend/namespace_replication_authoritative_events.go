package frontend

import (
	"context"
	"time"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/wideevents"
)

func (adh *AdminHandler) authoritativeNamespaceMutationObservation(
	request *adminservice.ApplyNamespaceMutationRequest,
	outcome string,
	applyErr error,
	startTime time.Time,
	details map[string]any,
) nsreplication.CHASMAuthoritativeApplyObservation {
	return nsreplication.CHASMAuthoritativeApplyObservation{
		Task:                request.GetNamespaceTask(),
		Stage:               nsreplication.CHASMApplyStageReceive,
		Outcome:             outcome,
		SourceCluster:       request.GetSourceCluster(),
		TargetCluster:       adh.clusterMetadata.GetCurrentClusterName(),
		ComponentBusinessID: request.GetComponentBusinessId(),
		ComponentRunID:      request.GetComponentRunId(),
		AttemptCount:        int(request.GetAttemptCount()),
		Duration:            time.Since(startTime),
		Error:               applyErr,
		Details:             details,
	}
}

func (adh *AdminHandler) recordAuthoritativeNamespaceMutation(
	request *adminservice.ApplyNamespaceMutationRequest,
	outcome string,
	applyErr error,
	startTime time.Time,
	details map[string]any,
) {
	nsreplication.RecordCHASMAuthoritativeApply(
		adh.metricsHandler,
		adh.authoritativeNamespaceMutationObservation(request, outcome, applyErr, startTime, details),
	)
}

func (adh *AdminHandler) observeAuthoritativeNamespaceMutation(
	request *adminservice.ApplyNamespaceMutationRequest,
	outcome string,
	applyErr error,
	startTime time.Time,
	details map[string]any,
) {
	observation := adh.authoritativeNamespaceMutationObservation(request, outcome, applyErr, startTime, details)
	nsreplication.RecordCHASMAuthoritativeApply(adh.metricsHandler, observation)
	if adh.config != nil &&
		adh.config.EmitNamespaceLifecycleEvents != nil &&
		adh.config.EmitNamespaceLifecycleEvents() {
		nsreplication.EmitCHASMAuthoritativeApply(adh.eventLogger, observation)
	}
}

func (adh *AdminHandler) withAuthoritativeNamespaceMutationEvent(
	ctx context.Context,
	request *adminservice.ApplyNamespaceMutationRequest,
) context.Context {
	eventData, ok := nsreplication.CHASMAuthoritativeApplyEventData(
		nsreplication.CHASMAuthoritativeApplyObservation{
			Task:                request.GetNamespaceTask(),
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
