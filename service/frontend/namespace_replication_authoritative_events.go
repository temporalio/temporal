package frontend

import (
	"context"
	"go.temporal.io/api/serviceerror"
	"maps"
	"time"

	"go.temporal.io/server/api/adminservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/wideevents"
)

func (adh *AdminHandler) authoritativeNamespaceMutationObservation(
	request *adminservice.ApplyNamespaceMutationRequest,
	task *replicationspb.NamespaceTaskAttributes,
	outcome string,
	applyErr error,
	startTime time.Time,
	details map[string]any,
) nsreplication.CHASMAuthoritativeApplyObservation {
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
		Details:             maps.Clone(details),
	}
	applyNamespaceMutationAttribution(request, &observation)
	return observation
}

func (adh *AdminHandler) recordAuthoritativeNamespaceMutation(
	request *adminservice.ApplyNamespaceMutationRequest,
	task *replicationspb.NamespaceTaskAttributes,
	outcome string,
	applyErr error,
	startTime time.Time,
	details map[string]any,
) {
	var telemetryPanic error
	defer log.CapturePanic(adh.logger, &telemetryPanic)
	nsreplication.RecordCHASMAuthoritativeApply(
		adh.metricsHandler,
		adh.authoritativeNamespaceMutationObservation(request, task, outcome, applyErr, startTime, details),
	)
}

func (adh *AdminHandler) observeAuthoritativeNamespaceMutation(
	request *adminservice.ApplyNamespaceMutationRequest,
	task *replicationspb.NamespaceTaskAttributes,
	outcome string,
	applyErr error,
	startTime time.Time,
	details map[string]any,
) {
	var telemetryPanic error
	defer log.CapturePanic(adh.logger, &telemetryPanic)
	observation := adh.authoritativeNamespaceMutationObservation(request, task, outcome, applyErr, startTime, details)
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
	task *replicationspb.NamespaceTaskAttributes,
) (observationCtx context.Context) {
	observationCtx = ctx
	var telemetryPanic error
	defer log.CapturePanic(adh.logger, &telemetryPanic)
	observation := nsreplication.CHASMAuthoritativeApplyObservation{
		Task:                task,
		Stage:               nsreplication.CHASMApplyStageReceive,
		ComponentBusinessID: request.GetComponentBusinessId(),
		ComponentRunID:      request.GetComponentRunId(),
	}
	applyNamespaceMutationAttribution(request, &observation)
	eventData, ok := nsreplication.CHASMAuthoritativeApplyEventData(observation)
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

func applyNamespaceMutationAttribution(
	request *adminservice.ApplyNamespaceMutationRequest,
	observation *nsreplication.CHASMAuthoritativeApplyObservation,
) {
	if attribution := request.GetAttribution(); attribution.GetOrigin() == adminservice.NamespaceMutationAttribution_ORIGIN_OPERATOR_REPAIR {
		if observation.Details == nil {
			observation.Details = make(map[string]any)
		}
		observation.Transport = nsreplication.OperatorRepairTransport
		observation.Mode = nsreplication.OperatorRepairMode
		observation.Details["origin"] = "operator_repair"
		observation.Details["repair_id"] = attribution.GetRepairId()
		observation.Details["operator_reason"] = attribution.GetReason()
	}
}

func validateNamespaceMutationAttribution(request *adminservice.ApplyNamespaceMutationRequest) error {
	attribution := request.GetAttribution()
	if attribution == nil {
		return nil
	}
	switch attribution.GetOrigin() {
	case adminservice.NamespaceMutationAttribution_ORIGIN_UNSPECIFIED:
		if attribution.GetRepairId() != "" || attribution.GetReason() != "" {
			return serviceerror.NewInvalidArgument("namespace mutation attribution origin is required")
		}
		return nil
	case adminservice.NamespaceMutationAttribution_ORIGIN_OPERATOR_REPAIR:
		if request.GetShadow() {
			return serviceerror.NewInvalidArgument("operator repair cannot use shadow apply")
		}
		if attribution.GetRepairId() == "" || attribution.GetReason() == "" {
			return serviceerror.NewInvalidArgument("operator repair attribution requires repair_id and reason")
		}
		return nil
	default:
		return serviceerror.NewInvalidArgument("unknown namespace mutation attribution origin")
	}
}

func namespaceMutationTransport(request *adminservice.ApplyNamespaceMutationRequest) string {
	if request.GetAttribution().GetOrigin() == adminservice.NamespaceMutationAttribution_ORIGIN_OPERATOR_REPAIR {
		return nsreplication.OperatorRepairTransport
	}
	return nsreplication.CHASMReplicationTransport
}
