package namespacereplication

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/wideevents"
)

const pendingAlertThreshold = time.Minute

type peerOutcomeRecord struct {
	outcome          namespacereplicationpb.PeerApplyOutcome
	attemptedOutcome namespacereplicationpb.PeerApplyOutcome
	stateUpdated     bool
	retryScheduled   bool
	retryExhausted   bool
	firstAttemptAt   time.Time
	resolvedAt       time.Time
	startedAt        time.Time
	completion       *shadowCompletionRecord
}

func (h *applyPeerTaskHandler) observePeerApply(request PeerApplyRequest, recorded peerOutcomeRecord, applyErr, saveErr error, duration time.Duration) {
	var telemetryPanic error
	defer log.CapturePanic(h.logger, &telemetryPanic)

	mode := "authoritative"
	if request.Shadow {
		mode = "shadow"
	}
	outcome := peerApplyMetricOutcome(recorded)
	if saveErr != nil {
		outcome = "state_transition_error"
	}
	tags := []metrics.Tag{
		metrics.SourceClusterTag(request.SourceCluster), metrics.TargetClusterTag(request.TargetCluster),
		metrics.OperationTag(nsreplication.OperationMetricValue(request.Operation)), metrics.OutcomeTag(outcome),
		metrics.StringTag("mode", mode), metrics.StringTag("apply_stage", "peer"),
	}
	if h.metricsHandler != nil {
		metrics.NamespaceReplicationCHASMApplyOutcomes.With(h.metricsHandler).Record(1, tags...)
		metrics.NamespaceReplicationCHASMApplyLatency.With(h.metricsHandler).Record(duration, tags...)
	}
	pendingAge, endToEndDuration := h.recordPeerDurations(recorded, saveErr, tags)
	// Metrics cover scheduled retries; wide events describe committed terminal
	// outcomes or failures to persist an attempt's outcome.
	if saveErr == nil && (recorded.retryScheduled || !recorded.stateUpdated) {
		return
	}
	if h.emitNamespaceLifecycleEvents == nil || !h.emitNamespaceLifecycleEvents() {
		return
	}
	details := map[string]any{
		"transport": "chasm", "mode": mode, "apply_stage": "peer",
		"component_business_id": request.ComponentBusinessID, "component_run_id": request.ComponentRunID,
		"attempted_peer_outcome": recorded.attemptedOutcome.String(),
		"retry_scheduled":        false,
		"retry_exhausted":        saveErr == nil && recorded.stateUpdated && recorded.retryExhausted,
		"attempt_duration_ms":    duration.Milliseconds(),
	}
	observationErr := applyErr
	if saveErr != nil {
		observationErr = saveErr
		if applyErr != nil {
			details["apply_error"] = applyErr.Error()
		}
	} else {
		details["persisted_peer_outcome"] = recorded.outcome.String()
	}
	if pendingAge != nil {
		details["pending_duration_ms"] = pendingAge.Milliseconds()
	}
	if endToEndDuration != nil {
		details["end_to_end_duration_ms"] = endToEndDuration.Milliseconds()
	}
	eventOutcome := wideevents.NamespaceReplicationOutcome(outcome)
	if request.Shadow {
		switch outcome {
		case "shadow_match":
			eventOutcome = wideevents.NamespaceReplicationOutcomeMatch
		case "shadow_mismatch":
			eventOutcome = wideevents.NamespaceReplicationOutcomeMismatch
		default:
			eventOutcome = wideevents.NamespaceReplicationOutcomeError
			details["reason"] = outcome
		}
	}
	task := nsreplication.NamespaceDetailToTaskAttributes(request.Operation, request.Detail)
	data, _ := wideevents.NewDefaultNamespaceReplicationTaskEventDataProvider().Extract(&replicationspb.ReplicationTask{
		TaskType:   enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
		Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{NamespaceTaskAttributes: task},
	})
	data.Details = details
	wideevents.EmitNamespaceReplicationLifecycle(h.eventLogger, wideevents.NamespaceReplicationLifecycleInput{
		Phase: wideevents.NamespaceReplicationPeerDelivery, Outcome: eventOutcome, EventData: data,
		SourceCluster: request.SourceCluster, TargetCluster: request.TargetCluster, AttemptCount: int(request.AttemptCount),
		Error: observationErr, EmitOnTaskSerializationError: true,
	})
}

func (h *applyPeerTaskHandler) recordPeerDurations(
	recorded peerOutcomeRecord,
	saveErr error,
	tags []metrics.Tag,
) (pendingAge, endToEndDuration *time.Duration) {
	if saveErr != nil || !recorded.stateUpdated {
		return nil, nil
	}
	if !recorded.firstAttemptAt.IsZero() {
		age := max(recorded.resolvedAt.Sub(recorded.firstAttemptAt), 0)
		pendingAge = &age
	}
	if !recorded.retryScheduled && !recorded.startedAt.IsZero() {
		duration := max(recorded.resolvedAt.Sub(recorded.startedAt), 0)
		endToEndDuration = &duration
	}
	if h.metricsHandler == nil {
		return pendingAge, endToEndDuration
	}
	if endToEndDuration != nil {
		metrics.NamespaceReplicationCHASMApplyEndToEndLatency.With(h.metricsHandler).Record(*endToEndDuration, tags...)
	}
	if pendingAge != nil && recorded.retryScheduled {
		metrics.NamespaceReplicationCHASMPeerPendingAge.With(h.metricsHandler).Record(*pendingAge, tags...)
		if *pendingAge >= pendingAlertThreshold {
			metrics.NamespaceReplicationCHASMPeerPendingThresholdExceeded.With(h.metricsHandler).Record(1, tags...)
		}
	}
	return pendingAge, endToEndDuration
}

func peerApplyMetricOutcome(record peerOutcomeRecord) string {
	if !record.stateUpdated {
		return "state_update_skipped"
	}
	if record.retryExhausted {
		return "retry_exhausted"
	}
	switch record.outcome {
	case namespacereplicationpb.PEER_APPLY_OUTCOME_SHADOW_MATCH:
		return "shadow_match"
	case namespacereplicationpb.PEER_APPLY_OUTCOME_SHADOW_MISMATCH:
		return "shadow_mismatch"
	case namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE:
		return "retryable_error"
	case namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED:
		return "applied"
	case namespacereplicationpb.PEER_APPLY_OUTCOME_NO_OP_STALE:
		return "no_change"
	case namespacereplicationpb.PEER_APPLY_OUTCOME_NOT_ADMITTED:
		return "not_admitted"
	default:
		return "terminal_error"
	}
}
