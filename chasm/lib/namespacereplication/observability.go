package namespacereplication

import (
	"time"

	otellog "go.opentelemetry.io/otel/log"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	nsreplicationcommon "go.temporal.io/server/common/namespace/nsreplication"
)

func stateTransitionOutcome(successOutcome string, transitionErr error) string {
	if transitionErr != nil {
		return nsreplicationcommon.CHASMApplyOutcomeStateTransitionError
	}
	return successOutcome
}

type componentTerminalObservation struct {
	shadowCompletion  *shadowCompletionRecord
	status            namespacereplicationpb.ComponentStatus
	localOutcome      namespacereplicationpb.LocalApplyOutcome
	peerOutcomes      map[string]string
	peerAttemptCounts map[string]int
	peerOutcomeCounts map[string]int
	peerCount         int
	peerAttemptCount  int
}

func newComponentTerminalObservation(
	c *NamespaceMutationComponent,
	status namespacereplicationpb.ComponentStatus,
) *componentTerminalObservation {
	observation := &componentTerminalObservation{
		status:            status,
		localOutcome:      c.GetLocalApply().GetOutcome(),
		peerOutcomes:      make(map[string]string, len(c.GetPeerApply())),
		peerAttemptCounts: make(map[string]int, len(c.GetPeerApply())),
		peerOutcomeCounts: make(map[string]int),
		peerCount:         len(c.GetPeerApply()),
	}
	for peerName, peer := range c.GetPeerApply() {
		observation.peerOutcomes[peerName] = peer.GetOutcome().String()
		observation.peerAttemptCounts[peerName] = int(peer.GetAttemptCount())
		observation.peerOutcomeCounts[peer.GetOutcome().String()]++
		observation.peerAttemptCount += int(peer.GetAttemptCount())
	}
	return observation
}

func emitComponentTerminal(
	eventLogger otellog.Logger,
	emitNamespaceLifecycleEvents func() bool,
	currentCluster string,
	ref chasm.ComponentRef,
	task *replicationspb.NamespaceTaskAttributes,
	terminal *componentTerminalObservation,
	terminalErr error,
) {
	if terminal == nil || emitNamespaceLifecycleEvents == nil || !emitNamespaceLifecycleEvents() {
		return
	}

	outcome := nsreplicationcommon.CHASMApplyOutcomeCompleted
	if terminal.status == namespacereplicationpb.COMPONENT_STATUS_FAILED {
		outcome = nsreplicationcommon.CHASMApplyOutcomeFailed
	} else if terminal.peerOutcomeCounts[namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL.String()] > 0 {
		outcome = nsreplicationcommon.CHASMApplyOutcomeCompletedWithFailures
	}
	nsreplicationcommon.EmitCHASMAuthoritativeApply(
		eventLogger,
		nsreplicationcommon.CHASMAuthoritativeApplyObservation{
			Task:                task,
			Stage:               nsreplicationcommon.CHASMApplyStageComponent,
			Outcome:             outcome,
			SourceCluster:       currentCluster,
			ComponentBusinessID: ref.BusinessID,
			ComponentRunID:      ref.RunID,
			Error:               terminalErr,
			Details: map[string]any{
				"component_status":    terminal.status.String(),
				"local_apply_outcome": terminal.localOutcome.String(),
				"peer_count":          terminal.peerCount,
				"peer_attempt_count":  terminal.peerAttemptCount,
				"peer_outcomes":       terminal.peerOutcomes,
				"peer_attempt_counts": terminal.peerAttemptCounts,
				"peer_outcome_counts": terminal.peerOutcomeCounts,
			},
		},
	)
}

func (h *applyLocalTaskHandler) observeLocalApplyResult(
	ref chasm.ComponentRef,
	task *replicationspb.NamespaceTaskAttributes,
	applyErr error,
	recordErr error,
	startTime time.Time,
) {
	if recordErr != nil {
		h.observeLocalApply(
			ref,
			task,
			nsreplicationcommon.CHASMApplyOutcomeStateTransitionError,
			recordErr,
			startTime,
			map[string]any{"apply_error": applyErr.Error()},
		)
		return
	}
	h.observeLocalApply(
		ref,
		task,
		nsreplicationcommon.CHASMApplyOutcomeTerminalError,
		applyErr,
		startTime,
		map[string]any{"error_type": classifyLocalErr(applyErr)},
	)
}

func (h *applyLocalTaskHandler) observeLocalApply(
	ref chasm.ComponentRef,
	task *replicationspb.NamespaceTaskAttributes,
	outcome string,
	applyErr error,
	startTime time.Time,
	details map[string]any,
) {
	observation := nsreplicationcommon.CHASMAuthoritativeApplyObservation{
		Task:                task,
		Stage:               nsreplicationcommon.CHASMApplyStageLocal,
		Outcome:             outcome,
		SourceCluster:       h.currentCluster,
		TargetCluster:       h.currentCluster,
		ComponentBusinessID: ref.BusinessID,
		ComponentRunID:      ref.RunID,
		Duration:            time.Since(startTime),
		Error:               applyErr,
		Details:             details,
	}
	nsreplicationcommon.RecordCHASMAuthoritativeApply(h.metricsHandler, observation)
	if h.emitNamespaceLifecycleEvents != nil && h.emitNamespaceLifecycleEvents() {
		nsreplicationcommon.EmitCHASMAuthoritativeApply(h.eventLogger, observation)
	}
}

func (h *applyLocalTaskHandler) observeLocalRetry(
	ref chasm.ComponentRef,
	task *replicationspb.NamespaceTaskAttributes,
	recorded localRetryRecord,
	retryErr error,
	saveErr error,
	startTime time.Time,
) {
	outcome := nsreplicationcommon.CHASMApplyOutcomeRetryableError
	observationErr := retryErr
	retryScheduled := saveErr == nil && recorded.retryScheduled
	details := map[string]any{
		"retry_scheduled": retryScheduled,
		"retry_exhausted": recorded.retryExhausted,
	}
	if saveErr != nil {
		outcome = nsreplicationcommon.CHASMApplyOutcomeStateTransitionError
		observationErr = saveErr
		if recorded.retryScheduled {
			details["retry_requested"] = true
		}
		details["apply_error"] = retryErr.Error()
	} else if recorded.stateUpdated {
		persistedOutcome := namespacereplicationpb.LOCAL_APPLY_OUTCOME_PENDING
		if recorded.retryExhausted {
			outcome = nsreplicationcommon.CHASMApplyOutcomeRetryExhausted
			persistedOutcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_INDETERMINATE
		}
		details["persisted_local_outcome"] = persistedOutcome.String()
	} else {
		details["state_update_skipped"] = true
	}

	var pendingAge *time.Duration
	pendingAgeThresholdExceeded := false
	if retryScheduled && !recorded.firstAttemptAt.IsZero() {
		age := max(recorded.recordedAt.Sub(recorded.firstAttemptAt), 0)
		pendingAge = &age
		pendingAgeThresholdExceeded = age >= pendingAlertThreshold
	}
	observation := nsreplicationcommon.CHASMAuthoritativeApplyObservation{
		Task:                        task,
		Stage:                       nsreplicationcommon.CHASMApplyStageLocal,
		Outcome:                     outcome,
		SourceCluster:               h.currentCluster,
		TargetCluster:               h.currentCluster,
		ComponentBusinessID:         ref.BusinessID,
		ComponentRunID:              ref.RunID,
		AttemptCount:                recorded.attemptCount,
		Duration:                    time.Since(startTime),
		PendingAge:                  pendingAge,
		PendingAgeThresholdExceeded: pendingAgeThresholdExceeded,
		Error:                       observationErr,
		Details:                     details,
	}
	nsreplicationcommon.RecordCHASMAuthoritativeApply(h.metricsHandler, observation)
	if (!recorded.retryScheduled || saveErr != nil) &&
		h.emitNamespaceLifecycleEvents != nil && h.emitNamespaceLifecycleEvents() {
		nsreplicationcommon.EmitCHASMAuthoritativeApply(h.eventLogger, observation)
	}
}
