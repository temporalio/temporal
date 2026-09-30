package namespacereplication

import (
	"time"

	otellog "go.opentelemetry.io/otel/log"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
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

func (h *applyPeerTaskHandler) observePeerApply(
	ref chasm.ComponentRef,
	operation enumsspb.NamespaceOperation,
	detail *persistencespb.NamespaceDetail,
	shadow bool,
	task *namespacereplicationpb.ApplyPeerTask,
	recorded peerOutcomeRecord,
	applyErr error,
	saveErr error,
	startTime time.Time,
) {
	if shadow {
		return
	}

	outcome := chasmPeerApplyOutcome(recorded)
	observationErr := applyErr
	retryScheduled := saveErr == nil && recorded.retryScheduled
	details := map[string]any{
		"attempted_peer_outcome": recorded.outcome.String(),
		"retry_scheduled":        retryScheduled,
		"retry_exhausted":        recorded.retryExhausted,
	}
	if saveErr != nil {
		outcome = nsreplicationcommon.CHASMApplyOutcomeStateTransitionError
		observationErr = saveErr
		if recorded.retryScheduled {
			details["retry_requested"] = true
		}
		if applyErr != nil {
			details["apply_error"] = applyErr.Error()
		}
	} else {
		persistedOutcome := recorded.outcome
		if recorded.retryScheduled {
			persistedOutcome = namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING
		}
		details["persisted_peer_outcome"] = persistedOutcome.String()
	}

	var pendingDuration *time.Duration
	if saveErr == nil && !recorded.retryScheduled && !recorded.firstAttemptAt.IsZero() {
		duration := recorded.resolvedAt.Sub(recorded.firstAttemptAt)
		pendingDuration = &duration
	}
	var pendingAge *time.Duration
	pendingAgeThresholdExceeded := false
	if retryScheduled && !recorded.firstAttemptAt.IsZero() {
		age := max(recorded.resolvedAt.Sub(recorded.firstAttemptAt), 0)
		pendingAge = &age
		pendingAgeThresholdExceeded = age >= peerPendingAlertThreshold
	}
	observation := nsreplicationcommon.CHASMAuthoritativeApplyObservation{
		Task: nsreplicationcommon.NamespaceDetailToTaskAttributes(
			operation,
			detail,
		),
		Stage:                       nsreplicationcommon.CHASMApplyStagePeer,
		Outcome:                     outcome,
		SourceCluster:               h.currentCluster,
		TargetCluster:               task.GetTargetCell(),
		ComponentBusinessID:         ref.BusinessID,
		ComponentRunID:              ref.RunID,
		AttemptCount:                int(task.GetAttempt()) + 1,
		Duration:                    time.Since(startTime),
		PendingDuration:             pendingDuration,
		PendingAge:                  pendingAge,
		PendingAgeThresholdExceeded: pendingAgeThresholdExceeded,
		Error:                       observationErr,
		Details:                     details,
	}
	nsreplicationcommon.RecordCHASMAuthoritativeApply(h.metricsHandler, observation)
	// Per-attempt metrics cover ordinary scheduled retries. Keep lifecycle events
	// for terminal outcomes and state-transition failures so a peer outage does
	// not produce one wide event per backoff attempt.
	if (!recorded.retryScheduled || saveErr != nil) &&
		h.emitNamespaceLifecycleEvents != nil && h.emitNamespaceLifecycleEvents() {
		nsreplicationcommon.EmitCHASMAuthoritativeApply(h.eventLogger, observation)
	}
	if saveErr == nil {
		emitComponentTerminal(
			h.eventLogger,
			h.emitNamespaceLifecycleEvents,
			h.currentCluster,
			ref,
			observation.Task,
			recorded.componentTerminal,
			nil,
		)
	}
}

func chasmPeerApplyOutcome(recorded peerOutcomeRecord) string {
	if recorded.retryExhausted {
		return nsreplicationcommon.CHASMApplyOutcomeRetryExhausted
	}
	switch recorded.outcome {
	case namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED:
		return nsreplicationcommon.CHASMApplyOutcomeApplied
	case namespacereplicationpb.PEER_APPLY_OUTCOME_NO_OP_STALE:
		return nsreplicationcommon.CHASMApplyOutcomeNoChange
	case namespacereplicationpb.PEER_APPLY_OUTCOME_NOT_ADMITTED:
		return nsreplicationcommon.CHASMApplyOutcomeNotAdmitted
	case namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE:
		return nsreplicationcommon.CHASMApplyOutcomeRetryableError
	default:
		return nsreplicationcommon.CHASMApplyOutcomeTerminalError
	}
}
