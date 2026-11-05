package namespacereplication

import (
	"time"

	otellog "go.opentelemetry.io/otel/log"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/log"
	nsreplicationcommon "go.temporal.io/server/common/namespace/nsreplication"
)

type componentTerminalObservation struct {
	shadowCompletion  *shadowCompletionRecord
	status            namespacereplicationpb.ComponentStatus
	localOutcome      namespacereplicationpb.LocalApplyOutcome
	peerOutcomes      map[string]string
	peerAttemptCounts map[string]int
	peerOutcomeCounts map[string]int
	peerCount         int
	peerAttemptCount  int
	startedAt         time.Time
	terminalAt        time.Time
}

func newComponentTerminalObservation(
	c *NamespaceMutationComponent,
	status namespacereplicationpb.ComponentStatus,
	terminalAt time.Time,
) *componentTerminalObservation {
	observation := &componentTerminalObservation{
		status:            status,
		localOutcome:      c.GetLocalApply().GetOutcome(),
		peerOutcomes:      make(map[string]string, len(c.GetPeerApply())),
		peerAttemptCounts: make(map[string]int, len(c.GetPeerApply())),
		peerOutcomeCounts: make(map[string]int),
		peerCount:         len(c.GetPeerApply()),
		terminalAt:        terminalAt,
	}
	if started := c.GetStartedAt(); started != nil && started.CheckValid() == nil {
		observation.startedAt = started.AsTime()
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
	logger log.Logger,
	eventLogger otellog.Logger,
	emitNamespaceLifecycleEvents func() bool,
	currentCluster string,
	ref chasm.ComponentRef,
	task *replicationspb.NamespaceTaskAttributes,
	terminal *componentTerminalObservation,
	terminalErr error,
) {
	var telemetryPanic error
	defer log.CapturePanic(logger, &telemetryPanic)
	if terminal == nil || emitNamespaceLifecycleEvents == nil || !emitNamespaceLifecycleEvents() {
		return
	}

	outcome := nsreplicationcommon.CHASMApplyOutcomeCompleted
	if terminal.status == namespacereplicationpb.COMPONENT_STATUS_FAILED {
		outcome = nsreplicationcommon.CHASMApplyOutcomeFailed
	} else if terminal.peerOutcomeCounts[namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL.String()] > 0 {
		outcome = nsreplicationcommon.CHASMApplyOutcomeCompletedWithFailures
	}
	details := map[string]any{
		"component_status":    terminal.status.String(),
		"local_apply_outcome": terminal.localOutcome.String(),
		"peer_count":          terminal.peerCount,
		"peer_attempt_count":  terminal.peerAttemptCount,
		"peer_outcomes":       terminal.peerOutcomes,
		"peer_attempt_counts": terminal.peerAttemptCounts,
		"peer_outcome_counts": terminal.peerOutcomeCounts,
	}
	if !terminal.startedAt.IsZero() {
		details["end_to_end_duration_ms"] = max(terminal.terminalAt.Sub(terminal.startedAt), 0).Milliseconds()
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
			Details:             details,
		},
	)
}

// observeLocal reports one local apply attempt. Every attempt records a
// local-stage metric. Attempts that resolve the local stage or fail to save
// also emit a local_apply event; scheduled retries and stale attempts do not.
func (h *applyLocalTaskHandler) observeLocal(
	ref chasm.ComponentRef,
	task *replicationspb.NamespaceTaskAttributes,
	attemptCount int,
	startTime time.Time,
	result *localApplyResult,
) {
	var telemetryPanic error
	defer log.CapturePanic(h.logger, &telemetryPanic)

	outcome, observationErr, record := result.outcome, result.err, result.record
	details := result.details
	if details == nil {
		details = make(map[string]any)
	}
	if result.saveErr != nil {
		// The record describes a state update that was not saved.
		outcome, observationErr, record = nsreplicationcommon.CHASMApplyOutcomeStateTransitionError, result.saveErr, localRecord{}
		if result.err != nil {
			details["apply_error"] = result.err.Error()
		}
		if result.record.retryScheduled {
			details["retry_requested"] = true
		}
	} else if !record.stateUpdated {
		outcome = nsreplicationcommon.CHASMApplyOutcomeStateUpdateSkipped
	}
	if result.retry {
		details["retry_scheduled"] = record.retryScheduled
		details["retry_exhausted"] = record.retryExhausted
		if record.stateUpdated {
			persistedOutcome := namespacereplicationpb.LOCAL_APPLY_OUTCOME_PENDING
			if record.retryExhausted {
				persistedOutcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_INDETERMINATE
			}
			details["persisted_local_outcome"] = persistedOutcome.String()
		}
	}

	var pendingAge *time.Duration
	pendingAgeThresholdExceeded := false
	if record.retryScheduled && !record.firstAttemptAt.IsZero() {
		age := max(record.recordedAt.Sub(record.firstAttemptAt), 0)
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
		AttemptCount:                attemptCount,
		Duration:                    time.Since(startTime),
		PendingAge:                  pendingAge,
		PendingAgeThresholdExceeded: pendingAgeThresholdExceeded,
		Error:                       observationErr,
		Details:                     details,
	}
	nsreplicationcommon.RecordCHASMAuthoritativeApply(h.metricsHandler, observation)
	if outcome == nsreplicationcommon.CHASMApplyOutcomeStateUpdateSkipped || record.retryScheduled {
		return
	}
	if h.emitNamespaceLifecycleEvents != nil && h.emitNamespaceLifecycleEvents() {
		nsreplicationcommon.EmitCHASMAuthoritativeApply(h.eventLogger, observation)
	}
}
