package nsreplication

import (
	"maps"
	"time"

	otellog "go.opentelemetry.io/otel/log"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/wideevents"
)

const (
	CHASMApplyStageReceive    = "receive"
	CHASMApplyStageLocal      = "local"
	CHASMApplyStagePeer       = "peer"
	CHASMApplyStageComponent  = "component"
	CHASMReplicationTransport = "chasm"

	CHASMApplyOutcomeApplied               = "applied"
	CHASMApplyOutcomeCreated               = "created"
	CHASMApplyOutcomeDuplicate             = "duplicate"
	CHASMApplyOutcomeNoChange              = "no_change"
	CHASMApplyOutcomeNotAdmitted           = "not_admitted"
	CHASMApplyOutcomeError                 = "error"
	CHASMApplyOutcomeRetryableError        = "retryable_error"
	CHASMApplyOutcomeTerminalError         = "terminal_error"
	CHASMApplyOutcomeRetryExhausted        = "retry_exhausted"
	CHASMApplyOutcomeFingerprintError      = "fingerprint_error"
	CHASMApplyOutcomeFingerprintMismatch   = "fingerprint_mismatch"
	CHASMApplyOutcomeStateTransitionError  = "state_transition_error"
	CHASMApplyOutcomeCompleted             = "completed"
	CHASMApplyOutcomeCompletedWithFailures = "completed_with_failures"
	CHASMApplyOutcomeFailed                = "failed"

	chasmAuthoritativeMode = "authoritative"
	chasmApplyStageTag     = "apply_stage"
)

// CHASMAuthoritativeApplyObservation describes one authoritative CHASM lifecycle observation.
type CHASMAuthoritativeApplyObservation struct {
	Task                        *replicationspb.NamespaceTaskAttributes
	Stage                       string
	Outcome                     string
	SourceCluster               string
	TargetCluster               string
	ComponentBusinessID         string
	ComponentRunID              string
	AttemptCount                int
	Duration                    time.Duration
	PendingDuration             *time.Duration
	PendingAge                  *time.Duration
	PendingAgeThresholdExceeded bool
	Error                       error
	Details                     map[string]any
}

// RecordCHASMAuthoritativeApply records bounded-cardinality metrics for one apply attempt.
func RecordCHASMAuthoritativeApply(
	metricsHandler metrics.Handler,
	observation CHASMAuthoritativeApplyObservation,
) {
	if metricsHandler == nil {
		return
	}

	tags := []metrics.Tag{
		metrics.SourceClusterTag(observation.SourceCluster),
		metrics.TargetClusterTag(observation.TargetCluster),
		metrics.OperationTag(namespaceReplicationOperation(observation.Task)),
		metrics.StringTag(chasmApplyStageTag, observation.Stage),
		metrics.OutcomeTag(observation.Outcome),
	}
	metrics.NamespaceReplicationCHASMApplyOutcomes.With(metricsHandler).Record(
		1,
		append(tags, metrics.NamespaceTag(observation.Task.GetInfo().GetName()))...,
	)
	metrics.NamespaceReplicationCHASMApplyLatency.With(metricsHandler).Record(observation.Duration, tags...)
	if observation.PendingDuration != nil {
		metrics.NamespaceReplicationCHASMPeerPendingLatency.With(metricsHandler).Record(
			max(*observation.PendingDuration, 0),
			tags...,
		)
	}
	if observation.PendingAge != nil {
		metrics.NamespaceReplicationCHASMPeerPendingAge.With(metricsHandler).Record(
			max(*observation.PendingAge, 0),
			tags...,
		)
	}
	if observation.PendingAgeThresholdExceeded {
		metrics.NamespaceReplicationCHASMPeerPendingThresholdExceeded.With(metricsHandler).Record(1, tags...)
	}
}

// EmitCHASMAuthoritativeApply emits an authoritative CHASM namespace lifecycle wide event.
func EmitCHASMAuthoritativeApply(
	eventLogger otellog.Logger,
	observation CHASMAuthoritativeApplyObservation,
) {
	if eventLogger == nil {
		return
	}

	eventData, ok := CHASMAuthoritativeApplyEventData(observation)
	if !ok {
		return
	}

	wideevents.EmitNamespaceReplicationLifecycle(eventLogger, wideevents.NamespaceReplicationLifecycleInput{
		Phase:                        wideevents.NamespaceReplicationProcessed,
		Outcome:                      wideevents.NamespaceReplicationOutcome(observation.Outcome),
		EventData:                    eventData,
		SourceCluster:                observation.SourceCluster,
		TargetCluster:                observation.TargetCluster,
		AttemptCount:                 observation.AttemptCount,
		Error:                        observation.Error,
		EmitOnTaskSerializationError: true,
	})
}

// CHASMAuthoritativeApplyEventData returns lifecycle event data enriched with
// CHASM correlation fields. Receiver-side execution uses the same data so its
// processed event can additionally include the local pre-mutation state and
// persistence request captured by the namespace task executor.
func CHASMAuthoritativeApplyEventData(
	observation CHASMAuthoritativeApplyObservation,
) (wideevents.NamespaceReplicationTaskEventData, bool) {
	if observation.Task == nil {
		return wideevents.NamespaceReplicationTaskEventData{}, false
	}

	eventData, ok := wideevents.NewDefaultNamespaceReplicationTaskEventDataProvider().Extract(
		&replicationspb.ReplicationTask{
			TaskType: enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
			Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{
				NamespaceTaskAttributes: observation.Task,
			},
		},
	)
	if !ok {
		return wideevents.NamespaceReplicationTaskEventData{}, false
	}
	eventData.Details = maps.Clone(observation.Details)
	if eventData.Details == nil {
		eventData.Details = make(map[string]any)
	}
	eventData.Details["transport"] = CHASMReplicationTransport
	eventData.Details["mode"] = chasmAuthoritativeMode
	eventData.Details[chasmApplyStageTag] = observation.Stage
	if observation.ComponentBusinessID != "" {
		eventData.Details["component_business_id"] = observation.ComponentBusinessID
	}
	if observation.ComponentRunID != "" {
		eventData.Details["component_run_id"] = observation.ComponentRunID
	}
	if observation.PendingDuration != nil {
		eventData.Details["pending_duration_ms"] = max(*observation.PendingDuration, 0).Milliseconds()
	}
	if observation.PendingAge != nil {
		eventData.Details["pending_age_ms"] = max(*observation.PendingAge, 0).Milliseconds()
	}
	return eventData, true
}
