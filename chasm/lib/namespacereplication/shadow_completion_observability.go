package namespacereplication

import (
	"time"

	otellog "go.opentelemetry.io/otel/log"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/wideevents"
)

// shadowCompletionRecord is a detached snapshot of the transition completing a
// shadow mutation. Observers consume it only after UpdateComponent succeeds.
type shadowCompletionRecord struct {
	startedAt         time.Time
	completedAt       time.Time
	operation         enumsspb.NamespaceOperation
	detail            *persistencespb.NamespaceDetail
	localOutcome      string
	peerOutcomes      map[string]string
	peerAttemptCounts map[string]int32
	peerOutcomeCounts map[string]int
	peerAttemptCount  int32
	outcome           string
}

func newShadowCompletionRecord(c *NamespaceMutationComponent, completedAt time.Time) *shadowCompletionRecord {
	if !c.GetMutation().GetShadow() || c.GetStatus() != namespacereplicationpb.COMPONENT_STATUS_COMPLETED {
		return nil
	}
	record := &shadowCompletionRecord{
		completedAt:       completedAt,
		operation:         c.GetMutation().GetOperation(),
		detail:            common.CloneProto(c.GetMutation().GetNamespaceDetail()),
		localOutcome:      c.GetLocalApply().GetOutcome().String(),
		peerOutcomes:      make(map[string]string, len(c.GetPeerApply())),
		peerAttemptCounts: make(map[string]int32, len(c.GetPeerApply())),
		peerOutcomeCounts: make(map[string]int),
		outcome:           "match",
	}
	if started := c.GetStartedAt(); started != nil && started.CheckValid() == nil {
		record.startedAt = started.AsTime()
	}
	for name, peer := range c.GetPeerApply() {
		outcome := peer.GetOutcome()
		record.peerOutcomes[name] = outcome.String()
		record.peerAttemptCounts[name] = peer.GetAttemptCount()
		record.peerOutcomeCounts[outcome.String()]++
		record.peerAttemptCount += peer.GetAttemptCount()
		if outcome == namespacereplicationpb.PEER_APPLY_OUTCOME_SHADOW_MISMATCH && record.outcome == "match" {
			record.outcome = "mismatch"
		} else if outcome != namespacereplicationpb.PEER_APPLY_OUTCOME_SHADOW_MATCH &&
			outcome != namespacereplicationpb.PEER_APPLY_OUTCOME_SHADOW_MISMATCH {
			record.outcome = "error"
		}
	}
	if len(record.peerOutcomes) == 0 {
		record.outcome = "no_peers"
	}
	return record
}

type shadowCompletionObserver struct {
	logger                       log.Logger
	metricsHandler               metrics.Handler
	eventLogger                  otellog.Logger
	emitNamespaceLifecycleEvents func() bool
	sourceCluster                string
}

func (o shadowCompletionObserver) observe(ref chasm.ComponentRef, record *shadowCompletionRecord) {
	if record == nil {
		return
	}
	var telemetryPanic error
	defer log.CapturePanic(o.logger, &telemetryPanic)

	tags := []metrics.Tag{
		metrics.SourceClusterTag(o.sourceCluster), metrics.OperationTag(nsreplication.OperationMetricValue(record.operation)),
		metrics.OutcomeTag(record.outcome), metrics.StringTag("mode", "shadow"), metrics.StringTag("apply_stage", "component"),
	}
	hasDuration := !record.startedAt.IsZero()
	duration := max(record.completedAt.Sub(record.startedAt), 0)
	if o.metricsHandler != nil {
		metrics.NamespaceReplicationShadowCompletionOutcomes.With(o.metricsHandler).Record(1, tags...)
		if hasDuration {
			metrics.NamespaceReplicationShadowCompletionLatency.With(o.metricsHandler).Record(duration, tags...)
		}
	}
	if o.emitNamespaceLifecycleEvents == nil || !o.emitNamespaceLifecycleEvents() {
		return
	}
	details := map[string]any{
		"transport": "chasm", "mode": "shadow", "apply_stage": "component",
		"component_business_id": ref.BusinessID, "component_run_id": ref.RunID,
		"component_status":    namespacereplicationpb.COMPONENT_STATUS_COMPLETED.String(),
		"local_apply_outcome": record.localOutcome,
		"peer_count":          len(record.peerOutcomes),
		"peer_attempt_count":  record.peerAttemptCount,
		"peer_outcomes":       record.peerOutcomes,
		"peer_attempt_counts": record.peerAttemptCounts,
		"peer_outcome_counts": record.peerOutcomeCounts,
	}
	if hasDuration {
		details["end_to_end_duration_ms"] = duration.Milliseconds()
	}
	task := nsreplication.NamespaceDetailToTaskAttributes(record.operation, record.detail)
	data, _ := wideevents.NewDefaultNamespaceReplicationTaskEventDataProvider().Extract(&replicationspb.ReplicationTask{
		TaskType:   enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
		Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{NamespaceTaskAttributes: task},
	})
	data.Details = details
	wideevents.EmitNamespaceReplicationLifecycle(o.eventLogger, wideevents.NamespaceReplicationLifecycleInput{
		Phase: wideevents.NamespaceReplicationCompleted, Outcome: wideevents.NamespaceReplicationOutcome(record.outcome),
		EventData: data, SourceCluster: o.sourceCluster, EmitOnTaskSerializationError: true,
	})
}
