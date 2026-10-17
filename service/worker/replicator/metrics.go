package replicator

import (
	"context"
	"time"

	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace/nsreplication"
)

const (
	taskQueueUserDataMetricsOutcomeApplied         = "applied"
	taskQueueUserDataMetricsOutcomeNotAdmitted     = "not_admitted"
	taskQueueUserDataMetricsOutcomeTerminalFailure = "terminal_failure"
)

func recordTaskQueueUserDataOutcome(
	ctx context.Context,
	metricsHandler metrics.Handler,
	task *replicationspb.TaskQueueUserDataAttributes,
	outcome string,
) {
	metadata, ok := nsreplication.TaskMetricsContextFromContext(ctx)
	if metricsHandler == nil || !ok {
		return
	}

	tags := []metrics.Tag{
		metrics.SourceClusterTag(metadata.SourceCluster),
		metrics.TargetClusterTag(metadata.TargetCluster),
		metrics.TransportTag(metadata.Transport),
		metrics.OutcomeTag(outcome),
	}
	counterTags := append(tags, metrics.NamespaceIDTag(task.GetNamespaceId()))
	metrics.TaskQueueUserDataReplicationApplyOutcomes.With(metricsHandler).Record(1, counterTags...)

	if metadata.VisibilityTime == nil || metadata.VisibilityTime.CheckValid() != nil {
		return
	}
	latency := max(time.Since(metadata.VisibilityTime.AsTime()), 0)
	metrics.TaskQueueUserDataReplicationApplyEndToEndLatency.With(metricsHandler).Record(latency, tags...)
}
