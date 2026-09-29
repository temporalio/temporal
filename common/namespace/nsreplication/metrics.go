package nsreplication

import (
	"context"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// LegacyMetricsTransport identifies the existing namespace replication queue.
	LegacyMetricsTransport = "legacy"

	metricsOutcomeApplied         = "applied"
	metricsOutcomeNoChange        = "no_change"
	metricsOutcomeNotAdmitted     = "not_admitted"
	metricsOutcomeTerminalFailure = "terminal_failure"

	metricsOperationCreate  = "create"
	metricsOperationUpdate  = "update"
	metricsOperationUnknown = "unknown"
)

type taskMetricsContextKey struct{}

// TaskMetricsContext contains transport metadata needed to record namespace replication metrics.
type TaskMetricsContext struct {
	SourceCluster  string
	TargetCluster  string
	Transport      string
	VisibilityTime *timestamppb.Timestamp
}

// WithTaskMetricsContext adds namespace replication metrics metadata to a context.
func WithTaskMetricsContext(ctx context.Context, metadata TaskMetricsContext) context.Context {
	return context.WithValue(ctx, taskMetricsContextKey{}, metadata)
}

// TaskMetricsContextFromContext returns namespace replication metrics metadata when present.
func TaskMetricsContextFromContext(ctx context.Context) (TaskMetricsContext, bool) {
	metadata, ok := ctx.Value(taskMetricsContextKey{}).(TaskMetricsContext)
	return metadata, ok
}

// RecordLegacyTerminalFailure records a namespace task that was successfully written to the legacy DLQ.
func RecordLegacyTerminalFailure(
	ctx context.Context,
	metricsHandler metrics.Handler,
	task *replicationspb.NamespaceTaskAttributes,
) {
	recordOutcome(ctx, metricsHandler, task, metricsOutcomeTerminalFailure)
}

func recordOutcome(
	ctx context.Context,
	metricsHandler metrics.Handler,
	task *replicationspb.NamespaceTaskAttributes,
	outcome string,
) {
	metadata, ok := TaskMetricsContextFromContext(ctx)
	if metricsHandler == nil || !ok {
		return
	}

	tags := []metrics.Tag{
		metrics.SourceClusterTag(metadata.SourceCluster),
		metrics.TargetClusterTag(metadata.TargetCluster),
		metrics.TransportTag(metadata.Transport),
		metrics.OperationTag(namespaceReplicationOperation(task)),
		metrics.OutcomeTag(outcome),
	}
	counterTags := append(tags, metrics.NamespaceTag(task.GetInfo().GetName()))
	metrics.NamespaceReplicationApplyOutcomes.With(metricsHandler).Record(1, counterTags...)

	if metadata.VisibilityTime == nil || metadata.VisibilityTime.CheckValid() != nil {
		return
	}
	latency := max(time.Since(metadata.VisibilityTime.AsTime()), 0)
	metrics.NamespaceReplicationApplyEndToEndLatency.With(metricsHandler).Record(latency, tags...)
}

func namespaceReplicationOperation(task *replicationspb.NamespaceTaskAttributes) string {
	switch task.GetNamespaceOperation() {
	case enumsspb.NAMESPACE_OPERATION_CREATE:
		return metricsOperationCreate
	case enumsspb.NAMESPACE_OPERATION_UPDATE:
		return metricsOperationUpdate
	default:
		return metricsOperationUnknown
	}
}
