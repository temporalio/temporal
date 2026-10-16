package wci

import (
	"time"

	wcimetrics "go.temporal.io/auto-scaled-workers/wci/metrics"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	scalingalgorithm "go.temporal.io/auto-scaled-workers/wci/workflow/scaling_algorithm"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/metrics"
)

// Emissions reuse the workflow's metric names and tag schemas (ASW wci/workflow/metrics_recorders.go),
// plus backend=chasm; workflow emissions carry no backend tag.

var MigrateToWorkflowCount = metrics.NewCounterDef(
	"worker_controller_instance_chasm_migrate_to_workflow_count",
	metrics.WithDescription("CHASM-to-workflow migration attempts by outcome."))

const (
	backendCHASM            = "chasm"
	migrationOutcomeSuccess = "success"
	migrationOutcomeError   = "error"
)

// wciMetrics tags a namespace-tagged handler with one WCI and provider ("" for none).
func wciMetrics(h metrics.Handler, deploymentName, buildID, providerType string) metrics.Handler {
	if providerType == "" {
		providerType = wcimetrics.ComputeProviderNone
	}
	return h.WithTags(
		metrics.StringTag(wcimetrics.WorkerDeploymentNameTag, deploymentName),
		metrics.StringTag(wcimetrics.WorkerDeploymentBuildIDTag, buildID),
		metrics.StringTag(wcimetrics.ComputeProviderTag, providerType),
		metrics.StringTag(wcimetrics.BackendTagName, backendCHASM),
	)
}

func metricsFor(ctx chasm.Context, w *WorkerControllerInstance, providerType string) metrics.Handler {
	return wciMetrics(ctx.MetricsHandler(), w.DeploymentName, w.BuildId, providerType)
}

func recordOperation(h metrics.Handler, op string, errorType wcimetrics.ErrorType) {
	wcimetrics.Operations.With(h).Record(1,
		metrics.StringTag(wcimetrics.OperationTagName, op),
		metrics.StringTag(wcimetrics.ErrorTypeTagName, string(errorType)),
		metrics.StringTag(wcimetrics.ActivityErrorTypeTagName, string(wcimetrics.ActivityErrorTypeNone)),
		metrics.StringTag(wcimetrics.SkipReasonTagName, string(wcimetrics.SkippedReasonNone)),
	)
}

func recordTaskAddSignal(h metrics.Handler, skip wcimetrics.SkippedReason) {
	wcimetrics.Signals.With(h).Record(1,
		metrics.StringTag(wcimetrics.SignalTypeTagName, wcimetrics.SignalTypeTaskAdd),
		metrics.StringTag(wcimetrics.ErrorTypeTagName, string(wcimetrics.ErrorTypeNone)),
		metrics.StringTag(wcimetrics.ActivityErrorTypeTagName, string(wcimetrics.ActivityErrorTypeNone)),
		metrics.StringTag(wcimetrics.SkipReasonTagName, string(skip)),
	)
}

func computeErrorType(err error) wcimetrics.ErrorType {
	if err == nil {
		return wcimetrics.ErrorTypeNone
	}
	return wcimetrics.ComputeProviderErrorType(err)
}

func recordBacklog(h metrics.Handler, s *scalingalgorithm.ScalingMetricsSnapshot) {
	var count int64
	var age time.Duration
	for _, q := range []*iface.QueueTypeScalingMetrics{s.Workflow, s.Activity, s.Nexus} {
		if q != nil {
			count += q.LastBacklogCount
			age = max(age, q.LastBacklogAge)
		}
	}
	wcimetrics.BacklogCount.With(h).Record(float64(count))
	wcimetrics.BacklogAge.With(h).Record(age.Seconds())
}
