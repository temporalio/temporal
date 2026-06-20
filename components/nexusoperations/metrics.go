package nexusoperations

import (
	"strings"
	"time"

	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/metrics"
)

// nexusEngineTagValue marks caller-side metrics emitted by the legacy HSM implementation, so
// operators can distinguish in-workflow HSM operations from CHASM operations during the
// HSM->CHASM migration. It is intended for internal use only and should be stripped at the
// external-observability boundary so the customer-facing rollup aggregates across engines.
const nexusEngineTagValue = "hsm"

// operationMetricsHandler returns a metrics handler enriched with caller-side Nexus operation
// tags. It mirrors chasm/lib/nexusoperation Operation.metricsHandler so the HSM and CHASM
// implementations emit identical metric names and labels (see the HSM->CHASM migration). Unlike
// CHASM, HSM Nexus operations are always in-workflow, so workflowType is always the real parent
// workflow type (there is no standalone placeholder case).
func (e taskExecutor) operationMetricsHandler(op Operation, namespaceName, workflowType string) metrics.Handler {
	tags := []metrics.Tag{
		metrics.NamespaceTag(namespaceName),
		metrics.NexusEndpointTag(op.Endpoint),
		metrics.WorkflowTypeTag(workflowType),
		metrics.StringTag("impl", nexusEngineTagValue),
	}
	var conf chasmnexus.NexusMetricTagConfig
	if e.Config.MetricTagConfig != nil {
		conf = e.Config.MetricTagConfig()
	}
	if conf.IncludeServiceTag {
		tags = append(tags, metrics.NexusServiceTag(op.Service))
	}
	if conf.IncludeOperationTag {
		tags = append(tags, metrics.NexusOperationTag(op.Operation))
	}
	return e.MetricsHandler.WithTags(tags...)
}

// recordOperationSucceeded emits the success counter and latency metrics for an operation that
// completed successfully.
func (e taskExecutor) recordOperationSucceeded(op Operation, namespaceName, workflowType string, closeTime time.Time) {
	e.recordOperationOutcome(op, namespaceName, workflowType, nexusoperationpb.OPERATION_STATUS_SUCCEEDED, closeTime, chasmnexus.NexusOperationSuccessCount.With)
}

// recordOperationFailed emits the failure counter and latency metrics for an operation that
// failed non-retryably.
func (e taskExecutor) recordOperationFailed(op Operation, namespaceName, workflowType string, closeTime time.Time) {
	e.recordOperationOutcome(op, namespaceName, workflowType, nexusoperationpb.OPERATION_STATUS_FAILED, closeTime, chasmnexus.NexusOperationFailedCount.With)
}

// recordOperationTimedOut emits the timeout counter (tagged with the timeout type) and latency
// metrics for an operation that timed out.
func (e taskExecutor) recordOperationTimedOut(op Operation, namespaceName, workflowType, timeoutType string, closeTime time.Time) {
	e.recordOperationOutcome(op, namespaceName, workflowType, nexusoperationpb.OPERATION_STATUS_TIMED_OUT, closeTime, chasmnexus.NexusOperationTimeoutCount.With, metrics.TimeoutTypeTag(timeoutType))
}

// recordOperationOutcome records the terminal outcome counter and the shared latency metrics for a
// closed operation. The outcome-specific counter is supplied as a metric definition's With method
// (e.g. NexusOperationSuccessCount.With), so each per-outcome recorder differs only by its counter
// and any extra counter tags (the timeout type, for timeouts).
func (e taskExecutor) recordOperationOutcome(
	op Operation,
	namespaceName, workflowType string,
	status nexusoperationpb.OperationStatus,
	closeTime time.Time,
	withCounter func(metrics.Handler) metrics.CounterIface,
	counterTags ...metrics.Tag,
) {
	handler := e.operationMetricsHandler(op, namespaceName, workflowType)
	withCounter(handler).Record(1, counterTags...)
	e.recordCompletionLatencies(handler, op, closeTime, metrics.OutcomeTag(strings.ToLower(status.String())))
}

// recordCompletionLatencies emits schedule-to-close plus either start-to-close (operations that
// started) or schedule-to-start (sync / never-started), mirroring chasm/lib/nexusoperation's
// emitLatencyMetrics. It is shared by the per-outcome recorders above.
func (e taskExecutor) recordCompletionLatencies(handler metrics.Handler, op Operation, closeTime time.Time, outcomeTag metrics.Tag) {
	if op.ScheduledTime == nil {
		return
	}
	scheduledTime := op.ScheduledTime.AsTime()
	chasmnexus.NexusOperationScheduleToCloseLatency.With(handler).Record(closeTime.Sub(scheduledTime), outcomeTag)
	if op.StartedTime != nil {
		// Async operation that was started; schedule-to-start latency was emitted at start time.
		chasmnexus.NexusOperationStartToCloseLatency.With(handler).Record(closeTime.Sub(op.StartedTime.AsTime()), outcomeTag)
	} else {
		// Sync operation or operation that never started.
		chasmnexus.NexusOperationScheduleToStartLatency.With(handler).Record(closeTime.Sub(scheduledTime))
	}
}

// recordScheduleToStartLatency emits the schedule-to-start latency when an async operation starts.
func (e taskExecutor) recordScheduleToStartLatency(op Operation, namespaceName, workflowType string, startedTime time.Time) {
	if op.ScheduledTime == nil {
		return
	}
	handler := e.operationMetricsHandler(op, namespaceName, workflowType)
	chasmnexus.NexusOperationScheduleToStartLatency.With(handler).Record(startedTime.Sub(op.ScheduledTime.AsTime()))
}
