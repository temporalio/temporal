package activity

import (
	"strconv"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/tqid"
)

func (a *Activity) taskScheduleToStartMetricsHandler(ctx chasm.Context) metrics.Handler {
	actCtx := activityContextFromChasm(ctx)
	namespaceEntry := ctx.NamespaceEntry()
	namespaceName := namespaceEntry.Name().String()
	taskQueue := a.GetTaskQueue().GetName()
	return metrics.GetPerTaskQueuePartitionTypeScope(
		a.baseMetricsHandler(ctx, metrics.HistoryRecordActivityTaskStartedScope),
		namespaceName,
		tqid.UnsafeTaskQueueFamily(namespaceEntry.ID().String(), taskQueue).
			TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY).
			RootPartition(),
		actCtx.config.BreakdownMetricsByTaskQueue(namespaceName, taskQueue, enumspb.TASK_QUEUE_TYPE_ACTIVITY),
	)
}

// baseMetricsHandler adds only the operation tag.
func (a *Activity) baseMetricsHandler(ctx chasm.Context, operation string) metrics.Handler {
	return ctx.MetricsHandler().WithTags(metrics.OperationTag(operation))
}

// enrichedMetricsHandler adds standard activity tags in addition to the operation tag.
func (a *Activity) enrichedMetricsHandler(ctx chasm.Context, operation string) metrics.Handler {
	namespaceName := ctx.NamespaceEntry().Name()
	// activityContextFromChasm panics if the context value is missing; this is intentional and
	// indicates a library registration bug rather than a runtime error.
	actCtx := activityContextFromChasm(ctx)
	breakdownMetricsByTaskQueue := actCtx.config.BreakdownMetricsByTaskQueue
	taskQueueFamily := a.GetTaskQueue().GetName()
	return metrics.GetPerTaskQueueFamilyScope(
		ctx.MetricsHandler(),
		namespaceName.String(),
		tqid.UnsafeTaskQueueFamily(namespaceName.String(), taskQueueFamily),
		breakdownMetricsByTaskQueue(namespaceName.String(), taskQueueFamily, enumspb.TASK_QUEUE_TYPE_ACTIVITY),
		metrics.OperationTag(operation),
		metrics.ActivityTypeTag(a.GetActivityType().GetName()),
		metrics.VersioningBehaviorTag(enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED),
		metrics.WorkflowTypeTag(WorkflowTypeTag),
	)
}

func (a *Activity) emitOnAttemptTimedOutMetrics(metricsHandler metrics.Handler, timeoutType enumspb.TimeoutType) {
	timeoutTag := metrics.StringTag("timeout_type", timeoutType.String())
	metrics.ActivityTaskTimeout.With(metricsHandler).Record(1, timeoutTag)
}

func (a *Activity) emitOnAttemptFailedMetrics(ctx chasm.Context, metricsHandler metrics.Handler) {
	attempt := a.LastAttempt.Get(ctx)
	startedTime := attempt.GetStartedTime().AsTime()

	latency := time.Since(startedTime)
	metrics.ActivityStartToCloseLatency.With(metricsHandler).Record(latency)

	metrics.ActivityTaskFail.With(metricsHandler).Record(1)
}

// emitPayloadSizeMetric records the serialized size of a user payload.
func emitPayloadSizeMetric(metricsHandler metrics.Handler, size int) {
	if size > 0 {
		metrics.ActivityPayloadSize.With(metricsHandler).Record(int64(size))
	}
}

// emitHeartbeatMetrics records the heartbeat count and payload size for heartbeat details.
func (a *Activity) emitHeartbeatMetrics(ctx chasm.Context, details *commonpb.Payloads) {
	metricsHandler := a.baseMetricsHandler(ctx, metrics.HistoryRecordActivityTaskHeartbeatScope)
	detailsSize := details.Size()
	emitPayloadSizeMetric(metricsHandler, detailsSize)
	metrics.ActivityHeartbeatCount.With(metricsHandler).Record(
		1,
		metrics.StringTag("has_details", strconv.FormatBool(detailsSize > 0)),
	)
}

func (a *Activity) emitOnCompletedMetrics(
	ctx chasm.Context,
	baseHandler metrics.Handler,
	enrichedHandler metrics.Handler,
	result *commonpb.Payloads,
	attemptWasStarted bool,
) {
	attempt := a.LastAttempt.Get(ctx)
	startedTime := attempt.GetStartedTime().AsTime()

	if attemptWasStarted {
		startToCloseLatency := time.Since(startedTime)
		metrics.ActivityStartToCloseLatency.With(enrichedHandler).Record(startToCloseLatency)
	}

	scheduleToCloseLatency := time.Since(a.GetScheduleTime().AsTime())
	metrics.ActivityScheduleToCloseLatency.With(enrichedHandler).Record(scheduleToCloseLatency)

	metrics.ActivitySuccess.With(enrichedHandler).Record(1)
	emitPayloadSizeMetric(baseHandler, result.Size())
}

func (a *Activity) emitOnFailedMetrics(
	ctx chasm.Context,
	baseHandler metrics.Handler,
	enrichedHandler metrics.Handler,
	failure *failurepb.Failure,
) {
	attempt := a.LastAttempt.Get(ctx)
	startedTime := attempt.GetStartedTime().AsTime()

	startToCloseLatency := time.Since(startedTime)
	metrics.ActivityStartToCloseLatency.With(enrichedHandler).Record(startToCloseLatency)

	scheduleToCloseLatency := time.Since(a.GetScheduleTime().AsTime())
	metrics.ActivityScheduleToCloseLatency.With(enrichedHandler).Record(scheduleToCloseLatency)

	metrics.ActivityTaskFail.With(enrichedHandler).Record(1)
	metrics.ActivityFail.With(enrichedHandler).Record(1)
	emitPayloadSizeMetric(baseHandler, failure.Size())
}

func (a *Activity) emitOnTerminatedMetrics(
	metricsHandler metrics.Handler,
) {
	// Terminated activities do not count as properly finished activities so we do not
	// record any of the latency metrics.
	metrics.ActivityTerminate.With(metricsHandler).Record(1)
}

func (a *Activity) emitOnCanceledMetrics(
	ctx chasm.Context,
	metricsHandler metrics.Handler,
	fromStatus activitypb.ActivityExecutionStatus,
) {
	// Record start-to-close latency only while an attempt is running. SCHEDULED (incl. retry
	// backoff) and PAUSED have no running attempt and a possibly-stale StartedTime.
	attemptRunning := fromStatus != activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED &&
		fromStatus != activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED
	if attemptRunning {
		if startedTime := a.LastAttempt.Get(ctx).GetStartedTime(); startedTime != nil {
			metrics.ActivityStartToCloseLatency.With(metricsHandler).Record(time.Since(startedTime.AsTime()))
		}
	}

	scheduleToCloseLatency := time.Since(a.GetScheduleTime().AsTime())
	metrics.ActivityScheduleToCloseLatency.With(metricsHandler).Record(scheduleToCloseLatency)

	metrics.ActivityCancel.With(metricsHandler).Record(1)
}

func (a *Activity) emitOnTimedOutMetrics(
	metricsHandler metrics.Handler,
	timeoutType enumspb.TimeoutType,
) {
	scheduleToCloseLatency := time.Since(a.GetScheduleTime().AsTime())
	metrics.ActivityScheduleToCloseLatency.With(metricsHandler).Record(scheduleToCloseLatency)

	timeoutTag := metrics.StringTag("timeout_type", timeoutType.String())
	metrics.ActivityTaskTimeout.With(metricsHandler).Record(1, timeoutTag)
	metrics.ActivityTimeout.With(metricsHandler).Record(1, timeoutTag)
}

func (a *Activity) emitOnPausedMetrics(
	metricsHandler metrics.Handler,
) {
	metrics.ActivityPause.With(metricsHandler).Record(1)
}

func (a *Activity) emitOnUpdateOptionsMetrics(
	metricsHandler metrics.Handler,
) {
	metrics.ActivityUpdateOptions.With(metricsHandler).Record(1)
}

func (a *Activity) emitOnUnpausedMetrics(
	metricsHandler metrics.Handler,
) {
	metrics.ActivityUnpause.With(metricsHandler).Record(1)
}

func (a *Activity) emitOnResetMetrics(
	metricsHandler metrics.Handler,
) {
	metrics.ActivityReset.With(metricsHandler).Record(1)
}
