package workflow

import (
	"strconv"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func emitWorkflowHistoryStats(
	metricsHandler metrics.Handler,
	namespace namespace.Name,
	state enumsspb.WorkflowExecutionState,
	historySize int,
	historyCount int,
) {
	handler := metricsHandler.WithTags(metrics.NamespaceTag(namespace.String()))
	executionScope := handler.WithTags(metrics.OperationTag(metrics.ExecutionStatsScope))
	metrics.HistorySize.With(executionScope).Record(int64(historySize))
	metrics.HistoryCount.With(executionScope).Record(int64(historyCount))

	if state == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		completionScope := handler.WithTags(metrics.OperationTag(metrics.WorkflowCompletionStatsScope))
		metrics.HistorySize.With(completionScope).Record(int64(historySize))
		metrics.HistoryCount.With(completionScope).Record(int64(historyCount))
	}
}

func emitMutableStateStatus(
	metricsHandler metrics.Handler,
	chasmRegistry *chasm.Registry,
	archetypeID chasm.ArchetypeID,
	stats *persistence.MutableStateStatistics,
) {
	if stats == nil {
		return
	}

	mutableStateMetricsHandler := metricsHandler
	if archetypeTag, ok := getArchetypeMetricTag(chasmRegistry, archetypeID); ok {
		mutableStateMetricsHandler = mutableStateMetricsHandler.WithTags(archetypeTag)
	}

	batchHandler := mutableStateMetricsHandler.StartBatch("mutable_state_status")
	defer batchHandler.Close()
	metrics.MutableStateSize.With(batchHandler).Record(int64(stats.TotalSize))
	metrics.ExecutionInfoSize.With(batchHandler).Record(int64(stats.ExecutionInfoSize))
	metrics.ExecutionStateSize.With(batchHandler).Record(int64(stats.ExecutionStateSize))
	metrics.ActivityInfoSize.With(batchHandler).Record(int64(stats.ActivityInfoSize))
	metrics.ActivityInfoCount.With(batchHandler).Record(int64(stats.ActivityInfoCount))
	metrics.TotalActivityCount.With(batchHandler).Record(stats.TotalActivityCount)
	metrics.TimerInfoSize.With(batchHandler).Record(int64(stats.TimerInfoSize))
	metrics.TimerInfoCount.With(batchHandler).Record(int64(stats.TimerInfoCount))
	metrics.TotalUserTimerCount.With(batchHandler).Record(stats.TotalUserTimerCount)
	metrics.ChildInfoSize.With(batchHandler).Record(int64(stats.ChildInfoSize))
	metrics.ChildInfoCount.With(batchHandler).Record(int64(stats.ChildInfoCount))
	metrics.TotalChildExecutionCount.With(batchHandler).Record(stats.TotalChildExecutionCount)
	metrics.RequestCancelInfoSize.With(batchHandler).Record(int64(stats.RequestCancelInfoSize))
	metrics.RequestCancelInfoCount.With(batchHandler).Record(int64(stats.RequestCancelInfoCount))
	metrics.TotalRequestCancelExternalCount.With(batchHandler).Record(stats.TotalRequestCancelExternalCount)
	metrics.SignalInfoSize.With(batchHandler).Record(int64(stats.SignalInfoSize))
	metrics.SignalInfoCount.With(batchHandler).Record(int64(stats.SignalInfoCount))
	metrics.TotalSignalExternalCount.With(batchHandler).Record(stats.TotalSignalExternalCount)
	metrics.SignalRequestIDSize.With(batchHandler).Record(int64(stats.SignalRequestIDSize))
	metrics.SignalRequestIDCount.With(batchHandler).Record(int64(stats.SignalRequestIDCount))
	metrics.TotalSignalCount.With(batchHandler).Record(stats.TotalSignalCount)
	metrics.BufferedEventsSize.With(batchHandler).Record(int64(stats.BufferedEventsSize))
	metrics.BufferedEventsCount.With(batchHandler).Record(int64(stats.BufferedEventsCount))
	metrics.ChasmTotalSize.With(batchHandler).Record(int64(stats.ChasmTotalSize))

	if stats.HistoryStatistics != nil {
		metrics.HistorySize.With(metricsHandler).Record(int64(stats.HistoryStatistics.SizeDiff))
		metrics.HistoryCount.With(metricsHandler).Record(int64(stats.HistoryStatistics.CountDiff))
	}

	for category, taskCount := range stats.TaskCountByCategory {
		metrics.TaskCount.With(batchHandler).Record(int64(taskCount), metrics.TaskCategoryTag(category))
	}
}

func getArchetypeMetricTag(
	chasmRegistry *chasm.Registry,
	archetypeID chasm.ArchetypeID,
) (metrics.Tag, bool) {
	switch archetypeID {
	case chasm.UnspecifiedArchetypeID:
		return metrics.ArchetypeTag(""), true
	case chasm.WorkflowArchetypeID:
		return metrics.ArchetypeTag(chasm.WorkflowComponentName), true
	}

	if name, ok := chasmRegistry.ArchetypeDisplayName(archetypeID); ok {
		return metrics.ArchetypeTag(name), true
	}
	return metrics.ArchetypeTag(strconv.FormatUint(uint64(archetypeID), 10)), true
}

func emitWorkflowCompletionStats(
	metricsHandler metrics.Handler,
	namespace namespace.Name,
	completion completionMetric,
	config *configs.Config,
	mapperProvider searchattribute.MapperProvider,
) {
	// Only emit metrics for Workflows, not other Chasm archetypes
	if !completion.isWorkflow {
		return
	}

	handler := GetPerTaskQueueFamilyScope(metricsHandler, namespace, completion.taskQueue, config,
		metrics.OperationTag(metrics.WorkflowCompletionStatsScope),
		metrics.NamespaceStateTag(completion.namespaceState),
		metrics.WorkflowTypeTag(completion.workflowTypeName),
	)
	if saTags := searchAttributeMetricTags(config, mapperProvider, namespace, completion.searchAttributes); len(saTags) > 0 {
		handler = handler.WithTags(saTags...)
	}

	closed := true
	switch completion.status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		metrics.WorkflowSuccessCount.With(handler).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		metrics.WorkflowCancelCount.With(handler).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		metrics.WorkflowFailedCount.With(handler).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		metrics.WorkflowTimeoutCount.With(handler).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		metrics.WorkflowTerminateCount.With(handler).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		metrics.WorkflowContinuedAsNewCount.With(handler).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		closed = false
	}
	if closed && completion.startTime != nil && completion.closeTime != nil {
		startTime := completion.startTime.AsTime()
		closeTime := completion.closeTime.AsTime()
		if closeTime.After(startTime) {
			metrics.WorkflowScheduleToCloseLatency.With(handler).Record(closeTime.Sub(startTime))
		}
	}
}

func GetPerTaskQueueFamilyScope(
	handler metrics.Handler,
	namespaceName namespace.Name,
	taskQueueFamily string,
	config *configs.Config,
	tags ...metrics.Tag,
) metrics.Handler {
	return metrics.GetPerTaskQueueFamilyScope(handler,
		namespaceName.String(),
		tqid.UnsafeTaskQueueFamily(namespaceName.String(), taskQueueFamily),
		config.BreakdownMetricsByTaskQueue(namespaceName.String(), taskQueueFamily, enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		tags...,
	)
}

// Caps each SA-derived label value; every distinct value becomes a new metric series.
const maxSearchAttributeMetricTagLength = 256

func searchAttributeMetricTags(
	config *configs.Config,
	mapperProvider searchattribute.MapperProvider,
	namespaceName namespace.Name,
	searchAttributes map[string]*commonpb.Payload,
) []metrics.Tag {
	// Nil-safe: tests and non-standard constructors may build partial Configs.
	if config.SearchAttributeLabels == nil {
		return nil
	}
	keys := config.SearchAttributeLabels(namespaceName.String())
	if len(keys) == 0 || len(searchAttributes) == 0 {
		return nil
	}

	fields := aliasSearchAttributeFields(mapperProvider, namespaceName, searchAttributes)
	tags := make([]metrics.Tag, 0, len(keys))
	for _, key := range keys {
		p, ok := fields[key]
		if !ok {
			continue
		}
		value, ok := searchAttributeScalarValue(p)
		if !ok {
			continue
		}
		if len(value) > maxSearchAttributeMetricTagLength {
			value = string([]rune(value)[:maxSearchAttributeMetricTagLength])
		}
		tags = append(tags, metrics.StringTag(key, value))
	}
	return tags
}

// Maps storage field names (Keyword08) to user names for allowlist matching; raw names on failure.
func aliasSearchAttributeFields(
	mapperProvider searchattribute.MapperProvider,
	namespaceName namespace.Name,
	fields map[string]*commonpb.Payload,
) map[string]*commonpb.Payload {
	if mapperProvider == nil {
		return fields
	}
	aliased, err := searchattribute.AliasFields(
		mapperProvider,
		&commonpb.SearchAttributes{IndexedFields: fields},
		namespaceName.String(),
	)
	// Never let a mapping failure break the close path.
	if err != nil || aliased == nil {
		return fields
	}
	return aliased.GetIndexedFields()
}

// Decodes a JSON scalar from a mutable-state payload (which carries no type metadata).
func searchAttributeScalarValue(p *commonpb.Payload) (string, bool) {
	var value any
	if err := payload.Decode(p, &value); err != nil {
		return "", false
	}
	// Legacy SDK keyword encoding sends scalars as one-element lists.
	if list, ok := value.([]any); ok {
		if len(list) != 1 {
			return "", false
		}
		value = list[0]
	}
	switch value := value.(type) {
	case string:
		return value, value != ""
	case bool:
		return strconv.FormatBool(value), true
	case float64:
		return strconv.FormatFloat(value, 'g', -1, 64), true
	default: // lists, maps and other non-scalars are not useful labels
		return "", false
	}
}

type VersioningMetricContext struct {
	Behavior          enumspb.VersioningBehavior
	DeploymentVersion *deploymentspb.WorkerDeploymentVersion
}

type WorkflowTaskCompletionMetrics struct {
	VersioningInfo VersioningMetricContext
	Attempt        int32
}

func RecordWorkflowTaskCompletedMetrics(
	config *configs.Config,
	handler metrics.Handler,
	namespaceName namespace.Name,
	taskQueue string,
	completion WorkflowTaskCompletionMetrics,
) {
	metrics.WorkflowTasksCompleted.With(handler).Record(
		1,
		workflowTaskCompletionMetricTags(config, namespaceName, taskQueue, completion)...,
	)
}

func RecordWorkflowTaskFailedMetrics(
	config *configs.Config,
	handler metrics.Handler,
	namespaceName namespace.Name,
	taskQueue string,
	operation string,
	failure string,
	completion WorkflowTaskCompletionMetrics,
) {
	tags := workflowTaskCompletionMetricTags(config, namespaceName, taskQueue, completion)
	tags = append(tags, metrics.OperationTag(operation), metrics.FailureTag(failure))
	metrics.FailedWorkflowTasksCounter.With(handler).Record(1, tags...)
}

func workflowTaskCompletionMetricTags(
	config *configs.Config,
	namespaceName namespace.Name,
	taskQueue string,
	completion WorkflowTaskCompletionMetrics,
) []metrics.Tag {
	tags := []metrics.Tag{
		metrics.NamespaceTag(namespaceName.String()),
		metrics.FirstAttemptTag(completion.Attempt),
	}
	return append(tags, versioningMetricTags(
		config,
		namespaceName,
		taskQueue,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		completion.VersioningInfo,
	)...)
}

func versioningMetricTags(
	config *configs.Config,
	namespaceName namespace.Name,
	taskQueue string,
	taskQueueType enumspb.TaskQueueType,
	versioning VersioningMetricContext,
) []metrics.Tag {
	breakdownMetricsByBuildID := config.BreakdownMetricsByBuildID(
		namespaceName.String(),
		taskQueue,
		taskQueueType,
	)

	return []metrics.Tag{
		metrics.VersioningBehaviorTag(versioning.Behavior),
		metrics.WorkerDeploymentNameTag(versioning.DeploymentVersion.GetDeploymentName(), breakdownMetricsByBuildID),
		metrics.WorkerDeploymentBuildIDTag(versioning.DeploymentVersion.GetBuildId(), breakdownMetricsByBuildID),
	}
}

type ActivityExecutionStatus int

const (
	ActivityStatusUnknown ActivityExecutionStatus = iota
	ActivityStatusSucceeded
	ActivityStatusFailed
	ActivityStatusCanceled
	ActivityStatusTimeout
)

type ActivityCompletionMetrics struct {
	// Status determines whether the activity succeeded, and whether it is/will be retried
	Status ActivityExecutionStatus
	// AttemptStartedTime is the start time of the current attempt
	AttemptStartedTime time.Time
	// FirstScheduledTime is the scheduled time of the first attempt
	FirstScheduledTime time.Time
	// Closed is true if no more attempts will be made to execute the activity.
	Closed bool
	// TimerType is the type of timer that caused the activity execution to timeout.
	TimerType      enumspb.TimeoutType
	VersioningInfo VersioningMetricContext
}

func RecordActivityCompletionMetrics(
	shard historyi.ShardContext,
	namespaceName namespace.Name,
	taskQueue string,
	completion ActivityCompletionMetrics,
	tags ...metrics.Tag,
) {
	config := shard.GetConfig()
	tags = append(tags, versioningMetricTags(
		config,
		namespaceName,
		taskQueue,
		enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		completion.VersioningInfo,
	)...)
	metricsHandler := GetPerTaskQueueFamilyScope(
		shard.GetMetricsHandler(),
		namespaceName,
		taskQueue,
		config,
		tags...,
	)

	now := shard.GetTimeSource().Now()
	if completion.Status != ActivityStatusTimeout &&
		!completion.AttemptStartedTime.IsZero() &&
		!completion.AttemptStartedTime.After(now) {
		latency := now.Sub(completion.AttemptStartedTime)
		// ActivityE2ELatency is deprecated due to its inaccurate naming. It captures the attempt duration instead of an end-to-end duration as its name suggests. For now record both metrics
		metrics.ActivityE2ELatency.With(metricsHandler).Record(latency)
		metrics.ActivityStartToCloseLatency.With(metricsHandler).Record(latency)
	}

	// Record true end-to-end duration only for terminal states (includes retries and backoffs)
	if completion.Closed && !completion.FirstScheduledTime.IsZero() {
		scheduleToCloseLatency := now.Sub(completion.FirstScheduledTime)
		metrics.ActivityScheduleToCloseLatency.With(metricsHandler).Record(scheduleToCloseLatency)
	}

	switch completion.Status {
	case ActivityStatusFailed:
		metrics.ActivityTaskFail.With(metricsHandler).Record(1)
		if completion.Closed {
			metrics.ActivityFail.With(metricsHandler).Record(1)
		}
	case ActivityStatusCanceled:
		metrics.ActivityCancel.With(metricsHandler).Record(1)
	case ActivityStatusSucceeded:
		metrics.ActivitySuccess.With(metricsHandler).Record(1)
	case ActivityStatusTimeout:
		timeoutTag := metrics.StringTag(
			"timeout_type",
			completion.TimerType.String(),
		)
		metrics.ActivityTaskTimeout.With(metricsHandler).Record(1, timeoutTag)
		if completion.Closed {
			metrics.ActivityTimeout.With(metricsHandler).Record(1, timeoutTag)
		}
	default:
		// Do nothing
	}
}

// ActivityMetricsInfo captures activity metric tags until the mutation commits.
type ActivityMetricsInfo struct {
	namespaceName      string
	taskQueue          string
	activityType       string
	workflowType       string
	versioningBehavior enumspb.VersioningBehavior
}

// NewActivityMetricsInfo captures activity metric tags from mutable state.
func NewActivityMetricsInfo(
	mutableState historyi.MutableState,
	activityInfo *persistencespb.ActivityInfo,
) ActivityMetricsInfo {
	return ActivityMetricsInfo{
		namespaceName:      mutableState.GetNamespaceEntry().Name().String(),
		taskQueue:          activityInfo.GetTaskQueue(),
		activityType:       activityInfo.GetActivityType().GetName(),
		workflowType:       mutableState.GetWorkflowType().GetName(),
		versioningBehavior: mutableState.GetEffectiveVersioningBehavior(),
	}
}

// MetricsHandler returns a metrics handler with the captured activity tags.
func (i ActivityMetricsInfo) MetricsHandler(
	shardContext historyi.ShardContext,
	operation string,
) metrics.Handler {
	return metrics.GetPerActivityScope(
		shardContext.GetMetricsHandler(),
		i.namespaceName,
		tqid.UnsafeTaskQueueFamily(i.namespaceName, i.taskQueue),
		shardContext.GetConfig().BreakdownMetricsByTaskQueue(
			i.namespaceName,
			i.taskQueue,
			enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		),
		operation,
		i.activityType,
		i.workflowType,
		i.versioningBehavior,
	)
}
