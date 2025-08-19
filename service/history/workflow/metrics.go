package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/service/history/configs"
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
	stats *persistence.MutableStateStatistics,
) {
	if stats == nil {
		return
	}

	batchHandler := metricsHandler.StartBatch("mutable_state_status")
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
		metrics.HistorySize.With(batchHandler).Record(int64(stats.HistoryStatistics.SizeDiff))
		metrics.HistoryCount.With(batchHandler).Record(int64(stats.HistoryStatistics.CountDiff))
	}

	for category, taskCount := range stats.TaskCountByCategory {
		metrics.TaskCount.With(batchHandler).Record(int64(taskCount), metrics.TaskCategoryTag(category))
	}
}

func emitWorkflowCompletionStats(
	metricsHandler metrics.Handler,
	namespace namespace.Name,
	completion completionMetric,
	config *configs.Config,
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
			metrics.WorkflowDuration.With(handler).Record(closeTime.Sub(startTime))
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
