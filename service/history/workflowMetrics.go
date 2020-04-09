package history

import (
	"time"

	eventpb "go.temporal.io/temporal-proto/event"

	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
)

func emitWorkflowHistoryStats(
	metricsClient metrics.Client,
	namespace string,
	historySize int,
	historyCount int,
) {

	sizeScope := metricsClient.Scope(metrics.ExecutionSizeStatsScope, metrics.NamespaceTag(namespace))
	countScope := metricsClient.Scope(metrics.ExecutionCountStatsScope, metrics.NamespaceTag(namespace))

	sizeScope.RecordTimer(metrics.HistorySize, time.Duration(historySize))
	countScope.RecordTimer(metrics.HistoryCount, time.Duration(historyCount))
}

func emitWorkflowExecutionStats(
	metricsClient metrics.Client,
	namespace string,
	stats *persistence.MutableStateStats,
	executionInfoHistorySize int64,
) {

	if stats == nil {
		return
	}

	sizeScope := metricsClient.Scope(metrics.ExecutionSizeStatsScope, metrics.NamespaceTag(namespace))
	countScope := metricsClient.Scope(metrics.ExecutionCountStatsScope, metrics.NamespaceTag(namespace))

	sizeScope.RecordTimer(metrics.HistorySize, time.Duration(executionInfoHistorySize))
	sizeScope.RecordTimer(metrics.MutableStateSize, time.Duration(stats.MutableStateSize))
	sizeScope.RecordTimer(metrics.ExecutionInfoSize, time.Duration(stats.MutableStateSize))
	sizeScope.RecordTimer(metrics.ActivityInfoSize, time.Duration(stats.ActivityInfoSize))
	sizeScope.RecordTimer(metrics.TimerInfoSize, time.Duration(stats.TimerInfoSize))
	sizeScope.RecordTimer(metrics.ChildInfoSize, time.Duration(stats.ChildInfoSize))
	sizeScope.RecordTimer(metrics.SignalInfoSize, time.Duration(stats.SignalInfoSize))
	sizeScope.RecordTimer(metrics.BufferedEventsSize, time.Duration(stats.BufferedEventsSize))

	countScope.RecordTimer(metrics.ActivityInfoCount, time.Duration(stats.ActivityInfoCount))
	countScope.RecordTimer(metrics.TimerInfoCount, time.Duration(stats.TimerInfoCount))
	countScope.RecordTimer(metrics.ChildInfoCount, time.Duration(stats.ChildInfoCount))
	countScope.RecordTimer(metrics.SignalInfoCount, time.Duration(stats.SignalInfoCount))
	countScope.RecordTimer(metrics.RequestCancelInfoCount, time.Duration(stats.RequestCancelInfoCount))
	countScope.RecordTimer(metrics.BufferedEventsCount, time.Duration(stats.BufferedEventsCount))
}

func emitSessionUpdateStats(
	metricsClient metrics.Client,
	namespace string,
	stats *persistence.MutableStateUpdateSessionStats,
) {

	if stats == nil {
		return
	}

	sizeScope := metricsClient.Scope(metrics.SessionSizeStatsScope, metrics.NamespaceTag(namespace))
	countScope := metricsClient.Scope(metrics.SessionCountStatsScope, metrics.NamespaceTag(namespace))

	sizeScope.RecordTimer(metrics.MutableStateSize, time.Duration(stats.MutableStateSize))
	sizeScope.RecordTimer(metrics.ExecutionInfoSize, time.Duration(stats.ExecutionInfoSize))
	sizeScope.RecordTimer(metrics.ActivityInfoSize, time.Duration(stats.ActivityInfoSize))
	sizeScope.RecordTimer(metrics.TimerInfoSize, time.Duration(stats.TimerInfoSize))
	sizeScope.RecordTimer(metrics.ChildInfoSize, time.Duration(stats.ChildInfoSize))
	sizeScope.RecordTimer(metrics.SignalInfoSize, time.Duration(stats.SignalInfoSize))
	sizeScope.RecordTimer(metrics.BufferedEventsSize, time.Duration(stats.BufferedEventsSize))

	countScope.RecordTimer(metrics.ActivityInfoCount, time.Duration(stats.ActivityInfoCount))
	countScope.RecordTimer(metrics.TimerInfoCount, time.Duration(stats.TimerInfoCount))
	countScope.RecordTimer(metrics.ChildInfoCount, time.Duration(stats.ChildInfoCount))
	countScope.RecordTimer(metrics.SignalInfoCount, time.Duration(stats.SignalInfoCount))
	countScope.RecordTimer(metrics.RequestCancelInfoCount, time.Duration(stats.RequestCancelInfoCount))
	countScope.RecordTimer(metrics.DeleteActivityInfoCount, time.Duration(stats.DeleteActivityInfoCount))
	countScope.RecordTimer(metrics.DeleteTimerInfoCount, time.Duration(stats.DeleteTimerInfoCount))
	countScope.RecordTimer(metrics.DeleteChildInfoCount, time.Duration(stats.DeleteChildInfoCount))
	countScope.RecordTimer(metrics.DeleteSignalInfoCount, time.Duration(stats.DeleteSignalInfoCount))
	countScope.RecordTimer(metrics.DeleteRequestCancelInfoCount, time.Duration(stats.DeleteRequestCancelInfoCount))
}

func emitWorkflowCompletionStats(
	metricsClient metrics.Client,
	namespace string,
	taskList string,
	event *eventpb.HistoryEvent,
) {
	scope := metricsClient.Scope(
		metrics.WorkflowCompletionStatsScope,
		metrics.NamespaceTag(namespace),
		metrics.TaskListTag(taskList),
	)

	switch event.EventType {
	case eventpb.EventType_WorkflowExecutionCompleted:
		scope.IncCounter(metrics.WorkflowSuccessCount)
	case eventpb.EventType_WorkflowExecutionCanceled:
		scope.IncCounter(metrics.WorkflowCancelCount)
	case eventpb.EventType_WorkflowExecutionFailed:
		scope.IncCounter(metrics.WorkflowFailedCount)
	case eventpb.EventType_WorkflowExecutionTimedOut:
		scope.IncCounter(metrics.WorkflowTimeoutCount)
	case eventpb.EventType_WorkflowExecutionTerminated:
		scope.IncCounter(metrics.WorkflowTerminateCount)
	}
}
