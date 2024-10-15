// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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

	if stats.HistoryStatistics != nil {
		metrics.HistorySize.With(batchHandler).Record(int64(stats.HistoryStatistics.SizeDiff))
		metrics.HistoryCount.With(batchHandler).Record(int64(stats.HistoryStatistics.CountDiff))
	}

	for category, taskCount := range stats.TaskCountByCategory {
		// We use the metricsHandler rather than the batchHandler here because the same metric is repeatedly sent
		metrics.TaskCount.With(metricsHandler).
			Record(int64(taskCount), metrics.TaskCategoryTag(category))
	}
}

func emitWorkflowCompletionStats(
	metricsHandler metrics.Handler,
	namespace namespace.Name,
	namespaceState string,
	taskQueue string,
	status enumspb.WorkflowExecutionStatus,
	config *configs.Config,
) {
	handler := GetPerTaskQueueFamilyScope(metricsHandler, namespace, taskQueue, config,
		metrics.OperationTag(metrics.WorkflowCompletionStatsScope),
		metrics.NamespaceStateTag(namespaceState),
	)

	switch status {
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
