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

	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
)

func emitWorkflowHistoryStats(
	metricsHandler metrics.Handler,
	namespace namespace.Name,
	historySize int,
	historyCount int,
) {

	executionScope := metricsHandler.WithTags(metrics.OperationTag(metrics.ExecutionStatsScope), metrics.NamespaceTag(namespace.String()))
	executionScope.Histogram(metrics.HistorySize.GetMetricName(), metrics.HistorySize.GetMetricUnit()).Record(int64(historySize))
	executionScope.Histogram(metrics.HistoryCount.GetMetricName(), metrics.HistoryCount.GetMetricUnit()).Record(int64(historyCount))
}

func emitMutableStateStatus(
	metricsHandler metrics.Handler,
	stats *persistence.MutableStateStatistics,
) {
	if stats == nil {
		return
	}

	metricsHandler.Histogram(metrics.MutableStateSize.GetMetricName(), metrics.MutableStateSize.GetMetricUnit()).Record(int64(stats.TotalSize))
	metricsHandler.Histogram(metrics.ExecutionInfoSize.GetMetricName(), metrics.ExecutionInfoSize.GetMetricUnit()).Record(int64(stats.ExecutionInfoSize))
	metricsHandler.Histogram(metrics.ExecutionStateSize.GetMetricName(), metrics.ExecutionStateSize.GetMetricUnit()).Record(int64(stats.ExecutionStateSize))
	metricsHandler.Histogram(metrics.ActivityInfoSize.GetMetricName(), metrics.ActivityInfoSize.GetMetricUnit()).Record(int64(stats.ActivityInfoSize))
	metricsHandler.Histogram(metrics.ActivityInfoCount.GetMetricName(), metrics.ActivityInfoCount.GetMetricUnit()).Record(int64(stats.ActivityInfoCount))
	metricsHandler.Histogram(metrics.TotalActivityCount.GetMetricName(), metrics.TotalActivityCount.GetMetricUnit()).Record(stats.TotalActivityCount)
	metricsHandler.Histogram(metrics.TimerInfoSize.GetMetricName(), metrics.TimerInfoSize.GetMetricUnit()).Record(int64(stats.TimerInfoSize))
	metricsHandler.Histogram(metrics.TimerInfoCount.GetMetricName(), metrics.TimerInfoCount.GetMetricUnit()).Record(int64(stats.TimerInfoCount))
	metricsHandler.Histogram(metrics.TotalUserTimerCount.GetMetricName(), metrics.TotalUserTimerCount.GetMetricUnit()).Record(stats.TotalUserTimerCount)
	metricsHandler.Histogram(metrics.ChildInfoSize.GetMetricName(), metrics.ChildInfoSize.GetMetricUnit()).Record(int64(stats.ChildInfoSize))
	metricsHandler.Histogram(metrics.ChildInfoCount.GetMetricName(), metrics.ChildInfoCount.GetMetricUnit()).Record(int64(stats.ChildInfoCount))
	metricsHandler.Histogram(metrics.TotalChildExecutionCount.GetMetricName(), metrics.TotalChildExecutionCount.GetMetricUnit()).Record(stats.TotalChildExecutionCount)
	metricsHandler.Histogram(metrics.RequestCancelInfoSize.GetMetricName(), metrics.RequestCancelInfoSize.GetMetricUnit()).Record(int64(stats.RequestCancelInfoSize))
	metricsHandler.Histogram(metrics.RequestCancelInfoCount.GetMetricName(), metrics.RequestCancelInfoCount.GetMetricUnit()).Record(int64(stats.RequestCancelInfoCount))
	metricsHandler.Histogram(metrics.TotalRequestCancelExternalCount.GetMetricName(), metrics.TotalRequestCancelExternalCount.GetMetricUnit()).Record(stats.TotalRequestCancelExternalCount)
	metricsHandler.Histogram(metrics.SignalInfoSize.GetMetricName(), metrics.SignalInfoSize.GetMetricUnit()).Record(int64(stats.SignalInfoSize))
	metricsHandler.Histogram(metrics.SignalInfoCount.GetMetricName(), metrics.SignalInfoCount.GetMetricUnit()).Record(int64(stats.SignalInfoCount))
	metricsHandler.Histogram(metrics.TotalSignalExternalCount.GetMetricName(), metrics.TotalSignalExternalCount.GetMetricUnit()).Record(stats.TotalSignalExternalCount)
	metricsHandler.Histogram(metrics.SignalRequestIDSize.GetMetricName(), metrics.SignalRequestIDSize.GetMetricUnit()).Record(int64(stats.SignalRequestIDSize))
	metricsHandler.Histogram(metrics.SignalRequestIDCount.GetMetricName(), metrics.SignalRequestIDCount.GetMetricUnit()).Record(int64(stats.SignalRequestIDCount))
	metricsHandler.Histogram(metrics.TotalSignalCount.GetMetricName(), metrics.TotalSignalCount.GetMetricUnit()).Record(stats.TotalSignalCount)
	metricsHandler.Histogram(metrics.BufferedEventsSize.GetMetricName(), metrics.BufferedEventsSize.GetMetricUnit()).Record(int64(stats.BufferedEventsSize))
	metricsHandler.Histogram(metrics.BufferedEventsCount.GetMetricName(), metrics.BufferedEventsCount.GetMetricUnit()).Record(int64(stats.BufferedEventsCount))

	if stats.HistoryStatistics != nil {
		metricsHandler.Histogram(metrics.HistorySize.GetMetricName(), metrics.HistorySize.GetMetricUnit()).Record(int64(stats.HistoryStatistics.SizeDiff))
		metricsHandler.Histogram(metrics.HistoryCount.GetMetricName(), metrics.HistoryCount.GetMetricUnit()).Record(int64(stats.HistoryStatistics.CountDiff))

	}

	for category, taskCount := range stats.TaskCountByCategory {
		metricsHandler.Histogram(metrics.TaskCount.GetMetricName(), metrics.TaskCount.GetMetricUnit()).
			Record(int64(taskCount), metrics.TaskCategoryTag(category))
	}
}

func emitWorkflowCompletionStats(
	metricsHandler metrics.Handler,
	namespace namespace.Name,
	namespaceState string,
	taskQueue string,
	status enumspb.WorkflowExecutionStatus,
) {
	handler := metricsHandler.WithTags(
		metrics.OperationTag(metrics.WorkflowCompletionStatsScope),
		metrics.NamespaceTag(namespace.String()),
		metrics.NamespaceStateTag(namespaceState),
		metrics.TaskQueueTag(taskQueue),
	)

	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		handler.Counter(metrics.WorkflowSuccessCount.GetMetricName()).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		handler.Counter(metrics.WorkflowCancelCount.GetMetricName()).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		handler.Counter(metrics.WorkflowFailedCount.GetMetricName()).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		handler.Counter(metrics.WorkflowTimeoutCount.GetMetricName()).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		handler.Counter(metrics.WorkflowTerminateCount.GetMetricName()).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		handler.Counter(metrics.WorkflowContinuedAsNewCount.GetMetricName()).Record(1)
	}
}
