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
	executionScope.Histogram(metrics.HistorySize.Name(), metrics.HistorySize.Unit()).Record(int64(historySize))
	executionScope.Histogram(metrics.HistoryCount.Name(), metrics.HistoryCount.Unit()).Record(int64(historyCount))
}

func emitMutableStateStatus(
	metricsHandler metrics.Handler,
	stats *persistence.MutableStateStatistics,
) {
	if stats == nil {
		return
	}

	metricsHandler.Histogram(metrics.MutableStateSize.Name(), metrics.MutableStateSize.Unit()).Record(int64(stats.TotalSize))
	metricsHandler.Histogram(metrics.ExecutionInfoSize.Name(), metrics.ExecutionInfoSize.Unit()).Record(int64(stats.ExecutionInfoSize))
	metricsHandler.Histogram(metrics.ExecutionStateSize.Name(), metrics.ExecutionStateSize.Unit()).Record(int64(stats.ExecutionStateSize))
	metricsHandler.Histogram(metrics.ActivityInfoSize.Name(), metrics.ActivityInfoSize.Unit()).Record(int64(stats.ActivityInfoSize))
	metricsHandler.Histogram(metrics.ActivityInfoCount.Name(), metrics.ActivityInfoCount.Unit()).Record(int64(stats.ActivityInfoCount))
	metricsHandler.Histogram(metrics.TotalActivityCount.Name(), metrics.TotalActivityCount.Unit()).Record(stats.TotalActivityCount)
	metricsHandler.Histogram(metrics.TimerInfoSize.Name(), metrics.TimerInfoSize.Unit()).Record(int64(stats.TimerInfoSize))
	metricsHandler.Histogram(metrics.TimerInfoCount.Name(), metrics.TimerInfoCount.Unit()).Record(int64(stats.TimerInfoCount))
	metricsHandler.Histogram(metrics.TotalUserTimerCount.Name(), metrics.TotalUserTimerCount.Unit()).Record(stats.TotalUserTimerCount)
	metricsHandler.Histogram(metrics.ChildInfoSize.Name(), metrics.ChildInfoSize.Unit()).Record(int64(stats.ChildInfoSize))
	metricsHandler.Histogram(metrics.ChildInfoCount.Name(), metrics.ChildInfoCount.Unit()).Record(int64(stats.ChildInfoCount))
	metricsHandler.Histogram(metrics.TotalChildExecutionCount.Name(), metrics.TotalChildExecutionCount.Unit()).Record(stats.TotalChildExecutionCount)
	metricsHandler.Histogram(metrics.RequestCancelInfoSize.Name(), metrics.RequestCancelInfoSize.Unit()).Record(int64(stats.RequestCancelInfoSize))
	metricsHandler.Histogram(metrics.RequestCancelInfoCount.Name(), metrics.RequestCancelInfoCount.Unit()).Record(int64(stats.RequestCancelInfoCount))
	metricsHandler.Histogram(metrics.TotalRequestCancelExternalCount.Name(), metrics.TotalRequestCancelExternalCount.Unit()).Record(stats.TotalRequestCancelExternalCount)
	metricsHandler.Histogram(metrics.SignalInfoSize.Name(), metrics.SignalInfoSize.Unit()).Record(int64(stats.SignalInfoSize))
	metricsHandler.Histogram(metrics.SignalInfoCount.Name(), metrics.SignalInfoCount.Unit()).Record(int64(stats.SignalInfoCount))
	metricsHandler.Histogram(metrics.TotalSignalExternalCount.Name(), metrics.TotalSignalExternalCount.Unit()).Record(stats.TotalSignalExternalCount)
	metricsHandler.Histogram(metrics.SignalRequestIDSize.Name(), metrics.SignalRequestIDSize.Unit()).Record(int64(stats.SignalRequestIDSize))
	metricsHandler.Histogram(metrics.SignalRequestIDCount.Name(), metrics.SignalRequestIDCount.Unit()).Record(int64(stats.SignalRequestIDCount))
	metricsHandler.Histogram(metrics.TotalSignalCount.Name(), metrics.TotalSignalCount.Unit()).Record(stats.TotalSignalCount)
	metricsHandler.Histogram(metrics.BufferedEventsSize.Name(), metrics.BufferedEventsSize.Unit()).Record(int64(stats.BufferedEventsSize))
	metricsHandler.Histogram(metrics.BufferedEventsCount.Name(), metrics.BufferedEventsCount.Unit()).Record(int64(stats.BufferedEventsCount))

	if stats.HistoryStatistics != nil {
		metricsHandler.Histogram(metrics.HistorySize.Name(), metrics.HistorySize.Unit()).Record(int64(stats.HistoryStatistics.SizeDiff))
		metricsHandler.Histogram(metrics.HistoryCount.Name(), metrics.HistoryCount.Unit()).Record(int64(stats.HistoryStatistics.CountDiff))

	}

	for category, taskCount := range stats.TaskCountByCategory {
		metricsHandler.Histogram(metrics.TaskCount.Name(), metrics.TaskCount.Unit()).
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
		handler.Counter(metrics.WorkflowSuccessCount.Name()).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		handler.Counter(metrics.WorkflowCancelCount.Name()).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		handler.Counter(metrics.WorkflowFailedCount.Name()).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		handler.Counter(metrics.WorkflowTimeoutCount.Name()).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		handler.Counter(metrics.WorkflowTerminateCount.Name()).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		handler.Counter(metrics.WorkflowContinuedAsNewCount.Name()).Record(1)
	}
}
