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
	metrics.MutableStateSize.With(metricsHandler).Record(int64(stats.TotalSize))
	metrics.ExecutionInfoSize.With(metricsHandler).Record(int64(stats.ExecutionInfoSize))
	metrics.ExecutionStateSize.With(metricsHandler).Record(int64(stats.ExecutionStateSize))
	metrics.ActivityInfoSize.With(metricsHandler).Record(int64(stats.ActivityInfoSize))
	metrics.ActivityInfoCount.With(metricsHandler).Record(int64(stats.ActivityInfoCount))
	metrics.TotalActivityCount.With(metricsHandler).Record(stats.TotalActivityCount)
	metrics.TimerInfoSize.With(metricsHandler).Record(int64(stats.TimerInfoSize))
	metrics.TimerInfoCount.With(metricsHandler).Record(int64(stats.TimerInfoCount))
	metrics.TotalUserTimerCount.With(metricsHandler).Record(stats.TotalUserTimerCount)
	metrics.ChildInfoSize.With(metricsHandler).Record(int64(stats.ChildInfoSize))
	metrics.ChildInfoCount.With(metricsHandler).Record(int64(stats.ChildInfoCount))
	metrics.TotalChildExecutionCount.With(metricsHandler).Record(stats.TotalChildExecutionCount)
	metrics.RequestCancelInfoSize.With(metricsHandler).Record(int64(stats.RequestCancelInfoSize))
	metrics.RequestCancelInfoCount.With(metricsHandler).Record(int64(stats.RequestCancelInfoCount))
	metrics.TotalRequestCancelExternalCount.With(metricsHandler).Record(stats.TotalRequestCancelExternalCount)
	metrics.SignalInfoSize.With(metricsHandler).Record(int64(stats.SignalInfoSize))
	metrics.SignalInfoCount.With(metricsHandler).Record(int64(stats.SignalInfoCount))
	metrics.TotalSignalExternalCount.With(metricsHandler).Record(stats.TotalSignalExternalCount)
	metrics.SignalRequestIDSize.With(metricsHandler).Record(int64(stats.SignalRequestIDSize))
	metrics.SignalRequestIDCount.With(metricsHandler).Record(int64(stats.SignalRequestIDCount))
	metrics.TotalSignalCount.With(metricsHandler).Record(stats.TotalSignalCount)
	metrics.BufferedEventsSize.With(metricsHandler).Record(int64(stats.BufferedEventsSize))
	metrics.BufferedEventsCount.With(metricsHandler).Record(int64(stats.BufferedEventsCount))

	if stats.HistoryStatistics != nil {
		metrics.HistorySize.With(metricsHandler).Record(int64(stats.HistoryStatistics.SizeDiff))
		metrics.HistoryCount.With(metricsHandler).Record(int64(stats.HistoryStatistics.CountDiff))
	}

	for category, taskCount := range stats.TaskCountByCategory {
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
) {
	handler := metricsHandler.WithTags(
		metrics.OperationTag(metrics.WorkflowCompletionStatsScope),
		metrics.NamespaceTag(namespace.String()),
		metrics.NamespaceStateTag(namespaceState),
		metrics.TaskQueueTag(taskQueue),
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
