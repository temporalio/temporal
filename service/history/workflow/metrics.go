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

	executionmetricsHandler := metricsHandler.WithTags(metrics.OperationTag(metrics.ExecutionStatsOperation), metrics.NamespaceTag(namespace.String()))
	executionmetricsHandler.Histogram(metrics.HistorySize.MetricName.String(), metrics.HistorySize.Unit).Record(int64(historySize))
	executionmetricsHandler.Histogram(metrics.HistoryCount.MetricName.String(), metrics.HistoryCount.Unit).Record(int64(historyCount))
}

func emitMutableStateStatus(
	metricsHandler metrics.Handler,
	stats *persistence.MutableStateStatistics,
) {
	if stats == nil {
		return
	}

	metricsHandler.Histogram(metrics.MutableStateSize.MetricName.String(), metrics.MutableStateSize.Unit).Record(int64(stats.TotalSize))
	metricsHandler.Histogram(metrics.ExecutionInfoSize.MetricName.String(), metrics.ExecutionInfoSize.Unit).Record(int64(stats.ExecutionInfoSize))
	metricsHandler.Histogram(metrics.ExecutionStateSize.MetricName.String(), metrics.ExecutionStateSize.Unit).Record(int64(stats.ExecutionStateSize))

	metricsHandler.Histogram(metrics.ActivityInfoSize.MetricName.String(), metrics.ActivityInfoSize.Unit).Record(int64(stats.ActivityInfoSize))
	metricsHandler.Histogram(metrics.ActivityInfoCount.MetricName.String(), metrics.ActivityInfoCount.Unit).Record(int64(stats.ActivityInfoCount))

	metricsHandler.Histogram(metrics.TimerInfoSize.MetricName.String(), metrics.TimerInfoSize.Unit).Record(int64(stats.TimerInfoSize))
	metricsHandler.Histogram(metrics.TimerInfoCount.MetricName.String(), metrics.TimerInfoCount.Unit).Record(int64(stats.TimerInfoCount))

	metricsHandler.Histogram(metrics.ChildInfoSize.MetricName.String(), metrics.ChildInfoSize.Unit).Record(int64(stats.ChildInfoSize))
	metricsHandler.Histogram(metrics.ChildInfoCount.MetricName.String(), metrics.ChildInfoCount.Unit).Record(int64(stats.ChildInfoCount))

	metricsHandler.Histogram(metrics.RequestCancelInfoSize.MetricName.String(), metrics.RequestCancelInfoSize.Unit).Record(int64(stats.RequestCancelInfoSize))
	metricsHandler.Histogram(metrics.RequestCancelInfoCount.MetricName.String(), metrics.RequestCancelInfoCount.Unit).Record(int64(stats.RequestCancelInfoCount))

	metricsHandler.Histogram(metrics.SignalInfoSize.MetricName.String(), metrics.SignalInfoSize.Unit).Record(int64(stats.SignalInfoSize))
	metricsHandler.Histogram(metrics.SignalInfoCount.MetricName.String(), metrics.SignalInfoCount.Unit).Record(int64(stats.SignalInfoCount))

	metricsHandler.Histogram(metrics.BufferedEventsSize.MetricName.String(), metrics.BufferedEventsSize.Unit).Record(int64(stats.BufferedEventsSize))
	metricsHandler.Histogram(metrics.BufferedEventsCount.MetricName.String(), metrics.BufferedEventsCount.Unit).Record(int64(stats.BufferedEventsCount))

	if stats.HistoryStatistics != nil {
		metricsHandler.Histogram(metrics.HistorySize.MetricName.String(), metrics.HistorySize.Unit).Record(int64(stats.HistoryStatistics.SizeDiff))
		metricsHandler.Histogram(metrics.HistoryCount.MetricName.String(), metrics.HistoryCount.Unit).Record(int64(stats.HistoryStatistics.CountDiff))
	}

	for category, taskCount := range stats.TaskCountByCategory {
		metricsHandler.Histogram(metrics.TaskCount.MetricName.String(), metrics.TaskCount.Unit).Record(
			int64(taskCount),
			metrics.TaskCategoryTag(category),
		)

	}
}

func emitWorkflowCompletionStats(
	metricsHandler metrics.Handler,
	namespace namespace.Name,
	taskQueue string,
	status enumspb.WorkflowExecutionStatus,
) {
	scope := metricsHandler.WithTags(
		metrics.OperationTag(metrics.WorkflowCompletionStatsOperation),
		metrics.NamespaceTag(namespace.String()),
		metrics.TaskQueueTag(taskQueue),
	)

	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		scope.Counter(metrics.WorkflowSuccessCount.MetricName.String()).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		scope.Counter(metrics.WorkflowCancelCount.MetricName.String()).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		scope.Counter(metrics.WorkflowFailedCount.MetricName.String()).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		scope.Counter(metrics.WorkflowTimeoutCount.MetricName.String()).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		scope.Counter(metrics.WorkflowTerminateCount.MetricName.String()).Record(1)
	case enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		scope.Counter(metrics.WorkflowContinuedAsNewCount.MetricName.String()).Record(1)
	}
}
