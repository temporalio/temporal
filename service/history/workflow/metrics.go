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
	metricsClient metrics.Client,
	namespace namespace.Name,
	historySize int,
	historyCount int,
) {

	executionScope := metricsClient.Scope(metrics.ExecutionStatsScope, metrics.NamespaceTag(namespace.String()))
	executionScope.RecordDistribution(metrics.HistorySize, historySize)
	executionScope.RecordDistribution(metrics.HistoryCount, historyCount)
}

func emitMutableStateStatus(
	scope metrics.Scope,
	stats *persistence.MutableStateStatistics,
) {
	if stats == nil {
		return
	}

	scope.RecordDistribution(metrics.MutableStateSize, stats.TotalSize)
	scope.RecordDistribution(metrics.ExecutionInfoSize, stats.ExecutionInfoSize)
	scope.RecordDistribution(metrics.ExecutionStateSize, stats.ExecutionStateSize)

	scope.RecordDistribution(metrics.ActivityInfoSize, stats.ActivityInfoSize)
	scope.RecordDistribution(metrics.ActivityInfoCount, stats.ActivityInfoCount)

	scope.RecordDistribution(metrics.TimerInfoSize, stats.TimerInfoSize)
	scope.RecordDistribution(metrics.TimerInfoCount, stats.TimerInfoCount)

	scope.RecordDistribution(metrics.ChildInfoSize, stats.ChildInfoSize)
	scope.RecordDistribution(metrics.ChildInfoCount, stats.ChildInfoCount)

	scope.RecordDistribution(metrics.RequestCancelInfoSize, stats.RequestCancelInfoSize)
	scope.RecordDistribution(metrics.RequestCancelInfoCount, stats.RequestCancelInfoCount)

	scope.RecordDistribution(metrics.SignalInfoSize, stats.SignalInfoSize)
	scope.RecordDistribution(metrics.SignalInfoCount, stats.SignalInfoCount)

	scope.RecordDistribution(metrics.BufferedEventsSize, stats.BufferedEventsSize)
	scope.RecordDistribution(metrics.BufferedEventsCount, stats.BufferedEventsCount)

	if stats.HistoryStatistics != nil {
		scope.RecordDistribution(metrics.HistorySize, stats.HistoryStatistics.SizeDiff)
		scope.RecordDistribution(metrics.HistoryCount, stats.HistoryStatistics.CountDiff)
	}

	for category, taskCount := range stats.TaskCountByCategory {
		scope.Tagged(metrics.TaskCategoryTag(category)).RecordDistribution(metrics.TaskCount, taskCount)
	}
}

func emitWorkflowCompletionStats(
	metricsClient metrics.Client,
	namespace namespace.Name,
	taskQueue string,
	status enumspb.WorkflowExecutionStatus,
) {
	scope := metricsClient.Scope(
		metrics.WorkflowCompletionStatsScope,
		metrics.NamespaceTag(namespace.String()),
		metrics.TaskQueueTag(taskQueue),
	)

	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		scope.IncCounter(metrics.WorkflowSuccessCount)
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		scope.IncCounter(metrics.WorkflowCancelCount)
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		scope.IncCounter(metrics.WorkflowFailedCount)
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		scope.IncCounter(metrics.WorkflowTimeoutCount)
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		scope.IncCounter(metrics.WorkflowTerminateCount)
	case enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		scope.IncCounter(metrics.WorkflowContinuedAsNewCount)
	}
}
