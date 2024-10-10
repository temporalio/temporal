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
	stats *persistence.MutableStateStatistics,
	metricsHandler metrics.Handler,
	batchMetricHandler metrics.BatchMetricsHandler,
	tags ...metrics.Tag,
) {
	if stats == nil {
		return
	}

	batch := batchMetricHandler.CreateBatch("mutable_state_status", tags...)
	batch.WithHistogram(metrics.MutableStateSize, int64(stats.TotalSize))
	batch.WithHistogram(metrics.ExecutionInfoSize, int64(stats.ExecutionInfoSize))
	batch.WithHistogram(metrics.ExecutionStateSize, int64(stats.ExecutionStateSize))
	batch.WithHistogram(metrics.ActivityInfoSize, int64(stats.ActivityInfoSize))
	batch.WithHistogram(metrics.ActivityInfoCount, int64(stats.ActivityInfoCount))
	batch.WithHistogram(metrics.TotalActivityCount, stats.TotalActivityCount)
	batch.WithHistogram(metrics.TimerInfoSize, int64(stats.TimerInfoSize))
	batch.WithHistogram(metrics.TimerInfoCount, int64(stats.TimerInfoCount))
	batch.WithHistogram(metrics.TotalUserTimerCount, stats.TotalUserTimerCount)
	batch.WithHistogram(metrics.ChildInfoSize, int64(stats.ChildInfoSize))
	batch.WithHistogram(metrics.ChildInfoCount, int64(stats.ChildInfoCount))
	batch.WithHistogram(metrics.TotalChildExecutionCount, stats.TotalChildExecutionCount)
	batch.WithHistogram(metrics.RequestCancelInfoSize, int64(stats.RequestCancelInfoSize))
	batch.WithHistogram(metrics.RequestCancelInfoCount, int64(stats.RequestCancelInfoCount))
	batch.WithHistogram(metrics.TotalRequestCancelExternalCount, stats.TotalRequestCancelExternalCount)
	batch.WithHistogram(metrics.SignalInfoSize, int64(stats.SignalInfoSize))
	batch.WithHistogram(metrics.SignalInfoCount, int64(stats.SignalInfoCount))
	batch.WithHistogram(metrics.TotalSignalExternalCount, stats.TotalSignalExternalCount)
	batch.WithHistogram(metrics.SignalRequestIDSize, int64(stats.SignalRequestIDSize))
	batch.WithHistogram(metrics.SignalRequestIDCount, int64(stats.SignalRequestIDCount))
	batch.WithHistogram(metrics.TotalSignalCount, stats.TotalSignalCount)
	batch.WithHistogram(metrics.BufferedEventsSize, int64(stats.BufferedEventsSize))
	batch.WithHistogram(metrics.BufferedEventsCount, int64(stats.BufferedEventsCount))
	if stats.HistoryStatistics != nil {
		batch.WithHistogram(metrics.HistorySize, int64(stats.HistoryStatistics.SizeDiff))
		batch.WithHistogram(metrics.HistoryCount, int64(stats.HistoryStatistics.CountDiff))
	}
	batch.Emit()

	metricsHandler = metricsHandler.WithTags(tags...)
	for category, taskCount := range stats.TaskCountByCategory {
		metrics.TaskCount.With(metricsHandler).Record(int64(taskCount), metrics.TaskCategoryTag(category))
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
