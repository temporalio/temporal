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
	historypb "go.temporal.io/api/history/v1"

	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

func emitWorkflowHistoryStats(
	metricsClient metrics.Client,
	namespace string,
	historySize int,
	historyCount int,
) {

	sizeScope := metricsClient.Scope(metrics.ExecutionSizeStatsScope, metrics.NamespaceTag(namespace))
	countScope := metricsClient.Scope(metrics.ExecutionCountStatsScope, metrics.NamespaceTag(namespace))

	sizeScope.RecordDistribution(metrics.HistorySize, historySize)
	countScope.RecordDistribution(metrics.HistoryCount, historyCount)
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

	sizeScope.RecordDistribution(metrics.HistorySize, int(executionInfoHistorySize))
	sizeScope.RecordDistribution(metrics.MutableStateSize, stats.MutableStateSize)
	sizeScope.RecordDistribution(metrics.ExecutionInfoSize, stats.MutableStateSize)
	sizeScope.RecordDistribution(metrics.ActivityInfoSize, stats.ActivityInfoSize)
	sizeScope.RecordDistribution(metrics.TimerInfoSize, stats.TimerInfoSize)
	sizeScope.RecordDistribution(metrics.ChildInfoSize, stats.ChildInfoSize)
	sizeScope.RecordDistribution(metrics.SignalInfoSize, stats.SignalInfoSize)
	sizeScope.RecordDistribution(metrics.BufferedEventsSize, stats.BufferedEventsSize)

	countScope.RecordDistribution(metrics.ActivityInfoCount, stats.ActivityInfoCount)
	countScope.RecordDistribution(metrics.TimerInfoCount, stats.TimerInfoCount)
	countScope.RecordDistribution(metrics.ChildInfoCount, stats.ChildInfoCount)
	countScope.RecordDistribution(metrics.SignalInfoCount, stats.SignalInfoCount)
	countScope.RecordDistribution(metrics.RequestCancelInfoCount, stats.RequestCancelInfoCount)
	countScope.RecordDistribution(metrics.BufferedEventsCount, stats.BufferedEventsCount)
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

	sizeScope.RecordDistribution(metrics.MutableStateSize, stats.MutableStateSize)
	sizeScope.RecordDistribution(metrics.ExecutionInfoSize, stats.ExecutionInfoSize)
	sizeScope.RecordDistribution(metrics.ActivityInfoSize, stats.ActivityInfoSize)
	sizeScope.RecordDistribution(metrics.TimerInfoSize, stats.TimerInfoSize)
	sizeScope.RecordDistribution(metrics.ChildInfoSize, stats.ChildInfoSize)
	sizeScope.RecordDistribution(metrics.SignalInfoSize, stats.SignalInfoSize)
	sizeScope.RecordDistribution(metrics.BufferedEventsSize, stats.BufferedEventsSize)

	countScope.RecordDistribution(metrics.ActivityInfoCount, stats.ActivityInfoCount)
	countScope.RecordDistribution(metrics.TimerInfoCount, stats.TimerInfoCount)
	countScope.RecordDistribution(metrics.ChildInfoCount, stats.ChildInfoCount)
	countScope.RecordDistribution(metrics.SignalInfoCount, stats.SignalInfoCount)
	countScope.RecordDistribution(metrics.RequestCancelInfoCount, stats.RequestCancelInfoCount)
	countScope.RecordDistribution(metrics.DeleteActivityInfoCount, stats.DeleteActivityInfoCount)
	countScope.RecordDistribution(metrics.DeleteTimerInfoCount, stats.DeleteTimerInfoCount)
	countScope.RecordDistribution(metrics.DeleteChildInfoCount, stats.DeleteChildInfoCount)
	countScope.RecordDistribution(metrics.DeleteSignalInfoCount, stats.DeleteSignalInfoCount)
	countScope.RecordDistribution(metrics.DeleteRequestCancelInfoCount, stats.DeleteRequestCancelInfoCount)
}

func emitWorkflowCompletionStats(
	metricsClient metrics.Client,
	namespace string,
	taskQueue string,
	event *historypb.HistoryEvent,
) {
	scope := metricsClient.Scope(
		metrics.WorkflowCompletionStatsScope,
		metrics.NamespaceTag(namespace),
		metrics.TaskQueueTag(taskQueue),
	)

	switch event.EventType {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		scope.IncCounter(metrics.WorkflowSuccessCount)
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		scope.IncCounter(metrics.WorkflowCancelCount)
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		scope.IncCounter(metrics.WorkflowFailedCount)
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		scope.IncCounter(metrics.WorkflowTimeoutCount)
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		scope.IncCounter(metrics.WorkflowTerminateCount)
	}
}
