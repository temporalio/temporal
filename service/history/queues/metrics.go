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

package queues

import (
	"strings"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/service/history/tasks"
)

// TODO: Following is a copy of existing metrics scope and name definition
// for using the new MetricProvider interface. We should probably move them
// to a common place once all the metrics refactoring work is done.

// Metric names
const (
	TaskRequests                = "task_requests"
	TaskLatency                 = "task_latency"
	TaskFailures                = "task_errors"
	TaskDiscarded               = "task_errors_discarded"
	TaskSkipped                 = "task_skipped"
	TaskAttempt                 = "task_attempt"
	TaskStandbyRetryCounter     = "task_errors_standby_retry_counter"
	TaskWorkflowBusyCounter     = "task_errors_workflow_busy"
	TaskNotActiveCounter        = "task_errors_not_active_counter"
	TaskProcessingLatency       = "task_latency_processing"
	TaskNoUserProcessingLatency = "task_latency_processing_nouserlatency"
	TaskQueueLatency            = "task_latency_queue"
	TaskNoUserLatency           = "task_latency_nouserlatency"
	TaskUserLatency             = "task_latency_userlatency"
	TaskNoUserQueueLatency      = "task_latency_queue_nouserlatency"
	TaskReschedulerPendingTasks = "task_rescheduler_pending_tasks"
)

// Operation tag value for queue processors
const (
	OperationTimerActiveQueueProcessor     = "TimerActiveQueueProcessor"
	OperationTimerStandbyQueueProcessor    = "TimerStandbyQueueProcessor"
	OperationTransferActiveQueueProcessor  = "TransferActiveQueueProcessor"
	OperationTransferStandbyQueueProcessor = "TransferStandbyQueueProcessor"
	OperationVisibilityQueueProcessor      = "VisibilityQueueProcessor"
)

func GetActiveTransferTaskTypeTagValue(
	task tasks.Task,
) string {
	typeString := task.GetType().String()
	if task.GetType() == enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK {
		typeString = "TransferActivity"
	}

	return strings.Replace(typeString, "Transfer", "TransferActiveTask", 1)
}

func GetStandbyTransferTaskTypeTagValue(
	task tasks.Task,
) string {
	typeString := task.GetType().String()
	if task.GetType() == enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK {
		typeString = "TransferActivity"
	}

	return strings.Replace(typeString, "Transfer", "TransferStandbyTask", 1)
}

func GetActiveTimerTaskTypeTagValue(
	task tasks.Task,
) string {
	typeString := task.GetType().String()
	if task.GetType() == enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT {
		typeString = "WorkflowTimeout"
	}

	return "TimerActiveTask" + typeString
}

func GetStandbyTimerTaskTypeTagValue(
	task tasks.Task,
) string {
	typeString := task.GetType().String()
	if task.GetType() == enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT {
		typeString = "WorkflowTimeout"
	}

	return "TimerStandbyTask" + typeString
}

func GetVisibilityTaskTypeTagValue(
	task tasks.Task,
) string {
	return strings.Replace(task.GetType().String(), "Visibility", "VisibilityTask", 1)
}
