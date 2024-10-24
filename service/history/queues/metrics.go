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
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tasks"
)

// TODO: task type tag value should be generated from enums.TaskType,
// but this is a non-trivial change, we will need to
// 1. Standardize existing naming in enums.TaskType definition
// 2. In release X, double emit metrics with both old and new values, with different tag names
// 3. Update all metrics dashboards & alerts to use new tag name & values
// 4. In release X+1, remove old tag name & values

func GetActiveTransferTaskTypeTagValue(
	task tasks.Task,
) string {
	switch task.(type) {
	case *tasks.ActivityTask:
		return metrics.TaskTypeTransferActiveTaskActivity
	case *tasks.WorkflowTask:
		return metrics.TaskTypeTransferActiveTaskWorkflowTask
	case *tasks.CloseExecutionTask:
		return metrics.TaskTypeTransferActiveTaskCloseExecution
	case *tasks.CancelExecutionTask:
		return metrics.TaskTypeTransferActiveTaskCancelExecution
	case *tasks.SignalExecutionTask:
		return metrics.TaskTypeTransferActiveTaskSignalExecution
	case *tasks.StartChildExecutionTask:
		return metrics.TaskTypeTransferActiveTaskStartChildExecution
	case *tasks.ResetWorkflowTask:
		return metrics.TaskTypeTransferActiveTaskResetWorkflow
	case *tasks.DeleteExecutionTask:
		return metrics.TaskTypeTransferActiveTaskDeleteExecution
	default:
		return "TransferActive" + task.GetType().String()
	}
}

func GetStandbyTransferTaskTypeTagValue(
	task tasks.Task,
) string {
	switch task.(type) {
	case *tasks.ActivityTask:
		return metrics.TaskTypeTransferStandbyTaskActivity
	case *tasks.WorkflowTask:
		return metrics.TaskTypeTransferStandbyTaskWorkflowTask
	case *tasks.CloseExecutionTask:
		return metrics.TaskTypeTransferStandbyTaskCloseExecution
	case *tasks.CancelExecutionTask:
		return metrics.TaskTypeTransferStandbyTaskCancelExecution
	case *tasks.SignalExecutionTask:
		return metrics.TaskTypeTransferStandbyTaskSignalExecution
	case *tasks.StartChildExecutionTask:
		return metrics.TaskTypeTransferStandbyTaskStartChildExecution
	case *tasks.ResetWorkflowTask:
		return metrics.TaskTypeTransferStandbyTaskResetWorkflow
	case *tasks.DeleteExecutionTask:
		return metrics.TaskTypeTransferStandbyTaskDeleteExecution
	default:
		return "TransferStandby" + task.GetType().String()
	}
}

func GetActiveTimerTaskTypeTagValue(
	task tasks.Task,
) string {
	switch t := task.(type) {
	case *tasks.WorkflowTaskTimeoutTask:
		if t.InMemory {
			return metrics.TaskTypeTimerActiveTaskSpeculativeWorkflowTaskTimeout
		}
		return metrics.TaskTypeTimerActiveTaskWorkflowTaskTimeout
	case *tasks.ActivityTimeoutTask:
		return metrics.TaskTypeTimerActiveTaskActivityTimeout
	case *tasks.UserTimerTask:
		return metrics.TaskTypeTimerActiveTaskUserTimer
	case *tasks.WorkflowRunTimeoutTask:
		return metrics.TaskTypeTimerActiveTaskWorkflowRunTimeout
	case *tasks.WorkflowExecutionTimeoutTask:
		return metrics.TaskTypeTimerActiveTaskWorkflowExecutionTimeout
	case *tasks.DeleteHistoryEventTask:
		return metrics.TaskTypeTimerActiveTaskDeleteHistoryEvent
	case *tasks.ActivityRetryTimerTask:
		return metrics.TaskTypeTimerActiveTaskActivityRetryTimer
	case *tasks.WorkflowBackoffTimerTask:
		return metrics.TaskTypeTimerActiveTaskWorkflowBackoffTimer
	default:
		return "TimerActive" + task.GetType().String()
	}
}

func GetStandbyTimerTaskTypeTagValue(
	task tasks.Task,
) string {
	switch task.(type) {
	case *tasks.WorkflowTaskTimeoutTask:
		return metrics.TaskTypeTimerStandbyTaskWorkflowTaskTimeout
	case *tasks.ActivityTimeoutTask:
		return metrics.TaskTypeTimerStandbyTaskActivityTimeout
	case *tasks.UserTimerTask:
		return metrics.TaskTypeTimerStandbyTaskUserTimer
	case *tasks.WorkflowRunTimeoutTask:
		return metrics.TaskTypeTimerStandbyTaskWorkflowRunTimeout
	case *tasks.WorkflowExecutionTimeoutTask:
		return metrics.TaskTypeTimerStandbyTaskWorkflowExecutionTimeout
	case *tasks.DeleteHistoryEventTask:
		return metrics.TaskTypeTimerStandbyTaskDeleteHistoryEvent
	case *tasks.ActivityRetryTimerTask:
		return metrics.TaskTypeTimerStandbyTaskActivityRetryTimer
	case *tasks.WorkflowBackoffTimerTask:
		return metrics.TaskTypeTimerStandbyTaskWorkflowBackoffTimer
	default:
		return "TimerStandby" + task.GetType().String()
	}
}

func GetVisibilityTaskTypeTagValue(
	task tasks.Task,
) string {
	switch task.(type) {
	case *tasks.StartExecutionVisibilityTask:
		return metrics.TaskTypeVisibilityTaskStartExecution
	case *tasks.UpsertExecutionVisibilityTask:
		return metrics.TaskTypeVisibilityTaskUpsertExecution
	case *tasks.CloseExecutionVisibilityTask:
		return metrics.TaskTypeVisibilityTaskCloseExecution
	case *tasks.DeleteExecutionVisibilityTask:
		return metrics.TaskTypeVisibilityTaskDeleteExecution
	default:
		return task.GetType().String()
	}
}

func GetArchivalTaskTypeTagValue(
	task tasks.Task,
) string {
	switch task.(type) {
	case *tasks.ArchiveExecutionTask:
		return metrics.TaskTypeArchivalTaskArchiveExecution
	default:
		return task.GetType().String()
	}
}

func GetOutboundTaskTypeTagValue(task tasks.Task, isActive bool) string {
	var prefix string
	if isActive {
		prefix = "OutboundActive"
	} else {
		prefix = "OutboundStandby"
	}

	outbound, ok := task.(*tasks.StateMachineOutboundTask)
	if !ok {
		return prefix + "Unknown"
	}
	return prefix + "." + outbound.StateMachineTaskType()
}

func GetTimerStateMachineTaskTypeTagValue(taskType string, isActive bool) string {
	var prefix string
	if isActive {
		prefix = "TimerActive"
	} else {
		prefix = "TimerStandby"
	}

	return prefix + "." + taskType
}

func getTaskTypeTagValue(
	executable Executable,
	isActive bool,
) string {
	task := executable.GetTask()
	switch task.GetCategory() {
	case tasks.CategoryTransfer:
		if isActive {
			return GetActiveTransferTaskTypeTagValue(task)
		}
		return GetStandbyTransferTaskTypeTagValue(task)
	case tasks.CategoryTimer:
		if isActive {
			return GetActiveTimerTaskTypeTagValue(task)
		}
		return GetStandbyTimerTaskTypeTagValue(task)
	case tasks.CategoryVisibility:
		return GetVisibilityTaskTypeTagValue(task)
	case tasks.CategoryArchival:
		return GetArchivalTaskTypeTagValue(task)
	case tasks.CategoryOutbound:
		return GetOutboundTaskTypeTagValue(task, isActive)
	default:
		return task.GetType().String()
	}
}
