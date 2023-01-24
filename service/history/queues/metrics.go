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
	default:
		return ""
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
	default:
		return ""
	}
}

func GetActiveTimerTaskTypeTagValue(
	task tasks.Task,
) string {
	switch task.(type) {
	case *tasks.WorkflowTaskTimeoutTask:
		return metrics.TaskTypeTimerActiveTaskWorkflowTaskTimeout
	case *tasks.ActivityTimeoutTask:
		return metrics.TaskTypeTimerActiveTaskActivityTimeout
	case *tasks.UserTimerTask:
		return metrics.TaskTypeTimerActiveTaskUserTimer
	case *tasks.WorkflowTimeoutTask:
		return metrics.TaskTypeTimerActiveTaskWorkflowTimeout
	case *tasks.DeleteHistoryEventTask:
		return metrics.TaskTypeTimerActiveTaskDeleteHistoryEvent
	case *tasks.ActivityRetryTimerTask:
		return metrics.TaskTypeTimerActiveTaskActivityRetryTimer
	case *tasks.WorkflowBackoffTimerTask:
		return metrics.TaskTypeTimerActiveTaskWorkflowBackoffTimer
	default:
		return ""
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
	case *tasks.WorkflowTimeoutTask:
		return metrics.TaskTypeTimerStandbyTaskWorkflowTimeout
	case *tasks.DeleteHistoryEventTask:
		return metrics.TaskTypeTimerStandbyTaskDeleteHistoryEvent
	case *tasks.ActivityRetryTimerTask:
		return metrics.TaskTypeTimerStandbyTaskActivityRetryTimer
	case *tasks.WorkflowBackoffTimerTask:
		return metrics.TaskTypeTimerStandbyTaskWorkflowBackoffTimer
	default:
		return ""
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
		return ""
	}
}

func GetArchivalTaskTypeTagValue(
	task tasks.Task,
) string {
	switch task.(type) {
	case *tasks.ArchiveExecutionTask:
		return metrics.TaskTypeArchivalTaskArchiveExecution
	default:
		return ""
	}
}

func getTaskTypeTagValue(
	task tasks.Task,
	isActive bool,
) string {
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
	default:
		return task.GetType().String()
	}
}
