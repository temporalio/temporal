package queues

import (
	"fmt"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tasks"
)

// TODO: task type tag value should be generated from enums.TaskType,
// but this is a non-trivial change, we will need to
// 1. Standardize existing naming in enums.TaskType definition
// 2. In release X, double emit metrics with both old and new values, with different tag names
// 3. Update all metrics dashboards & alerts to use new tag name & values
// 4. In release X+1, remove old tag name & values

func getCHASMTaskTypeTagValue(
	t *tasks.ChasmTask,
	chasmRegistry *chasm.Registry,
) string {
	taskFqn, ok := chasmRegistry.TaskFqnByID(t.Info.TypeId)
	if !ok {
		taskFqn = fmt.Sprintf("UnknownChasmTaskType: %d", t.Info.TypeId)
	}
	return taskFqn
}

func GetActiveTransferTaskTypeTagValue(
	task tasks.Task,
	chasmRegistry *chasm.Registry,
) string {
	prefix := "TransferActive"
	switch t := task.(type) {
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
	case *tasks.ChasmTask:
		return prefix + "." + getCHASMTaskTypeTagValue(t, chasmRegistry)
	default:
		return prefix + task.GetType().String()
	}
}

func GetStandbyTransferTaskTypeTagValue(
	task tasks.Task,
	chasmRegistry *chasm.Registry,
) string {
	prefix := "TransferStandby"
	switch t := task.(type) {
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
	case *tasks.ChasmTask:
		return prefix + "." + getCHASMTaskTypeTagValue(t, chasmRegistry)
	default:
		return prefix + task.GetType().String()
	}
}

func GetActiveTimerTaskTypeTagValue(
	task tasks.Task,
	chasmRegistry *chasm.Registry,
) string {
	prefix := "TimerActive"
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
	case *tasks.ChasmTask:
		return prefix + "." + getCHASMTaskTypeTagValue(t, chasmRegistry)
	case *tasks.ChasmTaskPure:
		return metrics.TaskTypeTimerActiveTaskChasmPureTask
	default:
		return prefix + task.GetType().String()
	}
}

func GetStandbyTimerTaskTypeTagValue(
	task tasks.Task,
	chasmRegistry *chasm.Registry,
) string {
	prefix := "TimerStandby"
	switch t := task.(type) {
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
	case *tasks.ChasmTask:
		return prefix + "." + getCHASMTaskTypeTagValue(t, chasmRegistry)
	case *tasks.ChasmTaskPure:
		return metrics.TaskTypeTimerStandbyTaskChasmPureTask
	default:
		return prefix + task.GetType().String()
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
	case *tasks.ChasmTask:
		return metrics.TaskTypeVisibilityTaskUpsertChasmExecution
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

func GetOutboundTaskTypeTagValue(
	task tasks.Task,
	isActive bool,
	chasmRegistry *chasm.Registry,
) string {
	var prefix string
	if isActive {
		prefix = "OutboundActive"
	} else {
		prefix = "OutboundStandby"
	}

	switch task := task.(type) {
	case *tasks.StateMachineOutboundTask:
		return prefix + "." + task.StateMachineTaskType()
	case *tasks.ChasmTask:
		return prefix + "." + getCHASMTaskTypeTagValue(task, chasmRegistry)
	default:
		return prefix + "Unknown"
	}
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

func GetTaskTypeTagValue(
	task tasks.Task,
	isActive bool,
	chasmRegistry *chasm.Registry,
) string {
	switch task.GetCategory() {
	case tasks.CategoryTransfer:
		if isActive {
			return GetActiveTransferTaskTypeTagValue(task, chasmRegistry)
		}
		return GetStandbyTransferTaskTypeTagValue(task, chasmRegistry)
	case tasks.CategoryTimer:
		if isActive {
			return GetActiveTimerTaskTypeTagValue(task, chasmRegistry)
		}
		return GetStandbyTimerTaskTypeTagValue(task, chasmRegistry)
	case tasks.CategoryVisibility:
		return GetVisibilityTaskTypeTagValue(task)
	case tasks.CategoryArchival:
		return GetArchivalTaskTypeTagValue(task)
	case tasks.CategoryOutbound:
		return GetOutboundTaskTypeTagValue(task, isActive, chasmRegistry)
	default:
		return task.GetType().String()
	}
}
