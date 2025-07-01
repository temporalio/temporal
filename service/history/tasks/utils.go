package tasks

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

func Tags(
	task Task,
) []tag.Tag {
	// TODO: convert this to a method GetEventID on task interface
	// or remove this tag as the value is visible in the Task tag value.
	taskEventID := common.EmptyEventID
	taskEidOk := false
	taskCategory := task.GetCategory()
	switch taskCategory.ID() {
	case CategoryIDTransfer:
		taskEventID, taskEidOk = GetTransferTaskEventID(task)
	case CategoryIDTimer, CategoryIDMemoryTimer:
		taskEventID, taskEidOk = GetTimerTaskEventID(task)
	default:
		// no-op, other task categories don't have task eventID
	}

	if !taskEidOk {
		taskEventID = common.EmptyEventID
	}

	return []tag.Tag{
		tag.WorkflowNamespaceID(task.GetNamespaceID()),
		tag.WorkflowID(task.GetWorkflowID()),
		tag.WorkflowRunID(task.GetRunID()),
		tag.TaskKey(task.GetKey()),
		tag.TaskType(task.GetType()),
		tag.Task(task),
		tag.WorkflowEventID(taskEventID),
	}
}

// TODO: deprecate this method, use logger from executable.Logger() instead
func InitializeLogger(
	task Task,
	logger log.Logger,
) log.Logger {
	return log.With(
		logger,
		Tags(task)...,
	)
}

// GetChasmTaskEventID is a dummy getter for CHASM tasks, as Components don't have events.
func getChasmTaskEventID() (int64, bool) {
	return 0, false
}

func GetTransferTaskEventID(
	transferTask Task,
) (int64, bool) {
	eventID := int64(0)
	switch task := transferTask.(type) {
	case *ActivityTask:
		eventID = task.ScheduledEventID
	case *WorkflowTask:
		eventID = task.ScheduledEventID
	case *CloseExecutionTask:
		eventID = common.FirstEventID
	case *DeleteExecutionTask:
		eventID = common.FirstEventID
	case *CancelExecutionTask:
		eventID = task.InitiatedEventID
	case *SignalExecutionTask:
		eventID = task.InitiatedEventID
	case *StartChildExecutionTask:
		eventID = task.InitiatedEventID
	case *ResetWorkflowTask:
		eventID = common.FirstEventID
	case *ChasmTask:
		return getChasmTaskEventID()
	case *FakeTask:
		// no-op
	default:
		panic(serviceerror.NewInternal("unknown transfer task"))
	}
	return eventID, true
}

func GetTimerTaskEventID(
	timerTask Task,
) (int64, bool) {
	eventID := int64(0)

	switch task := timerTask.(type) {
	case *UserTimerTask:
		eventID = task.EventID
	case *ActivityTimeoutTask:
		eventID = task.EventID
	case *WorkflowTaskTimeoutTask:
		eventID = task.EventID
	case *WorkflowBackoffTimerTask:
		eventID = common.FirstEventID
	case *ActivityRetryTimerTask:
		eventID = task.EventID
	case *WorkflowRunTimeoutTask:
		eventID = common.FirstEventID
	case *WorkflowExecutionTimeoutTask:
		eventID = common.FirstEventID
	case *DeleteHistoryEventTask:
		// Retention task will be used by chasm framework as well,
		// and it doesn't depend on any particular state (thus eventID) of the run.
		return getChasmTaskEventID()
	case *StateMachineTimerTask:
		eventID = common.FirstEventID
	case *ChasmTaskPure:
		return getChasmTaskEventID()
	case *ChasmTask:
		return getChasmTaskEventID()
	case *FakeTask:
		// no-op
	default:
		panic(serviceerror.NewInternal("unknown timer task"))
	}
	return eventID, true
}
