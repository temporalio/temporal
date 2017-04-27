package matching

import (
	"github.com/uber-common/bark"
)

// This is duplicated
// TODO: refactor into common and matching specific parts

const (
	// HistoryBuilder events
	invalidHistoryActionEventID = 1000

	// Engine events
	persistentStoreErrorEventID      = 2000
	historySerializationErrorEventID = 2001
	duplicateTaskEventID             = 2002
)

const (
	// workflow logging tags
	tagWorkflowEventID      = "wf-event-id"
	tagWorkflowComponent    = "wf-component"
	tagWorkflowErr          = "wf-error"
	tagHistoryBuilderAction = "history-builder-action"
	tagStoreOperation       = "store-operation"
	tagWorkflowExecutionID  = "execution-id"
	tagWorkflowRunID        = "run-id"

	// task list tags
	tagTaskListType = "task-list-type"
	tagTaskListName = "task-list-name"

	// workflow logging tag values
	// tagWorkflowComponent Values
	tagValueHistoryBuilderComponent = "history-builder"
	tagValueWorkflowEngineComponent = "wf-engine"

	// tagHistoryBuilderAction values
	tagValueActionWorkflowStarted       = "add-workflowexecution-started-event"
	tagValueActionDecisionTaskScheduled = "add-decisiontask-scheduled-event"
	tagValueActionDecisionTaskStarted   = "add-decisiontask-started-event"
	tagValueActionDecisionTaskCompleted = "add-decisiontask-completed-event"
	tagValueActionDecisionTaskTimedOut  = "add-decisiontask-timedout-event"
	tagValueActionActivityTaskScheduled = "add-activitytask-scheduled-event"
	tagValueActionActivityTaskStarted   = "add-activitytask-started-event"
	tagValueActionActivityTaskCompleted = "add-activitytask-completed-event"
	tagValueActionActivityTaskTimedOut  = "add-activitytask-timedout-event"
	tagValueActionActivityTaskFailed    = "add-activitytask-failed-event"
	tagValueActionCompleteWorkflow      = "add-complete-workflow-event"
	tagValueActionFailWorkflow          = "add-fail-workflow-event"
	tagValueActionUnknownEvent          = "add-unknown-event"
	tagValueActionTimerStarted          = "add-timer-started-event"
	tagValueActionTimerFired            = "add-timer-fired-event"

	// tagStoreOperation values
	tagValueStoreOperationGetTasks       = "get-tasks"
	tagValueStoreOperationCreateTask     = "create-task"
	tagValueStoreOperationCompleteTask   = "complete-task"
	tagValueStoreOperationUpdateTaskList = "update-task-list"
	tagValueStoreOperationStopTaskList   = "stop-task-list"

	tagValueStoreOperationCreateWorkflowExecution = "create-wf-execution"
	tagValueStoreOperationGetWorkflowExecution    = "get-wf-execution"
	tagValueStoreOperationGetWorkflowMutableState = "get-wf-mutable-state"
	tagValueStoreOperationUpdateWorkflowExecution = "get-wf-execution"
	tagValueStoreOperationDeleteWorkflowExecution = "delete-wf-execution"
)

func logInvalidHistoryActionEvent(logger bark.Logger, action string, eventID int64, state string) {
	logger.WithFields(bark.Fields{
		tagWorkflowEventID:      invalidHistoryActionEventID,
		tagHistoryBuilderAction: action,
	}).Errorf("Invalid history builder state for action: EventID: %v, State: %v", eventID, state)
}

func logHistorySerializationErrorEvent(logger bark.Logger, err error, msg string) {
	logger.WithFields(bark.Fields{
		tagWorkflowEventID: historySerializationErrorEventID,
		tagWorkflowErr:     err,
	}).Errorf("Error serializing workflow execution history.  Msg: %v", msg)
}

func logPersistantStoreErrorEvent(logger bark.Logger, operation string, err error, details string) {
	logger.WithFields(bark.Fields{
		tagWorkflowEventID: persistentStoreErrorEventID,
		tagStoreOperation:  operation,
		tagWorkflowErr:     err,
	}).Errorf("Persistent store operation failed. Operation Details: %v", details)
}

func logDuplicateTaskEvent(logger bark.Logger, taskType int, taskID int64, scheduleID, startedID int64,
	isRunning bool) {
	logger.WithFields(bark.Fields{
		tagWorkflowEventID: duplicateTaskEventID,
	}).Debugf("Potentially duplicate task.  TaskID: %v, TaskType: %v, scheduleID: %v, startedID: %v, isRunning: %v",
		taskID, taskType, scheduleID, startedID, isRunning)
}
