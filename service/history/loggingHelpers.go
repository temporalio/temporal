package history

import (
	"github.com/uber-common/bark"
)

// This is duplicated
// TODO: refactor into common and history specific parts

const (
	// HistoryBuilder events
	invalidHistoryActionEventID = 1000

	// Engine events
	persistentStoreErrorEventID      = 2000
	historySerializationErrorEventID = 2001
	duplicateTaskEventID             = 2002

	// Shard lifecycle events
	shardRangeUpdatedEventID = 3000

	// ShardController events
	shardControllerStarted          = 4000
	shardControllerShutdown         = 4001
	shardControllerShuttingDown     = 4002
	shardControllerShutdownTimedout = 4003
	ringMembershipChangedEvent      = 4004
	shardItemCreated                = 4010
	shardItemRemoved                = 4011
	shardEngineCreating             = 4020
	shardEngineCreated              = 4021
	shardEngineStopping             = 4022
	shardEngineStopped              = 4023

	// General purpose events
	operationFailed = 9000
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
	tagHistoryShardID       = "shard-id"

	// workflow logging tag values
	// tagWorkflowComponent Values
	tagValueHistoryBuilderComponent = "history-builder"
	tagValueHistoryEngineComponent  = "history-engine"
	tagValueTransferQueueComponent  = "transfer-queue-processor"
	tagValueTimerQueueComponent     = "timer-queue-processor"
	tagValueShardController         = "shard-controller"

	// tagHistoryBuilderAction values
	tagValueActionWorkflowStarted       = "add-workflowexecution-started-event"
	tagValueActionDecisionTaskScheduled = "add-decisiontask-scheduled-event"
	tagValueActionDecisionTaskStarted   = "add-decisiontask-started-event"
	tagValueActionDecisionTaskCompleted = "add-decisiontask-completed-event"
	tagValueActionDecisionTaskTimedOut  = "add-decisiontask-timedout-event"
	tagValueActionActivityTaskScheduled = "add-activitytask-scheduled-event"
	tagValueActionActivityTaskStarted   = "add-activitytask-started-event"
	tagValueActionActivityTaskCompleted = "add-activitytask-completed-event"
	tagValueActionActivityTaskFailed    = "add-activitytask-failed-event"
	tagValueActionActivityTaskTimedOut  = "add-activitytask-timed-event"
	tagValueActionCompleteWorkflow      = "add-complete-workflow-event"
	tagValueActionFailWorkflow          = "add-fail-workflow-event"
	tagValueActionUnknownEvent          = "add-unknown-event"
	tagValueActionTimerStarted          = "add-timer-started-event"
	tagValueActionTimerFired            = "add-timer-fired-event"

	// tagStoreOperation values
	tagValueStoreOperationGetTasks                = "get-tasks"
	tagValueStoreOperationCompleteTask            = "complete-task"
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
	}).Warnf("Invalid history builder state for action: EventID: %v, State: %v", eventID, state)
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
	}).Errorf("Persistent store operation failure. Operation Details: %v", details)
}

func logDuplicateTaskEvent(lg bark.Logger, taskType int, taskID int64, requestID string, scheduleID, startedID int64,
	isRunning bool) {
	lg.WithFields(bark.Fields{
		tagWorkflowEventID: duplicateTaskEventID,
	}).Debugf("Potentially duplicate task.  TaskID: %v, TaskType: %v, RequestID: %v, scheduleID: %v, startedID: %v, isRunning: %v",
		taskID, taskType, requestID, scheduleID, startedID, isRunning)
}

func logShardRangeUpdatedEvent(logger bark.Logger, shardID int, rangeID, startSequence, endSequence int64) {
	logger.WithFields(bark.Fields{
		tagWorkflowEventID: shardRangeUpdatedEventID,
	})
	logger.Infof("Range updated for shardID '%v'.  RangeID: %v, StartSequence: %v, EndSequence: %v", shardID,
		rangeID, startSequence, endSequence)
}

func logShardControllerStartedEvent(logger bark.Logger, host string) {
	logger.WithFields(bark.Fields{
		tagWorkflowEventID: shardControllerStarted,
	})
	logger.Infof("ShardController started on host: %v", host)
}

func logShardControllerShutdownEvent(logger bark.Logger, host string) {
	logger.WithFields(bark.Fields{
		tagWorkflowEventID: shardControllerShutdown,
	})
	logger.Infof("ShardController stopped on host: %v", host)
}

func logShardControllerShuttingDownEvent(logger bark.Logger, host string) {
	logger.WithFields(bark.Fields{
		tagWorkflowEventID: shardControllerShuttingDown,
	})
	logger.Infof("ShardController stopping on host: %v", host)
}

func logShardControllerShutdownTimedoutEvent(logger bark.Logger, host string) {
	logger.WithFields(bark.Fields{
		tagWorkflowEventID: shardControllerShutdownTimedout,
	})
	logger.Warnf("ShardController timed out during shutdown on host: %v", host)
}

func logRingMembershipChangedEvent(logger bark.Logger, host string, added, removed, updated int) {
	logger.WithFields(bark.Fields{
		tagWorkflowEventID: ringMembershipChangedEvent,
	})
	logger.Infof("ShardController on host '%v' received ring membership changed event: {Added: %v, Removed: %v, Updated: %v}",
		host, added, removed, updated)
}

func logShardItemCreatedEvent(logger bark.Logger, host string, shardID int) {
	logger.WithFields(bark.Fields{
		tagWorkflowEventID: shardItemCreated,
		tagHistoryShardID:  shardID,
	})
	logger.Infof("ShardController on host '%v' created a shard item for shardID '%v'.", host, shardID)
}

func logShardItemRemovedEvent(logger bark.Logger, host string, shardID int, remainingShards int) {
	logger.WithFields(bark.Fields{
		tagWorkflowEventID: shardItemRemoved,
		tagHistoryShardID:  shardID,
	})
	logger.Infof("ShardController on host '%v' removed shard item for shardID '%v'.  Remaining number of shards: %v",
		host, shardID, remainingShards)
}

func logShardEngineCreatingEvent(logger bark.Logger, host string, shardID int) {
	logger.WithFields(bark.Fields{
		tagWorkflowEventID: shardEngineCreating,
	})
	logger.Infof("ShardController on host '%v' creating engine for shardID '%v'.", host, shardID)
}

func logShardEngineCreatedEvent(logger bark.Logger, host string, shardID int) {
	logger.WithFields(bark.Fields{
		tagWorkflowEventID: shardEngineCreated,
	})
	logger.Infof("ShardController on host '%v' created engine for shardID '%v'.", host, shardID)
}

func logShardEngineStoppingEvent(logger bark.Logger, host string, shardID int) {
	logger.WithFields(bark.Fields{
		tagWorkflowEventID: shardEngineStopping,
	})
	logger.Infof("ShardController on host '%v' stopping engine for shardID '%v'.", host, shardID)
}

func logShardEngineStoppedEvent(logger bark.Logger, host string, shardID int) {
	logger.WithFields(bark.Fields{
		tagWorkflowEventID: shardEngineStopped,
	})
	logger.Infof("ShardController on host '%v' stopped engine for shardID '%v'.", host, shardID)
}

func logOperationFailedEvent(logger bark.Logger, msg string, err error) {
	logger.WithFields(bark.Fields{
		tagWorkflowEventID: operationFailed,
	})
	logger.Warnf("%v.  Error: %v", msg, err)
}
