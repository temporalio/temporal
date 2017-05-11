package logging

// TagErr is the tag for error object message
const TagErr = `err`

// TagHostname represents the hostname
const TagHostname = "hostname"

// Tags
const (
	// workflow logging tags
	TagWorkflowEventID      = "wf-event-id"
	TagWorkflowComponent    = "wf-component"
	TagWorkflowErr          = "wf-error"
	TagHistoryBuilderAction = "history-builder-action"
	TagStoreOperation       = "store-operation"
	TagWorkflowExecutionID  = "execution-id"
	TagWorkflowRunID        = "run-id"
	TagHistoryShardID       = "shard-id"
	TagDecisionType         = "decision-type"

	// workflow logging tag values
	// TagWorkflowComponent Values
	TagValueHistoryBuilderComponent = "history-builder"
	TagValueHistoryEngineComponent  = "history-engine"
	TagValueHistoryCacheComponent   = "history-cache"
	TagValueTransferQueueComponent  = "transfer-queue-processor"
	TagValueTimerQueueComponent     = "timer-queue-processor"
	TagValueShardController         = "shard-controller"
	TagValueMatchingEngineComponent = "matching-engine"

	// TagHistoryBuilderAction values
	TagValueActionWorkflowStarted                 = "add-workflowexecution-started-event"
	TagValueActionDecisionTaskScheduled           = "add-decisiontask-scheduled-event"
	TagValueActionDecisionTaskStarted             = "add-decisiontask-started-event"
	TagValueActionDecisionTaskCompleted           = "add-decisiontask-completed-event"
	TagValueActionDecisionTaskTimedOut            = "add-decisiontask-timedout-event"
	TagValueActionDecisionTaskFailed              = "add-decisiontask-failed-event"
	TagValueActionActivityTaskScheduled           = "add-activitytask-scheduled-event"
	TagValueActionActivityTaskStarted             = "add-activitytask-started-event"
	TagValueActionActivityTaskCompleted           = "add-activitytask-completed-event"
	TagValueActionActivityTaskFailed              = "add-activitytask-failed-event"
	TagValueActionActivityTaskTimedOut            = "add-activitytask-timed-event"
	TagValueActionActivityTaskCanceled            = "add-activitytask-canceled-event"
	TagValueActionActivityTaskCancelRequest       = "add-activitytask-cancel-request-event"
	TagValueActionActivityTaskCancelRequestFailed = "add-activitytask-cancel-request-failed-event"
	TagValueActionCompleteWorkflow                = "add-complete-workflow-event"
	TagValueActionFailWorkflow                    = "add-fail-workflow-event"
	TagValueActionCancelWorkflow                  = "add-cancel-workflow-event"
	TagValueActionUnknownEvent                    = "add-unknown-event"
	TagValueActionTimerStarted                    = "add-timer-started-event"
	TagValueActionTimerFired                      = "add-timer-fired-event"
	TagValueActionTimerCanceled                   = "add-timer-Canceled-event"
	TagValueActionWorkflowTerminated              = "add-workflowexecution-terminated-event"
	TagValueActionWorkflowSignaled                = "add-workflowexecution-signaled-event"
	TagValueActionContinueAsNew                   = "add-continue-as-new-event"
	TagValueActionWorkflowCanceled                = "add-workflowexecution-canceled-event"

	// TagStoreOperation values
	TagValueStoreOperationGetTasks                = "get-tasks"
	TagValueStoreOperationCompleteTask            = "complete-task"
	TagValueStoreOperationCreateWorkflowExecution = "create-wf-execution"
	TagValueStoreOperationGetWorkflowExecution    = "get-wf-execution"
	TagValueStoreOperationUpdateWorkflowExecution = "update-wf-execution"
	TagValueStoreOperationDeleteWorkflowExecution = "delete-wf-execution"
	TagValueStoreOperationUpdateShard             = "update-shard"
	TagValueStoreOperationCreateTask              = "create-task"
	TagValueStoreOperationUpdateTaskList          = "update-task-list"
	TagValueStoreOperationStopTaskList            = "stop-task-list"

	// task list tags
	TagTaskListType = "task-list-type"
	TagTaskListName = "task-list-name"
)
