// Copyright (c) 2017 Uber Technologies, Inc.
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
	TagDomainID             = "domain-id"
	TagWorkflowExecutionID  = "execution-id"
	TagWorkflowRunID        = "run-id"
	TagHistoryShardID       = "shard-id"
	TagDecisionType         = "decision-type"
	TagDecisionFailCause    = "decision-fail-cause"
	TagTaskID               = "task-id"
	TagTaskType             = "task-type"

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
	TagValueActionTimeoutWorkflow                 = "add-timeout-workflow-event"
	TagValueActionCancelWorkflow                  = "add-cancel-workflow-event"
	TagValueActionTimerStarted                    = "add-timer-started-event"
	TagValueActionTimerFired                      = "add-timer-fired-event"
	TagValueActionTimerCanceled                   = "add-timer-Canceled-event"
	TagValueActionWorkflowTerminated              = "add-workflowexecution-terminated-event"
	TagValueActionWorkflowSignaled                = "add-workflowexecution-signaled-event"
	TagValueActionContinueAsNew                   = "add-continue-as-new-event"
	TagValueActionWorkflowCanceled                = "add-workflowexecution-canceled-event"
	TagValueActionChildExecutionStarted           = "add-childexecution-started-event"
	TagValueActionStartChildExecutionFailed       = "add-start-childexecution-failed-event"
	TagValueActionChildExecutionCompleted         = "add-childexecution-completed-event"
	TagValueActionChildExecutionFailed            = "add-childexecution-failed-event"
	TagValueActionChildExecutionCanceled          = "add-childexecution-canceled-event"
	TagValueActionChildExecutionTerminated        = "add-childexecution-terminated-event"
	TagValueActionChildExecutionTimedOut          = "add-childexecution-timedout-event"
	TagValueActionRequestCancelWorkflow           = "add-request-cancel-workflow-event"
	TagValueActionWorkflowCancelRequested         = "add-workflow-execution-cancel-requested-event"
	TagValueActionWorkflowCancelFailed            = "add-workflow-execution-cancel-failed-event"
	TagValueActionUnknownEvent                    = "add-unknown-event"

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
