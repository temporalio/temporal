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

package tag

// Pre-defined values for TagWorkflowAction
var (
	// workflow start / finish
	WorkflowActionWorkflowStarted       = workflowAction("add-workflow-started-event")
	WorkflowActionWorkflowCanceled      = workflowAction("add-workflow-canceled-event")
	WorkflowActionWorkflowCompleted     = workflowAction("add-workflow-completed--event")
	WorkflowActionWorkflowFailed        = workflowAction("add-workflow-failed-event")
	WorkflowActionWorkflowTimeout       = workflowAction("add-workflow-timeout-event")
	WorkflowActionWorkflowTerminated    = workflowAction("add-workflow-terminated-event")
	WorkflowActionWorkflowContinueAsNew = workflowAction("add-workflow-continue-as-new-event")

	// workflow cancellation / sign
	WorkflowActionWorkflowCancelRequested        = workflowAction("add-workflow-cancel-requested-event")
	WorkflowActionWorkflowSignaled               = workflowAction("add-workflow-signaled-event")
	WorkflowActionWorkflowRecordMarker           = workflowAction("add-workflow-marker-record-event")
	WorkflowActionUpsertWorkflowSearchAttributes = workflowAction("add-workflow-upsert-search-attributes-event")
	WorkflowActionWorkflowPropertiesModified     = workflowAction("add-workflow-properties-modified-event")

	// workflow update
	WorkflowActionUpdateAccepted  = workflowAction("add-workflow-update-accepted-event")
	WorkflowActionUpdateCompleted = workflowAction("add-workflow-update-completed-event")
	WorkflowActionUpdateAdmitted  = workflowAction("add-workflow-update-admitted-event")

	// workflow task
	WorkflowActionWorkflowTaskScheduled = workflowAction("add-workflowtask-scheduled-event")
	WorkflowActionWorkflowTaskStarted   = workflowAction("add-workflowtask-started-event")
	WorkflowActionWorkflowTaskCompleted = workflowAction("add-workflowtask-completed-event")
	WorkflowActionWorkflowTaskTimedOut  = workflowAction("add-workflowtask-timedout-event")
	WorkflowActionWorkflowTaskFailed    = workflowAction("add-workflowtask-failed-event")

	// in memory workflow task
	WorkflowActionInMemoryWorkflowTaskScheduled = workflowAction("add-in-memory-workflowtask-scheduled")
	WorkflowActionInMemoryWorkflowTaskStarted   = workflowAction("add-in-memory-workflowtask-started")

	// activity
	WorkflowActionActivityTaskScheduled       = workflowAction("add-activitytask-scheduled-event")
	WorkflowActionActivityTaskStarted         = workflowAction("add-activitytask-started-event")
	WorkflowActionActivityTaskCompleted       = workflowAction("add-activitytask-completed-event")
	WorkflowActionActivityTaskFailed          = workflowAction("add-activitytask-failed-event")
	WorkflowActionActivityTaskTimedOut        = workflowAction("add-activitytask-timed-event")
	WorkflowActionActivityTaskCanceled        = workflowAction("add-activitytask-canceled-event")
	WorkflowActionActivityTaskCancelRequested = workflowAction("add-activitytask-cancel-requested-event")
	WorkflowActionActivityTaskCancelFailed    = workflowAction("add-activitytask-cancel-failed-event")
	WorkflowActionActivityTaskRetry           = workflowAction("add-activitytask-retry-event")

	// timer
	WorkflowActionTimerStarted      = workflowAction("add-timer-started-event")
	WorkflowActionTimerFired        = workflowAction("add-timer-fired-event")
	WorkflowActionTimerCanceled     = workflowAction("add-timer-canceled-event")
	WorkflowActionTimerCancelFailed = workflowAction("add-timer-cancel-failed-event")

	// child workflow start / finish
	WorkflowActionChildWorkflowInitiated        = workflowAction("add-childworkflow-initiated-event")
	WorkflowActionChildWorkflowStarted          = workflowAction("add-childworkflow-started-event")
	WorkflowActionChildWorkflowInitiationFailed = workflowAction("add-childworkflow-initiation-failed-event")
	WorkflowActionChildWorkflowCanceled         = workflowAction("add-childworkflow-canceled-event")
	WorkflowActionChildWorkflowCompleted        = workflowAction("add-childworkflow-completed-event")
	WorkflowActionChildWorkflowFailed           = workflowAction("add-childworkflow-failed-event")
	WorkflowActionChildWorkflowTerminated       = workflowAction("add-childworkflow-terminated-event")
	WorkflowActionChildWorkflowTimedOut         = workflowAction("add-childworkflow-timedout-event")

	// external workflow cancellation
	WorkflowActionExternalWorkflowCancelInitiated = workflowAction("add-externalworkflow-cancel-initiated-event")
	WorkflowActionExternalWorkflowCancelRequested = workflowAction("add-externalworkflow-cancel-requested-event")
	WorkflowActionExternalWorkflowCancelFailed    = workflowAction("add-externalworkflow-cancel-failed-event")

	// external workflow signal
	WorkflowActionExternalWorkflowSignalInitiated = workflowAction("add-externalworkflow-signal-initiated-event")
	WorkflowActionExternalWorkflowSignalRequested = workflowAction("add-externalworkflow-signal-requested-event")
	WorkflowActionExternalWorkflowSignalFailed    = workflowAction("add-externalworkflow-signal-failed-event")

	WorkflowActionUnknown = workflowAction("add-unknown-event")
)

// Pre-defined values for TagWorkflowListFilterType
var (
	WorkflowListWorkflowFilterByID     = workflowListFilterType("WID")
	WorkflowListWorkflowFilterByType   = workflowListFilterType("WType")
	WorkflowListWorkflowFilterByStatus = workflowListFilterType("status")
)

// Pre-defined values for TagSysComponent
var (
	ComponentFX                       = component("fx")
	ComponentTaskQueue                = component("taskqueue")
	ComponentHistoryEngine            = component("history-engine")
	ComponentHistoryCache             = component("history-cache")
	ComponentEventsCache              = component("events-cache")
	ComponentTransferQueue            = component("transfer-queue-processor")
	ComponentOutboundQueue            = component("outbound-queue-processor")
	ComponentVisibilityQueue          = component("visibility-queue-processor")
	ComponentArchivalQueue            = component("archival-queue-processor")
	ComponentTimerQueue               = component("timer-queue-processor")
	ComponentMemoryScheduledQueue     = component("memory-scheduled-queue-processor")
	ComponentTimerBuilder             = component("timer-builder")
	ComponentReplicatorQueue          = component("replicator-queue-processor")
	ComponentShardController          = component("shard-controller")
	ComponentShardContext             = component("shard-context")
	ComponentShardEngine              = component("shard-engine")
	ComponentMatchingEngine           = component("matching-engine")
	ComponentReplicator               = component("replicator")
	ComponentReplicationTaskProcessor = component("replication-task-processor")
	ComponentHSMStateReplicator       = component("hsm-state-replicator")
	ComponentActivityStateReplicator  = component("activity-state-replicator")
	ComponentWorkflowStateReplicator  = component("workflow-state-replicator")
	ComponentHistoryReplicator        = component("history-replicator")
	ComponentHistoryImporter          = component("history-importer")
	ComponentIndexer                  = component("indexer")
	ComponentIndexerProcessor         = component("indexer-processor")
	ComponentIndexerESProcessor       = component("indexer-es-processor")
	ComponentESVisibilityManager      = component("es-visibility-manager")
	ComponentArchiver                 = component("archiver")
	ComponentBatcher                  = component("batcher")
	ComponentWorker                   = component("worker")
	ComponentWorkerManager            = component("worker-manager")
	ComponentPerNSWorkerManager       = component("perns-worker-manager")
	ComponentServiceResolver          = component("service-resolver")
	ComponentMetadataInitializer      = component("metadata-initializer")
	ComponentAddSearchAttributes      = component("add-search-attributes")
	ComponentRPCHandler               = component("rpc-handler")
	ComponentLongPollHandler          = component("long-poll-handler")
	ComponentVisibilityHandler        = component("visibility-handler")
	ComponentNamespaceReplication     = component("namespace-replication")
	ComponentPersistence              = component("persistence")
	ComponentTaskScheduler            = component("task-scheduler")
	VersionChecker                    = component("version-checker")
)

// Pre-defined values for scope tag
var (
	ScopeHost      = scope("host")
	ScopeNamespace = scope("namespace")
)

// Pre-defined values for TagSysLifecycle
var (
	LifeCycleStarting         = lifecycle("Starting")
	LifeCycleStarted          = lifecycle("Started")
	LifeCycleStopping         = lifecycle("Stopping")
	LifeCycleStopped          = lifecycle("Stopped")
	LifeCycleStopTimedout     = lifecycle("StopTimedout")
	LifeCycleStartFailed      = lifecycle("StartFailed")
	LifeCycleStopFailed       = lifecycle("StopFailed")
	LifeCycleProcessingFailed = lifecycle("ProcessingFailed")
)

// Pre-defined values for SysErrorType
var (
	ErrorTypeInvalidHistoryAction         = errorType("InvalidHistoryAction")
	ErrorTypeInvalidQueryTask             = errorType("InvalidQueryTask")
	ErrorTypeQueryTaskFailed              = errorType("QueryTaskFailed")
	ErrorTypePersistentStoreError         = errorType("PersistentStoreError")
	ErrorTypeHistorySerializationError    = errorType("HistorySerializationError")
	ErrorTypeHistoryDeserializationError  = errorType("HistoryDeserializationError")
	ErrorTypeDuplicateTask                = errorType("DuplicateTask")
	ErrorTypeMultipleCompletionCommands   = errorType("MultipleCompletionCommands")
	ErrorTypeDuplicateTransferTask        = errorType("DuplicateTransferTask")
	ErrorTypeWorkflowTaskFailed           = errorType("WorkflowTaskFailed")
	ErrorTypeInvalidMutableStateAction    = errorType("InvalidMutableStateAction")
	ErrorTypeInvalidMemWorkflowTaskAction = errorType("InvalidMemWorkflowTaskAction")
)

// Pre-defined values for SysShardUpdate
var (
	// Shard context events
	ValueShardRangeUpdated            = shardupdate("ShardRangeUpdated")
	ValueShardAllocateTimerBeforeRead = shardupdate("ShardAllocateTimerBeforeRead")
	ValueRingMembershipChangedEvent   = shardupdate("RingMembershipChangedEvent")
)

// Pre-defined values for OperationResult
var (
	OperationFailed   = operationResult("OperationFailed")
	OperationStuck    = operationResult("OperationStuck")
	OperationCritical = operationResult("OperationCritical")
)

// Pre-defined values for TagSysStoreOperation
var (
	StoreOperationGetTasks                         = storeOperation("get-tasks")
	StoreOperationCompleteTask                     = storeOperation("complete-task")
	StoreOperationCompleteTasksLessThan            = storeOperation("complete-tasks-less-than")
	StoreOperationCreateWorkflowExecution          = storeOperation("create-wf-execution")
	StoreOperationConflictResolveWorkflowExecution = storeOperation("conflict-resolve-wf-execution")
	StoreOperationGetWorkflowExecution             = storeOperation("get-wf-execution")
	StoreOperationUpdateWorkflowExecution          = storeOperation("update-wf-execution")
	StoreOperationDeleteWorkflowExecution          = storeOperation("delete-wf-execution")
	StoreOperationUpdateShard                      = storeOperation("update-shard")
	StoreOperationCreateTask                       = storeOperation("create-task")
	StoreOperationUpdateTaskQueue                  = storeOperation("update-task-queue")
	StoreOperationStopTaskQueue                    = storeOperation("stop-task-queue")
)
