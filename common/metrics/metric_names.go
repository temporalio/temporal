// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software" ) to deal
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

package metrics

const (
	// FrontendStartWorkflowExecutionOperation is the metric operation for frontend.StartWorkflowExecution
	FrontendStartWorkflowExecutionOperation = "StartWorkflowExecution"
	// FrontendPollWorkflowTaskQueueOperation is the metric operation for frontend.PollWorkflowTaskQueue
	FrontendPollWorkflowTaskQueueOperation = "PollWorkflowTaskQueue"
	// FrontendPollActivityTaskQueueOperation is the metric operation for frontend.PollActivityTaskQueue
	FrontendPollActivityTaskQueueOperation = "PollActivityTaskQueue"
	// FrontendRecordActivityTaskHeartbeatOperation is the metric operation for frontend.RecordActivityTaskHeartbeat
	FrontendRecordActivityTaskHeartbeatOperation = "RecordActivityTaskHeartbeat"
	// FrontendRecordActivityTaskHeartbeatByIdOperation is the metric operation for frontend.RespondWorkflowTaskCompleted
	FrontendRecordActivityTaskHeartbeatByIdOperation = "RecordActivityTaskHeartbeatById"
	// FrontendRespondWorkflowTaskCompletedOperation is the metric operation for frontend.RespondWorkflowTaskCompleted
	FrontendRespondWorkflowTaskCompletedOperation = "RespondWorkflowTaskCompleted"
	// FrontendRespondWorkflowTaskFailedOperation is the metric operation for frontend.RespondWorkflowTaskFailed
	FrontendRespondWorkflowTaskFailedOperation
	// FrontendRespondQueryTaskCompletedOperation is the metric operation for frontend.RespondQueryTaskCompleted
	FrontendRespondQueryTaskCompletedOperation
	// FrontendRespondActivityTaskCompletedOperation is the metric operation for frontend.RespondActivityTaskCompleted
	FrontendRespondActivityTaskCompletedOperation
	// FrontendRespondActivityTaskFailedOperation is the metric operation for frontend.RespondActivityTaskFailed
	FrontendRespondActivityTaskFailedOperation
	// FrontendRespondActivityTaskCanceledOperation is the metric operation for frontend.RespondActivityTaskCanceled
	FrontendRespondActivityTaskCanceledOperation
	// FrontendRespondActivityTaskCompletedByIdOperation is the metric operation for frontend.RespondActivityTaskCompletedById
	FrontendRespondActivityTaskCompletedByIdOperation
	// FrontendRespondActivityTaskFailedByIdOperation is the metric operation for frontend.RespondActivityTaskFailedById
	FrontendRespondActivityTaskFailedByIdOperation
	// FrontendRespondActivityTaskCanceledByIdOperation is the metric operation for frontend.RespondActivityTaskCanceledById
	FrontendRespondActivityTaskCanceledByIdOperation
	// FrontendGetWorkflowExecutionHistoryOperation is the metric operation for non-long-poll frontend.GetWorkflowExecutionHistory
	FrontendGetWorkflowExecutionHistoryOperation
	// FrontendGetWorkflowExecutionHistoryReverseOperation is the metric for frontend.GetWorkflowExecutionHistoryReverse
	FrontendGetWorkflowExecutionHistoryReverseOperation
	// FrontendPollWorkflowExecutionHistoryOperation is the metric operation for long poll case of frontend.GetWorkflowExecutionHistory
	FrontendPollWorkflowExecutionHistoryOperation
	// FrontendGetWorkflowExecutionRawHistoryOperation is the metric operation for frontend.GetWorkflowExecutionRawHistory
	FrontendGetWorkflowExecutionRawHistoryOperation
	// FrontendPollForWorkflowExecutionRawHistoryOperation is the metric operation for frontend.GetWorkflowExecutionRawHistory
	FrontendPollForWorkflowExecutionRawHistoryOperation
	// FrontendSignalWorkflowExecutionOperation is the metric operation for frontend.SignalWorkflowExecution
	FrontendSignalWorkflowExecutionOperation = "SignalWorkflowExecution"
	// FrontendSignalWithStartWorkflowExecutionOperation is the metric operation for frontend.SignalWithStartWorkflowExecution
	FrontendSignalWithStartWorkflowExecutionOperation = "SignalWithStartWorkflowExecution"
	// FrontendTerminateWorkflowExecutionOperation is the metric operation for frontend.TerminateWorkflowExecution
	FrontendTerminateWorkflowExecutionOperation
	// FrontendRequestCancelWorkflowExecutionOperation is the metric operation for frontend.RequestCancelWorkflowExecution
	FrontendRequestCancelWorkflowExecutionOperation
	// FrontendListArchivedWorkflowExecutionsOperation is the metric operation for frontend.ListArchivedWorkflowExecutions
	FrontendListArchivedWorkflowExecutionsOperation
	// FrontendListOpenWorkflowExecutionsOperation is the metric operation for frontend.ListOpenWorkflowExecutions
	FrontendListOpenWorkflowExecutionsOperation
	// FrontendListClosedWorkflowExecutionsOperation is the metric operation for frontend.ListClosedWorkflowExecutions
	FrontendListClosedWorkflowExecutionsOperation
	// FrontendListWorkflowExecutionsOperation is the metric operation for frontend.ListWorkflowExecutions
	FrontendListWorkflowExecutionsOperation
	// FrontendScanWorkflowExecutionsOperation is the metric operation for frontend.ListWorkflowExecutions
	FrontendScanWorkflowExecutionsOperation
	// FrontendCountWorkflowExecutionsOperation is the metric operation for frontend.CountWorkflowExecutions
	FrontendCountWorkflowExecutionsOperation
	// FrontendRegisterNamespaceOperation is the metric operation for frontend.RegisterNamespace
	FrontendRegisterNamespaceOperation
	// FrontendDescribeNamespaceOperation is the metric operation for frontend.DescribeNamespace
	FrontendDescribeNamespaceOperation
	// FrontendUpdateNamespaceOperation is the metric operation for frontend.DescribeNamespace
	FrontendUpdateNamespaceOperation
	// FrontendDeprecateNamespaceOperation is the metric operation for frontend.DeprecateNamespace
	FrontendDeprecateNamespaceOperation
	// FrontendQueryWorkflowOperation is the metric operation for frontend.QueryWorkflow
	FrontendQueryWorkflowOperation = "QueryWorkflow"
	// FrontendDescribeWorkflowExecutionOperation is the metric operation for frontend.DescribeWorkflowExecution
	FrontendDescribeWorkflowExecutionOperation
	// FrontendDescribeTaskQueueOperation is the metric operation for frontend.DescribeTaskQueue
	FrontendDescribeTaskQueueOperation
	// FrontendListTaskQueuePartitionsOperation is the metric operation for frontend.ResetStickyTaskQueue
	FrontendListTaskQueuePartitionsOperation
	// FrontendResetStickyTaskQueueOperation is the metric operation for frontend.ResetStickyTaskQueue
	FrontendResetStickyTaskQueueOperation
	// FrontendListNamespacesOperation is the metric operation for frontend.ListNamespace
	FrontendListNamespacesOperation
	// FrontendResetWorkflowExecutionOperation is the metric operation for frontend.ResetWorkflowExecution
	FrontendResetWorkflowExecutionOperation = "ResetWorkflowExecution"
	// FrontendGetSearchAttributesOperation is the metric operation for frontend.GetSearchAttributes
	FrontendGetSearchAttributesOperation
	// FrontendGetClusterInfoOperation is the metric operation for frontend.GetClusterInfo
	FrontendGetClusterInfoOperation
	// FrontendGetSystemInfoOperation is the metric operation for frontend.GetSystemInfo
	FrontendGetSystemInfoOperation
	// FrontendCreateScheduleOperation is the metric operation for frontend.CreateScheduleOperation
	FrontendCreateScheduleOperation
	// FrontendDescribeScheduleOperation is the metric operation for frontend.DescribeScheduleOperation
	FrontendDescribeScheduleOperation
	// FrontendUpdateScheduleOperation is the metric operation for frontend.UpdateScheduleOperation
	FrontendUpdateScheduleOperation
	// FrontendPatchScheduleOperation is the metric operation for frontend.PatchScheduleOperation
	FrontendPatchScheduleOperation
	// FrontendListScheduleMatchingTimesOperation is the metric operation for frontend.ListScheduleMatchingTimesOperation
	FrontendListScheduleMatchingTimesOperation
	// FrontendDeleteScheduleOperation is the metric operation for frontend.DeleteScheduleOperation
	FrontendDeleteScheduleOperation
	// FrontendListSchedulesOperation is the metric operation for frontend.ListSchedulesOperation
	FrontendListSchedulesOperation
	// FrontendUpdateWorkerBuildIdOrderingOperation is the metric operation for frontend.UpdateWorkerBuildIdOrderingOperation
	FrontendUpdateWorkerBuildIdOrderingOperation
	// FrontendGetWorkerBuildIdOrderingOperation is the metric operation for frontend.GetWorkerBuildIdOrderingOperation
	FrontendGetWorkerBuildIdOrderingOperation
	// FrontendUpdateWorkflowOperation is the metric operation for frontend.UpdateWorkflow
	FrontendUpdateWorkflowOperation
	// FrontendDescribeBatchOperationOperation is the metric operation for frontend.DescribeBatchOperation
	FrontendDescribeBatchOperationOperation
	// FrontendListBatchOperationsOperation is the metric operation for frontend.ListBatchOperations
	FrontendListBatchOperationsOperation
	// FrontendStartBatchOperationOperation is the metric operation for frontend.StartBatchOperation
	FrontendStartBatchOperationOperation
	// FrontendStopBatchOperationOperation is the metric operation for frontend.StopBatchOperation
	FrontendStopBatchOperationOperation
	// VersionCheckOperation is Operation used by version checker
	VersionCheckOperation
	// AuthorizationOperation is the operation used by all metric emitted by authorization code
	AuthorizationOperation = "Authorization"
)

const (
	// AdminDescribeHistoryHostOperation is the metric operation for admin.AdminDescribeHistoryHostOperation
	AdminDescribeHistoryHostOperation = "AdminDescribeHistoryHost"
	// AdminAddSearchAttributesOperation is the metric operation for admin.AdminAddSearchAttributesOperation
	AdminAddSearchAttributesOperation
	// AdminRemoveSearchAttributesOperation is the metric operation for admin.AdminRemoveSearchAttributesOperation
	AdminRemoveSearchAttributesOperation
	// AdminGetSearchAttributesOperation is the metric operation for admin.AdminGetSearchAttributesOperation
	AdminGetSearchAttributesOperation
	// AdminRebuildMutableStateOperation is the metric operation for admin.AdminRebuildMutableStateOperation
	AdminRebuildMutableStateOperation
	// AdminDescribeMutableStateOperation is the metric operation for admin.AdminDescribeMutableStateOperation
	AdminDescribeMutableStateOperation
	// AdminGetWorkflowExecutionRawHistoryV2Operation is the metric operation for admin.GetWorkflowExecutionRawHistoryOperation
	AdminGetWorkflowExecutionRawHistoryV2Operation
	// AdminGetReplicationMessagesOperation is the metric operation for admin.GetReplicationMessages
	AdminGetReplicationMessagesOperation
	// AdminGetNamespaceReplicationMessagesOperation is the metric operation for admin.GetNamespaceReplicationMessages
	AdminGetNamespaceReplicationMessagesOperation
	// AdminGetDLQReplicationMessagesOperation is the metric operation for admin.GetDLQReplicationMessages
	AdminGetDLQReplicationMessagesOperation
	// AdminReapplyEventsOperation is the metric operation for admin.ReapplyEvents
	AdminReapplyEventsOperation
	// AdminRefreshWorkflowTasksOperation is the metric operation for admin.RefreshWorkflowTasks
	AdminRefreshWorkflowTasksOperation
	// AdminResendReplicationTasksOperation is the metric operation for admin.ResendReplicationTasks
	AdminResendReplicationTasksOperation
	// AdminGetTaskQueueTasksOperation is the metric operation for admin.GetTaskQueueTasks
	AdminGetTaskQueueTasksOperation
	// AdminRemoveTaskOperation is the metric operation for admin.AdminRemoveTaskOperation
	AdminRemoveTaskOperation
	// AdminCloseShardOperation is the metric operation for admin.AdminCloseShardOperation
	AdminCloseShardOperation
	// AdminGetShardOperation is the metric operation for admin.AdminGetShardOperation
	AdminGetShardOperation
	// AdminListHistoryTasksOperation is the metric operation for admin.ListHistoryTasksOperation
	AdminListHistoryTasksOperation
	// AdminGetDLQMessagesOperation is the metric operation for admin.AdminGetDLQMessagesOperation
	AdminGetDLQMessagesOperation
	// AdminPurgeDLQMessagesOperation is the metric operation for admin.AdminPurgeDLQMessagesOperation
	AdminPurgeDLQMessagesOperation
	// AdminMergeDLQMessagesOperation is the metric operation for admin.AdminMergeDLQMessagesOperation
	AdminMergeDLQMessagesOperation
	// AdminListClusterMembersOperation is the metric operation for admin.AdminListClusterMembersOperation
	AdminListClusterMembersOperation
	// AdminDescribeClusterOperation is the metric operation for admin.AdminDescribeClusterOperation
	AdminDescribeClusterOperation
	// AdminListClustersOperation is the metric operation for admin.AdminListClustersOperation
	AdminListClustersOperation
	// AdminAddOrUpdateRemoteClusterOperation is the metric operation for admin.AdminAddOrUpdateRemoteClusterOperation
	AdminAddOrUpdateRemoteClusterOperation
	// AdminRemoveRemoteClusterOperation is the metric operation for admin.AdminRemoveRemoteClusterOperation
	AdminRemoveRemoteClusterOperation
	// AdminDeleteWorkflowExecutionOperation is the metric operation for admin.AdminDeleteWorkflowExecutionOperation
	AdminDeleteWorkflowExecutionOperation
)

const (
	// OperatorAddSearchAttributesOperation is the metric operation for operator.AddSearchAttributes
	OperatorAddSearchAttributesOperation = "OperatorAddSearchAttributes"
	// OperatorRemoveSearchAttributesOperation is the metric operation for operator.RemoveSearchAttributes
	OperatorRemoveSearchAttributesOperation
	// OperatorListSearchAttributesOperation is the metric operation for operator.ListSearchAttributes
	OperatorListSearchAttributesOperation
	OperatorDeleteNamespaceOperation
	OperatorAddOrUpdateRemoteCluster
	OperatorDeleteWorkflowExecution
	OperatorDescribeCluster
	OperatorListClusterMembers
	OperatorListClusters
	OperatorRemoveRemoteCluster
)

const (
	// HistoryStartWorkflowExecutionOperation tracks StartWorkflowExecution API calls received by service
	HistoryStartWorkflowExecutionOperation = "StartWorkflowExecution"
	// HistoryRecordActivityTaskHeartbeatOperation tracks RecordActivityTaskHeartbeat API calls received by service
	HistoryRecordActivityTaskHeartbeatOperation
	// HistoryRespondWorkflowTaskCompletedOperation tracks RespondWorkflowTaskCompleted API calls received by service
	HistoryRespondWorkflowTaskCompletedOperation
	// HistoryRespondWorkflowTaskFailedOperation tracks RespondWorkflowTaskFailed API calls received by service
	HistoryRespondWorkflowTaskFailedOperation
	// HistoryRespondActivityTaskCompletedOperation tracks RespondActivityTaskCompleted API calls received by service
	HistoryRespondActivityTaskCompletedOperation
	// HistoryRespondActivityTaskFailedOperation tracks RespondActivityTaskFailed API calls received by service
	HistoryRespondActivityTaskFailedOperation
	// HistoryRespondActivityTaskCanceledOperation tracks RespondActivityTaskCanceled API calls received by service
	HistoryRespondActivityTaskCanceledOperation
	// HistoryGetMutableStateOperation tracks GetMutableStateOperation API calls received by service
	HistoryGetMutableStateOperation
	// HistoryPollMutableStateOperation tracks PollMutableStateOperation API calls received by service
	HistoryPollMutableStateOperation
	// HistoryResetStickyTaskQueueOperation tracks ResetStickyTaskQueueOperation API calls received by service
	HistoryResetStickyTaskQueueOperation
	// HistoryDescribeWorkflowExecutionOperation tracks DescribeWorkflowExecution API calls received by service
	HistoryDescribeWorkflowExecutionOperation
	// HistoryRecordWorkflowTaskStartedOperation tracks RecordWorkflowTaskStarted API calls received by service
	HistoryRecordWorkflowTaskStartedOperation
	// HistoryRecordActivityTaskStartedOperation tracks RecordActivityTaskStarted API calls received by service
	HistoryRecordActivityTaskStartedOperation
	// HistorySignalWorkflowExecutionOperation tracks SignalWorkflowExecution API calls received by service
	HistorySignalWorkflowExecutionOperation
	// HistorySignalWithStartWorkflowExecutionOperation tracks SignalWithStartWorkflowExecution API calls received by service
	HistorySignalWithStartWorkflowExecutionOperation
	// HistoryRemoveSignalMutableStateOperation tracks RemoveSignalMutableState API calls received by service
	HistoryRemoveSignalMutableStateOperation
	// HistoryTerminateWorkflowExecutionOperation tracks TerminateWorkflowExecution API calls received by service
	HistoryTerminateWorkflowExecutionOperation
	// HistoryScheduleWorkflowTaskOperation tracks ScheduleWorkflowTask API calls received by service
	HistoryScheduleWorkflowTaskOperation
	// HistoryVerifyFirstWorkflowTaskScheduled tracks VerifyFirstWorkflowTaskScheduled API calls received by service
	HistoryVerifyFirstWorkflowTaskScheduled
	// HistoryRecordChildExecutionCompletedOperation tracks RecordChildExecutionCompleted API calls received by service
	HistoryRecordChildExecutionCompletedOperation
	// HistoryVerifyChildExecutionCompletionRecordedOperation tracks VerifyChildExecutionCompletionRecorded API calls received by service
	HistoryVerifyChildExecutionCompletionRecordedOperation
	// HistoryRequestCancelWorkflowExecutionOperation tracks RequestCancelWorkflowExecution API calls received by service
	HistoryRequestCancelWorkflowExecutionOperation
	// HistorySyncShardStatusOperation tracks HistorySyncShardStatus API calls received by service
	HistorySyncShardStatusOperation
	// HistorySyncActivityOperation tracks HistoryActivity API calls received by service
	HistorySyncActivityOperation
	// HistoryRebuildMutableStateOperation tracks RebuildMutable API calls received by service
	HistoryRebuildMutableStateOperation
	// HistoryDescribeMutableStateOperation tracks DescribeMutableState API calls received by service
	HistoryDescribeMutableStateOperation
	// HistoryGetReplicationMessagesOperation tracks GetReplicationMessages API calls received by service
	HistoryGetReplicationMessagesOperation
	// HistoryGetDLQReplicationMessagesOperation tracks GetReplicationMessages API calls received by service
	HistoryGetDLQReplicationMessagesOperation
	// HistoryReadDLQMessagesOperation tracks GetDLQMessages API calls received by service
	HistoryReadDLQMessagesOperation
	// HistoryPurgeDLQMessagesOperation tracks PurgeDLQMessages API calls received by service
	HistoryPurgeDLQMessagesOperation
	// HistoryMergeDLQMessagesOperation tracks MergeDLQMessages API calls received by service
	HistoryMergeDLQMessagesOperation
	// HistoryShardControllerOperation is the operation used by shard controller
	HistoryShardControllerOperation
	// HistoryReapplyEventsOperation is the operation used by event reapplication
	HistoryReapplyEventsOperation
	// HistoryRefreshWorkflowTasksOperation is the operation used by refresh workflow tasks API
	HistoryRefreshWorkflowTasksOperation
	// HistoryGenerateLastHistoryReplicationTasksOperation is the operation used by generate last replication tasks API
	HistoryGenerateLastHistoryReplicationTasksOperation
	// HistoryGetReplicationStatusOperation is the operation used by GetReplicationStatus API
	HistoryGetReplicationStatusOperation
	// HistoryHistoryRemoveTaskOperation is the operation used by remove task API
	HistoryHistoryRemoveTaskOperation
	// HistoryCloseShard is the operation used by close shard API
	HistoryCloseShard
	// HistoryGetShard is the operation used by get shard API
	HistoryGetShard
	// HistoryReplicateEventsV2 is the operation used by replicate events API
	HistoryReplicateEventsV2
	// HistoryResetStickyTaskQueue is the operation used by reset sticky task queue API
	HistoryResetStickyTaskQueue
	// HistoryReapplyEvents is the operation used by reapply events API
	HistoryReapplyEvents
	// HistoryDescribeHistoryHost is the operation used by describe history host API
	HistoryDescribeHistoryHost
	// HistoryDeleteWorkflowVisibilityRecordOperation is the operation used by delete workflow visibility record API
	HistoryDeleteWorkflowVisibilityRecordOperation
	// HistoryUpdateWorkflowOperation is the operation used by update workflow API
	HistoryUpdateWorkflowOperation
	// TaskPriorityAssignerOperation is the operation used by all metric emitted by task priority assigner
	TaskPriorityAssignerOperation
	// TransferQueueProcessorOperation is the operation used by all metric emitted by transfer queue processor
	TransferQueueProcessorOperation
	// TransferActiveQueueProcessorOperation is the operation used by all metric emitted by transfer queue processor
	TransferActiveQueueProcessorOperation = "TransferActiveQueueProcessor"
	// TransferStandbyQueueProcessorOperation is the operation used by all metric emitted by transfer queue processor
	TransferStandbyQueueProcessorOperation = "TransferStandbyQueueProcessor"
	// TransferActiveTaskActivityOperation is the operation used for activity task processing by transfer queue processor
	TransferActiveTaskActivityOperation
	// TransferActiveTaskWorkflowTaskOperation is the operation used for workflow task processing by transfer queue processor
	TransferActiveTaskWorkflowTaskOperation
	// TransferActiveTaskCloseExecutionOperation is the operation used for close execution task processing by transfer queue processor
	TransferActiveTaskCloseExecutionOperation
	// TransferActiveTaskCancelExecutionOperation is the operation used for cancel execution task processing by transfer queue processor
	TransferActiveTaskCancelExecutionOperation
	// TransferActiveTaskSignalExecutionOperation is the operation used for signal execution task processing by transfer queue processor
	TransferActiveTaskSignalExecutionOperation
	// TransferActiveTaskStartChildExecutionOperation is the operation used for start child execution task processing by transfer queue processor
	TransferActiveTaskStartChildExecutionOperation
	// TransferActiveTaskResetWorkflowOperation is the operation used for record workflow started task processing by transfer queue processor
	TransferActiveTaskResetWorkflowOperation
	// TransferStandbyTaskResetWorkflowOperation is the operation used for record workflow started task processing by transfer queue processor
	TransferStandbyTaskResetWorkflowOperation
	// TransferStandbyTaskActivityOperation is the operation used for activity task processing by transfer queue processor
	TransferStandbyTaskActivityOperation
	// TransferStandbyTaskWorkflowTaskOperation is the operation used for workflow task processing by transfer queue processor
	TransferStandbyTaskWorkflowTaskOperation
	// TransferStandbyTaskCloseExecutionOperation is the operation used for close execution task processing by transfer queue processor
	TransferStandbyTaskCloseExecutionOperation
	// TransferStandbyTaskCancelExecutionOperation is the operation used for cancel execution task processing by transfer queue processor
	TransferStandbyTaskCancelExecutionOperation
	// TransferStandbyTaskSignalExecutionOperation is the operation used for signal execution task processing by transfer queue processor
	TransferStandbyTaskSignalExecutionOperation
	// TransferStandbyTaskStartChildExecutionOperation is the operation used for start child execution task processing by transfer queue processor
	TransferStandbyTaskStartChildExecutionOperation

	// VisibilityQueueProcessorOperation is the operation used by all metric emitted by visibility queue processor
	VisibilityQueueProcessorOperation = "VisibilityQueueProcessor"
	// VisibilityTaskStartExecutionOperation is the operation used for start execution processing by visibility queue processor
	VisibilityTaskStartExecutionOperation
	// VisibilityTaskUpsertExecutionOperation is the operation used for upsert execution processing by visibility queue processor
	VisibilityTaskUpsertExecutionOperation
	// VisibilityTaskCloseExecutionOperation is the operation used for close execution attributes processing by visibility queue processor
	VisibilityTaskCloseExecutionOperation
	// VisibilityTaskDeleteExecutionOperation is the operation used for delete by visibility queue processor
	VisibilityTaskDeleteExecutionOperation

	// TimerQueueProcessorOperation is the operation used by all metric emitted by timer queue processor
	TimerQueueProcessorOperation
	// TimerActiveQueueProcessorOperation is the operation used by all metric emitted by timer queue processor
	TimerActiveQueueProcessorOperation = "TimerActiveQueueProcessor"
	// TimerStandbyQueueProcessorOperation is the operation used by all metric emitted by timer queue processor
	TimerStandbyQueueProcessorOperation = "TimerStandbyQueueProcessor"
	// TimerActiveTaskActivityTimeoutOperation is the operation used by metric emitted by timer queue processor for processing activity timeouts
	TimerActiveTaskActivityTimeoutOperation
	// TimerActiveTaskWorkflowTaskTimeoutOperation is the operation used by metric emitted by timer queue processor for processing workflow task timeouts
	TimerActiveTaskWorkflowTaskTimeoutOperation
	// TimerActiveTaskUserTimerOperation is the operation used by metric emitted by timer queue processor for processing user timers
	TimerActiveTaskUserTimerOperation
	// TimerActiveTaskWorkflowTimeoutOperation is the operation used by metric emitted by timer queue processor for processing workflow timeouts.
	TimerActiveTaskWorkflowTimeoutOperation
	// TimerActiveTaskActivityRetryTimerOperation is the operation used by metric emitted by timer queue processor for processing retry task.
	TimerActiveTaskActivityRetryTimerOperation
	// TimerActiveTaskWorkflowBackoffTimerOperation is the operation used by metric emitted by timer queue processor for processing retry task.
	TimerActiveTaskWorkflowBackoffTimerOperation
	// TimerActiveTaskDeleteHistoryEventOperation is the operation used by metric emitted by timer queue processor for processing history event cleanup
	TimerActiveTaskDeleteHistoryEventOperation
	// TimerStandbyTaskActivityTimeoutOperation is the operation used by metric emitted by timer queue processor for processing activity timeouts
	TimerStandbyTaskActivityTimeoutOperation
	// TimerStandbyTaskWorkflowTaskTimeoutOperation is the operation used by metric emitted by timer queue processor for processing workflow task timeouts
	TimerStandbyTaskWorkflowTaskTimeoutOperation
	// TimerStandbyTaskUserTimerOperation is the operation used by metric emitted by timer queue processor for processing user timers
	TimerStandbyTaskUserTimerOperation
	// TimerStandbyTaskWorkflowTimeoutOperation is the operation used by metric emitted by timer queue processor for processing workflow timeouts.
	TimerStandbyTaskWorkflowTimeoutOperation
	// TimerStandbyTaskActivityRetryTimerOperation is the operation used by metric emitted by timer queue processor for processing retry task.
	TimerStandbyTaskActivityRetryTimerOperation
	// TimerStandbyTaskDeleteHistoryEventOperation is the operation used by metric emitted by timer queue processor for processing history event cleanup
	TimerStandbyTaskDeleteHistoryEventOperation
	// TimerStandbyTaskWorkflowBackoffTimerOperation is the operation used by metric emitted by timer queue processor for processing retry task.
	TimerStandbyTaskWorkflowBackoffTimerOperation
	// ReplicatorQueueProcessorOperation is the operation used by all metric emitted by replicator queue processor
	ReplicatorQueueProcessorOperation = "ReplicatorQueueProcessor"
	// ReplicatorTaskHistoryOperation is the operation used for history task processing by replicator queue processor
	ReplicatorTaskHistoryOperation
	// ReplicatorTaskSyncActivityOperation is the operation used for sync activity by replicator queue processor
	ReplicatorTaskSyncActivityOperation
	// ReplicateHistoryEventsOperation is the operation used by historyReplicator API for applying events
	ReplicateHistoryEventsOperation
	// ShardInfoOperation is the operation used when updating shard info
	ShardInfoOperation
	// WorkflowContextOperation is the operation used by WorkflowContext component
	WorkflowContextOperation
	// HistoryCacheGetOrCreateOperation is the operation used by history cache
	HistoryCacheGetOrCreateOperation
	// HistoryCacheGetOrCreateCurrentOperation is the operation used by history cache
	HistoryCacheGetOrCreateCurrentOperation
	// ExecutionStatsOperation is the operation used for emiting workflow execution related stats
	ExecutionStatsOperation
	// SessionStatsOperation is the operation used for emiting session update related stats
	SessionStatsOperation
	// HistoryResetWorkflowExecutionOperation tracks ResetWorkflowExecution API calls received by service
	HistoryResetWorkflowExecutionOperation
	// HistoryQueryWorkflowOperation tracks QueryWorkflow API calls received by service
	HistoryQueryWorkflowOperation
	// HistoryProcessDeleteHistoryEventOperation tracks ProcessDeleteHistoryEvent processing calls
	HistoryProcessDeleteHistoryEventOperation
	// HistoryDeleteWorkflowExecutionOperation tracks DeleteWorkflowExecutions API calls
	HistoryDeleteWorkflowExecutionOperation
	// WorkflowCompletionStatsOperation tracks workflow completion updates
	WorkflowCompletionStatsOperation
	// ReplicationTaskFetcherOperation is the operation used by all metrics emitted by ReplicationTaskFetcher
	ReplicationTaskFetcherOperation
	// ReplicationTaskCleanupOperation is the operation used by all metrics emitted by ReplicationTaskProcessor cleanup
	ReplicationTaskCleanupOperation
	// ReplicationDLQStatsOperation is the operation used by all metrics emitted related to replication DLQ
	ReplicationDLQStatsOperation
	// SyncWorkflowStateTaskOperation is the operation used by closed workflow task replication processing
	SyncWorkflowStateTaskOperation

	// HistoryEventNotificationOperation is the operation used by shard history event nitification
	HistoryEventNotificationOperation = "HistoryEventNotification"
	// EventsCacheGetEventOperation is the operation used by events cache
	EventsCacheGetEventOperation = "EventsCacheGetEvent"
	// EventsCachePutEventOperation is the operation used by events cache
	EventsCachePutEventOperation = "EventsCachePutEvent"
	// EventsCacheDeleteEventOperation is the operation used by events cache
	EventsCacheDeleteEventOperation = "EventsCacheDeleteEvent"
	// EventsCacheGetFromStoreOperation is the operation used by events cache
	EventsCacheGetFromStoreOperation = "EventsCacheGetFromStore"

	// ReplicatorOperation is the operation used by all metric emitted by replicator
	ReplicatorOperation = "Replicator"
	// HistoryReplicationTaskOperation is the operation used by history task replication processing
	HistoryReplicationTaskOperation
	// HistoryMetadataReplicationTaskOperation is the operation used by history metadata task replication processing
	HistoryMetadataReplicationTaskOperation
	// SyncShardTaskOperation is the operation used by sync shrad information processing
	SyncShardTaskOperation
	// SyncActivityTaskOperation is the operation used by sync activity information processing
	SyncActivityTaskOperation
	// ESProcessorOperation is operation used by all metric emitted by esProcessor
	ESProcessorOperation
	// IndexProcessorOperation is operation used by all metric emitted by index processor
	IndexProcessorOperation

	// HistoryRereplicationByTransferTaskOperation tracks history replication calls made by transfer task
	HistoryRereplicationByTransferTaskOperation = "HistoryRereplicationByTransferTask"
	// HistoryRereplicationByTimerTaskOperation tracks history replication calls made by timer task
	HistoryRereplicationByTimerTaskOperation = "HistoryRereplicationByTimerTask"
	// HistoryRereplicationByHistoryReplicationOperation tracks history replication calls made by history replication
	HistoryRereplicationByHistoryReplicationOperation = "HistoryRereplicationByHistoryReplication"
	// HistoryRereplicationByHistoryMetadataReplicationOperation tracks history replication calls made by history replication
	HistoryRereplicationByHistoryMetadataReplicationOperation = "HistoryRereplicationByHistoryMetadataReplication"
	// HistoryRereplicationByActivityReplicationOperation tracks history replication calls made by activity replication
	HistoryRereplicationByActivityReplicationOperation = "HistoryRereplicationByActivityReplication"
)

// Operations enum
const (
	UnknownOperation = "UnknownOperation"

	ServerTlsOperation = "ServerTls"
	// -- Common Operation Operations --

	// PersistenceGetOrCreateShardOperation tracks GetOrCreateShard calls made by service to persistence layer
	PersistenceGetOrCreateShardOperation = "GetOrCreateShard"
	// PersistenceUpdateShardOperation tracks UpdateShard calls made by service to persistence layer
	PersistenceUpdateShardOperation
	// PersistenceAssertShardOwnershipOperation tracks UpdateShard calls made by service to persistence layer
	PersistenceAssertShardOwnershipOperation
	// PersistenceCreateWorkflowExecutionOperation tracks CreateWorkflowExecution calls made by service to persistence layer
	PersistenceCreateWorkflowExecutionOperation
	// PersistenceGetWorkflowExecutionOperation tracks GetWorkflowExecution calls made by service to persistence layer
	PersistenceGetWorkflowExecutionOperation
	// PersistenceSetWorkflowExecutionOperation tracks SetWorkflowExecution calls made by service to persistence layer
	PersistenceSetWorkflowExecutionOperation
	// PersistenceUpdateWorkflowExecutionOperation tracks UpdateWorkflowExecution calls made by service to persistence layer
	PersistenceUpdateWorkflowExecutionOperation
	// PersistenceConflictResolveWorkflowExecutionOperation tracks ConflictResolveWorkflowExecution calls made by service to persistence layer
	PersistenceConflictResolveWorkflowExecutionOperation
	// PersistenceResetWorkflowExecutionOperation tracks ResetWorkflowExecution calls made by service to persistence layer
	PersistenceResetWorkflowExecutionOperation
	// PersistenceDeleteWorkflowExecutionOperation tracks DeleteWorkflowExecution calls made by service to persistence layer
	PersistenceDeleteWorkflowExecutionOperation
	// PersistenceDeleteCurrentWorkflowExecutionOperation tracks DeleteCurrentWorkflowExecution calls made by service to persistence layer
	PersistenceDeleteCurrentWorkflowExecutionOperation
	// PersistenceGetCurrentExecutionOperation tracks GetCurrentExecution calls made by service to persistence layer
	PersistenceGetCurrentExecutionOperation
	// PersistenceListConcreteExecutionsOperation tracks ListConcreteExecutions calls made by service to persistence layer
	PersistenceListConcreteExecutionsOperation
	// PersistenceAddTasksOperation tracks AddTasks calls made by service to persistence layer
	PersistenceAddTasksOperation
	// PersistenceGetTransferTaskOperation tracks GetTransferTask calls made by service to persistence layer
	PersistenceGetTransferTaskOperation
	// PersistenceGetTransferTasksOperation tracks GetTransferTasks calls made by service to persistence layer
	PersistenceGetTransferTasksOperation
	// PersistenceCompleteTransferTaskOperation tracks CompleteTransferTasks calls made by service to persistence layer
	PersistenceCompleteTransferTaskOperation
	// PersistenceRangeCompleteTransferTasksOperation tracks CompleteTransferTasks calls made by service to persistence layer
	PersistenceRangeCompleteTransferTasksOperation

	// PersistenceGetVisibilityTaskOperation tracks GetVisibilityTask calls made by service to persistence layer
	PersistenceGetVisibilityTaskOperation
	// PersistenceGetVisibilityTasksOperation tracks GetVisibilityTasks calls made by service to persistence layer
	PersistenceGetVisibilityTasksOperation
	// PersistenceCompleteVisibilityTaskOperation tracks CompleteVisibilityTasks calls made by service to persistence layer
	PersistenceCompleteVisibilityTaskOperation
	// PersistenceRangeCompleteVisibilityTasksOperation tracks CompleteVisibilityTasks calls made by service to persistence layer
	PersistenceRangeCompleteVisibilityTasksOperation

	// PersistenceGetReplicationTaskOperation tracks GetReplicationTask calls made by service to persistence layer
	PersistenceGetReplicationTaskOperation
	// PersistenceGetReplicationTasksOperation tracks GetReplicationTasks calls made by service to persistence layer
	PersistenceGetReplicationTasksOperation
	// PersistenceCompleteReplicationTaskOperation tracks CompleteReplicationTasks calls made by service to persistence layer
	PersistenceCompleteReplicationTaskOperation
	// PersistenceRangeCompleteReplicationTasksOperation tracks RangeCompleteReplicationTasks calls made by service to persistence layer
	PersistenceRangeCompleteReplicationTasksOperation
	// PersistencePutReplicationTaskToDLQOperation tracks PersistencePutReplicationTaskToDLQOperation calls made by service to persistence layer
	PersistencePutReplicationTaskToDLQOperation
	// PersistenceGetReplicationTasksFromDLQOperation tracks PersistenceGetReplicationTasksFromDLQOperation calls made by service to persistence layer
	PersistenceGetReplicationTasksFromDLQOperation
	// PersistenceDeleteReplicationTaskFromDLQOperation tracks PersistenceDeleteReplicationTaskFromDLQOperation calls made by service to persistence layer
	PersistenceDeleteReplicationTaskFromDLQOperation
	// PersistenceRangeDeleteReplicationTaskFromDLQOperation tracks PersistenceRangeDeleteReplicationTaskFromDLQOperation calls made by service to persistence layer
	PersistenceRangeDeleteReplicationTaskFromDLQOperation
	// PersistenceGetTimerTaskOperation tracks GetTimerTask calls made by service to persistence layer
	PersistenceGetTimerTaskOperation
	// PersistenceGetTimerTasksOperation tracks GetTimerTasks calls made by service to persistence layer
	PersistenceGetTimerTasksOperation
	// PersistenceCompleteTimerTaskOperation tracks CompleteTimerTasks calls made by service to persistence layer
	PersistenceCompleteTimerTaskOperation
	// PersistenceRangeCompleteTimerTasksOperation tracks CompleteTimerTasks calls made by service to persistence layer
	PersistenceRangeCompleteTimerTasksOperation
	// PersistenceCreateTaskOperation tracks CreateTask calls made by service to persistence layer
	PersistenceCreateTaskOperation
	// PersistenceGetTasksOperation tracks GetTasks calls made by service to persistence layer
	PersistenceGetTasksOperation
	// PersistenceCompleteTaskOperation tracks CompleteTask calls made by service to persistence layer
	PersistenceCompleteTaskOperation
	// PersistenceCompleteTasksLessThanOperation is the metric operation for persistence.TaskManager.PersistenceCompleteTasksLessThan API
	PersistenceCompleteTasksLessThanOperation
	// PersistenceCreateTaskQueueOperation tracks PersistenceCreateTaskQueueOperation calls made by service to persistence layer
	PersistenceCreateTaskQueueOperation
	// PersistenceUpdateTaskQueueOperation tracks PersistenceUpdateTaskQueueOperation calls made by service to persistence layer
	PersistenceUpdateTaskQueueOperation
	// PersistenceGetTaskQueueOperation tracks PersistenceGetTaskQueueOperation calls made by service to persistence layer
	PersistenceGetTaskQueueOperation
	// PersistenceListTaskQueueOperation is the metric operation for persistence.TaskManager.ListTaskQueue API
	PersistenceListTaskQueueOperation
	// PersistenceDeleteTaskQueueOperation is the metric operation for persistence.TaskManager.DeleteTaskQueue API
	PersistenceDeleteTaskQueueOperation
	// PersistenceAppendHistoryEventsOperation tracks AppendHistoryEvents calls made by service to persistence layer
	PersistenceAppendHistoryEventsOperation
	// PersistenceGetWorkflowExecutionHistoryOperation tracks GetWorkflowExecutionHistory calls made by service to persistence layer
	PersistenceGetWorkflowExecutionHistoryOperation
	// PersistenceDeleteWorkflowExecutionHistoryOperation tracks DeleteWorkflowExecutionHistory calls made by service to persistence layer
	PersistenceDeleteWorkflowExecutionHistoryOperation
	// PersistenceInitializeSystemNamespaceOperation tracks InitializeSystemNamespaceOperation calls made by service to persistence layer
	PersistenceInitializeSystemNamespaceOperation
	// PersistenceCreateNamespaceOperation tracks CreateNamespace calls made by service to persistence layer
	PersistenceCreateNamespaceOperation
	// PersistenceGetNamespaceOperation tracks GetNamespace calls made by service to persistence layer
	PersistenceGetNamespaceOperation
	// PersistenceUpdateNamespaceOperation tracks UpdateNamespace calls made by service to persistence layer
	PersistenceUpdateNamespaceOperation
	// PersistenceDeleteNamespaceOperation tracks DeleteNamespace calls made by service to persistence layer
	PersistenceDeleteNamespaceOperation
	// PersistenceRenameNamespaceOperation tracks RenameNamespace calls made by service to persistence layer
	PersistenceRenameNamespaceOperation
	// PersistenceDeleteNamespaceByNameOperation tracks DeleteNamespaceByName calls made by service to persistence layer
	PersistenceDeleteNamespaceByNameOperation
	// PersistenceListNamespaceOperation tracks DeleteNamespaceByName calls made by service to persistence layer
	PersistenceListNamespaceOperation
	// PersistenceGetMetadataOperation tracks DeleteNamespaceByName calls made by service to persistence layer
	PersistenceGetMetadataOperation

	// PersistenceEnqueueMessageOperation tracks Enqueue calls made by service to persistence layer
	PersistenceEnqueueMessageOperation
	// PersistenceEnqueueMessageToDLQOperation tracks Enqueue DLQ calls made by service to persistence layer
	PersistenceEnqueueMessageToDLQOperation
	// PersistenceReadQueueMessagesOperation tracks ReadMessages calls made by service to persistence layer
	PersistenceReadQueueMessagesOperation
	// PersistenceReadQueueMessagesFromDLQOperation tracks ReadMessagesFromDLQ calls made by service to persistence layer
	PersistenceReadQueueMessagesFromDLQOperation
	// PersistenceDeleteQueueMessagesOperation tracks DeleteMessages calls made by service to persistence layer
	PersistenceDeleteQueueMessagesOperation
	// PersistenceDeleteQueueMessageFromDLQOperation tracks DeleteMessageFromDLQ calls made by service to persistence layer
	PersistenceDeleteQueueMessageFromDLQOperation
	// PersistenceRangeDeleteMessagesFromDLQOperation tracks RangeDeleteMessagesFromDLQ calls made by service to persistence layer
	PersistenceRangeDeleteMessagesFromDLQOperation
	// PersistenceUpdateAckLevelOperation tracks UpdateAckLevel calls made by service to persistence layer
	PersistenceUpdateAckLevelOperation
	// PersistenceGetAckLevelOperation tracks GetAckLevel calls made by service to persistence layer
	PersistenceGetAckLevelOperation
	// PersistenceUpdateDLQAckLevelOperation tracks UpdateDLQAckLevel calls made by service to persistence layer
	PersistenceUpdateDLQAckLevelOperation
	// PersistenceGetDLQAckLevelOperation tracks GetDLQAckLevel calls made by service to persistence layer
	PersistenceGetDLQAckLevelOperation
	// PersistenceListClusterMetadataOperation tracks ListClusterMetadata calls made by service to persistence layer
	PersistenceListClusterMetadataOperation
	// PersistenceGetClusterMetadataOperation tracks GetClusterMetadata calls made by service to persistence layer
	PersistenceGetClusterMetadataOperation
	// PersistenceSaveClusterMetadataOperation tracks SaveClusterMetadata calls made by service to persistence layer
	PersistenceSaveClusterMetadataOperation
	// PersistenceDeleteClusterMetadataOperation tracks DeleteClusterMetadata calls made by service to persistence layer
	PersistenceDeleteClusterMetadataOperation
	// PersistenceUpsertClusterMembershipOperation tracks UpsertClusterMembership calls made by service to persistence layer
	PersistenceUpsertClusterMembershipOperation
	// PersistencePruneClusterMembershipOperation tracks PruneClusterMembership calls made by service to persistence layer
	PersistencePruneClusterMembershipOperation
	// PersistenceGetClusterMembersOperation tracks GetClusterMembers calls made by service to persistence layer
	PersistenceGetClusterMembersOperation
	// HistoryClientStartWorkflowExecutionOperation tracks RPC calls to history service
	HistoryClientStartWorkflowExecutionOperation
	// HistoryClientRecordActivityTaskHeartbeatOperation tracks RPC calls to history service
	HistoryClientRecordActivityTaskHeartbeatOperation
	// HistoryClientRespondWorkflowTaskCompletedOperation tracks RPC calls to history service
	HistoryClientRespondWorkflowTaskCompletedOperation
	// HistoryClientRespondWorkflowTaskFailedOperation tracks RPC calls to history service
	HistoryClientRespondWorkflowTaskFailedOperation
	// HistoryClientRespondActivityTaskCompletedOperation tracks RPC calls to history service
	HistoryClientRespondActivityTaskCompletedOperation
	// HistoryClientRespondActivityTaskFailedOperation tracks RPC calls to history service
	HistoryClientRespondActivityTaskFailedOperation
	// HistoryClientRespondActivityTaskCanceledOperation tracks RPC calls to history service
	HistoryClientRespondActivityTaskCanceledOperation
	// HistoryClientGetMutableStateOperation tracks RPC calls to history service
	HistoryClientGetMutableStateOperation
	// HistoryClientPollMutableStateOperation tracks RPC calls to history service
	HistoryClientPollMutableStateOperation
	// HistoryClientResetStickyTaskQueueOperation tracks RPC calls to history service
	HistoryClientResetStickyTaskQueueOperation
	// HistoryClientDescribeWorkflowExecutionOperation tracks RPC calls to history service
	HistoryClientDescribeWorkflowExecutionOperation
	// HistoryClientRecordWorkflowTaskStartedOperation tracks RPC calls to history service
	HistoryClientRecordWorkflowTaskStartedOperation
	// HistoryClientRecordActivityTaskStartedOperation tracks RPC calls to history service
	HistoryClientRecordActivityTaskStartedOperation
	// HistoryClientRequestCancelWorkflowExecutionOperation tracks RPC calls to history service
	HistoryClientRequestCancelWorkflowExecutionOperation
	// HistoryClientSignalWorkflowExecutionOperation tracks RPC calls to history service
	HistoryClientSignalWorkflowExecutionOperation
	// HistoryClientSignalWithStartWorkflowExecutionOperation tracks RPC calls to history service
	HistoryClientSignalWithStartWorkflowExecutionOperation
	// HistoryClientRemoveSignalMutableStateOperation tracks RPC calls to history service
	HistoryClientRemoveSignalMutableStateOperation
	// HistoryClientTerminateWorkflowExecutionOperation tracks RPC calls to history service
	HistoryClientTerminateWorkflowExecutionOperation
	// HistoryClientUpdateWorkflowOperation tracks RPC calls to history service
	HistoryClientUpdateWorkflowOperation
	// HistoryClientDeleteWorkflowExecutionOperation tracks RPC calls to history service
	HistoryClientDeleteWorkflowExecutionOperation
	// HistoryClientResetWorkflowExecutionOperation tracks RPC calls to history service
	HistoryClientResetWorkflowExecutionOperation
	// HistoryClientScheduleWorkflowTaskOperation tracks RPC calls to history service
	HistoryClientScheduleWorkflowTaskOperation
	// HistoryClientVerifyFirstWorkflowTaskScheduledOperation tracks RPC calls to history service
	HistoryClientVerifyFirstWorkflowTaskScheduledOperation
	// HistoryClientRecordChildExecutionCompletedOperation tracks RPC calls to history service
	HistoryClientRecordChildExecutionCompletedOperation
	// HistoryClientVerifyChildExecutionCompletionRecordedOperation tracks RPC calls to history service
	HistoryClientVerifyChildExecutionCompletionRecordedOperation
	// HistoryClientReplicateEventsV2Operation tracks RPC calls to history service
	HistoryClientReplicateEventsV2Operation
	// HistoryClientSyncShardStatusOperation tracks RPC calls to history service
	HistoryClientSyncShardStatusOperation
	// HistoryClientSyncActivityOperation tracks RPC calls to history service
	HistoryClientSyncActivityOperation
	// HistoryClientGetReplicationTasksOperation tracks RPC calls to history service
	HistoryClientGetReplicationTasksOperation
	// HistoryClientGetDLQReplicationTasksOperation tracks RPC calls to history service
	HistoryClientGetDLQReplicationTasksOperation
	// HistoryClientQueryWorkflowOperation tracks RPC calls to history service
	HistoryClientQueryWorkflowOperation
	// HistoryClientReapplyEventsOperation tracks RPC calls to history service
	HistoryClientReapplyEventsOperation
	// HistoryClientGetDLQMessagesOperation tracks RPC calls to history service
	HistoryClientGetDLQMessagesOperation
	// HistoryClientPurgeDLQMessagesOperation tracks RPC calls to history service
	HistoryClientPurgeDLQMessagesOperation
	// HistoryClientMergeDLQMessagesOperation tracks RPC calls to history service
	HistoryClientMergeDLQMessagesOperation
	// HistoryClientRefreshWorkflowTasksOperation tracks RPC calls to history service
	HistoryClientRefreshWorkflowTasksOperation
	// HistoryClientGenerateLastHistoryReplicationTasksOperation tracks RPC calls to history service
	HistoryClientGenerateLastHistoryReplicationTasksOperation
	// HistoryClientGetReplicationStatusOperation tracks RPC calls to history service
	HistoryClientGetReplicationStatusOperation
	// HistoryClientDeleteWorkflowVisibilityRecordOperation tracks RPC calls to history service
	HistoryClientDeleteWorkflowVisibilityRecordOperation
	// HistoryClientCloseShardOperation tracks RPC calls to history service
	HistoryClientCloseShardOperation
	// HistoryClientDescribeMutableStateOperation tracks RPC calls to history service
	HistoryClientDescribeMutableStateOperation
	// HistoryClientGetDLQReplicationMessagesOperation tracks RPC calls to history service
	HistoryClientGetDLQReplicationMessagesOperation
	// HistoryClientGetShardOperation tracks RPC calls to history service
	HistoryClientGetShardOperation
	// HistoryClientRebuildMutableStateOperation tracks RPC calls to history service
	HistoryClientRebuildMutableStateOperation
	// HistoryClientRemoveTaskOperation tracks RPC calls to history service
	HistoryClientRemoveTaskOperation
	// HistoryClientDescribeHistoryHostOperation tracks RPC calls to history service
	HistoryClientDescribeHistoryHostOperation
	// HistoryClientGetReplicationMessagesOperation tracks RPC calls to history service
	HistoryClientGetReplicationMessagesOperation
	// MatchingClientPollWorkflowTaskQueueOperation tracks RPC calls to matching service
	MatchingClientPollWorkflowTaskQueueOperation = "MatchingClientPollWorkflowTaskQueue"
	// MatchingClientPollActivityTaskQueueOperation tracks RPC calls to matching service
	MatchingClientPollActivityTaskQueueOperation = "MatchingClientPollActivityTaskQueue"
	// MatchingClientAddActivityTaskOperation tracks RPC calls to matching service
	MatchingClientAddActivityTaskOperation = "MatchingClientAddActivityTask"
	// MatchingClientAddWorkflowTaskOperation tracks RPC calls to matching service
	MatchingClientAddWorkflowTaskOperation = "MatchingClientAddWorkflowTask"
	// MatchingClientQueryWorkflowOperation tracks RPC calls to matching service
	MatchingClientQueryWorkflowOperation = "MatchingClientQueryWorkflow"
	// MatchingClientRespondQueryTaskCompletedOperation tracks RPC calls to matching service
	MatchingClientRespondQueryTaskCompletedOperation = "MatchingClientRespondQueryTaskCompleted"
	// MatchingClientCancelOutstandingPollOperation tracks RPC calls to matching service
	MatchingClientCancelOutstandingPollOperation = "MatchingClientCancelOutstandingPoll"
	// MatchingClientDescribeTaskQueueOperation tracks RPC calls to matching service
	MatchingClientDescribeTaskQueueOperation = "MatchingClientDescribeTaskQueue"
	// MatchingClientListTaskQueuePartitionsOperation tracks RPC calls to matching service
	MatchingClientListTaskQueuePartitionsOperation = "MatchingClientListTaskQueuePartitions"
	// MatchingClientUpdateWorkerBuildIdOrderingOperation tracks RPC calls to matching service
	MatchingClientUpdateWorkerBuildIdOrderingOperation = "MatchingClientUpdateWorkerBuildIdOrdering"
	// MatchingGetWorkerBuildIdOrderingOperation tracks RPC calls to matching service
	MatchingClientGetWorkerBuildIdOrderingOperation = "MatchingClientGetWorkerBuildIdOrdering"
	// MatchingClientInvalidateTaskQueueMetadataOperation tracks RPC calls to matching service
	MatchingClientInvalidateTaskQueueMetadataOperation = "MatchingClientInvalidateTaskQueueMetadata"
	// MatchingClientGetTaskQueueMetadataOperation tracks RPC calls to matching service
	MatchingClientGetTaskQueueMetadataOperation = "MatchingClientGetTaskQueueMetadata"
	// FrontendClientDeprecateNamespaceOperation tracks RPC calls to frontend service
	FrontendClientDeprecateNamespaceOperation
	// FrontendClientDescribeNamespaceOperation tracks RPC calls to frontend service
	FrontendClientDescribeNamespaceOperation
	// FrontendClientDescribeTaskQueueOperation tracks RPC calls to frontend service
	FrontendClientDescribeTaskQueueOperation
	// FrontendClientDescribeWorkflowExecutionOperation tracks RPC calls to frontend service
	FrontendClientDescribeWorkflowExecutionOperation
	// FrontendClientGetWorkflowExecutionHistoryOperation tracks RPC calls to frontend service
	FrontendClientGetWorkflowExecutionHistoryOperation
	// FrontendClientGetWorkflowExecutionHistoryReverseOperation tracks RPC calls to frontend service
	FrontendClientGetWorkflowExecutionHistoryReverseOperation
	// FrontendClientGetWorkflowExecutionRawHistoryOperation tracks RPC calls to frontend service
	FrontendClientGetWorkflowExecutionRawHistoryOperation
	// FrontendClientPollForWorkflowExecutionRawHistoryOperation tracks RPC calls to frontend service
	FrontendClientPollForWorkflowExecutionRawHistoryOperation
	// FrontendClientListArchivedWorkflowExecutionsOperation tracks RPC calls to frontend service
	FrontendClientListArchivedWorkflowExecutionsOperation
	// FrontendClientListClosedWorkflowExecutionsOperation tracks RPC calls to frontend service
	FrontendClientListClosedWorkflowExecutionsOperation
	// FrontendClientListNamespacesOperation tracks RPC calls to frontend service
	FrontendClientListNamespacesOperation
	// FrontendClientListOpenWorkflowExecutionsOperation tracks RPC calls to frontend service
	FrontendClientListOpenWorkflowExecutionsOperation
	// FrontendClientPollActivityTaskQueueOperation tracks RPC calls to frontend service
	FrontendClientPollActivityTaskQueueOperation
	// FrontendClientPollWorkflowTaskQueueOperation tracks RPC calls to frontend service
	FrontendClientPollWorkflowTaskQueueOperation
	// FrontendClientQueryWorkflowOperation tracks RPC calls to frontend service
	FrontendClientQueryWorkflowOperation
	// FrontendClientRecordActivityTaskHeartbeatOperation tracks RPC calls to frontend service
	FrontendClientRecordActivityTaskHeartbeatOperation
	// FrontendClientRecordActivityTaskHeartbeatByIdOperation tracks RPC calls to frontend service
	FrontendClientRecordActivityTaskHeartbeatByIdOperation
	// FrontendClientRegisterNamespaceOperation tracks RPC calls to frontend service
	FrontendClientRegisterNamespaceOperation
	// FrontendClientRequestCancelWorkflowExecutionOperation tracks RPC calls to frontend service
	FrontendClientRequestCancelWorkflowExecutionOperation
	// FrontendClientResetStickyTaskQueueOperation tracks RPC calls to frontend service
	FrontendClientResetStickyTaskQueueOperation
	// FrontendClientResetWorkflowExecutionOperation tracks RPC calls to frontend service
	FrontendClientResetWorkflowExecutionOperation
	// FrontendClientRespondActivityTaskCanceledOperation tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCanceledOperation
	// FrontendClientRespondActivityTaskCanceledByIdOperation tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCanceledByIdOperation
	// FrontendClientRespondActivityTaskCompletedOperation tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCompletedOperation
	// FrontendClientRespondActivityTaskCompletedByIdOperation tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCompletedByIdOperation
	// FrontendClientRespondActivityTaskFailedOperation tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskFailedOperation
	// FrontendClientRespondActivityTaskFailedByIdOperation tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskFailedByIdOperation
	// FrontendClientRespondWorkflowTaskCompletedOperation tracks RPC calls to frontend service
	FrontendClientRespondWorkflowTaskCompletedOperation
	// FrontendClientRespondWorkflowTaskFailedOperation tracks RPC calls to frontend service
	FrontendClientRespondWorkflowTaskFailedOperation
	// FrontendClientRespondQueryTaskCompletedOperation tracks RPC calls to frontend service
	FrontendClientRespondQueryTaskCompletedOperation
	// FrontendClientSignalWithStartWorkflowExecutionOperation tracks RPC calls to frontend service
	FrontendClientSignalWithStartWorkflowExecutionOperation
	// FrontendClientSignalWorkflowExecutionOperation tracks RPC calls to frontend service
	FrontendClientSignalWorkflowExecutionOperation
	// FrontendClientStartWorkflowExecutionOperation tracks RPC calls to frontend service
	FrontendClientStartWorkflowExecutionOperation
	// FrontendClientTerminateWorkflowExecutionOperation tracks RPC calls to frontend service
	FrontendClientTerminateWorkflowExecutionOperation
	// FrontendClientUpdateNamespaceOperation tracks RPC calls to frontend service
	FrontendClientUpdateNamespaceOperation
	// FrontendClientListWorkflowExecutionsOperation tracks RPC calls to frontend service
	FrontendClientListWorkflowExecutionsOperation
	// FrontendClientScanWorkflowExecutionsOperation tracks RPC calls to frontend service
	FrontendClientScanWorkflowExecutionsOperation
	// FrontendClientCountWorkflowExecutionsOperation tracks RPC calls to frontend service
	FrontendClientCountWorkflowExecutionsOperation
	// FrontendClientGetSearchAttributesOperation tracks RPC calls to frontend service
	FrontendClientGetSearchAttributesOperation
	// FrontendClientGetClusterInfoOperation tracks RPC calls to frontend
	FrontendClientGetClusterInfoOperation
	// FrontendClientGetSystemInfoOperation tracks RPC calls to frontend
	FrontendClientGetSystemInfoOperation
	// FrontendClientListTaskQueuePartitionsOperation tracks RPC calls to frontend service
	FrontendClientListTaskQueuePartitionsOperation
	// FrontendClientCreateScheduleOperation tracks RPC calls to frontend service
	FrontendClientCreateScheduleOperation
	// FrontendClientDescribeScheduleOperation tracks RPC calls to frontend service
	FrontendClientDescribeScheduleOperation
	// FrontendClientUpdateScheduleOperation tracks RPC calls to frontend service
	FrontendClientUpdateScheduleOperation
	// FrontendClientPatchScheduleOperation tracks RPC calls to frontend service
	FrontendClientPatchScheduleOperation
	// FrontendClientListScheduleMatchingTimesOperation tracks RPC calls to frontend service
	FrontendClientListScheduleMatchingTimesOperation
	// FrontendClientDeleteScheduleOperation tracks RPC calls to frontend service
	FrontendClientDeleteScheduleOperation
	// FrontendClientListSchedulesOperation tracks RPC calls to frontend service
	FrontendClientListSchedulesOperation
	// FrontendClientUpdateWorkerBuildIdOrderingOperation tracks RPC calls to frontend service
	FrontendClientUpdateWorkerBuildIdOrderingOperation
	// FrontendClientUpdateWorkflowOperation tracks RPC calls to frontend service
	FrontendClientUpdateWorkflowOperation
	// FrontendClientGetWorkerBuildIdOrderingOperation tracks RPC calls to frontend service
	FrontendClientGetWorkerBuildIdOrderingOperation
	// FrontendClientDescribeBatchOperationOperation tracks RPC calls to frontend service
	FrontendClientDescribeBatchOperationOperation
	// FrontendClientListBatchOperationsOperation tracks RPC calls to frontend service
	FrontendClientListBatchOperationsOperation
	// FrontendClientStartBatchOperationOperation tracks RPC calls to frontend service
	FrontendClientStartBatchOperationOperation
	// FrontendClientStopBatchOperationOperation tracks RPC calls to frontend service
	FrontendClientStopBatchOperationOperation

	// AdminClientAddSearchAttributesOperation tracks RPC calls to admin service
	AdminClientAddSearchAttributesOperation
	// AdminClientRemoveSearchAttributesOperation tracks RPC calls to admin service
	AdminClientRemoveSearchAttributesOperation
	// AdminClientGetSearchAttributesOperation tracks RPC calls to admin service
	AdminClientGetSearchAttributesOperation
	// AdminClientCloseShardOperation tracks RPC calls to admin service
	AdminClientCloseShardOperation
	// AdminClientGetShardOperation tracks RPC calls to admin service
	AdminClientGetShardOperation
	// AdminClientListHistoryTasksOperation tracks RPC calls to admin service
	AdminClientListHistoryTasksOperation
	// AdminClientRemoveTaskOperation tracks RPC calls to admin service
	AdminClientRemoveTaskOperation
	// AdminClientDescribeHistoryHostOperation tracks RPC calls to admin service
	AdminClientDescribeHistoryHostOperation
	// AdminClientRebuildMutableStateOperation tracks RPC calls to admin service
	AdminClientRebuildMutableStateOperation
	// AdminClientDescribeMutableStateOperation tracks RPC calls to admin service
	AdminClientDescribeMutableStateOperation
	// AdminClientGetWorkflowExecutionRawHistoryV2Operation tracks RPC calls to admin service
	AdminClientGetWorkflowExecutionRawHistoryV2Operation
	// AdminClientDescribeClusterOperation tracks RPC calls to admin service
	AdminClientDescribeClusterOperation
	// AdminClientListClustersOperation tracks RPC calls to admin service
	AdminClientListClustersOperation
	// AdminClientListClusterMembersOperation tracks RPC calls to admin service
	AdminClientListClusterMembersOperation
	// AdminClientAddOrUpdateRemoteClusterOperation tracks RPC calls to admin service
	AdminClientAddOrUpdateRemoteClusterOperation
	// AdminClientRemoveRemoteClusterOperation tracks RPC calls to admin service
	AdminClientRemoveRemoteClusterOperation
	// AdminClientGetReplicationMessagesOperation tracks RPC calls to admin service
	AdminClientGetReplicationMessagesOperation
	// AdminClientGetNamespaceReplicationMessagesOperation tracks RPC calls to admin service
	AdminClientGetNamespaceReplicationMessagesOperation
	// AdminClientGetDLQReplicationMessagesOperation tracks RPC calls to admin service
	AdminClientGetDLQReplicationMessagesOperation
	// AdminClientReapplyEventsOperation tracks RPC calls to admin service
	AdminClientReapplyEventsOperation
	// AdminClientGetDLQMessagesOperation tracks RPC calls to admin service
	AdminClientGetDLQMessagesOperation
	// AdminClientPurgeDLQMessagesOperation tracks RPC calls to admin service
	AdminClientPurgeDLQMessagesOperation
	// AdminClientMergeDLQMessagesOperation tracks RPC calls to admin service
	AdminClientMergeDLQMessagesOperation
	// AdminClientRefreshWorkflowTasksOperation tracks RPC calls to admin service
	AdminClientRefreshWorkflowTasksOperation
	// AdminClientResendReplicationTasksOperation tracks RPC calls to admin service
	AdminClientResendReplicationTasksOperation
	// AdminClientGetTaskQueueTasksOperation tracks RPC calls to admin service
	AdminClientGetTaskQueueTasksOperation
	// AdminClientDeleteWorkflowExecutionOperation tracks RPC calls to admin service
	AdminClientDeleteWorkflowExecutionOperation

	// DCRedirectionDeprecateNamespaceOperation tracks RPC calls for dc redirection
	DCRedirectionDeprecateNamespaceOperation
	// DCRedirectionDescribeNamespaceOperation tracks RPC calls for dc redirection
	DCRedirectionDescribeNamespaceOperation
	// DCRedirectionDescribeTaskQueueOperation tracks RPC calls for dc redirection
	DCRedirectionDescribeTaskQueueOperation
	// DCRedirectionDescribeWorkflowExecutionOperation tracks RPC calls for dc redirection
	DCRedirectionDescribeWorkflowExecutionOperation
	// DCRedirectionGetWorkflowExecutionHistoryOperation tracks RPC calls for dc redirection
	DCRedirectionGetWorkflowExecutionHistoryOperation
	// DCRedirectionGetWorkflowExecutionHistoryReverseOperation tracks RPC calls for dc redirection
	DCRedirectionGetWorkflowExecutionHistoryReverseOperation
	// DCRedirectionGetWorkflowExecutionRawHistoryOperation tracks RPC calls for dc redirection
	DCRedirectionGetWorkflowExecutionRawHistoryOperation
	// DCRedirectionPollForWorkflowExecutionRawHistoryOperation tracks RPC calls for dc redirection
	DCRedirectionPollForWorkflowExecutionRawHistoryOperation
	// DCRedirectionListArchivedWorkflowExecutionsOperation tracks RPC calls for dc redirection
	DCRedirectionListArchivedWorkflowExecutionsOperation
	// DCRedirectionListClosedWorkflowExecutionsOperation tracks RPC calls for dc redirection
	DCRedirectionListClosedWorkflowExecutionsOperation
	// DCRedirectionListNamespacesOperation tracks RPC calls for dc redirection
	DCRedirectionListNamespacesOperation
	// DCRedirectionListOpenWorkflowExecutionsOperation tracks RPC calls for dc redirection
	DCRedirectionListOpenWorkflowExecutionsOperation
	// DCRedirectionListWorkflowExecutionsOperation tracks RPC calls for dc redirection
	DCRedirectionListWorkflowExecutionsOperation
	// DCRedirectionScanWorkflowExecutionsOperation tracks RPC calls for dc redirection
	DCRedirectionScanWorkflowExecutionsOperation
	// DCRedirectionCountWorkflowExecutionsOperation tracks RPC calls for dc redirection
	DCRedirectionCountWorkflowExecutionsOperation
	// DCRedirectionGetSearchAttributesOperation tracks RPC calls for dc redirection
	DCRedirectionGetSearchAttributesOperation
	// DCRedirectionPollActivityTaskQueueOperation tracks RPC calls for dc redirection
	DCRedirectionPollActivityTaskQueueOperation
	// DCRedirectionPollWorkflowTaskQueueOperation tracks RPC calls for dc redirection
	DCRedirectionPollWorkflowTaskQueueOperation
	// DCRedirectionQueryWorkflowOperation tracks RPC calls for dc redirection
	DCRedirectionQueryWorkflowOperation
	// DCRedirectionRecordActivityTaskHeartbeatOperation tracks RPC calls for dc redirection
	DCRedirectionRecordActivityTaskHeartbeatOperation
	// DCRedirectionRecordActivityTaskHeartbeatByIdOperation tracks RPC calls for dc redirection
	DCRedirectionRecordActivityTaskHeartbeatByIdOperation
	// DCRedirectionRegisterNamespaceOperation tracks RPC calls for dc redirection
	DCRedirectionRegisterNamespaceOperation
	// DCRedirectionRequestCancelWorkflowExecutionOperation tracks RPC calls for dc redirection
	DCRedirectionRequestCancelWorkflowExecutionOperation
	// DCRedirectionResetStickyTaskQueueOperation tracks RPC calls for dc redirection
	DCRedirectionResetStickyTaskQueueOperation
	// DCRedirectionResetWorkflowExecutionOperation tracks RPC calls for dc redirection
	DCRedirectionResetWorkflowExecutionOperation
	// DCRedirectionRespondActivityTaskCanceledOperation tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskCanceledOperation
	// DCRedirectionRespondActivityTaskCanceledByIdOperation tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskCanceledByIdOperation
	// DCRedirectionRespondActivityTaskCompletedOperation tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskCompletedOperation
	// DCRedirectionRespondActivityTaskCompletedByIdOperation tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskCompletedByIdOperation
	// DCRedirectionRespondActivityTaskFailedOperation tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskFailedOperation
	// DCRedirectionRespondActivityTaskFailedByIdOperation tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskFailedByIdOperation
	// DCRedirectionRespondWorkflowTaskCompletedOperation tracks RPC calls for dc redirection
	DCRedirectionRespondWorkflowTaskCompletedOperation
	// DCRedirectionRespondWorkflowTaskFailedOperation tracks RPC calls for dc redirection
	DCRedirectionRespondWorkflowTaskFailedOperation
	// DCRedirectionRespondQueryTaskCompletedOperation tracks RPC calls for dc redirection
	DCRedirectionRespondQueryTaskCompletedOperation
	// DCRedirectionSignalWithStartWorkflowExecutionOperation tracks RPC calls for dc redirection
	DCRedirectionSignalWithStartWorkflowExecutionOperation
	// DCRedirectionSignalWorkflowExecutionOperation tracks RPC calls for dc redirection
	DCRedirectionSignalWorkflowExecutionOperation
	// DCRedirectionStartWorkflowExecutionOperation tracks RPC calls for dc redirection
	DCRedirectionStartWorkflowExecutionOperation
	// DCRedirectionTerminateWorkflowExecutionOperation tracks RPC calls for dc redirection
	DCRedirectionTerminateWorkflowExecutionOperation
	// DCRedirectionUpdateNamespaceOperation tracks RPC calls for dc redirection
	DCRedirectionUpdateNamespaceOperation
	// DCRedirectionListTaskQueuePartitionsOperation tracks RPC calls for dc redirection
	DCRedirectionListTaskQueuePartitionsOperation
	// DCRedirectionCreateScheduleOperation tracks RPC calls for dc redirection
	DCRedirectionCreateScheduleOperation
	// DCRedirectionDescribeScheduleOperation tracks RPC calls for dc redirection
	DCRedirectionDescribeScheduleOperation
	// DCRedirectionUpdateScheduleOperation tracks RPC calls for dc redirection
	DCRedirectionUpdateScheduleOperation
	// DCRedirectionPatchScheduleOperation tracks RPC calls for dc redirection
	DCRedirectionPatchScheduleOperation
	// DCRedirectionListScheduleMatchingTimesOperation tracks RPC calls for dc redirection
	DCRedirectionListScheduleMatchingTimesOperation
	// DCRedirectionDeleteScheduleOperation tracks RPC calls for dc redirection
	DCRedirectionDeleteScheduleOperation
	// DCRedirectionListSchedulesOperation tracks RPC calls for dc redirection
	DCRedirectionListSchedulesOperation
	// DCRedirectionUpdateWorkerBuildIdOrderingOperation tracks RPC calls for dc redirection
	DCRedirectionUpdateWorkerBuildIdOrderingOperation
	// DCRedirectionGetWorkerBuildIdOrderingOperation tracks RPC calls for dc redirection
	DCRedirectionGetWorkerBuildIdOrderingOperation
	// DCRedirectionUpdateWorkflowOperation tracks RPC calls for dc redirection
	DCRedirectionUpdateWorkflowOperation
	// DCRedirectionDescribeBatchOperationOperation tracks RPC calls for dc redirection
	DCRedirectionDescribeBatchOperationOperation
	// DCRedirectionListBatchOperationsOperation tracks RPC calls for dc redirection
	DCRedirectionListBatchOperationsOperation
	// DCRedirectionStartBatchOperationOperation tracks RPC calls for dc redirection
	DCRedirectionStartBatchOperationOperation
	// DCRedirectionStopBatchOperationOperation tracks RPC calls for dc redirection
	DCRedirectionStopBatchOperationOperation

	// MessagingClientPublishOperation tracks Publish calls made by service to messaging layer
	MessagingClientPublishOperation
	// MessagingClientPublishBatchOperation tracks Publish calls made by service to messaging layer
	MessagingClientPublishBatchOperation

	// NamespaceCacheOperation tracks namespace cache callbacks
	NamespaceCacheOperation

	// PersistenceAppendHistoryNodesOperation tracks AppendHistoryNodes calls made by service to persistence layer
	PersistenceAppendHistoryNodesOperation
	// PersistenceAppendRawHistoryNodesOperation tracks AppendRawHistoryNodes calls made by service to persistence layer
	PersistenceAppendRawHistoryNodesOperation
	// PersistenceDeleteHistoryNodesOperation tracks DeleteHistoryNodes calls made by service to persistence layer
	PersistenceDeleteHistoryNodesOperation
	// PersistenceParseHistoryBranchInfoOperation tracks NewHistoryBranch calls made by service to persistence layer
	PersistenceParseHistoryBranchInfoOperation
	// PersistenceUpdateHistoryBranchInfoOperation tracks NewHistoryBranch calls made by service to persistence layer
	PersistenceUpdateHistoryBranchInfoOperation
	// PersistenceNewHistoryBranchOperation tracks NewHistoryBranch calls made by service to persistence layer
	PersistenceNewHistoryBranchOperation
	// PersistenceReadHistoryBranchOperation tracks ReadHistoryBranch calls made by service to persistence layer
	PersistenceReadHistoryBranchOperation
	// PersistenceReadHistoryBranchReverseOperation tracks ReadHistoryBranchReverse calls made by service to persistence layer
	PersistenceReadHistoryBranchReverseOperation
	// PersistenceForkHistoryBranchOperation tracks ForkHistoryBranch calls made by service to persistence layer
	PersistenceForkHistoryBranchOperation
	// PersistenceDeleteHistoryBranchOperation tracks DeleteHistoryBranch calls made by service to persistence layer
	PersistenceDeleteHistoryBranchOperation
	// PersistenceTrimHistoryBranchOperation tracks TrimHistoryBranch calls made by service to persistence layer
	PersistenceTrimHistoryBranchOperation
	// PersistenceCompleteForkBranchOperation tracks CompleteForkBranch calls made by service to persistence layer
	PersistenceCompleteForkBranchOperation
	// PersistenceGetHistoryTreeOperation tracks GetHistoryTree calls made by service to persistence layer
	PersistenceGetHistoryTreeOperation
	// PersistenceGetAllHistoryTreeBranchesOperation tracks GetHistoryTree calls made by service to persistence layer
	PersistenceGetAllHistoryTreeBranchesOperation
	// PersistenceNamespaceReplicationQueueOperation is the metrics Operation for namespace replication queue
	PersistenceNamespaceReplicationQueueOperation = "NamespaceReplicationQueue"

	// ClusterMetadataArchivalConfigOperation tracks ArchivalConfig calls to ClusterMetadata
	ClusterMetadataArchivalConfigOperation

	// ElasticsearchBulkProcessorOperation is Operation used by all metric emitted by Elasticsearch bulk processor
	ElasticsearchBulkProcessorOperation = "ElasticsearchBulkProcessor"

	// ElasticsearchVisibilityOperation is Operation used by all Elasticsearch visibility metrics
	ElasticsearchVisibilityOperation = "ElasticsearchVisibility"

	// SequentialTaskProcessingOperation is used by sequential task processing logic
	SequentialTaskProcessingOperation
	// ParallelTaskProcessingOperation is used by parallel task processing logic
	ParallelTaskProcessingOperation
	// TaskSchedulerOperation is used by task scheduler logic
	TaskSchedulerOperation

	// VisibilityArchiverOperation is used by visibility archivers
	VisibilityArchiverOperation

	// The following metrics are only used by internal archiver implemention.
	// TODO=move them to internal repo once temporal plugin model is in place.

	// BlobstoreClientUploadOperation tracks Upload calls to blobstore
	BlobstoreClientUploadOperation
	// BlobstoreClientDownloadOperation tracks Download calls to blobstore
	BlobstoreClientDownloadOperation
	// BlobstoreClientGetMetadataOperation tracks GetMetadata calls to blobstore
	BlobstoreClientGetMetadataOperation
	// BlobstoreClientExistsOperation tracks Exists calls to blobstore
	BlobstoreClientExistsOperation
	// BlobstoreClientDeleteOperation tracks Delete calls to blobstore
	BlobstoreClientDeleteOperation
	// BlobstoreClientDirectoryExistsOperation tracks DirectoryExists calls to blobstore
	BlobstoreClientDirectoryExistsOperation

	// ExecutionsScavengerOperation is the operation used by all metrics emitted by worker.executions.Scavenger module
	ExecutionsScavengerOperation = "executionsscavenger"
	// HistoryScavengerOperation is the operation used by all metrics emitted by worker.history.Scavenger module
	HistoryScavengerOperation = "historyscavenger"
	// TaskQueueScavengerOperation is the operation used by all metrics emitted by worker.taskqueue.Scavenger module
	TaskQueueScavengerOperation = "taskqueuescavenger"
	// HistoryArchiverOperation is used by history archivers
	HistoryArchiverOperation = "HistoryArchiver"
	// BatcherOperation is the operation used by all metrics emitted by worker.Batcher module
	BatcherOperation                  = "Batcher"
	DeleteNamespaceWorkflowOperation  = "DeleteNamespaceWorkflow"
	ReclaimResourcesWorkflowOperation = "ReclaimResourcesWorkflow"
	DeleteExecutionsWorkflowOperation = "DeleteExecutionsWorkflow"
	// AddSearchAttributesWorkflowOperation is the operation used by all metrics emitted by worker.AddSearchAttributesWorkflowthe operation module
	AddSearchAttributesWorkflowOperation
	// ArchiverDeleteHistoryActivityOperation is the operation used by all metrics emitted by archiver.DeleteHistoryActivity
	ArchiverDeleteHistoryActivityOperation = "ArchiverDeleteHistoryActivity"
	// ArchiverUploadHistoryActivityOperation is the operation used by all metrics emitted by archiver.UploadHistoryActivity
	ArchiverUploadHistoryActivityOperation = "ArchiverUploadHistoryActivity"
	// ArchiverArchiveVisibilityActivityOperation is the operation used by all metrics emitted by archiver.ArchiveVisibilityActivity
	ArchiverArchiveVisibilityActivityOperation = "ArchiverArchiveVisibilityActivity"
	// ArchiverOperation is the operation used by all metrics emitted by archiver.Archiver
	ArchiverOperation = "Archiver"
	// ArchiverPumpOperation is the operation used by all metrics emitted by archiver.Pump
	ArchiverPumpOperation = "ArchiverPump"
	// ArchiverArchivalWorkflowOperation is the operation used by all metrics emitted by archiver.ArchivalWorkflow
	ArchiverArchivalWorkflowOperation = "ArchiverArchivalWorkflow"
	// ArchiverClientOperation is the operation used by all metrics emitted by archiver.Client
	ArchiverClientOperation = "ArchiverClient"
	// NamespaceReplicationTaskOperation is the operation used by namespace task replication processing
	NamespaceReplicationTaskOperation = "NamespaceReplicationTask"
	// MigrationWorkflowOperation is the operation used by metrics emitted by migration related workflows
	MigrationWorkflowOperation = "MigrationWorkflow"
	// ParentClosePolicyProcessorOperation is the operation used by all metrics emitted by worker.ParentClosePolicyProcessor
	ParentClosePolicyProcessorOperation = "ParentClosePolicyProcessor"

	// VisibilityPersistenceRecordWorkflowExecutionStartedOperation tracks RecordWorkflowExecutionStarted calls made by service to visibility persistence layer
	VisibilityPersistenceRecordWorkflowExecutionStartedOperation = "RecordWorkflowExecutionStarted"
	// VisibilityPersistenceRecordWorkflowExecutionClosedOperation tracks RecordWorkflowExecutionClosed calls made by service to visibility persistence layer
	VisibilityPersistenceRecordWorkflowExecutionClosedOperation = "RecordWorkflowExecutionClosed"
	// VisibilityPersistenceUpsertWorkflowExecutionOperation tracks UpsertWorkflowExecution calls made by service to persistence visibility layer
	VisibilityPersistenceUpsertWorkflowExecutionOperation = "UpsertWorkflowExecution"
	// VisibilityPersistenceListOpenWorkflowExecutionsOperation tracks ListOpenWorkflowExecutions calls made by service to visibility persistence layer
	VisibilityPersistenceListOpenWorkflowExecutionsOperation = "ListOpenWorkflowExecutions"
	// VisibilityPersistenceListClosedWorkflowExecutionsOperation tracks ListClosedWorkflowExecutions calls made by service to visibility persistence layer
	VisibilityPersistenceListClosedWorkflowExecutionsOperation = "ListClosedWorkflowExecutions"
	// VisibilityPersistenceListOpenWorkflowExecutionsByTypeOperation tracks ListOpenWorkflowExecutionsByType calls made by service to visibility persistence layer
	VisibilityPersistenceListOpenWorkflowExecutionsByTypeOperation = "ListOpenWorkflowExecutionsByType"
	// VisibilityPersistenceListClosedWorkflowExecutionsByTypeOperation tracks ListClosedWorkflowExecutionsByType calls made by service to visibility persistence layer
	VisibilityPersistenceListClosedWorkflowExecutionsByTypeOperation = "ListClosedWorkflowExecutionsByType"
	// VisibilityPersistenceListOpenWorkflowExecutionsByWorkflowIDOperation tracks ListOpenWorkflowExecutionsByWorkflowID calls made by service to visibility persistence layer
	VisibilityPersistenceListOpenWorkflowExecutionsByWorkflowIDOperation = "ListOpenWorkflowExecutionsByWorkflowID"
	// VisibilityPersistenceListClosedWorkflowExecutionsByWorkflowIDOperation tracks ListClosedWorkflowExecutionsByWorkflowID calls made by service to visibility persistence layer
	VisibilityPersistenceListClosedWorkflowExecutionsByWorkflowIDOperation = "ListClosedWorkflowExecutionsByWorkflowID"
	// VisibilityPersistenceListClosedWorkflowExecutionsByStatusOperation tracks ListClosedWorkflowExecutionsByStatus calls made by service to visibility persistence layer
	VisibilityPersistenceListClosedWorkflowExecutionsByStatusOperation = "ListClosedWorkflowExecutionsByStatus"
	// VisibilityPersistenceDeleteWorkflowExecutionOperation tracks DeleteWorkflowExecutions calls made by service to visibility persistence layer
	VisibilityPersistenceDeleteWorkflowExecutionOperation = "DeleteWorkflowExecutions"
	// VisibilityPersistenceListWorkflowExecutionsOperation tracks ListWorkflowExecutions calls made by service to visibility persistence layer
	VisibilityPersistenceListWorkflowExecutionsOperation = "ListWorkflowExecutions"
	// VisibilityPersistenceScanWorkflowExecutionsOperation tracks ScanWorkflowExecutions calls made by service to visibility persistence layer
	VisibilityPersistenceScanWorkflowExecutionsOperation = "ScanWorkflowExecutions"
	// VisibilityPersistenceCountWorkflowExecutionsOperation tracks CountWorkflowExecutions calls made by service to visibility persistence layer
	VisibilityPersistenceCountWorkflowExecutionsOperation = "CountWorkflowExecutions"

	// MatchingPollWorkflowTaskQueueOperation tracks PollWorkflowTaskQueue API calls received by service
	MatchingPollWorkflowTaskQueueOperation = "PollWorkflowTaskQueue"
	// MatchingPollActivityTaskQueueOperation tracks PollActivityTaskQueue API calls received by service
	MatchingPollActivityTaskQueueOperation = "PollActivityTaskQueue"
	// MatchingAddActivityTaskOperation tracks AddActivityTask API calls received by service
	MatchingAddActivityTaskOperation = "AddActivityTask"
	// MatchingAddWorkflowTaskOperation tracks AddWorkflowTask API calls received by service
	MatchingAddWorkflowTaskOperation = "AddWorkflowTask"
	// MatchingTaskQueueMgrOperation is the metrics operation for matching.TaskQueueManager component
	MatchingTaskQueueMgrOperation = "TaskQueueMgr"
	// MatchingEngineOperation is the metrics Operation for matchingEngine component
	MatchingEngineOperation = "MatchingEngine"
	// MatchingQueryWorkflowOperation tracks AddWorkflowTask API calls received by service
	MatchingQueryWorkflowOperation = "QueryWorkflow"
	// MatchingRespondQueryTaskCompletedOperation tracks AddWorkflowTask API calls received by service
	MatchingRespondQueryTaskCompletedOperation = "RespondQueryTaskCompleted"
	// MatchingCancelOutstandingPollOperation tracks CancelOutstandingPoll API calls received by service
	MatchingCancelOutstandingPollOperation = "CancelOutstandingPoll"
	// MatchingDescribeTaskQueueOperation tracks DescribeTaskQueue API calls received by service
	MatchingDescribeTaskQueueOperation = "DescribeTaskQueue"
	// MatchingListTaskQueuePartitionsOperation tracks ListTaskQueuePartitions API calls received by service
	MatchingListTaskQueuePartitionsOperation = "ListTaskQueuePartitions"
	// MatchingUpdateWorkerBuildIdOrderingOperation tracks UpdateWorkerBuildIdOrdering API calls received by service
	MatchingUpdateWorkerBuildIdOrderingOperation = "UpdateWorkerBuildIdOrdering"
	// MatchingGetWorkerBuildIdOrderingOperation tracks GetWorkerBuildIdOrdering API calls received by service
	MatchingGetWorkerBuildIdOrderingOperation
	// MatchingInvalidateTaskQueueMetadataOperation tracks GetWorkerBuildIdOrdering API calls received by service
	MatchingInvalidateTaskQueueMetadataOperation
	// MatchingGetTaskQueueMetadataOperation tracks GetWorkerBuildIdOrdering API calls received by service
	MatchingGetTaskQueueMetadataOperation
)

var (
	ServiceErrUnauthorizedCounter         = NewCounterDef("service_errors_unauthorized")
	ServiceErrAuthorizeFailedCounter      = NewCounterDef("service_errors_authorize_failed")
	ServiceAuthorizationLatency           = NewTimerDef("service_authorization_latency")
	TlsCertsExpired                       = NewGaugeDef("certificates_expired")
	TlsCertsExpiring                      = NewGaugeDef("certificates_expiring")
	NamespaceCachePrepareCallbacksLatency = NewTimerDef("namespace_cache_prepare_callbacks_latency")
	NamespaceCacheCallbacksLatency        = NewTimerDef("namespace_cache_callbacks_latency")
	CacheRequests                         = NewCounterDef("cache_requests")
	CacheFailures                         = NewCounterDef("cache_errors")
	CacheLatency                          = NewTimerDef("cache_latency")
	CacheMissCounter                      = NewCounterDef("cache_miss")
	ActionCounter                         = NewCounterDef("action")
	StateTransitionCount                  = NewDimensionlessHistogramDef("state_transition_count")
	HistorySize                           = NewBytesHistogramDef("history_size")
	HistoryCount                          = NewDimensionlessHistogramDef("history_count")
	ClientRedirectionRequests             = NewCounterDef("client_redirection_requests")
	ClientRedirectionFailures             = NewCounterDef("client_redirection_errors")
	ClientRedirectionLatency              = NewTimerDef("client_redirection_latency")
	VersionCheckSuccessCount              = NewCounterDef("version_check_success")
	VersionCheckFailedCount               = NewCounterDef("version_check_failed")
	VersionCheckRequestFailedCount        = NewCounterDef("version_check_request_failed")
	VersionCheckLatency                   = NewTimerDef("version_check_latency")
	LockRequests                          = NewCounterDef("lock_requests")
	LockFailures                          = NewCounterDef("lock_failures")
	LockLatency                           = NewTimerDef("lock_latency")
	SearchAttributesSize                  = NewBytesHistogramDef("search_attributes_size")

	EventBlobSize                                 = NewBytesHistogramDef("event_blob_size")
	ClientRequests                                = NewCounterDef("client_requests")
	ClientFailures                                = NewCounterDef("client_errors")
	ClientLatency                                 = NewTimerDef("client_latency")
	MatchingClientForwardedCounter                = NewCounterDef("forwarded")
	MatchingClientInvalidTaskQueueName            = NewCounterDef("invalid_task_queue_name")
	ServiceLatency                                = NewTimerDef("service_latency")
	ServiceLatencyNoUserLatency                   = NewTimerDef("service_latency_nouserlatency")
	ServiceLatencyUserLatency                     = NewTimerDef("service_latency_userlatency")
	ServiceRequests                               = NewCounterDef("service_requests")
	ServicePendingRequests                        = NewGaugeDef("service_pending_requests")
	ServiceFailures                               = NewCounterDef("service_errors")
	ServiceErrorWithType                          = NewCounterDef("service_error_with_type")
	ServiceCriticalFailures                       = NewCounterDef("service_errors_critical")
	ServiceErrInvalidArgumentCounter              = NewCounterDef("service_errors_invalid_argument")
	ServiceErrNamespaceNotActiveCounter           = NewCounterDef("service_errors_namespace_not_active")
	ServiceErrResourceExhaustedCounter            = NewCounterDef("service_errors_resource_exhausted")
	ServiceErrNotFoundCounter                     = NewCounterDef("service_errors_entity_not_found")
	ServiceErrExecutionAlreadyStartedCounter      = NewCounterDef("service_errors_execution_already_started")
	ServiceErrNamespaceAlreadyExistsCounter       = NewCounterDef("service_errors_namespace_already_exists")
	ServiceErrCancellationAlreadyRequestedCounter = NewCounterDef("service_errors_cancellation_already_requested")
	ServiceErrQueryFailedCounter                  = NewCounterDef("service_errors_query_failed")
	ServiceErrContextCancelledCounter             = NewCounterDef("service_errors_context_cancelled")
	ServiceErrContextTimeoutCounter               = NewCounterDef("service_errors_context_timeout")
	ServiceErrRetryTaskCounter                    = NewCounterDef("service_errors_retry_task")
	ServiceErrBadBinaryCounter                    = NewCounterDef("service_errors_bad_binary")
	ServiceErrClientVersionNotSupportedCounter    = NewCounterDef("service_errors_client_version_not_supported")
	ServiceErrIncompleteHistoryCounter            = NewCounterDef("service_errors_incomplete_history")
	ServiceErrNonDeterministicCounter             = NewCounterDef("service_errors_nondeterministic")

	NamespaceReplicationDLQMaxLevelGauge                = NewGaugeDef("namespace_dlq_max_level")
	NamespaceReplicationTaskAckLevelGauge               = NewGaugeDef("namespace_replication_task_ack_level")
	PersistenceRequests                                 = NewCounterDef("persistence_requests")
	PersistenceFailures                                 = NewCounterDef("persistence_errors")
	PersistenceErrorWithType                            = NewCounterDef("persistence_error_with_type")
	PersistenceLatency                                  = NewTimerDef("persistence_latency")
	PersistenceErrShardExistsCounter                    = NewCounterDef("persistence_errors_shard_exists")
	PersistenceErrShardOwnershipLostCounter             = NewCounterDef("persistence_errors_shard_ownership_lost")
	PersistenceErrConditionFailedCounter                = NewCounterDef("persistence_errors_condition_failed")
	PersistenceErrCurrentWorkflowConditionFailedCounter = NewCounterDef("persistence_errors_current_workflow_condition_failed")
	PersistenceErrWorkflowConditionFailedCounter        = NewCounterDef("persistence_errors_workflow_condition_failed")
	PersistenceErrTimeoutCounter                        = NewCounterDef("persistence_errors_timeout")
	PersistenceErrBusyCounter                           = NewCounterDef("persistence_errors_busy")
	PersistenceErrEntityNotExistsCounter                = NewCounterDef("persistence_errors_entity_not_exists")
	PersistenceErrNamespaceAlreadyExistsCounter         = NewCounterDef("persistence_errors_namespace_already_exists")
	PersistenceErrBadRequestCounter                     = NewCounterDef("persistence_errors_bad_request")
	PersistenceErrResourceExhaustedCounter              = NewCounterDef("persistence_errors_resource_exhausted")
	ElasticsearchBulkProcessorRequests                  = NewCounterDef("elasticsearch_bulk_processor_requests")
	ElasticsearchBulkProcessorQueuedRequests            = NewDimensionlessHistogramDef("elasticsearch_bulk_processor_queued_requests")
	ElasticsearchBulkProcessorRetries                   = NewCounterDef("elasticsearch_bulk_processor_retries")
	ElasticsearchBulkProcessorFailures                  = NewCounterDef("elasticsearch_bulk_processor_errors")
	ElasticsearchBulkProcessorCorruptedData             = NewCounterDef("elasticsearch_bulk_processor_corrupted_data")
	ElasticsearchBulkProcessorDuplicateRequest          = NewCounterDef("elasticsearch_bulk_processor_duplicate_request")
	ElasticsearchBulkProcessorRequestLatency            = NewTimerDef("elasticsearch_bulk_processor_request_latency")
	ElasticsearchBulkProcessorCommitLatency             = NewTimerDef("elasticsearch_bulk_processor_commit_latency")
	ElasticsearchBulkProcessorWaitAddLatency            = NewTimerDef("elasticsearch_bulk_processor_wait_add_latency")
	ElasticsearchBulkProcessorWaitStartLatency          = NewTimerDef("elasticsearch_bulk_processor_wait_start_latency")
	ElasticsearchBulkProcessorBulkSize                  = NewDimensionlessHistogramDef("elasticsearch_bulk_processor_bulk_size")
	ElasticsearchDocumentParseFailuresCount             = NewCounterDef("elasticsearch_document_parse_failures_counter")
	ElasticsearchDocumentGenerateFailuresCount          = NewCounterDef("elasticsearch_document_generate_failures_counter")
	VisibilityPersistenceRequests                       = NewCounterDef("visibility_persistence_requests")
	VisibilityPersistenceErrorWithType                  = NewCounterDef("visibility_persistence_error_with_type")
	VisibilityPersistenceFailures                       = NewCounterDef("visibility_persistence_errors")
	VisibilityPersistenceResourceExhausted              = NewCounterDef("visibility_persistence_resource_exhausted")
	VisibilityPersistenceLatency                        = NewTimerDef("visibility_persistence_latency")

	StartedCount                                              = NewCounterDef("started")
	StoppedCount                                              = NewCounterDef("stopped")
	ExecutorTasksDoneCount                                    = NewCounterDef("executor_done")
	ExecutorTasksErrCount                                     = NewCounterDef("executor_err")
	ExecutorTasksDeferredCount                                = NewCounterDef("executor_deferred")
	ExecutorTasksDroppedCount                                 = NewCounterDef("executor_dropped")
	ScavengerValidationRequestsCount                          = NewCounterDef("scavenger_validation_requests")
	ScavengerValidationFailuresCount                          = NewCounterDef("scavenger_validation_failures")
	HistoryScavengerSuccessCount                              = NewCounterDef("scavenger_success")
	HistoryScavengerErrorCount                                = NewCounterDef("scavenger_errors")
	HistoryScavengerSkipCount                                 = NewCounterDef("scavenger_skips")
	ExecutionsOutstandingCount                                = NewGaugeDef("executions_outstanding")
	TaskProcessedCount                                        = NewGaugeDef("task_processed")
	TaskDeletedCount                                          = NewGaugeDef("task_deleted")
	TaskQueueProcessedCount                                   = NewGaugeDef("taskqueue_processed")
	TaskQueueDeletedCount                                     = NewGaugeDef("taskqueue_deleted")
	TaskQueueOutstandingCount                                 = NewGaugeDef("taskqueue_outstanding")
	HistoryArchiverArchiveNonRetryableErrorCount              = NewCounterDef("history_archiver_archive_non_retryable_error")
	HistoryArchiverArchiveTransientErrorCount                 = NewCounterDef("history_archiver_archive_transient_error")
	HistoryArchiverArchiveSuccessCount                        = NewCounterDef("history_archiver_archive_success")
	HistoryArchiverHistoryMutatedCount                        = NewCounterDef("history_archiver_history_mutated")
	HistoryArchiverTotalUploadSize                            = NewBytesHistogramDef("history_archiver_total_upload_size")
	HistoryArchiverHistorySize                                = NewBytesHistogramDef("history_archiver_history_size")
	HistoryArchiverDuplicateArchivalsCount                    = NewCounterDef("history_archiver_duplicate_archivals")
	HistoryArchiverBlobExistsCount                            = NewCounterDef("history_archiver_blob_exists")
	HistoryArchiverBlobSize                                   = NewBytesHistogramDef("history_archiver_blob_size")
	HistoryArchiverRunningDeterministicConstructionCheckCount = NewCounterDef("history_archiver_running_deterministic_construction_check")
	HistoryArchiverDeterministicConstructionCheckFailedCount  = NewCounterDef("history_archiver_deterministic_construction_check_failed")
	HistoryArchiverRunningBlobIntegrityCheckCount             = NewCounterDef("history_archiver_running_blob_integrity_check")
	HistoryArchiverBlobIntegrityCheckFailedCount              = NewCounterDef("history_archiver_blob_integrity_check_failed")
	VisibilityArchiverArchiveNonRetryableErrorCount           = NewCounterDef("visibility_archiver_archive_non_retryable_error")
	VisibilityArchiverArchiveTransientErrorCount              = NewCounterDef("visibility_archiver_archive_transient_error")
	VisibilityArchiveSuccessCount                             = NewCounterDef("visibility_archiver_archive_success")
	BatcherProcessorSuccess                                   = NewCounterDef("batcher_processor_requests")
	BatcherProcessorFailures                                  = NewCounterDef("batcher_processor_errors")
	DeleteNamespaceSuccessCount                               = NewCounterDef("delete_namespace_success")
	RenameNamespaceSuccessCount                               = NewCounterDef("rename_namespace_success")
	DeleteExecutionsSuccessCount                              = NewCounterDef("delete_executions_success")
	DeleteNamespaceFailuresCount                              = NewCounterDef("delete_namespace_failures")
	UpdateNamespaceFailuresCount                              = NewCounterDef("update_namespace_failures")
	RenameNamespaceFailuresCount                              = NewCounterDef("rename_namespace_failures")
	ReadNamespaceFailuresCount                                = NewCounterDef("read_namespace_failures")
	ListExecutionsFailuresCount                               = NewCounterDef("list_executions_failures")
	CountExecutionsFailuresCount                              = NewCounterDef("count_executions_failures")
	DeleteExecutionFailuresCount                              = NewCounterDef("delete_execution_failures")
	DeleteExecutionNotFoundCount                              = NewCounterDef("delete_execution_not_found")
	RateLimiterFailuresCount                                  = NewCounterDef("rate_limiter_failures")
	AddSearchAttributesFailuresCount                          = NewCounterDef("add_search_attributes_failures")
	ArchiverNonRetryableErrorCount                            = NewCounterDef("archiver_non_retryable_error")
	ArchiverStartedCount                                      = NewCounterDef("archiver_started")
	ArchiverStoppedCount                                      = NewCounterDef("archiver_stopped")
	ArchiverCoroutineStartedCount                             = NewCounterDef("archiver_coroutine_started")
	ArchiverCoroutineStoppedCount                             = NewCounterDef("archiver_coroutine_stopped")
	ArchiverHandleHistoryRequestLatency                       = NewTimerDef("archiver_handle_history_request_latency")
	ArchiverHandleVisibilityRequestLatency                    = NewTimerDef("archiver_handle_visibility_request_latency")
	ArchiverUploadWithRetriesLatency                          = NewTimerDef("archiver_upload_with_retries_latency")
	ArchiverDeleteWithRetriesLatency                          = NewTimerDef("archiver_delete_with_retries_latency")
	ArchiverUploadFailedAllRetriesCount                       = NewCounterDef("archiver_upload_failed_all_retries")
	ArchiverUploadSuccessCount                                = NewCounterDef("archiver_upload_success")
	ArchiverDeleteFailedAllRetriesCount                       = NewCounterDef("archiver_delete_failed_all_retries")
	ArchiverDeleteSuccessCount                                = NewCounterDef("archiver_delete_success")
	ArchiverHandleVisibilityFailedAllRetiresCount             = NewCounterDef("archiver_handle_visibility_failed_all_retries")
	ArchiverHandleVisibilitySuccessCount                      = NewCounterDef("archiver_handle_visibility_success")
	ArchiverBacklogSizeGauge                                  = NewCounterDef("archiver_backlog_size")
	ArchiverPumpTimeoutCount                                  = NewCounterDef("archiver_pump_timeout")
	ArchiverPumpSignalThresholdCount                          = NewCounterDef("archiver_pump_signal_threshold")
	ArchiverPumpTimeoutWithoutSignalsCount                    = NewCounterDef("archiver_pump_timeout_without_signals")
	ArchiverPumpSignalChannelClosedCount                      = NewCounterDef("archiver_pump_signal_channel_closed")
	ArchiverWorkflowStartedCount                              = NewCounterDef("archiver_workflow_started")
	ArchiverNumPumpedRequestsCount                            = NewCounterDef("archiver_num_pumped_requests")
	ArchiverNumHandledRequestsCount                           = NewCounterDef("archiver_num_handled_requests")
	ArchiverPumpedNotEqualHandledCount                        = NewCounterDef("archiver_pumped_not_equal_handled")
	ArchiverHandleAllRequestsLatency                          = NewTimerDef("archiver_handle_all_requests_latency")
	ArchiverWorkflowStoppingCount                             = NewCounterDef("archiver_workflow_stopping")
	ArchiverClientSendSignalCount                             = NewCounterDef("archiver_client_sent_signal")
	ArchiverClientSendSignalFailureCount                      = NewCounterDef("archiver_client_send_signal_error")
	ArchiverClientHistoryRequestCount                         = NewCounterDef("archiver_client_history_request")
	ArchiverClientHistoryInlineArchiveAttemptCount            = NewCounterDef("archiver_client_history_inline_archive_attempt")
	ArchiverClientHistoryInlineArchiveFailureCount            = NewCounterDef("archiver_client_history_inline_archive_failure")
	ArchiverClientVisibilityRequestCount                      = NewCounterDef("archiver_client_visibility_request")
	ArchiverClientVisibilityInlineArchiveAttemptCount         = NewCounterDef("archiver_client_visibility_inline_archive_attempt")
	ArchiverClientVisibilityInlineArchiveFailureCount         = NewCounterDef("archiver_client_visibility_inline_archive_failure")
	ReplicatorMessages                                        = NewCounterDef("replicator_messages")
	ReplicatorFailures                                        = NewCounterDef("replicator_errors")
	ReplicatorLatency                                         = NewTimerDef("replicator_latency")
	ReplicatorDLQFailures                                     = NewCounterDef("replicator_dlq_enqueue_fails")
	ScanDuration                                              = NewTimerDef("scan_duration")
	NamespaceReplicationEnqueueDLQCount                       = NewCounterDef("namespace_replication_dlq_enqueue_requests")
	CatchUpReadyShardCountGauge                               = NewGaugeDef("catchup_ready_shard_count")
	HandoverReadyShardCountGauge                              = NewGaugeDef("handover_ready_shard_count")
	ParentClosePolicyProcessorSuccess                         = NewCounterDef("parent_close_policy_processor_requests")
	ParentClosePolicyProcessorFailures                        = NewCounterDef("parent_close_policy_processor_errors")

	HistoryEventNotificationQueueingLatency      = NewTimerDef("history_event_notification_queueing_latency")
	HistoryEventNotificationFanoutLatency        = NewTimerDef("history_event_notification_fanout_latency")
	HistoryEventNotificationInFlightMessageGauge = NewGaugeDef("history_event_notification_inflight_message_gauge")
	HistoryEventNotificationFailDeliveryCount    = NewCounterDef("history_event_notification_fail_delivery_count")

	AddSearchAttributesWorkflowSuccessCount  = NewCounterDef("add_search_attributes_workflow_success")
	AddSearchAttributesWorkflowFailuresCount = NewCounterDef("add_search_attributes_workflow_failure")

	DeleteNamespaceWorkflowSuccessCount  = NewCounterDef("delete_namespace_workflow_success")
	DeleteNamespaceWorkflowFailuresCount = NewCounterDef("delete_namespace_workflow_failure")
)

// Matching
var (
	PollSuccessPerTaskQueueCounter            = NewRollupCounterDef("poll_success_per_tl", "poll_success")
	PollTimeoutPerTaskQueueCounter            = NewRollupCounterDef("poll_timeouts_per_tl", "poll_timeouts")
	PollSuccessWithSyncPerTaskQueueCounter    = NewRollupCounterDef("poll_success_sync_per_tl", "poll_success_sync")
	LeaseRequestPerTaskQueueCounter           = NewRollupCounterDef("lease_requests_per_tl", "lease_requests")
	LeaseFailurePerTaskQueueCounter           = NewRollupCounterDef("lease_failures_per_tl", "lease_failures")
	ConditionFailedErrorPerTaskQueueCounter   = NewRollupCounterDef("condition_failed_errors_per_tl", "condition_failed_errors")
	RespondQueryTaskFailedPerTaskQueueCounter = NewRollupCounterDef("respond_query_failed_per_tl", "respond_query_failed")
	SyncThrottlePerTaskQueueCounter           = NewRollupCounterDef("sync_throttle_count_per_tl", "sync_throttle_count")
	BufferThrottlePerTaskQueueCounter         = NewRollupCounterDef("buffer_throttle_count_per_tl", "buffer_throttle_count")
	ExpiredTasksPerTaskQueueCounter           = NewRollupCounterDef("tasks_expired_per_tl", "tasks_expired")
	ForwardedPerTaskQueueCounter              = NewCounterDef("forwarded_per_tl")
	ForwardTaskCallsPerTaskQueue              = NewRollupCounterDef("forward_task_calls_per_tl", "forward_task_calls")
	ForwardTaskErrorsPerTaskQueue             = NewRollupCounterDef("forward_task_errors_per_tl", "forward_task_errors")
	ForwardQueryCallsPerTaskQueue             = NewRollupCounterDef("forward_query_calls_per_tl", "forward_query_calls")
	ForwardQueryErrorsPerTaskQueue            = NewRollupCounterDef("forward_query_errors_per_tl", "forward_query_errors")
	ForwardPollCallsPerTaskQueue              = NewRollupCounterDef("forward_poll_calls_per_tl", "forward_poll_calls")
	ForwardPollErrorsPerTaskQueue             = NewRollupCounterDef("forward_poll_errors_per_tl", "forward_poll_errors")
	SyncMatchLatencyPerTaskQueue              = NewRollupTimerDef("syncmatch_latency_per_tl", "syncmatch_latency")
	AsyncMatchLatencyPerTaskQueue             = NewRollupTimerDef("asyncmatch_latency_per_tl", "asyncmatch_latency")
	ForwardTaskLatencyPerTaskQueue            = NewRollupTimerDef("forward_task_latency_per_tl", "forward_task_latency")
	ForwardQueryLatencyPerTaskQueue           = NewRollupTimerDef("forward_query_latency_per_tl", "forward_query_latency")
	ForwardPollLatencyPerTaskQueue            = NewRollupTimerDef("forward_poll_latency_per_tl", "forward_poll_latency")
	LocalToLocalMatchPerTaskQueueCounter      = NewRollupCounterDef("local_to_local_matches_per_tl", "local_to_local_matches")
	LocalToRemoteMatchPerTaskQueueCounter     = NewRollupCounterDef("local_to_remote_matches_per_tl", "local_to_remote_matches")
	RemoteToLocalMatchPerTaskQueueCounter     = NewRollupCounterDef("remote_to_local_matches_per_tl", "remote_to_local_matches")
	RemoteToRemoteMatchPerTaskQueueCounter    = NewRollupCounterDef("remote_to_remote_matches_per_tl", "remote_to_remote_matches")
	LoadedTaskQueueGauge                      = NewGaugeDef("loaded_task_queue_count")
	TaskQueueStartedCounter                   = NewCounterDef("task_queue_started")
	TaskQueueStoppedCounter                   = NewCounterDef("task_queue_stopped")
	TaskWriteThrottlePerTaskQueueCounter      = NewRollupCounterDef("task_write_throttle_count_per_tl", "task_write_throttle_count")
	TaskWriteLatencyPerTaskQueue              = NewRollupTimerDef("task_write_latency_per_tl", "task_write_latency")
	TaskLagPerTaskQueueGauge                  = NewGaugeDef("task_lag_per_tl")
	NoRecentPollerTasksPerTaskQueueCounter    = NewRollupCounterDef("no_poller_tasks_per_tl", "no_poller_tasks")
)

// History
var (
	TaskRequests = NewCounterDef("task_requests")

	TaskLatency       = NewTimerDef("task_latency")               // overall/all attempts within single worker
	TaskUserLatency   = NewTimerDef("task_latency_userlatency")   // from task generated to task complete
	TaskNoUserLatency = NewTimerDef("task_latency_nouserlatency") // from task generated to task complete

	TaskAttemptTimer         = NewDimensionlessHistogramDef("task_attempt")
	TaskFailures             = NewCounterDef("task_errors")
	TaskDiscarded            = NewCounterDef("task_errors_discarded")
	TaskSkipped              = NewCounterDef("task_skipped")
	TaskStandbyRetryCounter  = NewCounterDef("task_errors_standby_retry_counter")
	TaskWorkflowBusyCounter  = NewCounterDef("task_errors_workflow_busy")
	TaskNotActiveCounter     = NewCounterDef("task_errors_not_active_counter")
	TaskLimitExceededCounter = NewCounterDef("task_errors_limit_exceeded_counter")

	TaskScheduleToStartLatency = NewTimerDef("task_schedule_to_start_latency")

	TaskProcessingLatency       = NewTimerDef("task_latency_processing")               // per-attempt
	TaskNoUserProcessingLatency = NewTimerDef("task_latency_processing_nouserlatency") // per-attempt

	TaskQueueLatency       = NewTimerDef("task_latency_queue")               // from task generated to task complete
	TaskNoUserQueueLatency = NewTimerDef("task_latency_queue_nouserlatency") // from task generated to task complete

	TransferTaskMissingEventCounter                  = NewCounterDef("transfer_task_missing_event_counter")
	TaskBatchCompleteCounter                         = NewCounterDef("task_batch_complete_counter")
	TaskReschedulerPendingTasks                      = NewDimensionlessHistogramDef("task_rescheduler_pending_tasks")
	TaskThrottledCounter                             = NewCounterDef("task_throttled_counter")
	ActivityE2ELatency                               = NewTimerDef("activity_end_to_end_latency")
	AckLevelUpdateCounter                            = NewCounterDef("ack_level_update")
	AckLevelUpdateFailedCounter                      = NewCounterDef("ack_level_update_failed")
	CommandTypeScheduleActivityCounter               = NewCounterDef("schedule_activity_command")
	CommandTypeCompleteWorkflowCounter               = NewCounterDef("complete_workflow_command")
	CommandTypeFailWorkflowCounter                   = NewCounterDef("fail_workflow_command")
	CommandTypeCancelWorkflowCounter                 = NewCounterDef("cancel_workflow_command")
	CommandTypeStartTimerCounter                     = NewCounterDef("start_timer_command")
	CommandTypeCancelActivityCounter                 = NewCounterDef("cancel_activity_command")
	CommandTypeCancelTimerCounter                    = NewCounterDef("cancel_timer_command")
	CommandTypeRecordMarkerCounter                   = NewCounterDef("record_marker_command")
	CommandTypeCancelExternalWorkflowCounter         = NewCounterDef("cancel_external_workflow_command")
	CommandTypeContinueAsNewCounter                  = NewCounterDef("continue_as_new_command")
	CommandTypeSignalExternalWorkflowCounter         = NewCounterDef("signal_external_workflow_command")
	CommandTypeUpsertWorkflowSearchAttributesCounter = NewCounterDef("upsert_workflow_search_attributes_command")
	CommandTypeModifyWorkflowPropertiesCounter       = NewCounterDef("modify_workflow_properties_command")
	CommandTypeChildWorkflowCounter                  = NewCounterDef("child_workflow_command")
	ActivityEagerExecutionCounter                    = NewCounterDef("activity_eager_execution")
	EmptyCompletionCommandsCounter                   = NewCounterDef("empty_completion_commands")
	MultipleCompletionCommandsCounter                = NewCounterDef("multiple_completion_commands")
	FailedWorkflowTasksCounter                       = NewCounterDef("failed_workflow_tasks")
	WorkflowTaskAttempt                              = NewDimensionlessHistogramDef("workflow_task_attempt")
	StaleMutableStateCounter                         = NewCounterDef("stale_mutable_state")
	AutoResetPointsLimitExceededCounter              = NewCounterDef("auto_reset_points_exceed_limit")
	AutoResetPointCorruptionCounter                  = NewCounterDef("auto_reset_point_corruption")
	ConcurrencyUpdateFailureCounter                  = NewCounterDef("concurrency_update_failure")
	ServiceErrShardOwnershipLostCounter              = NewCounterDef("service_errors_shard_ownership_lost")
	ServiceErrTaskAlreadyStartedCounter              = NewCounterDef("service_errors_task_already_started")
	HeartbeatTimeoutCounter                          = NewCounterDef("heartbeat_timeout")
	ScheduleToStartTimeoutCounter                    = NewCounterDef("schedule_to_start_timeout")
	StartToCloseTimeoutCounter                       = NewCounterDef("start_to_close_timeout")
	ScheduleToCloseTimeoutCounter                    = NewCounterDef("schedule_to_close_timeout")
	NewTimerNotifyCounter                            = NewCounterDef("new_timer_notifications")
	AcquireShardsCounter                             = NewCounterDef("acquire_shards_count")
	AcquireShardsLatency                             = NewTimerDef("acquire_shards_latency")
	ShardContextClosedCounter                        = NewCounterDef("shard_closed_count")
	ShardContextCreatedCounter                       = NewCounterDef("sharditem_created_count")
	ShardContextRemovedCounter                       = NewCounterDef("sharditem_removed_count")
	ShardContextAcquisitionLatency                   = NewTimerDef("sharditem_acquisition_latency")
	ShardInfoReplicationPendingTasksTimer            = NewDimensionlessHistogramDef("shardinfo_replication_pending_task")
	ShardInfoTransferActivePendingTasksTimer         = NewDimensionlessHistogramDef("shardinfo_transfer_active_pending_task")
	ShardInfoTransferStandbyPendingTasksTimer        = NewDimensionlessHistogramDef("shardinfo_transfer_standby_pending_task")
	ShardInfoTimerActivePendingTasksTimer            = NewDimensionlessHistogramDef("shardinfo_timer_active_pending_task")
	ShardInfoTimerStandbyPendingTasksTimer           = NewDimensionlessHistogramDef("shardinfo_timer_standby_pending_task")
	ShardInfoVisibilityPendingTasksTimer             = NewDimensionlessHistogramDef("shardinfo_visibility_pending_task")
	ShardInfoReplicationLagHistogram                 = NewDimensionlessHistogramDef("shardinfo_replication_lag")
	ShardInfoTransferLagHistogram                    = NewDimensionlessHistogramDef("shardinfo_transfer_lag")
	ShardInfoTimerLagTimer                           = NewTimerDef("shardinfo_timer_lag")
	ShardInfoVisibilityLagHistogram                  = NewDimensionlessHistogramDef("shardinfo_visibility_lag")
	ShardInfoTransferDiffHistogram                   = NewDimensionlessHistogramDef("shardinfo_transfer_diff")
	ShardInfoTimerDiffTimer                          = NewTimerDef("shardinfo_timer_diff")
	ShardInfoTransferFailoverInProgressHistogram     = NewDimensionlessHistogramDef("shardinfo_transfer_failover_in_progress")
	ShardInfoTimerFailoverInProgressHistogram        = NewDimensionlessHistogramDef("shardinfo_timer_failover_in_progress")
	ShardInfoTransferFailoverLatencyTimer            = NewTimerDef("shardinfo_transfer_failover_latency")
	ShardInfoTimerFailoverLatencyTimer               = NewTimerDef("shardinfo_timer_failover_latency")
	SyncShardFromRemoteCounter                       = NewCounterDef("syncshard_remote_count")
	SyncShardFromRemoteFailure                       = NewCounterDef("syncshard_remote_failed")
	MembershipChangedCounter                         = NewCounterDef("membership_changed_count")
	NumShardsGauge                                   = NewGaugeDef("numshards_gauge")
	GetEngineForShardErrorCounter                    = NewCounterDef("get_engine_for_shard_errors")
	GetEngineForShardLatency                         = NewTimerDef("get_engine_for_shard_latency")
	RemoveEngineForShardLatency                      = NewTimerDef("remove_engine_for_shard_latency")
	CompleteWorkflowTaskWithStickyEnabledCounter     = NewCounterDef("complete_workflow_task_sticky_enabled_count")
	CompleteWorkflowTaskWithStickyDisabledCounter    = NewCounterDef("complete_workflow_task_sticky_disabled_count")
	WorkflowTaskHeartbeatTimeoutCounter              = NewCounterDef("workflow_task_heartbeat_timeout_count")
	EmptyReplicationEventsCounter                    = NewCounterDef("empty_replication_events")
	DuplicateReplicationEventsCounter                = NewCounterDef("duplicate_replication_events")
	StaleReplicationEventsCounter                    = NewCounterDef("stale_replication_events")
	ReplicationEventsSizeTimer                       = NewTimerDef("replication_events_size")
	BufferReplicationTaskTimer                       = NewTimerDef("buffer_replication_tasks")
	UnbufferReplicationTaskTimer                     = NewTimerDef("unbuffer_replication_tasks")
	HistoryConflictsCounter                          = NewCounterDef("history_conflicts")
	CompleteTaskFailedCounter                        = NewCounterDef("complete_task_fail_count")
	AcquireLockFailedCounter                         = NewCounterDef("acquire_lock_failed")
	WorkflowContextCleared                           = NewCounterDef("workflow_context_cleared")
	MutableStateSize                                 = NewBytesHistogramDef("mutable_state_size")
	ExecutionInfoSize                                = NewBytesHistogramDef("execution_info_size")
	ExecutionStateSize                               = NewBytesHistogramDef("execution_state_size")
	ActivityInfoSize                                 = NewBytesHistogramDef("activity_info_size")
	TimerInfoSize                                    = NewBytesHistogramDef("timer_info_size")
	ChildInfoSize                                    = NewBytesHistogramDef("child_info_size")
	RequestCancelInfoSize                            = NewBytesHistogramDef("request_cancel_info_size")
	SignalInfoSize                                   = NewBytesHistogramDef("signal_info_size")
	BufferedEventsSize                               = NewBytesHistogramDef("buffered_events_size")
	ActivityInfoCount                                = NewDimensionlessHistogramDef("activity_info_count")
	TimerInfoCount                                   = NewDimensionlessHistogramDef("timer_info_count")
	ChildInfoCount                                   = NewDimensionlessHistogramDef("child_info_count")
	SignalInfoCount                                  = NewDimensionlessHistogramDef("signal_info_count")
	RequestCancelInfoCount                           = NewDimensionlessHistogramDef("request_cancel_info_count")
	BufferedEventsCount                              = NewDimensionlessHistogramDef("buffered_events_count")
	TaskCount                                        = NewDimensionlessHistogramDef("task_count")
	WorkflowRetryBackoffTimerCount                   = NewCounterDef("workflow_retry_backoff_timer")
	WorkflowCronBackoffTimerCount                    = NewCounterDef("workflow_cron_backoff_timer")
	WorkflowCleanupDeleteCount                       = NewCounterDef("workflow_cleanup_delete")
	WorkflowCleanupArchiveCount                      = NewCounterDef("workflow_cleanup_archive")
	WorkflowCleanupNopCount                          = NewCounterDef("workflow_cleanup_nop")
	WorkflowCleanupDeleteHistoryInlineCount          = NewCounterDef("workflow_cleanup_delete_history_inline")
	WorkflowSuccessCount                             = NewCounterDef("workflow_success")
	WorkflowCancelCount                              = NewCounterDef("workflow_cancel")
	WorkflowFailedCount                              = NewCounterDef("workflow_failed")
	WorkflowTimeoutCount                             = NewCounterDef("workflow_timeout")
	WorkflowTerminateCount                           = NewCounterDef("workflow_terminate")
	WorkflowContinuedAsNewCount                      = NewCounterDef("workflow_continued_as_new")
	LastRetrievedMessageID                           = NewGaugeDef("last_retrieved_message_id")
	LastProcessedMessageID                           = NewGaugeDef("last_processed_message_id")
	ReplicationTasksApplied                          = NewCounterDef("replication_tasks_applied")
	ReplicationTasksFailed                           = NewCounterDef("replication_tasks_failed")
	ReplicationTasksLag                              = NewTimerDef("replication_tasks_lag")
	ReplicationLatency                               = NewTimerDef("replication_latency")
	ReplicationTasksFetched                          = NewTimerDef("replication_tasks_fetched")
	ReplicationTasksReturned                         = NewTimerDef("replication_tasks_returned")
	ReplicationTasksAppliedLatency                   = NewTimerDef("replication_tasks_applied_latency")
	ReplicationDLQFailed                             = NewCounterDef("replication_dlq_enqueue_failed")
	ReplicationDLQMaxLevelGauge                      = NewGaugeDef("replication_dlq_max_level")
	ReplicationDLQAckLevelGauge                      = NewGaugeDef("replication_dlq_ack_level")
	GetReplicationMessagesForShardLatency            = NewTimerDef("get_replication_messages_for_shard")
	GetDLQReplicationMessagesLatency                 = NewTimerDef("get_dlq_replication_messages")
	EventReapplySkippedCount                         = NewCounterDef("event_reapply_skipped_count")
	DirectQueryDispatchLatency                       = NewTimerDef("direct_query_dispatch_latency")
	DirectQueryDispatchStickyLatency                 = NewTimerDef("direct_query_dispatch_sticky_latency")
	DirectQueryDispatchNonStickyLatency              = NewTimerDef("direct_query_dispatch_non_sticky_latency")
	DirectQueryDispatchStickySuccessCount            = NewCounterDef("direct_query_dispatch_sticky_success")
	DirectQueryDispatchNonStickySuccessCount         = NewCounterDef("direct_query_dispatch_non_sticky_success")
	DirectQueryDispatchClearStickinessLatency        = NewTimerDef("direct_query_dispatch_clear_stickiness_latency")
	DirectQueryDispatchClearStickinessSuccessCount   = NewCounterDef("direct_query_dispatch_clear_stickiness_success")
	DirectQueryDispatchTimeoutBeforeNonStickyCount   = NewCounterDef("direct_query_dispatch_timeout_before_non_sticky")
	WorkflowTaskQueryLatency                         = NewTimerDef("workflow_task_query_latency")
	ConsistentQueryTimeoutCount                      = NewCounterDef("consistent_query_timeout")
	QueryBeforeFirstWorkflowTaskCount                = NewCounterDef("query_before_first_workflow_task")
	QueryBufferExceededCount                         = NewCounterDef("query_buffer_exceeded")
	QueryRegistryInvalidStateCount                   = NewCounterDef("query_registry_invalid_state")
	WorkerNotSupportsConsistentQueryCount            = NewCounterDef("worker_not_supports_consistent_query")
	WorkflowTaskTimeoutOverrideCount                 = NewCounterDef("workflow_task_timeout_overrides")
	WorkflowRunTimeoutOverrideCount                  = NewCounterDef("workflow_run_timeout_overrides")
	ReplicationTaskCleanupCount                      = NewCounterDef("replication_task_cleanup_count")
	ReplicationTaskCleanupFailure                    = NewCounterDef("replication_task_cleanup_failed")
	MutableStateChecksumMismatch                     = NewCounterDef("mutable_state_checksum_mismatch")
	MutableStateChecksumInvalidated                  = NewCounterDef("mutable_state_checksum_invalidated")
)
