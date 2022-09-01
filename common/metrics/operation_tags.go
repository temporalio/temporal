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

// Common operation tags
const (
	// VersionCheckOperation is Operation used by version checker
	VersionCheckOperation = "VersionCheck"
	// AuthorizationOperation is the operation used by all metric emitted by authorization code
	AuthorizationOperation = "Authorization"
	// ServerTlsOperation is the operation used by all metric emitted by tls
	ServerTlsOperation = "ServerTls"
)

// Frontend API operation metric tags
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
	FrontendRespondWorkflowTaskFailedOperation = "RespondWorkflowTaskFailed"
	// FrontendRespondQueryTaskCompletedOperation is the metric operation for frontend.RespondQueryTaskCompleted
	FrontendRespondQueryTaskCompletedOperation = "RespondQueryTaskCompleted"
	// FrontendRespondActivityTaskCompletedOperation is the metric operation for frontend.RespondActivityTaskCompleted
	FrontendRespondActivityTaskCompletedOperation = "RespondActivityTaskCompleted"
	// FrontendRespondActivityTaskFailedOperation is the metric operation for frontend.RespondActivityTaskFailed
	FrontendRespondActivityTaskFailedOperation = "RespondActivityTaskFailed"
	// FrontendRespondActivityTaskCanceledOperation is the metric operation for frontend.RespondActivityTaskCanceled
	FrontendRespondActivityTaskCanceledOperation = "RespondActivityTaskCanceled"
	// FrontendRespondActivityTaskCompletedByIdOperation is the metric operation for frontend.RespondActivityTaskCompletedById
	FrontendRespondActivityTaskCompletedByIdOperation = "RespondActivityTaskCompletedById"
	// FrontendRespondActivityTaskFailedByIdOperation is the metric operation for frontend.RespondActivityTaskFailedById
	FrontendRespondActivityTaskFailedByIdOperation = "RespondActivityTaskFailedById"
	// FrontendRespondActivityTaskCanceledByIdOperation is the metric operation for frontend.RespondActivityTaskCanceledById
	FrontendRespondActivityTaskCanceledByIdOperation = "RespondActivityTaskCanceledById"
	// FrontendGetWorkflowExecutionHistoryOperation is the metric operation for non-long-poll frontend.GetWorkflowExecutionHistory
	FrontendGetWorkflowExecutionHistoryOperation = "GetWorkflowExecutionHistory"
	// FrontendGetWorkflowExecutionHistoryReverseOperation is the metric for frontend.GetWorkflowExecutionHistoryReverse
	FrontendGetWorkflowExecutionHistoryReverseOperation = "GetWorkflowExecutionHistoryReverse"
	// FrontendPollWorkflowExecutionHistoryOperation is the metric operation for long poll case of frontend.GetWorkflowExecutionHistory
	FrontendPollWorkflowExecutionHistoryOperation = "PollWorkflowExecutionHistory"
	// FrontendGetWorkflowExecutionRawHistoryOperation is the metric operation for frontend.GetWorkflowExecutionRawHistory
	FrontendGetWorkflowExecutionRawHistoryOperation = "GetWorkflowExecutionRawHistory"
	// FrontendPollForWorkflowExecutionRawHistoryOperation is the metric operation for frontend.GetWorkflowExecutionRawHistory
	FrontendPollForWorkflowExecutionRawHistoryOperation = "PollForWorkflowExecutionRawHistory"
	// FrontendSignalWorkflowExecutionOperation is the metric operation for frontend.SignalWorkflowExecution
	FrontendSignalWorkflowExecutionOperation = "SignalWorkflowExecution"
	// FrontendSignalWithStartWorkflowExecutionOperation is the metric operation for frontend.SignalWithStartWorkflowExecution
	FrontendSignalWithStartWorkflowExecutionOperation = "SignalWithStartWorkflowExecution"
	// FrontendTerminateWorkflowExecutionOperation is the metric operation for frontend.TerminateWorkflowExecution
	FrontendTerminateWorkflowExecutionOperation = "TerminateWorkflowExecution"
	// FrontendRequestCancelWorkflowExecutionOperation is the metric operation for frontend.RequestCancelWorkflowExecution
	FrontendRequestCancelWorkflowExecutionOperation = "RequestCancelWorkflowExecution"
	// FrontendListArchivedWorkflowExecutionsOperation is the metric operation for frontend.ListArchivedWorkflowExecutions
	FrontendListArchivedWorkflowExecutionsOperation = "ListArchivedWorkflowExecutions"
	// FrontendListOpenWorkflowExecutionsOperation is the metric operation for frontend.ListOpenWorkflowExecutions
	FrontendListOpenWorkflowExecutionsOperation = "ListOpenWorkflowExecutions"
	// FrontendListClosedWorkflowExecutionsOperation is the metric operation for frontend.ListClosedWorkflowExecutions
	FrontendListClosedWorkflowExecutionsOperation = "ListClosedWorkflowExecutions"
	// FrontendListWorkflowExecutionsOperation is the metric operation for frontend.ListWorkflowExecutions
	FrontendListWorkflowExecutionsOperation = "ListWorkflowExecutions"
	// FrontendScanWorkflowExecutionsOperation is the metric operation for frontend.ListWorkflowExecutions
	FrontendScanWorkflowExecutionsOperation = "ScanWorkflowExecutions"
	// FrontendCountWorkflowExecutionsOperation is the metric operation for frontend.CountWorkflowExecutions
	FrontendCountWorkflowExecutionsOperation = "CountWorkflowExecutions"
	// FrontendRegisterNamespaceOperation is the metric operation for frontend.RegisterNamespace
	FrontendRegisterNamespaceOperation = "RegisterNamespace"
	// FrontendDescribeNamespaceOperation is the metric operation for frontend.DescribeNamespace
	FrontendDescribeNamespaceOperation = "DescribeNamespace"
	// FrontendUpdateNamespaceOperation is the metric operation for frontend.DescribeNamespace
	FrontendUpdateNamespaceOperation = "UpdateNamespace"
	// FrontendDeprecateNamespaceOperation is the metric operation for frontend.DeprecateNamespace
	FrontendDeprecateNamespaceOperation = "DeprecateNamespace"
	// FrontendQueryWorkflowOperation is the metric operation for frontend.QueryWorkflow
	FrontendQueryWorkflowOperation = "QueryWorkflow"
	// FrontendDescribeWorkflowExecutionOperation is the metric operation for frontend.DescribeWorkflowExecution
	FrontendDescribeWorkflowExecutionOperation = "DescribeWorkflowExecution"
	// FrontendDescribeTaskQueueOperation is the metric operation for frontend.DescribeTaskQueue
	FrontendDescribeTaskQueueOperation = "DescribeTaskQueue"
	// FrontendListTaskQueuePartitionsOperation is the metric operation for frontend.ResetStickyTaskQueue
	FrontendListTaskQueuePartitionsOperation = "ListTaskQueuePartitions"
	// FrontendResetStickyTaskQueueOperation is the metric operation for frontend.ResetStickyTaskQueue
	FrontendResetStickyTaskQueueOperation = "ResetStickyTaskQueue"
	// FrontendListNamespacesOperation is the metric operation for frontend.ListNamespace
	FrontendListNamespacesOperation = "ListNamespaces"
	// FrontendResetWorkflowExecutionOperation is the metric operation for frontend.ResetWorkflowExecution
	FrontendResetWorkflowExecutionOperation = "ResetWorkflowExecution"
	// FrontendGetSearchAttributesOperation is the metric operation for frontend.GetSearchAttributes
	FrontendGetSearchAttributesOperation = "GetSearchAttributes"
	// FrontendGetClusterInfoOperation is the metric operation for frontend.GetClusterInfo
	FrontendGetClusterInfoOperation = "GetClusterInfo"
	// FrontendGetSystemInfoOperation is the metric operation for frontend.GetSystemInfo
	FrontendGetSystemInfoOperation = "GetSystemInfo"
	// FrontendCreateScheduleOperation is the metric operation for frontend.CreateScheduleOperation = ""
	FrontendCreateScheduleOperation = "CreateSchedule"
	// FrontendDescribeScheduleOperation is the metric operation for frontend.DescribeScheduleOperation = ""
	FrontendDescribeScheduleOperation = "DescribeSchedule"
	// FrontendUpdateScheduleOperation is the metric operation for frontend.UpdateScheduleOperation = ""
	FrontendUpdateScheduleOperation = "UpdateSchedule"
	// FrontendPatchScheduleOperation is the metric operation for frontend.PatchScheduleOperation = ""
	FrontendPatchScheduleOperation = "PatchSchedule"
	// FrontendListScheduleMatchingTimesOperation is the metric operation for frontend.ListScheduleMatchingTimesOperation = ""
	FrontendListScheduleMatchingTimesOperation = "ListScheduleMatchingTimes"
	// FrontendDeleteScheduleOperation is the metric operation for frontend.DeleteScheduleOperation = ""
	FrontendDeleteScheduleOperation = "DeleteSchedule"
	// FrontendListSchedulesOperation is the metric operation for frontend.ListSchedulesOperation = ""
	FrontendListSchedulesOperation = "ListSchedules"
	// FrontendUpdateWorkerBuildIdOrderingOperation is the metric operation for frontend.UpdateWorkerBuildIdOrderingOperation = ""
	FrontendUpdateWorkerBuildIdOrderingOperation = "UpdateWorkerBuildIdOrdering"
	// FrontendGetWorkerBuildIdOrderingOperation is the metric operation for frontend.GetWorkerBuildIdOrderingOperation = ""
	FrontendGetWorkerBuildIdOrderingOperation = "GetWorkerBuildIdOrdering"
	// FrontendUpdateWorkflowOperation is the metric operation for frontend.UpdateWorkflow
	FrontendUpdateWorkflowOperation = "UpdateWorkflow"
	// FrontendDescribeBatchOperationOperation is the metric operation for frontend.DescribeBatchOperation = ""
	FrontendDescribeBatchOperationOperation = "DescribeBatchOperation"
	// FrontendListBatchOperationsOperation is the metric operation for frontend.ListBatchOperations
	FrontendListBatchOperationsOperation = "ListBatchOperations"
	// FrontendStartBatchOperationOperation is the metric operation for frontend.StartBatchOperation = ""
	FrontendStartBatchOperationOperation = "StartBatchOperation"
	// FrontendStopBatchOperationOperation is the metric operation for frontend.StopBatchOperation = ""
	FrontendStopBatchOperationOperation = "StopBatchOperation"

	// Admin handler operation tags

	// AdminDescribeHistoryHostOperation is the metric operation for admin.AdminDescribeHistoryHostOperation = ""
	AdminDescribeHistoryHostOperation = "AdminDescribeHistoryHost"
	// AdminAddSearchAttributesOperation is the metric operation for admin.AdminAddSearchAttributesOperation = ""
	AdminAddSearchAttributesOperation = "AdminAddSearchAttributes"
	// AdminRemoveSearchAttributesOperation is the metric operation for admin.AdminRemoveSearchAttributesOperation = ""
	AdminRemoveSearchAttributesOperation = "AdminRemoveSearchAttributes"
	// AdminGetSearchAttributesOperation is the metric operation for admin.AdminGetSearchAttributesOperation = ""
	AdminGetSearchAttributesOperation = "AdminGetSearchAttributes"
	// AdminRebuildMutableStateOperation is the metric operation for admin.AdminRebuildMutableStateOperation = ""
	AdminRebuildMutableStateOperation = "AdminRebuildMutableState"
	// AdminDescribeMutableStateOperation is the metric operation for admin.AdminDescribeMutableStateOperation = ""
	AdminDescribeMutableStateOperation = "AdminDescribeMutableState"
	// AdminGetWorkflowExecutionRawHistoryV2Operation is the metric operation for admin.GetWorkflowExecutionRawHistoryOperation = ""
	AdminGetWorkflowExecutionRawHistoryV2Operation = "AdminGetWorkflowExecutionRawHistoryV2"
	// AdminGetReplicationMessagesOperation is the metric operation for admin.GetReplicationMessages
	AdminGetReplicationMessagesOperation = "AdminGetReplicationMessages"
	// AdminGetNamespaceReplicationMessagesOperation is the metric operation for admin.GetNamespaceReplicationMessages
	AdminGetNamespaceReplicationMessagesOperation = "AdminGetNamespaceReplicationMessages"
	// AdminGetDLQReplicationMessagesOperation is the metric operation for admin.GetDLQReplicationMessages
	AdminGetDLQReplicationMessagesOperation = "AdminGetDLQReplicationMessages"
	// AdminReapplyEventsOperation is the metric operation for admin.ReapplyEvents
	AdminReapplyEventsOperation = "AdminReapplyEvents"
	// AdminRefreshWorkflowTasksOperation is the metric operation for admin.RefreshWorkflowTasks
	AdminRefreshWorkflowTasksOperation = "AdminRefreshWorkflowTasks"
	// AdminResendReplicationTasksOperation is the metric operation for admin.ResendReplicationTasks
	AdminResendReplicationTasksOperation = "AdminResendReplicationTasks"
	// AdminGetTaskQueueTasksOperation is the metric operation for admin.GetTaskQueueTasks
	AdminGetTaskQueueTasksOperation = "AdminGetTaskQueueTasks"
	// AdminRemoveTaskOperation is the metric operation for admin.AdminRemoveTaskOperation = ""
	AdminRemoveTaskOperation = "AdminRemoveTask"
	// AdminCloseShardOperation is the metric operation for admin.AdminCloseShardOperation = ""
	AdminCloseShardOperation = "AdminCloseShard"
	// AdminGetShardOperation is the metric operation for admin.AdminGetShardOperation = ""
	AdminGetShardOperation = "AdminGetShard"
	// AdminListHistoryTasksOperation is the metric operation for admin.ListHistoryTasksOperation = ""
	AdminListHistoryTasksOperation = "AdminListHistoryTasks"
	// AdminGetDLQMessagesOperation is the metric operation for admin.AdminGetDLQMessagesOperation = ""
	AdminGetDLQMessagesOperation = "AdminGetDLQMessages"
	// AdminPurgeDLQMessagesOperation is the metric operation for admin.AdminPurgeDLQMessagesOperation = ""
	AdminPurgeDLQMessagesOperation = "AdminPurgeDLQMessages"
	// AdminMergeDLQMessagesOperation is the metric operation for admin.AdminMergeDLQMessagesOperation = ""
	AdminMergeDLQMessagesOperation = "AdminMergeDLQMessages"
	// AdminListClusterMembersOperation is the metric operation for admin.AdminListClusterMembersOperation = ""
	AdminListClusterMembersOperation = "AdminListClusterMembers"
	// AdminDescribeClusterOperation is the metric operation for admin.AdminDescribeClusterOperation = ""
	AdminDescribeClusterOperation = "AdminDescribeCluster"
	// AdminListClustersOperation is the metric operation for admin.AdminListClustersOperation = ""
	AdminListClustersOperation = "AdminListClusters"
	// AdminAddOrUpdateRemoteClusterOperation is the metric operation for admin.AdminAddOrUpdateRemoteClusterOperation = ""
	AdminAddOrUpdateRemoteClusterOperation = "AdminAddOrUpdateRemoteCluster"
	// AdminRemoveRemoteClusterOperation is the metric operation for admin.AdminRemoveRemoteClusterOperation = ""
	AdminRemoveRemoteClusterOperation = "AdminRemoveRemoteCluster"
	// AdminDeleteWorkflowExecutionOperation is the metric operation for admin.AdminDeleteWorkflowExecutionOperation = ""
	AdminDeleteWorkflowExecutionOperation = "AdminDeleteWorkflowExecution"

	// Operator handler operation tags

	// OperatorAddSearchAttributesOperation is the metric operation for operator.AddSearchAttributes
	OperatorAddSearchAttributesOperation = "OperatorAddSearchAttributes"
	// OperatorRemoveSearchAttributesOperation is the metric operation for operator.RemoveSearchAttributes
	OperatorRemoveSearchAttributesOperation = "OperatorRemoveSearchAttributes"
	// OperatorListSearchAttributesOperation is the metric operation for operator.ListSearchAttributes
	OperatorListSearchAttributesOperation     = "OperatorListSearchAttributes"
	OperatorDeleteNamespaceOperation          = "OperatorDeleteNamespace"
	OperatorAddOrUpdateRemoteClusterOperation = "OperatorAddOrUpdateRemoteCluster"
	OperatorDeleteWorkflowExecutionOperation  = "OperatorDeleteWorkflowExecution"
	OperatorDescribeClusterOperation          = "OperatorDescribeCluster"
	OperatorListClusterMembersOperation       = "OperatorListClusterMembers"
	OperatorListClustersOperation             = "OperatorListClusters"
	OperatorRemoveRemoteClusterOperation      = "OperatorRemoveRemoteCluster"

	NumberOfFrontendAPIMetrics = 91
)

// History API operation metric tags
const (
	// HistoryStartWorkflowExecutionOperation tracks StartWorkflowExecution API calls received by service
	HistoryStartWorkflowExecutionOperation = "StartWorkflowExecution"
	// HistoryRecordActivityTaskHeartbeatOperation tracks RecordActivityTaskHeartbeat API calls received by service
	HistoryRecordActivityTaskHeartbeatOperation = "RecordActivityTaskHeartbeat"
	// HistoryRespondWorkflowTaskCompletedOperation tracks RespondWorkflowTaskCompleted API calls received by service
	HistoryRespondWorkflowTaskCompletedOperation = "RespondWorkflowTaskCompleted"
	// HistoryRespondWorkflowTaskFailedOperation tracks RespondWorkflowTaskFailed API calls received by service
	HistoryRespondWorkflowTaskFailedOperation = "RespondWorkflowTaskFailed"
	// HistoryRespondActivityTaskCompletedOperation tracks RespondActivityTaskCompleted API calls received by service
	HistoryRespondActivityTaskCompletedOperation = "RespondActivityTaskCompleted"
	// HistoryRespondActivityTaskFailedOperation tracks RespondActivityTaskFailed API calls received by service
	HistoryRespondActivityTaskFailedOperation = "RespondActivityTaskFailed"
	// HistoryRespondActivityTaskCanceledOperation tracks RespondActivityTaskCanceled API calls received by service
	HistoryRespondActivityTaskCanceledOperation = "RespondActivityTaskCanceled"
	// HistoryGetMutableStateOperation tracks GetMutableStateOperation API calls received by service
	HistoryGetMutableStateOperation = "GetMutableState"
	// HistoryPollMutableStateOperation tracks PollMutableStateOperation API calls received by service
	HistoryPollMutableStateOperation = "PollMutableState"
	// HistoryResetStickyTaskQueueOperation tracks ResetStickyTaskQueueOperation API calls received by service
	HistoryResetStickyTaskQueueOperation = "ResetStickyTaskQueue"
	// HistoryDescribeWorkflowExecutionOperation tracks DescribeWorkflowExecution API calls received by service
	HistoryDescribeWorkflowExecutionOperation = "DescribeWorkflowExecution"
	// HistoryRecordWorkflowTaskStartedOperation tracks RecordWorkflowTaskStarted API calls received by service
	HistoryRecordWorkflowTaskStartedOperation = "RecordWorkflowTaskStarted"
	// HistoryRecordActivityTaskStartedOperation tracks RecordActivityTaskStarted API calls received by service
	HistoryRecordActivityTaskStartedOperation = "RecordActivityTaskStarted"
	// HistorySignalWorkflowExecutionOperation tracks SignalWorkflowExecution API calls received by service
	HistorySignalWorkflowExecutionOperation = "SignalWorkflowExecution"
	// HistorySignalWithStartWorkflowExecutionOperation tracks SignalWithStartWorkflowExecution API calls received by service
	HistorySignalWithStartWorkflowExecutionOperation = "SignalWithStartWorkflowExecution"
	// HistoryRemoveSignalMutableStateOperation tracks RemoveSignalMutableState API calls received by service
	HistoryRemoveSignalMutableStateOperation = "RemoveSignalMutableState"
	// HistoryTerminateWorkflowExecutionOperation tracks TerminateWorkflowExecution API calls received by service
	HistoryTerminateWorkflowExecutionOperation = "TerminateWorkflowExecution"
	// HistoryScheduleWorkflowTaskOperation tracks ScheduleWorkflowTask API calls received by service
	HistoryScheduleWorkflowTaskOperation = "ScheduleWorkflowTask"
	// HistoryVerifyFirstWorkflowTaskScheduledOperation tracks VerifyFirstWorkflowTaskScheduled API calls received by service
	HistoryVerifyFirstWorkflowTaskScheduledOperation = "VerifyFirstWorkflowTaskScheduled"
	// HistoryRecordChildExecutionCompletedOperation tracks RecordChildExecutionCompleted API calls received by service
	HistoryRecordChildExecutionCompletedOperation = "RecordChildExecutionCompleted"
	// HistoryVerifyChildExecutionCompletionRecordedOperation tracks VerifyChildExecutionCompletionRecorded API calls received by service
	HistoryVerifyChildExecutionCompletionRecordedOperation = "VerifyChildExecutionCompletionRecorded"
	// HistoryRequestCancelWorkflowExecutionOperation tracks RequestCancelWorkflowExecution API calls received by service
	HistoryRequestCancelWorkflowExecutionOperation = "RequestCancelWorkflowExecution"
	// HistorySyncShardStatusOperation tracks SyncShardStatus API calls received by service
	HistorySyncShardStatusOperation = "SyncShardStatus"
	// HistorySyncActivityOperation tracks SyncActivity API calls received by service
	HistorySyncActivityOperation = "SyncActivity"
	// HistoryRebuildMutableStateOperation tracks RebuildMutableState API calls received by service
	HistoryRebuildMutableStateOperation = "RebuildMutableState"
	// HistoryDescribeMutableStateOperation tracks DescribeMutableState API calls received by service
	HistoryDescribeMutableStateOperation = "DescribeMutableState"
	// HistoryGetReplicationMessagesOperation tracks GetReplicationMessages API calls received by service
	HistoryGetReplicationMessagesOperation = "GetReplicationMessages"
	// HistoryGetDLQReplicationMessagesOperation tracks GetDLQReplicationMessages API calls received by service
	HistoryGetDLQReplicationMessagesOperation = "GetDLQReplicationMessages"
	// HistoryReadDLQMessagesOperation tracks ReadDLQMessages API calls received by service
	HistoryReadDLQMessagesOperation = "ReadDLQMessages"
	// HistoryPurgeDLQMessagesOperation tracks PurgeDLQMessages API calls received by service
	HistoryPurgeDLQMessagesOperation = "PurgeDLQMessages"
	// HistoryMergeDLQMessagesOperation tracks MergeDLQMessages API calls received by service
	HistoryMergeDLQMessagesOperation = "MergeDLQMessages"
	// HistoryShardControllerOperation is the operation used by shard controller
	HistoryShardControllerOperation = "ShardController"
	// HistoryReapplyEventsOperation is the operation used by event reapplication
	HistoryReapplyEventsOperation = "ReapplyEvents"
	// HistoryRefreshWorkflowTasksOperation is the operation used by refresh workflow tasks API
	HistoryRefreshWorkflowTasksOperation = "RefreshWorkflowTasks"
	// HistoryGenerateLastHistoryReplicationTasksOperation is the operation used by generate last replication tasks API
	HistoryGenerateLastHistoryReplicationTasksOperation = "GenerateLastHistoryReplicationTasks"
	// HistoryGetReplicationStatusOperation is the operation used by GetReplicationStatus API
	HistoryGetReplicationStatusOperation = "GetReplicationStatus"
	// HistoryRemoveTaskOperation is the operation used by remove task API
	HistoryRemoveTaskOperation = "RemoveTask"
	// HistoryCloseShardOperation is the operation used by close shard API
	HistoryCloseShardOperation = "CloseShard"
	// HistoryGetShardOperation is the operation used by get shard API
	HistoryGetShardOperation = "GetShard"
	// HistoryReplicateEventsV2Operation is the operation used by replicate events API
	HistoryReplicateEventsV2Operation = "ReplicateEventsV2"
	// HistoryDescribeHistoryHostOperation is the operation used by describe history host API
	HistoryDescribeHistoryHostOperation = "DescribeHistoryHost"
	// HistoryDeleteWorkflowVisibilityRecordOperation is the operation used by delete workflow visibility record API
	HistoryDeleteWorkflowVisibilityRecordOperation = "DeleteWorkflowVisibilityRecord"
	// HistoryUpdateWorkflowOperation is the operation used by update workflow API
	HistoryUpdateWorkflowOperation = "UpdateWorkflow"
	// TODO: miss 2
	NumberOfHistoryAPIMetrics = 45
)

// History operation tags
const (
	// TaskPriorityAssignerOperation is the operation used by all metric emitted by task priority assigner
	TaskPriorityAssignerOperation = "TaskPriorityAssigner"
	// TransferQueueProcessorOperation is the operation used by all metric emitted by transfer queue processor
	TransferQueueProcessorOperation = "TransferQueueProcessor"
	// TransferActiveQueueProcessorOperation is the operation used by all metric emitted by transfer queue processor
	TransferActiveQueueProcessorOperation = "TransferActiveQueueProcessor"
	// TransferStandbyQueueProcessorOperation is the operation used by all metric emitted by transfer queue processor
	TransferStandbyQueueProcessorOperation = "TransferStandbyQueueProcessor"
	// TransferActiveTaskActivityOperation is the operation used for activity task processing by transfer queue processor
	TransferActiveTaskActivityOperation = "TransferActiveTaskActivity"
	// TransferActiveTaskWorkflowTaskOperation is the operation used for workflow task processing by transfer queue processor
	TransferActiveTaskWorkflowTaskOperation = "TransferActiveTaskWorkflowTask"
	// TransferActiveTaskCloseExecutionOperation is the operation used for close execution task processing by transfer queue processor
	TransferActiveTaskCloseExecutionOperation = "TransferActiveTaskCloseExecution"
	// TransferActiveTaskCancelExecutionOperation is the operation used for cancel execution task processing by transfer queue processor
	TransferActiveTaskCancelExecutionOperation = "TransferActiveTaskCancelExecution"
	// TransferActiveTaskSignalExecutionOperation is the operation used for signal execution task processing by transfer queue processor
	TransferActiveTaskSignalExecutionOperation = "TransferActiveTaskSignalExecution"
	// TransferActiveTaskStartChildExecutionOperation is the operation used for start child execution task processing by transfer queue processor
	TransferActiveTaskStartChildExecutionOperation = "TransferActiveTaskStartChildExecution"
	// TransferActiveTaskResetWorkflowOperation is the operation used for record workflow started task processing by transfer queue processor
	TransferActiveTaskResetWorkflowOperation = "TransferActiveTaskResetWorkflow"
	// TransferStandbyTaskResetWorkflowOperation is the operation used for record workflow started task processing by transfer queue processor
	TransferStandbyTaskResetWorkflowOperation = "TransferStandbyTaskResetWorkflow"
	// TransferStandbyTaskActivityOperation is the operation used for activity task processing by transfer queue processor
	TransferStandbyTaskActivityOperation = "TransferStandbyTaskActivity"
	// TransferStandbyTaskWorkflowTaskOperation is the operation used for workflow task processing by transfer queue processor
	TransferStandbyTaskWorkflowTaskOperation = "TransferStandbyTaskWorkflowTask"
	// TransferStandbyTaskCloseExecutionOperation is the operation used for close execution task processing by transfer queue processor
	TransferStandbyTaskCloseExecutionOperation = "TransferStandbyTaskCloseExecution"
	// TransferStandbyTaskCancelExecutionOperation is the operation used for cancel execution task processing by transfer queue processor
	TransferStandbyTaskCancelExecutionOperation = "TransferStandbyTaskCancelExecution"
	// TransferStandbyTaskSignalExecutionOperation is the operation used for signal execution task processing by transfer queue processor
	TransferStandbyTaskSignalExecutionOperation = "TransferStandbyTaskSignalExecution"
	// TransferStandbyTaskStartChildExecutionOperation is the operation used for start child execution task processing by transfer queue processor
	TransferStandbyTaskStartChildExecutionOperation = "TransferStandbyTaskStartChildExecution"

	// VisibilityQueueProcessorOperation is the operation used by all metric emitted by visibility queue processor
	VisibilityQueueProcessorOperation = "VisibilityQueueProcessor"
	// VisibilityTaskStartExecutionOperation is the operation used for start execution processing by visibility queue processor
	VisibilityTaskStartExecutionOperation = "VisibilityTaskStartExecution"
	// VisibilityTaskUpsertExecutionOperation is the operation used for upsert execution processing by visibility queue processor
	VisibilityTaskUpsertExecutionOperation = "VisibilityTaskUpsertExecution"
	// VisibilityTaskCloseExecutionOperation is the operation used for close execution attributes processing by visibility queue processor
	VisibilityTaskCloseExecutionOperation = "VisibilityTaskCloseExecution"
	// VisibilityTaskDeleteExecutionOperation is the operation used for delete by visibility queue processor
	VisibilityTaskDeleteExecutionOperation = "VisibilityTaskDeleteExecution"

	// TimerQueueProcessorOperation is the operation used by all metric emitted by timer queue processor
	TimerQueueProcessorOperation = "TimerQueueProcessor"
	// TimerActiveQueueProcessorOperation is the operation used by all metric emitted by timer queue processor
	TimerActiveQueueProcessorOperation = "TimerActiveQueueProcessor"
	// TimerStandbyQueueProcessorOperation is the operation used by all metric emitted by timer queue processor
	TimerStandbyQueueProcessorOperation = "TimerStandbyQueueProcessor"
	// TimerActiveTaskActivityTimeoutOperation is the operation used by metric emitted by timer queue processor for processing activity timeouts
	TimerActiveTaskActivityTimeoutOperation = "TimerActiveTaskActivityTimeout"
	// TimerActiveTaskWorkflowTaskTimeoutOperation is the operation used by metric emitted by timer queue processor for processing workflow task timeouts
	TimerActiveTaskWorkflowTaskTimeoutOperation = "TimerActiveTaskWorkflowTaskTimeout"
	// TimerActiveTaskUserTimerOperation is the operation used by metric emitted by timer queue processor for processing user timers
	TimerActiveTaskUserTimerOperation = "TimerActiveTaskUserTimer"
	// TimerActiveTaskWorkflowTimeoutOperation is the operation used by metric emitted by timer queue processor for processing workflow timeouts.
	TimerActiveTaskWorkflowTimeoutOperation = "TimerActiveTaskWorkflowTimeout"
	// TimerActiveTaskActivityRetryTimerOperation is the operation used by metric emitted by timer queue processor for processing retry task.
	TimerActiveTaskActivityRetryTimerOperation = "TimerActiveTaskActivityRetryTimer"
	// TimerActiveTaskWorkflowBackoffTimerOperation is the operation used by metric emitted by timer queue processor for processing retry task.
	TimerActiveTaskWorkflowBackoffTimerOperation = "TimerActiveTaskWorkflowBackoffTimer"
	// TimerActiveTaskDeleteHistoryEventOperation is the operation used by metric emitted by timer queue processor for processing history event cleanup
	TimerActiveTaskDeleteHistoryEventOperation = "TimerActiveTaskDeleteHistoryEvent"
	// TimerStandbyTaskActivityTimeoutOperation is the operation used by metric emitted by timer queue processor for processing activity timeouts
	TimerStandbyTaskActivityTimeoutOperation = "TimerStandbyTaskActivityTimeout"
	// TimerStandbyTaskWorkflowTaskTimeoutOperation is the operation used by metric emitted by timer queue processor for processing workflow task timeouts
	TimerStandbyTaskWorkflowTaskTimeoutOperation = "TimerStandbyTaskWorkflowTaskTimeout"
	// TimerStandbyTaskUserTimerOperation is the operation used by metric emitted by timer queue processor for processing user timers
	TimerStandbyTaskUserTimerOperation = "TimerStandbyTaskUserTimer"
	// TimerStandbyTaskWorkflowTimeoutOperation is the operation used by metric emitted by timer queue processor for processing workflow timeouts.
	TimerStandbyTaskWorkflowTimeoutOperation = "TimerStandbyTaskWorkflowTimeout"
	// TimerStandbyTaskActivityRetryTimerOperation is the operation used by metric emitted by timer queue processor for processing retry task.
	TimerStandbyTaskActivityRetryTimerOperation = "TimerStandbyTaskActivityRetryTimer"
	// TimerStandbyTaskDeleteHistoryEventOperation is the operation used by metric emitted by timer queue processor for processing history event cleanup
	TimerStandbyTaskDeleteHistoryEventOperation = "TimerStandbyTaskDeleteHistoryEvent"
	// TimerStandbyTaskWorkflowBackoffTimerOperation is the operation used by metric emitted by timer queue processor for processing retry task.
	TimerStandbyTaskWorkflowBackoffTimerOperation = "TimerStandbyTaskWorkflowBackoffTimer"
	// ReplicatorQueueProcessorOperation is the operation used by all metric emitted by replicator queue processor
	ReplicatorQueueProcessorOperation = "ReplicatorQueueProcessor"
	// ReplicatorTaskHistoryOperation is the operation used for history task processing by replicator queue processor
	ReplicatorTaskHistoryOperation = "ReplicatorTaskHistory"
	// ReplicatorTaskSyncActivityOperation is the operation used for sync activity by replicator queue processor
	ReplicatorTaskSyncActivityOperation = "ReplicatorTaskSyncActivity"
	// ReplicateHistoryEventsOperation is the operation used by historyReplicator API for applying events
	ReplicateHistoryEventsOperation = "ReplicateHistoryEvents"
	// ShardInfoOperation is the operation used when updating shard info
	ShardInfoOperation = "ShardInfo"
	// WorkflowContextOperation is the operation used by WorkflowContext component
	WorkflowContextOperation = "WorkflowContext"
	// HistoryCacheGetOrCreateOperation is the operation used by history cache
	HistoryCacheGetOrCreateOperation = "HistoryCacheGetOrCreate"
	// ExecutionStatsOperation is the operation used for emiting workflow execution related stats
	ExecutionStatsOperation = "ExecutionStats"
	// SessionStatsOperation is the operation used for emiting session update related stats
	SessionStatsOperation = "SessionStats"
	// HistoryResetWorkflowExecutionOperation tracks ResetWorkflowExecution API calls received by service
	HistoryResetWorkflowExecutionOperation = "HistoryResetWorkflowExecution"
	// HistoryQueryWorkflowOperation tracks QueryWorkflow API calls received by service
	HistoryQueryWorkflowOperation = "HistoryQueryWorkflow"
	// HistoryProcessDeleteHistoryEventOperation tracks ProcessDeleteHistoryEvent processing calls
	HistoryProcessDeleteHistoryEventOperation = "HistoryProcessDeleteHistoryEvent"
	// HistoryDeleteWorkflowExecutionOperation tracks DeleteWorkflowExecutions API calls
	HistoryDeleteWorkflowExecutionOperation = "HistoryDeleteWorkflowExecution"
	// WorkflowCompletionStatsOperation tracks workflow completion updates
	WorkflowCompletionStatsOperation = "WorkflowCompletionStats"
	// ReplicationTaskFetcherOperation is the operation used by all metrics emitted by ReplicationTaskFetcher
	ReplicationTaskFetcherOperation = "ReplicationTaskFetcher"
	// ReplicationTaskCleanupOperation is the operation used by all metrics emitted by ReplicationTaskProcessor cleanup
	ReplicationTaskCleanupOperation = "ReplicationTaskCleanup"
	// ReplicationDLQStatsOperation is the operation used by all metrics emitted related to replication DLQ
	ReplicationDLQStatsOperation = "ReplicationDLQStats"
	// SyncWorkflowStateTaskOperation is the operation used by closed workflow task replication processing
	SyncWorkflowStateTaskOperation = "SyncWorkflowStateTask"

	// HistoryEventNotificationOperation is the operation used by shard history event notification
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
	HistoryReplicationTaskOperation = "HistoryReplicationTask"
	// HistoryMetadataReplicationTaskOperation is the operation used by history metadata task replication processing
	HistoryMetadataReplicationTaskOperation = "HistoryMetadataReplicationTask"
	// SyncShardTaskOperation is the operation used by sync shrad information processing
	SyncShardTaskOperation = "SyncShardTas"
	// SyncActivityTaskOperation is the operation used by sync activity information processing
	SyncActivityTaskOperation = "SyncActivityTask"
	// ESProcessorOperation is operation used by all metric emitted by esProcessor
	ESProcessorOperation = "ESProcessor"
	// IndexProcessorOperation is operation used by all metric emitted by index processor
	IndexProcessorOperation = "IndexProcessor"

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

	// ClusterMetadataArchivalConfigOperation tracks ArchivalConfig calls to ClusterMetadata
	ClusterMetadataArchivalConfigOperation = "ClusterMetadataArchivalConfig"

	// ElasticsearchBulkProcessorOperation is Operation used by all metric emitted by Elasticsearch bulk processor
	ElasticsearchBulkProcessorOperation = "ElasticsearchBulkProcessor"

	// ElasticsearchVisibilityOperation is Operation used by all Elasticsearch visibility metrics
	ElasticsearchVisibilityOperation = "ElasticsearchVisibility"

	// SequentialTaskProcessingOperation is used by sequential task processing logic
	SequentialTaskProcessingOperation = "SequentialTaskProcessing"
	// ParallelTaskProcessingOperation is used by parallel task processing logic
	ParallelTaskProcessingOperation = "ParallelTaskProcessing"
	// TaskSchedulerOperation is used by task scheduler logic
	TaskSchedulerOperation = "TaskScheduler"

	// MessagingClientPublishOperation tracks Publish calls made by service to messaging layer
	MessagingClientPublishOperation = "MessagingClientPublish"
	// MessagingClientPublishBatchOperation tracks Publish calls made by service to messaging layer
	MessagingClientPublishBatchOperation = "MessagingClientPublishBatch"

	// NamespaceCacheOperation tracks namespace cache callbacks
	NamespaceCacheOperation = "NamespaceCache"

	// PersistenceGetOrCreateShardOperation tracks GetOrCreateShard calls made by service to persistence layer
	PersistenceGetOrCreateShardOperation = "GetOrCreateShard"
	// PersistenceUpdateShardOperation tracks UpdateShard calls made by service to persistence layer
	PersistenceUpdateShardOperation = "UpdateShard"
	// PersistenceAssertShardOwnershipOperation tracks UpdateShard calls made by service to persistence layer
	PersistenceAssertShardOwnershipOperation = "AssertShardOwnership"
	// PersistenceCreateWorkflowExecutionOperation tracks CreateWorkflowExecution calls made by service to persistence layer
	PersistenceCreateWorkflowExecutionOperation = "CreateWorkflowExecution"
	// PersistenceGetWorkflowExecutionOperation tracks GetWorkflowExecution calls made by service to persistence layer
	PersistenceGetWorkflowExecutionOperation = "GetWorkflowExecution"
	// PersistenceSetWorkflowExecutionOperation tracks SetWorkflowExecution calls made by service to persistence layer
	PersistenceSetWorkflowExecutionOperation = "SetWorkflowExecution"
	// PersistenceUpdateWorkflowExecutionOperation tracks UpdateWorkflowExecution calls made by service to persistence layer
	PersistenceUpdateWorkflowExecutionOperation = "UpdateWorkflowExecution"
	// PersistenceConflictResolveWorkflowExecutionOperation tracks ConflictResolveWorkflowExecution calls made by service to persistence layer
	PersistenceConflictResolveWorkflowExecutionOperation = "ConflictResolveWorkflowExecution"
	// PersistenceResetWorkflowExecutionOperation tracks ResetWorkflowExecution calls made by service to persistence layer
	PersistenceResetWorkflowExecutionOperation = "ResetWorkflowExecution"
	// PersistenceDeleteWorkflowExecutionOperation tracks DeleteWorkflowExecution calls made by service to persistence layer
	PersistenceDeleteWorkflowExecutionOperation = "DeleteWorkflowExecution"
	// PersistenceDeleteCurrentWorkflowExecutionOperation tracks DeleteCurrentWorkflowExecution calls made by service to persistence layer
	PersistenceDeleteCurrentWorkflowExecutionOperation = "DeleteCurrentWorkflowExecution"
	// PersistenceGetCurrentExecutionOperation tracks GetCurrentExecution calls made by service to persistence layer
	PersistenceGetCurrentExecutionOperation = "GetCurrentExecution"
	// PersistenceListConcreteExecutionsOperation tracks ListConcreteExecutions calls made by service to persistence layer
	PersistenceListConcreteExecutionsOperation = "ListConcreteExecutions"
	// PersistenceAddTasksOperation tracks AddTasks calls made by service to persistence layer
	PersistenceAddTasksOperation = "AddTasks"
	// PersistenceGetTransferTaskOperation tracks GetTransferTask calls made by service to persistence layer
	PersistenceGetTransferTaskOperation = "GetTransferTask"
	// PersistenceGetTransferTasksOperation tracks GetTransferTasks calls made by service to persistence layer
	PersistenceGetTransferTasksOperation = "GetTransferTasks"
	// PersistenceCompleteTransferTaskOperation tracks CompleteTransferTasks calls made by service to persistence layer
	PersistenceCompleteTransferTaskOperation = "CompleteTransferTask"
	// PersistenceRangeCompleteTransferTasksOperation tracks CompleteTransferTasks calls made by service to persistence layer
	PersistenceRangeCompleteTransferTasksOperation = "RangeCompleteTransferTasks"

	// PersistenceGetVisibilityTaskOperation = "GetVisibilityTask" tracks GetVisibilityTask calls made by service to persistence layer
	PersistenceGetVisibilityTaskOperation = "GetVisibilityTask"
	// PersistenceGetVisibilityTasksOperation = "GetVisibilityTasks" tracks GetVisibilityTasks calls made by service to persistence layer
	PersistenceGetVisibilityTasksOperation = "GetVisibilityTasks"
	// PersistenceCompleteVisibilityTaskOperation = "CompleteVisibilityTask" tracks CompleteVisibilityTasks calls made by service to persistence layer
	PersistenceCompleteVisibilityTaskOperation = "CompleteVisibilityTask"
	// PersistenceRangeCompleteVisibilityTasksOperation = "RangeCompleteVisibilityTasks" tracks CompleteVisibilityTasks calls made by service to persistence layer
	PersistenceRangeCompleteVisibilityTasksOperation = "RangeCompleteVisibilityTasks"

	// PersistenceGetReplicationTaskOperation tracks GetReplicationTask calls made by service to persistence layer
	PersistenceGetReplicationTaskOperation = "GetReplicationTask"
	// PersistenceGetReplicationTasksOperation tracks GetReplicationTasks calls made by service to persistence layer
	PersistenceGetReplicationTasksOperation = "GetReplicationTasks"
	// PersistenceCompleteReplicationTaskOperation tracks CompleteReplicationTasks calls made by service to persistence layer
	PersistenceCompleteReplicationTaskOperation = "CompleteReplicationTask"
	// PersistenceRangeCompleteReplicationTasksOperation tracks RangeCompleteReplicationTasks calls made by service to persistence layer
	PersistenceRangeCompleteReplicationTasksOperation = "RangeCompleteReplicationTasks"
	// PersistencePutReplicationTaskToDLQOperation tracks PersistencePutReplicationTaskToDLQOperation calls made by service to persistence layer
	PersistencePutReplicationTaskToDLQOperation = "PutReplicationTaskToDLQ"
	// PersistenceGetReplicationTasksFromDLQOperation tracks PersistenceGetReplicationTasksFromDLQOperation calls made by service to persistence layer
	PersistenceGetReplicationTasksFromDLQOperation = "GetReplicationTasksFromDLQ"
	// PersistenceDeleteReplicationTaskFromDLQOperation tracks PersistenceDeleteReplicationTaskFromDLQOperation calls made by service to persistence layer
	PersistenceDeleteReplicationTaskFromDLQOperation = "DeleteReplicationTaskFromDLQ"
	// PersistenceRangeDeleteReplicationTaskFromDLQOperation tracks PersistenceRangeDeleteReplicationTaskFromDLQOperation calls made by service to persistence layer
	PersistenceRangeDeleteReplicationTaskFromDLQOperation = "RangeDeleteReplicationTaskFromDLQ"
	// PersistenceGetTimerTaskOperation tracks GetTimerTask calls made by service to persistence layer
	PersistenceGetTimerTaskOperation = "GetTimerTask"
	// PersistenceGetTimerTasksOperation tracks GetTimerTasks calls made by service to persistence layer
	PersistenceGetTimerTasksOperation = "GetTimerTasks"
	// PersistenceCompleteTimerTaskOperation tracks CompleteTimerTasks calls made by service to persistence layer
	PersistenceCompleteTimerTaskOperation = "CompleteTimerTask"
	// PersistenceRangeCompleteTimerTasksOperation tracks CompleteTimerTasks calls made by service to persistence layer
	PersistenceRangeCompleteTimerTasksOperation = "RangeCompleteTimerTasks"
	// PersistenceCreateTaskOperation tracks CreateTask calls made by service to persistence layer
	PersistenceCreateTaskOperation = "CreateTask"
	// PersistenceGetTasksOperation tracks GetTasks calls made by service to persistence layer
	PersistenceGetTasksOperation = "GetTasks"
	// PersistenceCompleteTaskOperation tracks CompleteTask calls made by service to persistence layer
	PersistenceCompleteTaskOperation = "CompleteTask"
	// PersistenceCompleteTasksLessThanOperation is the metric operation for persistence.TaskManager.PersistenceCompleteTasksLessThan API
	PersistenceCompleteTasksLessThanOperation = "CompleteTasksLessThan"
	// PersistenceCreateTaskQueueOperation tracks PersistenceCreateTaskQueueOperation calls made by service to persistence layer
	PersistenceCreateTaskQueueOperation = "CreateTaskQueue"
	// PersistenceUpdateTaskQueueOperation tracks PersistenceUpdateTaskQueueOperation calls made by service to persistence layer
	PersistenceUpdateTaskQueueOperation = "UpdateTaskQueue"
	// PersistenceGetTaskQueueOperation tracks PersistenceGetTaskQueueOperation calls made by service to persistence layer
	PersistenceGetTaskQueueOperation = "GetTaskQueue"
	// PersistenceListTaskQueueOperation is the metric operation for persistence.TaskManager.ListTaskQueue API
	PersistenceListTaskQueueOperation = "ListTaskQueue"
	// PersistenceDeleteTaskQueueOperation is the metric operation for persistence.TaskManager.DeleteTaskQueue API
	PersistenceDeleteTaskQueueOperation = "DeleteTaskQueue"
	// PersistenceAppendHistoryEventsOperation tracks AppendHistoryEvents calls made by service to persistence layer
	PersistenceAppendHistoryEventsOperation = "AppendHistoryEvents"
	// PersistenceGetWorkflowExecutionHistoryOperation tracks GetWorkflowExecutionHistory calls made by service to persistence layer
	PersistenceGetWorkflowExecutionHistoryOperation = "GetWorkflowExecutionHistory"
	// PersistenceDeleteWorkflowExecutionHistoryOperation tracks DeleteWorkflowExecutionHistory calls made by service to persistence layer
	PersistenceDeleteWorkflowExecutionHistoryOperation = "DeleteWorkflowExecutionHistory"
	// PersistenceInitializeSystemNamespaceOperation tracks InitializeSystemNamespaceOperation calls made by service to persistence layer
	PersistenceInitializeSystemNamespaceOperation = "InitializeSystemNamespace"
	// PersistenceCreateNamespaceOperation tracks CreateNamespace calls made by service to persistence layer
	PersistenceCreateNamespaceOperation = "CreateNamespace"
	// PersistenceGetNamespaceOperation tracks GetNamespace calls made by service to persistence layer
	PersistenceGetNamespaceOperation = "GetNamespace"
	// PersistenceUpdateNamespaceOperation tracks UpdateNamespace calls made by service to persistence layer
	PersistenceUpdateNamespaceOperation = "UpdateNamespace"
	// PersistenceDeleteNamespaceOperation tracks DeleteNamespace calls made by service to persistence layer
	PersistenceDeleteNamespaceOperation = "DeleteNamespace"
	// PersistenceRenameNamespaceOperation tracks RenameNamespace calls made by service to persistence layer
	PersistenceRenameNamespaceOperation = "RenameNamespace"
	// PersistenceDeleteNamespaceByNameOperation tracks DeleteNamespaceByName calls made by service to persistence layer
	PersistenceDeleteNamespaceByNameOperation = "DeleteNamespaceByName"
	// PersistenceListNamespaceOperation tracks DeleteNamespaceByName calls made by service to persistence layer
	PersistenceListNamespaceOperation = "ListNamespace"
	// PersistenceGetMetadataOperation tracks DeleteNamespaceByName calls made by service to persistence layer
	PersistenceGetMetadataOperation = "GetMetadata"

	// PersistenceEnqueueMessageOperation tracks Enqueue calls made by service to persistence layer
	PersistenceEnqueueMessageOperation = "EnqueueMessage"
	// PersistenceEnqueueMessageToDLQOperation tracks Enqueue DLQ calls made by service to persistence layer
	PersistenceEnqueueMessageToDLQOperation = "EnqueueMessageToDLQ"
	// PersistenceReadQueueMessagesOperation tracks ReadMessages calls made by service to persistence layer
	PersistenceReadQueueMessagesOperation = "ReadQueueMessages"
	// PersistenceReadQueueMessagesFromDLQOperation tracks ReadMessagesFromDLQ calls made by service to persistence layer
	PersistenceReadQueueMessagesFromDLQOperation = "ReadQueueMessagesFromDLQ"
	// PersistenceDeleteQueueMessagesOperation tracks DeleteMessages calls made by service to persistence layer
	PersistenceDeleteQueueMessagesOperation = "DeleteQueueMessages"
	// PersistenceDeleteQueueMessageFromDLQOperation tracks DeleteMessageFromDLQ calls made by service to persistence layer
	PersistenceDeleteQueueMessageFromDLQOperation = "DeleteQueueMessageFromDLQ"
	// PersistenceRangeDeleteMessagesFromDLQOperation tracks RangeDeleteMessagesFromDLQ calls made by service to persistence layer
	PersistenceRangeDeleteMessagesFromDLQOperation = "RangeDeleteMessagesFromDLQ"
	// PersistenceUpdateAckLevelOperation tracks UpdateAckLevel calls made by service to persistence layer
	PersistenceUpdateAckLevelOperation = "UpdateAckLevel"
	// PersistenceGetAckLevelOperation tracks GetAckLevel calls made by service to persistence layer
	PersistenceGetAckLevelOperation = "GetAckLevel"
	// PersistenceUpdateDLQAckLevelOperation tracks UpdateDLQAckLevel calls made by service to persistence layer
	PersistenceUpdateDLQAckLevelOperation = "UpdateDLQAckLevel"
	// PersistenceGetDLQAckLevelOperation tracks GetDLQAckLevel calls made by service to persistence layer
	PersistenceGetDLQAckLevelOperation = "GetDLQAckLevel"
	// PersistenceListClusterMetadataOperation tracks ListClusterMetadata calls made by service to persistence layer
	PersistenceListClusterMetadataOperation = "ListClusterMetadata"
	// PersistenceGetClusterMetadataOperation tracks GetClusterMetadata calls made by service to persistence layer
	PersistenceGetClusterMetadataOperation = "GetClusterMetadata"
	// PersistenceSaveClusterMetadataOperation tracks SaveClusterMetadata calls made by service to persistence layer
	PersistenceSaveClusterMetadataOperation = "SaveClusterMetadata"
	// PersistenceDeleteClusterMetadataOperation tracks DeleteClusterMetadata calls made by service to persistence layer
	PersistenceDeleteClusterMetadataOperation = "DeleteClusterMetadata"
	// PersistenceUpsertClusterMembershipOperation tracks UpsertClusterMembership calls made by service to persistence layer
	PersistenceUpsertClusterMembershipOperation = "UpsertClusterMembership"
	// PersistencePruneClusterMembershipOperation tracks PruneClusterMembership calls made by service to persistence layer
	PersistencePruneClusterMembershipOperation = "PruneClusterMembership"
	// PersistenceGetClusterMembersOperation tracks GetClusterMembers calls made by service to persistence layer
	PersistenceGetClusterMembersOperation = "GetClusterMembers"

	// PersistenceAppendHistoryNodesOperation tracks AppendHistoryNodes calls made by service to persistence layer
	PersistenceAppendHistoryNodesOperation = "AppendHistoryNodes"
	// PersistenceAppendRawHistoryNodesOperation tracks AppendRawHistoryNodes calls made by service to persistence layer
	PersistenceAppendRawHistoryNodesOperation = "AppendRawHistoryNodes"
	// PersistenceDeleteHistoryNodesOperation tracks DeleteHistoryNodes calls made by service to persistence layer
	PersistenceDeleteHistoryNodesOperation = "DeleteHistoryNodes"
	// PersistenceParseHistoryBranchInfoOperation tracks NewHistoryBranch calls made by service to persistence layer
	PersistenceParseHistoryBranchInfoOperation = "ParseHistoryBranchInfo"
	// PersistenceUpdateHistoryBranchInfoOperation tracks NewHistoryBranch calls made by service to persistence layer
	PersistenceUpdateHistoryBranchInfoOperation = "UpdateHistoryBranchInfo"
	// PersistenceNewHistoryBranchOperation tracks NewHistoryBranch calls made by service to persistence layer
	PersistenceNewHistoryBranchOperation = "NewHistoryBranch"
	// PersistenceReadHistoryBranchOperation tracks ReadHistoryBranch calls made by service to persistence layer
	PersistenceReadHistoryBranchOperation = "ReadHistoryBranch"
	// PersistenceReadHistoryBranchReverseOperation tracks ReadHistoryBranchReverse calls made by service to persistence layer
	PersistenceReadHistoryBranchReverseOperation = "ReadHistoryBranchReverse"
	// PersistenceForkHistoryBranchOperation tracks ForkHistoryBranch calls made by service to persistence layer
	PersistenceForkHistoryBranchOperation = "ForkHistoryBranch"
	// PersistenceDeleteHistoryBranchOperation tracks DeleteHistoryBranch calls made by service to persistence layer
	PersistenceDeleteHistoryBranchOperation = "DeleteHistoryBranch"
	// PersistenceTrimHistoryBranchOperation tracks TrimHistoryBranch calls made by service to persistence layer
	PersistenceTrimHistoryBranchOperation = "TrimHistoryBranch"
	// PersistenceCompleteForkBranchOperation tracks CompleteForkBranch calls made by service to persistence layer
	PersistenceCompleteForkBranchOperation = "CompleteForkBranch"
	// PersistenceGetHistoryTreeOperation tracks GetHistoryTree calls made by service to persistence layer
	PersistenceGetHistoryTreeOperation = "GetHistoryTree"
	// PersistenceGetAllHistoryTreeBranchesOperation tracks GetHistoryTree calls made by service to persistence layer
	PersistenceGetAllHistoryTreeBranchesOperation = "GetAllHistoryTreeBranches"
	// PersistenceNamespaceReplicationQueueOperation is the metrics Operation for namespace replication queue
	PersistenceNamespaceReplicationQueueOperation = "NamespaceReplicationQueue"

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
)

// Matching operation tags
const (
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
	MatchingGetWorkerBuildIdOrderingOperation = "GetWorkerBuildIdOrdering"
	// MatchingInvalidateTaskQueueMetadataOperation tracks GetWorkerBuildIdOrdering API calls received by service
	MatchingInvalidateTaskQueueMetadataOperation = "InvalidateTaskQueueMetadata"
	// MatchingGetTaskQueueMetadataOperation tracks GetWorkerBuildIdOrdering API calls received by service
	MatchingGetTaskQueueMetadataOperation = "GetTaskQueueMetadata"
)

const (
	// ExecutionsScavengerOperation is the operation used by all metrics emitted by worker.executions.Scavenger module
	ExecutionsScavengerOperation = "executionsscavenger"
	// HistoryScavengerOperation is the operation used by all metrics emitted by worker.history.Scavenger module
	HistoryScavengerOperation = "historyscavenger"
	// TaskQueueScavengerOperation is the operation used by all metrics emitted by worker.taskqueue.Scavenger module
	TaskQueueScavengerOperation = "taskqueuescavenger"
	// VisibilityArchiverOperation is used by visibility archivers
	VisibilityArchiverOperation = "VisibilityArchiver"
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

	// BlobstoreClientUploadOperation tracks Upload calls to blobstore
	BlobstoreClientUploadOperation = "BlobstoreClientUpload"
	// BlobstoreClientDownloadOperation tracks Download calls to blobstore
	BlobstoreClientDownloadOperation = "BlobstoreClientDownload"
	// BlobstoreClientGetMetadataOperation tracks GetMetadata calls to blobstore
	BlobstoreClientGetMetadataOperation = "BlobstoreClientGetMetadata"
	// BlobstoreClientExistsOperation tracks Exists calls to blobstore
	BlobstoreClientExistsOperation = "BlobstoreClientExists"
	// BlobstoreClientDeleteOperation tracks Delete calls to blobstore
	BlobstoreClientDeleteOperation = "BlobstoreClientDelete"
	// BlobstoreClientDirectoryExistsOperation tracks DirectoryExists calls to blobstore
	BlobstoreClientDirectoryExistsOperation = "BlobstoreClientDirectoryExists"
)

// Clients operation tags
const (
	// Frontend Client

	// FrontendClientDeprecateNamespaceOperation tracks RPC calls to frontend service
	FrontendClientDeprecateNamespaceOperation = "FrontendClientDeprecateNamespace"
	// FrontendClientDescribeNamespaceOperation tracks RPC calls to frontend service
	FrontendClientDescribeNamespaceOperation = "FrontendClientDescribeNamespace"
	// FrontendClientDescribeTaskQueueOperation tracks RPC calls to frontend service
	FrontendClientDescribeTaskQueueOperation = "FrontendClientDescribeTaskQueue"
	// FrontendClientDescribeWorkflowExecutionOperation tracks RPC calls to frontend service
	FrontendClientDescribeWorkflowExecutionOperation = "FrontendClientDescribeWorkflowExecution"
	// FrontendClientGetWorkflowExecutionHistoryOperation tracks RPC calls to frontend service
	FrontendClientGetWorkflowExecutionHistoryOperation = "FrontendClientGetWorkflowExecutionHistory"
	// FrontendClientGetWorkflowExecutionHistoryReverseOperation tracks RPC calls to frontend service
	FrontendClientGetWorkflowExecutionHistoryReverseOperation = "FrontendClientGetWorkflowExecutionHistoryReverse"
	// FrontendClientGetWorkflowExecutionRawHistoryOperation tracks RPC calls to frontend service
	FrontendClientGetWorkflowExecutionRawHistoryOperation = "FrontendClientGetWorkflowExecutionRawHistory"
	// FrontendClientPollForWorkflowExecutionRawHistoryOperation tracks RPC calls to frontend service
	FrontendClientPollForWorkflowExecutionRawHistoryOperation = "FrontendClientPollForWorkflowExecutionRawHistory"
	// FrontendClientListArchivedWorkflowExecutionsOperation tracks RPC calls to frontend service
	FrontendClientListArchivedWorkflowExecutionsOperation = "FrontendClientListArchivedWorkflowExecutions"
	// FrontendClientListClosedWorkflowExecutionsOperation tracks RPC calls to frontend service
	FrontendClientListClosedWorkflowExecutionsOperation = "FrontendClientListClosedWorkflowExecutions"
	// FrontendClientListNamespacesOperation tracks RPC calls to frontend service
	FrontendClientListNamespacesOperation = "FrontendClientListNamespaces"
	// FrontendClientListOpenWorkflowExecutionsOperation tracks RPC calls to frontend service
	FrontendClientListOpenWorkflowExecutionsOperation = "FrontendClientListOpenWorkflowExecutions"
	// FrontendClientPollActivityTaskQueueOperation tracks RPC calls to frontend service
	FrontendClientPollActivityTaskQueueOperation = "FrontendClientPollActivityTaskQueue"
	// FrontendClientPollWorkflowTaskQueueOperation tracks RPC calls to frontend service
	FrontendClientPollWorkflowTaskQueueOperation = "FrontendClientPollWorkflowTaskQueue"
	// FrontendClientQueryWorkflowOperation tracks RPC calls to frontend service
	FrontendClientQueryWorkflowOperation = "FrontendClientQueryWorkflow"
	// FrontendClientRecordActivityTaskHeartbeatOperation tracks RPC calls to frontend service
	FrontendClientRecordActivityTaskHeartbeatOperation = "FrontendClientRecordActivityTaskHeartbeat"
	// FrontendClientRecordActivityTaskHeartbeatByIdOperation tracks RPC calls to frontend service
	FrontendClientRecordActivityTaskHeartbeatByIdOperation = "FrontendClientRecordActivityTaskHeartbeatById"
	// FrontendClientRegisterNamespaceOperation tracks RPC calls to frontend service
	FrontendClientRegisterNamespaceOperation = "FrontendClientRegisterNamespace"
	// FrontendClientRequestCancelWorkflowExecutionOperation tracks RPC calls to frontend service
	FrontendClientRequestCancelWorkflowExecutionOperation = "FrontendClientRequestCancelWorkflowExecution"
	// FrontendClientResetStickyTaskQueueOperation tracks RPC calls to frontend service
	FrontendClientResetStickyTaskQueueOperation = "FrontendClientResetStickyTaskQueue"
	// FrontendClientResetWorkflowExecutionOperation tracks RPC calls to frontend service
	FrontendClientResetWorkflowExecutionOperation = "FrontendClientResetWorkflowExecution"
	// FrontendClientRespondActivityTaskCanceledOperation tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCanceledOperation = "FrontendClientRespondActivityTaskCanceled"
	// FrontendClientRespondActivityTaskCanceledByIdOperation tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCanceledByIdOperation = "FrontendClientRespondActivityTaskCanceledById"
	// FrontendClientRespondActivityTaskCompletedOperation tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCompletedOperation = "FrontendClientRespondActivityTaskCompleted"
	// FrontendClientRespondActivityTaskCompletedByIdOperation tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCompletedByIdOperation = "FrontendClientRespondActivityTaskCompletedById"
	// FrontendClientRespondActivityTaskFailedOperation tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskFailedOperation = "FrontendClientRespondActivityTaskFailed"
	// FrontendClientRespondActivityTaskFailedByIdOperation tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskFailedByIdOperation = "FrontendClientRespondActivityTaskFailedById"
	// FrontendClientRespondWorkflowTaskCompletedOperation tracks RPC calls to frontend service
	FrontendClientRespondWorkflowTaskCompletedOperation = "FrontendClientRespondWorkflowTaskCompleted"
	// FrontendClientRespondWorkflowTaskFailedOperation tracks RPC calls to frontend service
	FrontendClientRespondWorkflowTaskFailedOperation = "FrontendClientRespondWorkflowTaskFailed"
	// FrontendClientRespondQueryTaskCompletedOperation tracks RPC calls to frontend service
	FrontendClientRespondQueryTaskCompletedOperation = "FrontendClientRespondQueryTaskCompleted"
	// FrontendClientSignalWithStartWorkflowExecutionOperation tracks RPC calls to frontend service
	FrontendClientSignalWithStartWorkflowExecutionOperation = "FrontendClientSignalWithStartWorkflowExecution"
	// FrontendClientSignalWorkflowExecutionOperation tracks RPC calls to frontend service
	FrontendClientSignalWorkflowExecutionOperation = "FrontendClientSignalWorkflowExecution"
	// FrontendClientStartWorkflowExecutionOperation tracks RPC calls to frontend service
	FrontendClientStartWorkflowExecutionOperation = "FrontendClientStartWorkflowExecution"
	// FrontendClientTerminateWorkflowExecutionOperation tracks RPC calls to frontend service
	FrontendClientTerminateWorkflowExecutionOperation = "FrontendClientTerminateWorkflowExecution"
	// FrontendClientUpdateNamespaceOperation tracks RPC calls to frontend service
	FrontendClientUpdateNamespaceOperation = "FrontendClientUpdateNamespace"
	// FrontendClientListWorkflowExecutionsOperation tracks RPC calls to frontend service
	FrontendClientListWorkflowExecutionsOperation = "FrontendClientListWorkflowExecutions"
	// FrontendClientScanWorkflowExecutionsOperation tracks RPC calls to frontend service
	FrontendClientScanWorkflowExecutionsOperation = "FrontendClientScanWorkflowExecutions"
	// FrontendClientCountWorkflowExecutionsOperation tracks RPC calls to frontend service
	FrontendClientCountWorkflowExecutionsOperation = "FrontendClientCountWorkflowExecutions"
	// FrontendClientGetSearchAttributesOperation tracks RPC calls to frontend service
	FrontendClientGetSearchAttributesOperation = "FrontendClientGetSearchAttributes"
	// FrontendClientGetClusterInfoOperation tracks RPC calls to frontend
	FrontendClientGetClusterInfoOperation = "FrontendClientGetClusterInfo"
	// FrontendClientGetSystemInfoOperation tracks RPC calls to frontend
	FrontendClientGetSystemInfoOperation = "FrontendClientGetSystemInfo"
	// FrontendClientListTaskQueuePartitionsOperation tracks RPC calls to frontend service
	FrontendClientListTaskQueuePartitionsOperation = "FrontendClientListTaskQueuePartitions"
	// FrontendClientCreateScheduleOperation tracks RPC calls to frontend service
	FrontendClientCreateScheduleOperation = "FrontendClientCreateSchedule"
	// FrontendClientDescribeScheduleOperation tracks RPC calls to frontend service
	FrontendClientDescribeScheduleOperation = "FrontendClientDescribeSchedule"
	// FrontendClientUpdateScheduleOperation tracks RPC calls to frontend service
	FrontendClientUpdateScheduleOperation = "FrontendClientUpdateSchedule"
	// FrontendClientPatchScheduleOperation tracks RPC calls to frontend service
	FrontendClientPatchScheduleOperation = "FrontendClientPatchSchedule"
	// FrontendClientListScheduleMatchingTimesOperation tracks RPC calls to frontend service
	FrontendClientListScheduleMatchingTimesOperation = "FrontendClientListScheduleMatchingTimes"
	// FrontendClientDeleteScheduleOperation tracks RPC calls to frontend service
	FrontendClientDeleteScheduleOperation = "FrontendClientDeleteSchedule"
	// FrontendClientListSchedulesOperation tracks RPC calls to frontend service
	FrontendClientListSchedulesOperation = "FrontendClientListSchedules"
	// FrontendClientUpdateWorkerBuildIdOrderingOperation tracks RPC calls to frontend service
	FrontendClientUpdateWorkerBuildIdOrderingOperation = "FrontendClientUpdateWorkerBuildIdOrdering"
	// FrontendClientUpdateWorkflowOperation tracks RPC calls to frontend service
	FrontendClientUpdateWorkflowOperation = "FrontendClientUpdateWorkflow"
	// FrontendClientGetWorkerBuildIdOrderingOperation tracks RPC calls to frontend service
	FrontendClientGetWorkerBuildIdOrderingOperation = "FrontendClientGetWorkerBuildIdOrdering"
	// FrontendClientDescribeBatchOperationOperation tracks RPC calls to frontend service
	FrontendClientDescribeBatchOperationOperation = "FrontendClientDescribeBatchOperation"
	// FrontendClientListBatchOperationsOperation tracks RPC calls to frontend service
	FrontendClientListBatchOperationsOperation = "FrontendClientListBatchOperations"
	// FrontendClientStartBatchOperationOperation tracks RPC calls to frontend service
	FrontendClientStartBatchOperationOperation = "FrontendClientStartBatchOperation"
	// FrontendClientStopBatchOperationOperation tracks RPC calls to frontend service
	FrontendClientStopBatchOperationOperation = "FrontendClientStopBatchOperation"

	// AdminClientAddSearchAttributesOperation tracks RPC calls to admin service
	AdminClientAddSearchAttributesOperation = "AdminClientAddSearchAttributes"
	// AdminClientRemoveSearchAttributesOperation tracks RPC calls to admin service
	AdminClientRemoveSearchAttributesOperation = "AdminClientRemoveSearchAttributes"
	// AdminClientGetSearchAttributesOperation tracks RPC calls to admin service
	AdminClientGetSearchAttributesOperation = "AdminClientGetSearchAttributes"
	// AdminClientCloseShardOperation tracks RPC calls to admin service
	AdminClientCloseShardOperation = "AdminClientCloseShard"
	// AdminClientGetShardOperation tracks RPC calls to admin service
	AdminClientGetShardOperation = "AdminClientGetShard"
	// AdminClientListHistoryTasksOperation tracks RPC calls to admin service
	AdminClientListHistoryTasksOperation = "AdminClientListHistoryTasks"
	// AdminClientRemoveTaskOperation tracks RPC calls to admin service
	AdminClientRemoveTaskOperation = "AdminClientRemoveTask"
	// AdminClientDescribeHistoryHostOperation tracks RPC calls to admin service
	AdminClientDescribeHistoryHostOperation = "AdminClientDescribeHistoryHost"
	// AdminClientRebuildMutableStateOperation tracks RPC calls to admin service
	AdminClientRebuildMutableStateOperation = "AdminClientRebuildMutableState"
	// AdminClientDescribeMutableStateOperation tracks RPC calls to admin service
	AdminClientDescribeMutableStateOperation = "AdminClientDescribeMutableState"
	// AdminClientGetWorkflowExecutionRawHistoryV2Operation tracks RPC calls to admin service
	AdminClientGetWorkflowExecutionRawHistoryV2Operation = "AdminClientGetWorkflowExecutionRawHistoryV2"
	// AdminClientDescribeClusterOperation tracks RPC calls to admin service
	AdminClientDescribeClusterOperation = "AdminClientDescribeCluster"
	// AdminClientListClustersOperation tracks RPC calls to admin service
	AdminClientListClustersOperation = "AdminClientListClusters"
	// AdminClientListClusterMembersOperation tracks RPC calls to admin service
	AdminClientListClusterMembersOperation = "AdminClientListClusterMembers"
	// AdminClientAddOrUpdateRemoteClusterOperation tracks RPC calls to admin service
	AdminClientAddOrUpdateRemoteClusterOperation = "AdminClientAddOrUpdateRemoteCluster"
	// AdminClientRemoveRemoteClusterOperation tracks RPC calls to admin service
	AdminClientRemoveRemoteClusterOperation = "AdminClientRemoveRemoteCluster"
	// AdminClientGetReplicationMessagesOperation tracks RPC calls to admin service
	AdminClientGetReplicationMessagesOperation = "AdminClientGetReplicationMessages"
	// AdminClientGetNamespaceReplicationMessagesOperation tracks RPC calls to admin service
	AdminClientGetNamespaceReplicationMessagesOperation = "AdminClientGetNamespaceReplicationMessages"
	// AdminClientGetDLQReplicationMessagesOperation tracks RPC calls to admin service
	AdminClientGetDLQReplicationMessagesOperation = "AdminClientGetDLQReplicationMessages"
	// AdminClientReapplyEventsOperation tracks RPC calls to admin service
	AdminClientReapplyEventsOperation = "AdminClientReapplyEvents"
	// AdminClientGetDLQMessagesOperation tracks RPC calls to admin service
	AdminClientGetDLQMessagesOperation = "AdminClientGetDLQMessages"
	// AdminClientPurgeDLQMessagesOperation tracks RPC calls to admin service
	AdminClientPurgeDLQMessagesOperation = "AdminClientPurgeDLQMessages"
	// AdminClientMergeDLQMessagesOperation tracks RPC calls to admin service
	AdminClientMergeDLQMessagesOperation = "AdminClientMergeDLQMessages"
	// AdminClientRefreshWorkflowTasksOperation tracks RPC calls to admin service
	AdminClientRefreshWorkflowTasksOperation = "AdminClientRefreshWorkflowTasks"
	// AdminClientResendReplicationTasksOperation tracks RPC calls to admin service
	AdminClientResendReplicationTasksOperation = "AdminClientResendReplicationTasks"
	// AdminClientGetTaskQueueTasksOperation tracks RPC calls to admin service
	AdminClientGetTaskQueueTasksOperation = "AdminClientGetTaskQueueTasks"
	// AdminClientDeleteWorkflowExecutionOperation tracks RPC calls to admin service
	AdminClientDeleteWorkflowExecutionOperation = "AdminClientDeleteWorkflowExecution"

	// DCRedirectionDeprecateNamespaceOperation tracks RPC calls for dc redirection
	DCRedirectionDeprecateNamespaceOperation = "DCRedirectionDeprecateNamespace"
	// DCRedirectionDescribeNamespaceOperation tracks RPC calls for dc redirection
	DCRedirectionDescribeNamespaceOperation = "DCRedirectionDescribeNamespace"
	// DCRedirectionDescribeTaskQueueOperation tracks RPC calls for dc redirection
	DCRedirectionDescribeTaskQueueOperation = "DCRedirectionDescribeTaskQueue"
	// DCRedirectionDescribeWorkflowExecutionOperation tracks RPC calls for dc redirection
	DCRedirectionDescribeWorkflowExecutionOperation = "DCRedirectionDescribeWorkflowExecution"
	// DCRedirectionGetWorkflowExecutionHistoryOperation tracks RPC calls for dc redirection
	DCRedirectionGetWorkflowExecutionHistoryOperation = "DCRedirectionGetWorkflowExecutionHistory"
	// DCRedirectionGetWorkflowExecutionHistoryReverseOperation tracks RPC calls for dc redirection
	DCRedirectionGetWorkflowExecutionHistoryReverseOperation = "DCRedirectionGetWorkflowExecutionHistoryReverse"
	// DCRedirectionGetWorkflowExecutionRawHistoryOperation tracks RPC calls for dc redirection
	DCRedirectionGetWorkflowExecutionRawHistoryOperation = "DCRedirectionGetWorkflowExecutionRawHistory"
	// DCRedirectionPollForWorkflowExecutionRawHistoryOperation tracks RPC calls for dc redirection
	DCRedirectionPollForWorkflowExecutionRawHistoryOperation = "DCRedirectionPollForWorkflowExecutionRawHistory"
	// DCRedirectionListArchivedWorkflowExecutionsOperation tracks RPC calls for dc redirection
	DCRedirectionListArchivedWorkflowExecutionsOperation = "DCRedirectionListArchivedWorkflowExecutions"
	// DCRedirectionListClosedWorkflowExecutionsOperation tracks RPC calls for dc redirection
	DCRedirectionListClosedWorkflowExecutionsOperation = "DCRedirectionListClosedWorkflowExecutions"
	// DCRedirectionListNamespacesOperation tracks RPC calls for dc redirection
	DCRedirectionListNamespacesOperation = "DCRedirectionListNamespaces"
	// DCRedirectionListOpenWorkflowExecutionsOperation tracks RPC calls for dc redirection
	DCRedirectionListOpenWorkflowExecutionsOperation = "DCRedirectionListOpenWorkflowExecutions"
	// DCRedirectionListWorkflowExecutionsOperation tracks RPC calls for dc redirection
	DCRedirectionListWorkflowExecutionsOperation = "DCRedirectionListWorkflowExecutions"
	// DCRedirectionScanWorkflowExecutionsOperation tracks RPC calls for dc redirection
	DCRedirectionScanWorkflowExecutionsOperation = "DCRedirectionScanWorkflowExecutions"
	// DCRedirectionCountWorkflowExecutionsOperation tracks RPC calls for dc redirection
	DCRedirectionCountWorkflowExecutionsOperation = "DCRedirectionCountWorkflowExecutions"
	// DCRedirectionGetSearchAttributesOperation tracks RPC calls for dc redirection
	DCRedirectionGetSearchAttributesOperation = "DCRedirectionGetSearchAttributes"
	// DCRedirectionPollActivityTaskQueueOperation tracks RPC calls for dc redirection
	DCRedirectionPollActivityTaskQueueOperation = "DCRedirectionPollActivityTaskQueue"
	// DCRedirectionPollWorkflowTaskQueueOperation tracks RPC calls for dc redirection
	DCRedirectionPollWorkflowTaskQueueOperation = "DCRedirectionPollWorkflowTaskQueue"
	// DCRedirectionQueryWorkflowOperation tracks RPC calls for dc redirection
	DCRedirectionQueryWorkflowOperation = "DCRedirectionQueryWorkflow"
	// DCRedirectionRecordActivityTaskHeartbeatOperation tracks RPC calls for dc redirection
	DCRedirectionRecordActivityTaskHeartbeatOperation = "DCRedirectionRecordActivityTaskHeartbeat"
	// DCRedirectionRecordActivityTaskHeartbeatByIdOperation tracks RPC calls for dc redirection
	DCRedirectionRecordActivityTaskHeartbeatByIdOperation = "DCRedirectionRecordActivityTaskHeartbeatById"
	// DCRedirectionRegisterNamespaceOperation tracks RPC calls for dc redirection
	DCRedirectionRegisterNamespaceOperation = "DCRedirectionRegisterNamespace"
	// DCRedirectionRequestCancelWorkflowExecutionOperation tracks RPC calls for dc redirection
	DCRedirectionRequestCancelWorkflowExecutionOperation = "DCRedirectionRequestCancelWorkflowExecution"
	// DCRedirectionResetStickyTaskQueueOperation tracks RPC calls for dc redirection
	DCRedirectionResetStickyTaskQueueOperation = "DCRedirectionResetStickyTaskQueue"
	// DCRedirectionResetWorkflowExecutionOperation tracks RPC calls for dc redirection
	DCRedirectionResetWorkflowExecutionOperation = "DCRedirectionResetWorkflowExecution"
	// DCRedirectionRespondActivityTaskCanceledOperation tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskCanceledOperation = "DCRedirectionRespondActivityTaskCanceled"
	// DCRedirectionRespondActivityTaskCanceledByIdOperation tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskCanceledByIdOperation = "DCRedirectionRespondActivityTaskCanceledById"
	// DCRedirectionRespondActivityTaskCompletedOperation tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskCompletedOperation = "DCRedirectionRespondActivityTaskCompleted"
	// DCRedirectionRespondActivityTaskCompletedByIdOperation tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskCompletedByIdOperation = "DCRedirectionRespondActivityTaskCompletedById"
	// DCRedirectionRespondActivityTaskFailedOperation tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskFailedOperation = "DCRedirectionRespondActivityTaskFailed"
	// DCRedirectionRespondActivityTaskFailedByIdOperation tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskFailedByIdOperation = "DCRedirectionRespondActivityTaskFailedById"
	// DCRedirectionRespondWorkflowTaskCompletedOperation tracks RPC calls for dc redirection
	DCRedirectionRespondWorkflowTaskCompletedOperation = "DCRedirectionRespondWorkflowTaskCompleted"
	// DCRedirectionRespondWorkflowTaskFailedOperation tracks RPC calls for dc redirection
	DCRedirectionRespondWorkflowTaskFailedOperation = "DCRedirectionRespondWorkflowTaskFailed"
	// DCRedirectionRespondQueryTaskCompletedOperation tracks RPC calls for dc redirection
	DCRedirectionRespondQueryTaskCompletedOperation = "DCRedirectionRespondQueryTaskCompleted"
	// DCRedirectionSignalWithStartWorkflowExecutionOperation tracks RPC calls for dc redirection
	DCRedirectionSignalWithStartWorkflowExecutionOperation = "DCRedirectionSignalWithStartWorkflowExecution"
	// DCRedirectionSignalWorkflowExecutionOperation tracks RPC calls for dc redirection
	DCRedirectionSignalWorkflowExecutionOperation = "DCRedirectionSignalWorkflowExecution"
	// DCRedirectionStartWorkflowExecutionOperation tracks RPC calls for dc redirection
	DCRedirectionStartWorkflowExecutionOperation = "DCRedirectionStartWorkflowExecution"
	// DCRedirectionTerminateWorkflowExecutionOperation tracks RPC calls for dc redirection
	DCRedirectionTerminateWorkflowExecutionOperation = "DCRedirectionTerminateWorkflowExecution"
	// DCRedirectionUpdateNamespaceOperation tracks RPC calls for dc redirection
	DCRedirectionUpdateNamespaceOperation = "DCRedirectionUpdateNamespace"
	// DCRedirectionListTaskQueuePartitionsOperation tracks RPC calls for dc redirection
	DCRedirectionListTaskQueuePartitionsOperation = "DCRedirectionListTaskQueuePartitions"
	// DCRedirectionCreateScheduleOperation tracks RPC calls for dc redirection
	DCRedirectionCreateScheduleOperation = "DCRedirectionCreateSchedule"
	// DCRedirectionDescribeScheduleOperation tracks RPC calls for dc redirection
	DCRedirectionDescribeScheduleOperation = "DCRedirectionDescribeSchedule"
	// DCRedirectionUpdateScheduleOperation tracks RPC calls for dc redirection
	DCRedirectionUpdateScheduleOperation = "DCRedirectionUpdateSchedule"
	// DCRedirectionPatchScheduleOperation tracks RPC calls for dc redirection
	DCRedirectionPatchScheduleOperation = "DCRedirectionPatchSchedule"
	// DCRedirectionListScheduleMatchingTimesOperation tracks RPC calls for dc redirection
	DCRedirectionListScheduleMatchingTimesOperation = "DCRedirectionListScheduleMatchingTimes"
	// DCRedirectionDeleteScheduleOperation tracks RPC calls for dc redirection
	DCRedirectionDeleteScheduleOperation = "DCRedirectionDeleteSchedule"
	// DCRedirectionListSchedulesOperation tracks RPC calls for dc redirection
	DCRedirectionListSchedulesOperation = "DCRedirectionListSchedules"
	// DCRedirectionUpdateWorkerBuildIdOrderingOperation tracks RPC calls for dc redirection
	DCRedirectionUpdateWorkerBuildIdOrderingOperation = "DCRedirectionUpdateWorkerBuildIdOrdering"
	// DCRedirectionGetWorkerBuildIdOrderingOperation tracks RPC calls for dc redirection
	DCRedirectionGetWorkerBuildIdOrderingOperation = "DCRedirectionGetWorkerBuildIdOrdering"
	// DCRedirectionUpdateWorkflowOperation tracks RPC calls for dc redirection
	DCRedirectionUpdateWorkflowOperation = "DCRedirectionUpdateWorkflow"
	// DCRedirectionDescribeBatchOperationOperation tracks RPC calls for dc redirection
	DCRedirectionDescribeBatchOperationOperation = "DCRedirectionDescribeBatchOperation"
	// DCRedirectionListBatchOperationsOperation tracks RPC calls for dc redirection
	DCRedirectionListBatchOperationsOperation = "DCRedirectionListBatchOperations"
	// DCRedirectionStartBatchOperationOperation tracks RPC calls for dc redirection
	DCRedirectionStartBatchOperationOperation = "DCRedirectionStartBatchOperation"
	// DCRedirectionStopBatchOperationOperation tracks RPC calls for dc redirection
	DCRedirectionStopBatchOperationOperation = "DCRedirectionStopBatchOperation"

	// History Client

	// HistoryClientStartWorkflowExecutionOperation tracks RPC calls to history service
	HistoryClientStartWorkflowExecutionOperation = "HistoryClientStartWorkflowExecution"
	// HistoryClientRecordActivityTaskHeartbeatOperation tracks RPC calls to history service
	HistoryClientRecordActivityTaskHeartbeatOperation = "HistoryClientRecordActivityTaskHeartbeat"
	// HistoryClientRespondWorkflowTaskCompletedOperation tracks RPC calls to history service
	HistoryClientRespondWorkflowTaskCompletedOperation = "HistoryClientRespondWorkflowTaskCompleted"
	// HistoryClientRespondWorkflowTaskFailedOperation tracks RPC calls to history service
	HistoryClientRespondWorkflowTaskFailedOperation = "HistoryClientRespondWorkflowTaskFailed"
	// HistoryClientRespondActivityTaskCompletedOperation tracks RPC calls to history service
	HistoryClientRespondActivityTaskCompletedOperation = "HistoryClientRespondActivityTaskCompleted"
	// HistoryClientRespondActivityTaskFailedOperation tracks RPC calls to history service
	HistoryClientRespondActivityTaskFailedOperation = "HistoryClientRespondActivityTaskFailed"
	// HistoryClientRespondActivityTaskCanceledOperation tracks RPC calls to history service
	HistoryClientRespondActivityTaskCanceledOperation = "HistoryClientRespondActivityTaskCanceled"
	// HistoryClientGetMutableStateOperation tracks RPC calls to history service
	HistoryClientGetMutableStateOperation = "HistoryClientGetMutableState"
	// HistoryClientPollMutableStateOperation tracks RPC calls to history service
	HistoryClientPollMutableStateOperation = "HistoryClientPollMutableState"
	// HistoryClientResetStickyTaskQueueOperation tracks RPC calls to history service
	HistoryClientResetStickyTaskQueueOperation = "HistoryClientResetStickyTaskQueue"
	// HistoryClientDescribeWorkflowExecutionOperation tracks RPC calls to history service
	HistoryClientDescribeWorkflowExecutionOperation = "HistoryClientDescribeWorkflowExecution"
	// HistoryClientRecordWorkflowTaskStartedOperation tracks RPC calls to history service
	HistoryClientRecordWorkflowTaskStartedOperation = "HistoryClientRecordWorkflowTaskStarted"
	// HistoryClientRecordActivityTaskStartedOperation tracks RPC calls to history service
	HistoryClientRecordActivityTaskStartedOperation = "HistoryClientRecordActivityTaskStarted"
	// HistoryClientRequestCancelWorkflowExecutionOperation tracks RPC calls to history service
	HistoryClientRequestCancelWorkflowExecutionOperation = "HistoryClientRequestCancelWorkflowExecution"
	// HistoryClientSignalWorkflowExecutionOperation tracks RPC calls to history service
	HistoryClientSignalWorkflowExecutionOperation = "HistoryClientSignalWorkflowExecution"
	// HistoryClientSignalWithStartWorkflowExecutionOperation tracks RPC calls to history service
	HistoryClientSignalWithStartWorkflowExecutionOperation = "HistoryClientSignalWithStartWorkflowExecution"
	// HistoryClientRemoveSignalMutableStateOperation tracks RPC calls to history service
	HistoryClientRemoveSignalMutableStateOperation = "HistoryClientRemoveSignalMutableState"
	// HistoryClientTerminateWorkflowExecutionOperation tracks RPC calls to history service
	HistoryClientTerminateWorkflowExecutionOperation = "HistoryClientTerminateWorkflowExecution"
	// HistoryClientUpdateWorkflowOperation tracks RPC calls to history service
	HistoryClientUpdateWorkflowOperation = "HistoryClientUpdateWorkflow"
	// HistoryClientDeleteWorkflowExecutionOperation tracks RPC calls to history service
	HistoryClientDeleteWorkflowExecutionOperation = "HistoryClientDeleteWorkflowExecution"
	// HistoryClientResetWorkflowExecutionOperation tracks RPC calls to history service
	HistoryClientResetWorkflowExecutionOperation = "HistoryClientResetWorkflowExecution"
	// HistoryClientScheduleWorkflowTaskOperation tracks RPC calls to history service
	HistoryClientScheduleWorkflowTaskOperation = "HistoryClientScheduleWorkflowTask"
	// HistoryClientVerifyFirstWorkflowTaskScheduledOperation tracks RPC calls to history service
	HistoryClientVerifyFirstWorkflowTaskScheduledOperation = "HistoryClientVerifyFirstWorkflowTaskScheduled"
	// HistoryClientRecordChildExecutionCompletedOperation tracks RPC calls to history service
	HistoryClientRecordChildExecutionCompletedOperation = "HistoryClientRecordChildExecutionCompleted"
	// HistoryClientVerifyChildExecutionCompletionRecordedOperation tracks RPC calls to history service
	HistoryClientVerifyChildExecutionCompletionRecordedOperation = "HistoryClientVerifyChildExecutionCompletionRecorded"
	// HistoryClientReplicateEventsV2Operation tracks RPC calls to history service
	HistoryClientReplicateEventsV2Operation = "HistoryClientReplicateEventsV2"
	// HistoryClientSyncShardStatusOperation tracks RPC calls to history service
	HistoryClientSyncShardStatusOperation = "HistoryClientSyncShardStatus"
	// HistoryClientSyncActivityOperation tracks RPC calls to history service
	HistoryClientSyncActivityOperation = "HistoryClientSyncActivity"
	// HistoryClientGetReplicationTasksOperation tracks RPC calls to history service
	HistoryClientGetReplicationTasksOperation = "HistoryClientGetReplicationTasks"
	// HistoryClientGetDLQReplicationTasksOperation tracks RPC calls to history service
	HistoryClientGetDLQReplicationTasksOperation = "HistoryClientGetDLQReplicationTasks"
	// HistoryClientQueryWorkflowOperation tracks RPC calls to history service
	HistoryClientQueryWorkflowOperation = "HistoryClientQueryWorkflow"
	// HistoryClientReapplyEventsOperation tracks RPC calls to history service
	HistoryClientReapplyEventsOperation = "HistoryClientReapplyEvents"
	// HistoryClientGetDLQMessagesOperation tracks RPC calls to history service
	HistoryClientGetDLQMessagesOperation = "HistoryClientGetDLQMessages"
	// HistoryClientPurgeDLQMessagesOperation tracks RPC calls to history service
	HistoryClientPurgeDLQMessagesOperation = "HistoryClientPurgeDLQMessages"
	// HistoryClientMergeDLQMessagesOperation tracks RPC calls to history service
	HistoryClientMergeDLQMessagesOperation = "HistoryClientMergeDLQMessages"
	// HistoryClientRefreshWorkflowTasksOperation tracks RPC calls to history service
	HistoryClientRefreshWorkflowTasksOperation = "HistoryClientRefreshWorkflowTasks"
	// HistoryClientGenerateLastHistoryReplicationTasksOperation tracks RPC calls to history service
	HistoryClientGenerateLastHistoryReplicationTasksOperation = "HistoryClientGenerateLastHistoryReplicationTasks"
	// HistoryClientGetReplicationStatusOperation tracks RPC calls to history service
	HistoryClientGetReplicationStatusOperation = "HistoryClientGetReplicationStatus"
	// HistoryClientDeleteWorkflowVisibilityRecordOperation tracks RPC calls to history service
	HistoryClientDeleteWorkflowVisibilityRecordOperation = "HistoryClientDeleteWorkflowVisibilityRecord"
	// HistoryClientCloseShardOperation tracks RPC calls to history service
	HistoryClientCloseShardOperation = "HistoryClientCloseShard"
	// HistoryClientDescribeMutableStateOperation tracks RPC calls to history service
	HistoryClientDescribeMutableStateOperation = "HistoryClientDescribeMutableState"
	// HistoryClientGetDLQReplicationMessagesOperation tracks RPC calls to history service
	HistoryClientGetDLQReplicationMessagesOperation = "HistoryClientGetDLQReplicationMessages"
	// HistoryClientGetShardOperation tracks RPC calls to history service
	HistoryClientGetShardOperation = "HistoryClientGetShard"
	// HistoryClientRebuildMutableStateOperation tracks RPC calls to history service
	HistoryClientRebuildMutableStateOperation = "HistoryClientRebuildMutableState"
	// HistoryClientRemoveTaskOperation tracks RPC calls to history service
	HistoryClientRemoveTaskOperation = "HistoryClientRemoveTask"
	// HistoryClientDescribeHistoryHostOperation tracks RPC calls to history service
	HistoryClientDescribeHistoryHostOperation = "HistoryClientDescribeHistoryHost"
	// HistoryClientGetReplicationMessagesOperation tracks RPC calls to history service
	HistoryClientGetReplicationMessagesOperation = "HistoryClientGetReplicationMessages"

	// Matching Client

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
)
