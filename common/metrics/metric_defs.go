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

package metrics

// Service names for all services that emit metrics.
const (
	Common ServiceIdx = iota
	Frontend
	History
	Matching
	Worker
	Server
	UnitTestService
)

// Common tags for all services
const (
	OperationTagName           = "operation"
	ServiceRoleTagName         = "service_role"
	CacheTypeTagName           = "cache_type"
	FailureTagName             = "failure"
	TaskCategoryTagName        = "task_category"
	TaskTypeTagName            = "task_type"
	TaskPriorityTagName        = "task_priority"
	QueueReaderIDTagName       = "queue_reader_id"
	QueueAlertTypeTagName      = "queue_alert_type"
	QueueTypeTagName           = "queue_type"
	visibilityTypeTagName      = "visibility_type"
	ErrorTypeTagName           = "error_type"
	httpStatusTagName          = "http_status"
	resourceExhaustedTag       = "resource_exhausted_cause"
	standardVisibilityTagValue = "standard_visibility"
	advancedVisibilityTagValue = "advanced_visibility"
)

// This package should hold all the metrics and tags for temporal
const (
	HistoryRoleTagValue       = "history"
	MatchingRoleTagValue      = "matching"
	FrontendRoleTagValue      = "frontend"
	AdminRoleTagValue         = "admin"
	DCRedirectionRoleTagValue = "dc_redirection"
	BlobstoreRoleTagValue     = "blobstore"

	MutableStateCacheTypeTagValue = "mutablestate"
	EventsCacheTypeTagValue       = "events"
)

// Common service base metrics
const (
	RestartCount         = "restarts"
	NumGoRoutinesGauge   = "num_goroutines"
	GoMaxProcsGauge      = "gomaxprocs"
	MemoryAllocatedGauge = "memory_allocated"
	MemoryHeapGauge      = "memory_heap"
	MemoryHeapIdleGauge  = "memory_heapidle"
	MemoryHeapInuseGauge = "memory_heapinuse"
	MemoryStackGauge     = "memory_stack"
	NumGCCounter         = "memory_num_gc"
	GcPauseMsTimer       = "memory_gc_pause_ms"
)

// Admin Client Operations
const (
	// AdminClientAddSearchAttributesScope tracks RPC calls to admin service
	AdminClientAddSearchAttributesScope = "AdminClientAddSearchAttributes"
	// AdminClientRemoveSearchAttributesScope tracks RPC calls to admin service
	AdminClientRemoveSearchAttributesScope = "AdminClientRemoveSearchAttributes"
	// AdminClientGetSearchAttributesScope tracks RPC calls to admin service
	AdminClientGetSearchAttributesScope = "AdminClientGetSearchAttributes"
	// AdminClientCloseShardScope tracks RPC calls to admin service
	AdminClientCloseShardScope = "AdminClientCloseShard"
	// AdminClientGetShardScope tracks RPC calls to admin service
	AdminClientGetShardScope = "AdminClientGetShard"
	// AdminClientListHistoryTasksScope tracks RPC calls to admin service
	AdminClientListHistoryTasksScope = "AdminClientListHistoryTasks"
	// AdminClientRemoveTaskScope tracks RPC calls to admin service
	AdminClientRemoveTaskScope = "AdminClientRemoveTask"
	// AdminClientDescribeHistoryHostScope tracks RPC calls to admin service
	AdminClientDescribeHistoryHostScope = "AdminClientDescribeHistoryHost"
	// AdminClientRebuildMutableStateScope tracks RPC calls to admin service
	AdminClientRebuildMutableStateScope = "AdminClientRebuildMutableState"
	// AdminClientDescribeMutableStateScope tracks RPC calls to admin service
	AdminClientDescribeMutableStateScope = "AdminClientDescribeMutableState"
	// AdminClientGetWorkflowExecutionRawHistoryV2Scope tracks RPC calls to admin service
	AdminClientGetWorkflowExecutionRawHistoryV2Scope = "AdminClientGetWorkflowExecutionRawHistoryV2"
	// AdminClientDescribeClusterScope tracks RPC calls to admin service
	AdminClientDescribeClusterScope = "AdminClientDescribeCluster"
	// AdminClientListClustersScope tracks RPC calls to admin service
	AdminClientListClustersScope = "AdminClientListClusters"
	// AdminClientListClusterMembersScope tracks RPC calls to admin service
	AdminClientListClusterMembersScope = "AdminClientListClusterMembers"
	// AdminClientAddOrUpdateRemoteClusterScope tracks RPC calls to admin service
	AdminClientAddOrUpdateRemoteClusterScope = "AdminClientAddOrUpdateRemoteCluster"
	// AdminClientRemoveRemoteClusterScope tracks RPC calls to admin service
	AdminClientRemoveRemoteClusterScope = "AdminClientRemoveRemoteCluster"
	// AdminClientGetReplicationMessagesScope tracks RPC calls to admin service
	AdminClientGetReplicationMessagesScope = "AdminClientGetReplicationMessages"
	// AdminClientGetNamespaceReplicationMessagesScope tracks RPC calls to admin service
	AdminClientGetNamespaceReplicationMessagesScope = "AdminClientGetNamespaceReplicationMessages"
	// AdminClientGetDLQReplicationMessagesScope tracks RPC calls to admin service
	AdminClientGetDLQReplicationMessagesScope = "AdminClientGetDLQReplicationMessages"
	// AdminClientReapplyEventsScope tracks RPC calls to admin service
	AdminClientReapplyEventsScope = "AdminClientReapplyEvents"
	// AdminClientGetDLQMessagesScope tracks RPC calls to admin service
	AdminClientGetDLQMessagesScope = "AdminClientGetDLQMessages"
	// AdminClientPurgeDLQMessagesScope tracks RPC calls to admin service
	AdminClientPurgeDLQMessagesScope = "AdminClientPurgeDLQMessages"
	// AdminClientMergeDLQMessagesScope tracks RPC calls to admin service
	AdminClientMergeDLQMessagesScope = "AdminClientMergeDLQMessages"
	// AdminClientRefreshWorkflowTasksScope tracks RPC calls to admin service
	AdminClientRefreshWorkflowTasksScope = "AdminClientRefreshWorkflowTasks"
	// AdminClientResendReplicationTasksScope tracks RPC calls to admin service
	AdminClientResendReplicationTasksScope = "AdminClientResendReplicationTasks"
	// AdminClientGetTaskQueueTasksScope tracks RPC calls to admin service
	AdminClientGetTaskQueueTasksScope = "AdminClientGetTaskQueueTasks"
	// AdminClientDeleteWorkflowExecutionScope tracks RPC calls to admin service
	AdminClientDeleteWorkflowExecutionScope = "AdminClientDeleteWorkflowExecution"

	// AdminDescribeHistoryHostScope is the metric scope for admin.AdminDescribeHistoryHost
	AdminDescribeHistoryHostScope = "AdminDescribeHistoryHost"
	// AdminAddSearchAttributesScope is the metric scope for admin.AdminAddSearchAttributes
	AdminAddSearchAttributesScope = "AdminAddSearchAttributes"
	// AdminRemoveSearchAttributesScope is the metric scope for admin.AdminRemoveSearchAttributes
	AdminRemoveSearchAttributesScope = "AdminRemoveSearchAttributes"
	// AdminGetSearchAttributesScope is the metric scope for admin.AdminGetSearchAttributes
	AdminGetSearchAttributesScope = "AdminGetSearchAttributes"
	// AdminRebuildMutableStateScope is the metric scope for admin.AdminRebuildMutableState
	AdminRebuildMutableStateScope = "AdminRebuildMutableState"
	// AdminDescribeMutableStateScope is the metric scope for admin.AdminDescribeMutableState
	AdminDescribeMutableStateScope = "AdminDescribeMutableState"
	// AdminGetWorkflowExecutionRawHistoryV2Scope is the metric scope for admin.GetWorkflowExecutionRawHistoryScope
	AdminGetWorkflowExecutionRawHistoryV2Scope = "AdminGetWorkflowExecutionRawHistoryV2"
	// AdminGetReplicationMessagesScope is the metric scope for admin.GetReplicationMessages
	AdminGetReplicationMessagesScope = "AdminGetReplicationMessages"
	// AdminGetNamespaceReplicationMessagesScope is the metric scope for admin.GetNamespaceReplicationMessages
	AdminGetNamespaceReplicationMessagesScope = "AdminGetNamespaceReplicationMessages"
	// AdminGetDLQReplicationMessagesScope is the metric scope for admin.GetDLQReplicationMessages
	AdminGetDLQReplicationMessagesScope = "AdminGetDLQReplicationMessages"
	// AdminReapplyEventsScope is the metric scope for admin.ReapplyEvents
	AdminReapplyEventsScope = "AdminReapplyEvents"
	// AdminRefreshWorkflowTasksScope is the metric scope for admin.RefreshWorkflowTasks
	AdminRefreshWorkflowTasksScope = "AdminRefreshWorkflowTasks"
	// AdminResendReplicationTasksScope is the metric scope for admin.ResendReplicationTasks
	AdminResendReplicationTasksScope = "AdminResendReplicationTasks"
	// AdminGetTaskQueueTasksScope is the metric scope for admin.GetTaskQueueTasks
	AdminGetTaskQueueTasksScope = "AdminGetTaskQueueTasks"
	// AdminRemoveTaskScope is the metric scope for admin.AdminRemoveTask
	AdminRemoveTaskScope = "AdminRemoveTask"
	// AdminCloseShardScope is the metric scope for admin.AdminCloseShard
	AdminCloseShardScope = "AdminCloseShard"
	// AdminGetShardScope is the metric scope for admin.AdminGetShard
	AdminGetShardScope = "AdminGetShard"
	// AdminListHistoryTasksScope is the metric scope for admin.ListHistoryTasks
	AdminListHistoryTasksScope = "AdminListHistoryTasks"
	// AdminGetDLQMessagesScope is the metric scope for admin.AdminGetDLQMessages
	AdminGetDLQMessagesScope = "AdminGetDLQMessages"
	// AdminPurgeDLQMessagesScope is the metric scope for admin.AdminPurgeDLQMessages
	AdminPurgeDLQMessagesScope = "AdminPurgeDLQMessages"
	// AdminMergeDLQMessagesScope is the metric scope for admin.AdminMergeDLQMessages
	AdminMergeDLQMessagesScope = "AdminMergeDLQMessages"
	// AdminListClusterMembersScope is the metric scope for admin.AdminListClusterMembers
	AdminListClusterMembersScope = "AdminListClusterMembers"
	// AdminDescribeClusterScope is the metric scope for admin.AdminDescribeCluster
	AdminDescribeClusterScope = "AdminDescribeCluster"
	// AdminListClustersScope is the metric scope for admin.AdminListClusters
	AdminListClustersScope = "AdminListClusters"
	// AdminAddOrUpdateRemoteClusterScope is the metric scope for admin.AdminAddOrUpdateRemoteCluster
	AdminAddOrUpdateRemoteClusterScope = "AdminAddOrUpdateRemoteCluster"
	// AdminRemoveRemoteClusterScope is the metric scope for admin.AdminRemoveRemoteCluster
	AdminRemoveRemoteClusterScope = "AdminRemoveRemoteCluster"
	// AdminDeleteWorkflowExecutionScope is the metric scope for admin.AdminDeleteWorkflowExecution
	AdminDeleteWorkflowExecutionScope = "AdminDeleteWorkflowExecution"

	// DCRedirectionDeleteWorkflowExecutionScope tracks RPC calls for dc redirection
	DCRedirectionDeleteWorkflowExecutionScope = "DCRedirectionDeleteWorkflowExecution"
	// DCRedirectionDeprecateNamespaceScope tracks RPC calls for dc redirection
	DCRedirectionDeprecateNamespaceScope = "DCRedirectionDeprecateNamespace"
	// DCRedirectionDescribeNamespaceScope tracks RPC calls for dc redirection
	DCRedirectionDescribeNamespaceScope = "DCRedirectionDescribeNamespace"
	// DCRedirectionDescribeTaskQueueScope tracks RPC calls for dc redirection
	DCRedirectionDescribeTaskQueueScope = "DCRedirectionDescribeTaskQueue"
	// DCRedirectionDescribeWorkflowExecutionScope tracks RPC calls for dc redirection
	DCRedirectionDescribeWorkflowExecutionScope = "DCRedirectionDescribeWorkflowExecution"
	// DCRedirectionGetWorkflowExecutionHistoryScope tracks RPC calls for dc redirection
	DCRedirectionGetWorkflowExecutionHistoryScope = "DCRedirectionGetWorkflowExecutionHistory"
	// DCRedirectionGetWorkflowExecutionHistoryReverseScope tracks RPC calls for dc redirection
	DCRedirectionGetWorkflowExecutionHistoryReverseScope = "DCRedirectionGetWorkflowExecutionHistoryReverse"
	// DCRedirectionGetWorkflowExecutionRawHistoryScope tracks RPC calls for dc redirection
	DCRedirectionGetWorkflowExecutionRawHistoryScope = "DCRedirectionGetWorkflowExecutionRawHistory"
	// DCRedirectionPollForWorkflowExecutionRawHistoryScope tracks RPC calls for dc redirection
	DCRedirectionPollForWorkflowExecutionRawHistoryScope = "DCRedirectionPollForWorkflowExecutionRawHistory"
	// DCRedirectionListArchivedWorkflowExecutionsScope tracks RPC calls for dc redirection
	DCRedirectionListArchivedWorkflowExecutionsScope = "DCRedirectionListArchivedWorkflowExecutions"
	// DCRedirectionListClosedWorkflowExecutionsScope tracks RPC calls for dc redirection
	DCRedirectionListClosedWorkflowExecutionsScope = "DCRedirectionListClosedWorkflowExecutions"
	// DCRedirectionListNamespacesScope tracks RPC calls for dc redirection
	DCRedirectionListNamespacesScope = "DCRedirectionListNamespaces"
	// DCRedirectionListOpenWorkflowExecutionsScope tracks RPC calls for dc redirection
	DCRedirectionListOpenWorkflowExecutionsScope = "DCRedirectionListOpenWorkflowExecutions"
	// DCRedirectionListWorkflowExecutionsScope tracks RPC calls for dc redirection
	DCRedirectionListWorkflowExecutionsScope = "DCRedirectionListWorkflowExecutions"
	// DCRedirectionScanWorkflowExecutionsScope tracks RPC calls for dc redirection
	DCRedirectionScanWorkflowExecutionsScope = "DCRedirectionScanWorkflowExecutions"
	// DCRedirectionCountWorkflowExecutionsScope tracks RPC calls for dc redirection
	DCRedirectionCountWorkflowExecutionsScope = "DCRedirectionCountWorkflowExecutions"
	// DCRedirectionGetSearchAttributesScope tracks RPC calls for dc redirection
	DCRedirectionGetSearchAttributesScope = "DCRedirectionGetSearchAttributes"
	// DCRedirectionPollActivityTaskQueueScope tracks RPC calls for dc redirection
	DCRedirectionPollActivityTaskQueueScope = "DCRedirectionPollActivityTaskQueue"
	// DCRedirectionPollWorkflowTaskQueueScope tracks RPC calls for dc redirection
	DCRedirectionPollWorkflowTaskQueueScope = "DCRedirectionPollWorkflowTaskQueue"
	// DCRedirectionQueryWorkflowScope tracks RPC calls for dc redirection
	DCRedirectionQueryWorkflowScope = "DCRedirectionQueryWorkflow"
	// DCRedirectionRecordActivityTaskHeartbeatScope tracks RPC calls for dc redirection
	DCRedirectionRecordActivityTaskHeartbeatScope = "DCRedirectionRecordActivityTaskHeartbeat"
	// DCRedirectionRecordActivityTaskHeartbeatByIdScope tracks RPC calls for dc redirection
	DCRedirectionRecordActivityTaskHeartbeatByIdScope = "DCRedirectionRecordActivityTaskHeartbeatById"
	// DCRedirectionRegisterNamespaceScope tracks RPC calls for dc redirection
	DCRedirectionRegisterNamespaceScope = "DCRedirectionRegisterNamespace"
	// DCRedirectionRequestCancelWorkflowExecutionScope tracks RPC calls for dc redirection
	DCRedirectionRequestCancelWorkflowExecutionScope = "DCRedirectionRequestCancelWorkflowExecution"
	// DCRedirectionResetStickyTaskQueueScope tracks RPC calls for dc redirection
	DCRedirectionResetStickyTaskQueueScope = "DCRedirectionResetStickyTaskQueue"
	// DCRedirectionResetWorkflowExecutionScope tracks RPC calls for dc redirection
	DCRedirectionResetWorkflowExecutionScope = "DCRedirectionResetWorkflowExecution"
	// DCRedirectionRespondActivityTaskCanceledScope tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskCanceledScope = "DCRedirectionRespondActivityTaskCanceled"
	// DCRedirectionRespondActivityTaskCanceledByIdScope tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskCanceledByIdScope = "DCRedirectionRespondActivityTaskCanceledById"
	// DCRedirectionRespondActivityTaskCompletedScope tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskCompletedScope = "DCRedirectionRespondActivityTaskCompleted"
	// DCRedirectionRespondActivityTaskCompletedByIdScope tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskCompletedByIdScope = "DCRedirectionRespondActivityTaskCompletedById"
	// DCRedirectionRespondActivityTaskFailedScope tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskFailedScope = "DCRedirectionRespondActivityTaskFailed"
	// DCRedirectionRespondActivityTaskFailedByIdScope tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskFailedByIdScope = "DCRedirectionRespondActivityTaskFailedById"
	// DCRedirectionRespondWorkflowTaskCompletedScope tracks RPC calls for dc redirection
	DCRedirectionRespondWorkflowTaskCompletedScope = "DCRedirectionRespondWorkflowTaskCompleted"
	// DCRedirectionRespondWorkflowTaskFailedScope tracks RPC calls for dc redirection
	DCRedirectionRespondWorkflowTaskFailedScope = "DCRedirectionRespondWorkflowTaskFailed"
	// DCRedirectionRespondQueryTaskCompletedScope tracks RPC calls for dc redirection
	DCRedirectionRespondQueryTaskCompletedScope = "DCRedirectionRespondQueryTaskCompleted"
	// DCRedirectionSignalWithStartWorkflowExecutionScope tracks RPC calls for dc redirection
	DCRedirectionSignalWithStartWorkflowExecutionScope = "DCRedirectionSignalWithStartWorkflowExecution"
	// DCRedirectionSignalWorkflowExecutionScope tracks RPC calls for dc redirection
	DCRedirectionSignalWorkflowExecutionScope = "DCRedirectionSignalWorkflowExecution"
	// DCRedirectionStartWorkflowExecutionScope tracks RPC calls for dc redirection
	DCRedirectionStartWorkflowExecutionScope = "DCRedirectionStartWorkflowExecution"
	// DCRedirectionTerminateWorkflowExecutionScope tracks RPC calls for dc redirection
	DCRedirectionTerminateWorkflowExecutionScope = "DCRedirectionTerminateWorkflowExecution"
	// DCRedirectionUpdateNamespaceScope tracks RPC calls for dc redirection
	DCRedirectionUpdateNamespaceScope = "DCRedirectionUpdateNamespace"
	// DCRedirectionListTaskQueuePartitionsScope tracks RPC calls for dc redirection
	DCRedirectionListTaskQueuePartitionsScope = "DCRedirectionListTaskQueuePartitions"
	// DCRedirectionCreateScheduleScope tracks RPC calls for dc redirection
	DCRedirectionCreateScheduleScope = "DCRedirectionCreateSchedule"
	// DCRedirectionDescribeScheduleScope tracks RPC calls for dc redirection
	DCRedirectionDescribeScheduleScope = "DCRedirectionDescribeSchedule"
	// DCRedirectionUpdateScheduleScope tracks RPC calls for dc redirection
	DCRedirectionUpdateScheduleScope = "DCRedirectionUpdateSchedule"
	// DCRedirectionPatchScheduleScope tracks RPC calls for dc redirection
	DCRedirectionPatchScheduleScope = "DCRedirectionPatchSchedule"
	// DCRedirectionListScheduleMatchingTimesScope tracks RPC calls for dc redirection
	DCRedirectionListScheduleMatchingTimesScope = "DCRedirectionListScheduleMatchingTimes"
	// DCRedirectionDeleteScheduleScope tracks RPC calls for dc redirection
	DCRedirectionDeleteScheduleScope = "DCRedirectionDeleteSchedule"
	// DCRedirectionListSchedulesScope tracks RPC calls for dc redirection
	DCRedirectionListSchedulesScope = "DCRedirectionListSchedules"
	// DCRedirectionUpdateWorkerBuildIdOrderingScope tracks RPC calls for dc redirection
	DCRedirectionUpdateWorkerBuildIdOrderingScope = "DCRedirectionUpdateWorkerBuildIdOrdering"
	// DCRedirectionGetWorkerBuildIdOrderingScope tracks RPC calls for dc redirection
	DCRedirectionGetWorkerBuildIdOrderingScope = "DCRedirectionGetWorkerBuildIdOrdering"
	// DCRedirectionUpdateWorkflowScope tracks RPC calls for dc redirection
	DCRedirectionUpdateWorkflowScope = "DCRedirectionUpdateWorkflow"
	// DCRedirectionDescribeBatchOperationScope tracks RPC calls for dc redirection
	DCRedirectionDescribeBatchOperationScope = "DCRedirectionDescribeBatchOperation"
	// DCRedirectionListBatchOperationsScope tracks RPC calls for dc redirection
	DCRedirectionListBatchOperationsScope = "DCRedirectionListBatchOperations"
	// DCRedirectionStartBatchOperationScope tracks RPC calls for dc redirection
	DCRedirectionStartBatchOperationScope = "DCRedirectionStartBatchOperation"
	// DCRedirectionStopBatchOperationScope tracks RPC calls for dc redirection
	DCRedirectionStopBatchOperationScope = "DCRedirectionStopBatchOperation"

	// OperatorAddSearchAttributesScope is the metric scope for operator.AddSearchAttributes
	OperatorAddSearchAttributesScope
	// OperatorRemoveSearchAttributesScope is the metric scope for operator.RemoveSearchAttributes
	OperatorRemoveSearchAttributesScope = "OperatorRemoveSearchAttributes"
	// OperatorListSearchAttributesScope is the metric scope for operator.ListSearchAttributes
	OperatorListSearchAttributesScope = "OperatorListSearchAttributes"
	OperatorDeleteNamespaceScope      = "OperatorDeleteNamespace"
	// OperatorAddOrUpdateRemoteClusterScope is the metric scope for operator.AddOrUpdateRemoteCluster
	OperatorAddOrUpdateRemoteClusterScope = "OperatorAddOrUpdateRemoteCluster"
	// OperatorRemoveRemoteClusterScope is the metric scope for operator.RemoveRemoteCluster
	OperatorRemoveRemoteClusterScope = "OperatorRemoveRemoteCluster"
	// OperatorListClustersScope is the metric scope for operator.OperatorListClusters
	OperatorListClustersScope = "OperatorListClusters"
	// OperatorDeleteWorkflowExecutionScope is the metric scope for operator.DeleteWorkflowExecution
	OperatorDeleteWorkflowExecutionScope = "OperatorDeleteWorkflowExecution"
)

// Frontend Client Operations
const (
	// FrontendClientDeleteWorkflowExecutionScope tracks RPC calls to frontend service
	FrontendClientDeleteWorkflowExecutionScope = "FrontendClientDeleteWorkflowExecution"
	// FrontendClientDeprecateNamespaceScope tracks RPC calls to frontend service
	FrontendClientDeprecateNamespaceScope = "FrontendClientDeprecateNamespace"
	// FrontendClientDescribeNamespaceScope tracks RPC calls to frontend service
	FrontendClientDescribeNamespaceScope = "FrontendClientDescribeNamespace"
	// FrontendClientDescribeTaskQueueScope tracks RPC calls to frontend service
	FrontendClientDescribeTaskQueueScope = "FrontendClientDescribeTaskQueue"
	// FrontendClientDescribeWorkflowExecutionScope tracks RPC calls to frontend service
	FrontendClientDescribeWorkflowExecutionScope = "FrontendClientDescribeWorkflowExecution"
	// FrontendClientGetWorkflowExecutionHistoryScope tracks RPC calls to frontend service
	FrontendClientGetWorkflowExecutionHistoryScope = "FrontendClientGetWorkflowExecutionHistory"
	// FrontendClientGetWorkflowExecutionHistoryReverseScope tracks RPC calls to frontend service
	FrontendClientGetWorkflowExecutionHistoryReverseScope = "FrontendClientGetWorkflowExecutionHistoryReverse"
	// FrontendClientGetWorkflowExecutionRawHistoryScope tracks RPC calls to frontend service
	FrontendClientGetWorkflowExecutionRawHistoryScope = "FrontendClientGetWorkflowExecutionRawHistory"
	// FrontendClientPollForWorkflowExecutionRawHistoryScope tracks RPC calls to frontend service
	FrontendClientPollForWorkflowExecutionRawHistoryScope = "FrontendClientPollForWorkflowExecutionRawHistory"
	// FrontendClientListArchivedWorkflowExecutionsScope tracks RPC calls to frontend service
	FrontendClientListArchivedWorkflowExecutionsScope = "FrontendClientListArchivedWorkflowExecutions"
	// FrontendClientListClosedWorkflowExecutionsScope tracks RPC calls to frontend service
	FrontendClientListClosedWorkflowExecutionsScope = "FrontendClientListClosedWorkflowExecutions"
	// FrontendClientListNamespacesScope tracks RPC calls to frontend service
	FrontendClientListNamespacesScope = "FrontendClientListNamespaces"
	// FrontendClientListOpenWorkflowExecutionsScope tracks RPC calls to frontend service
	FrontendClientListOpenWorkflowExecutionsScope = "FrontendClientListOpenWorkflowExecutions"
	// FrontendClientPollActivityTaskQueueScope tracks RPC calls to frontend service
	FrontendClientPollActivityTaskQueueScope = "FrontendClientPollActivityTaskQueue"
	// FrontendClientPollWorkflowTaskQueueScope tracks RPC calls to frontend service
	FrontendClientPollWorkflowTaskQueueScope = "FrontendClientPollWorkflowTaskQueue"
	// FrontendClientQueryWorkflowScope tracks RPC calls to frontend service
	FrontendClientQueryWorkflowScope = "FrontendClientQueryWorkflow"
	// FrontendClientRecordActivityTaskHeartbeatScope tracks RPC calls to frontend service
	FrontendClientRecordActivityTaskHeartbeatScope = "FrontendClientRecordActivityTaskHeartbeat"
	// FrontendClientRecordActivityTaskHeartbeatByIdScope tracks RPC calls to frontend service
	FrontendClientRecordActivityTaskHeartbeatByIdScope = "FrontendClientRecordActivityTaskHeartbeatById"
	// FrontendClientRegisterNamespaceScope tracks RPC calls to frontend service
	FrontendClientRegisterNamespaceScope = "FrontendClientRegisterNamespace"
	// FrontendClientRequestCancelWorkflowExecutionScope tracks RPC calls to frontend service
	FrontendClientRequestCancelWorkflowExecutionScope = "FrontendClientRequestCancelWorkflowExecution"
	// FrontendClientResetStickyTaskQueueScope tracks RPC calls to frontend service
	FrontendClientResetStickyTaskQueueScope = "FrontendClientResetStickyTaskQueue"
	// FrontendClientResetWorkflowExecutionScope tracks RPC calls to frontend service
	FrontendClientResetWorkflowExecutionScope = "FrontendClientResetWorkflowExecution"
	// FrontendClientRespondActivityTaskCanceledScope tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCanceledScope = "FrontendClientRespondActivityTaskCanceled"
	// FrontendClientRespondActivityTaskCanceledByIdScope tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCanceledByIdScope = "FrontendClientRespondActivityTaskCanceledById"
	// FrontendClientRespondActivityTaskCompletedScope tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCompletedScope = "FrontendClientRespondActivityTaskCompleted"
	// FrontendClientRespondActivityTaskCompletedByIdScope tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCompletedByIdScope = "FrontendClientRespondActivityTaskCompletedById"
	// FrontendClientRespondActivityTaskFailedScope tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskFailedScope = "FrontendClientRespondActivityTaskFailed"
	// FrontendClientRespondActivityTaskFailedByIdScope tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskFailedByIdScope = "FrontendClientRespondActivityTaskFailedById"
	// FrontendClientRespondWorkflowTaskCompletedScope tracks RPC calls to frontend service
	FrontendClientRespondWorkflowTaskCompletedScope = "FrontendClientRespondWorkflowTaskCompleted"
	// FrontendClientRespondWorkflowTaskFailedScope tracks RPC calls to frontend service
	FrontendClientRespondWorkflowTaskFailedScope = "FrontendClientRespondWorkflowTaskFailed"
	// FrontendClientRespondQueryTaskCompletedScope tracks RPC calls to frontend service
	FrontendClientRespondQueryTaskCompletedScope = "FrontendClientRespondQueryTaskCompleted"
	// FrontendClientSignalWithStartWorkflowExecutionScope tracks RPC calls to frontend service
	FrontendClientSignalWithStartWorkflowExecutionScope = "FrontendClientSignalWithStartWorkflowExecution"
	// FrontendClientSignalWorkflowExecutionScope tracks RPC calls to frontend service
	FrontendClientSignalWorkflowExecutionScope = "FrontendClientSignalWorkflowExecution"
	// FrontendClientStartWorkflowExecutionScope tracks RPC calls to frontend service
	FrontendClientStartWorkflowExecutionScope = "FrontendClientStartWorkflowExecution"
	// FrontendClientTerminateWorkflowExecutionScope tracks RPC calls to frontend service
	FrontendClientTerminateWorkflowExecutionScope = "FrontendClientTerminateWorkflowExecution"
	// FrontendClientUpdateNamespaceScope tracks RPC calls to frontend service
	FrontendClientUpdateNamespaceScope = "FrontendClientUpdateNamespace"
	// FrontendClientListWorkflowExecutionsScope tracks RPC calls to frontend service
	FrontendClientListWorkflowExecutionsScope = "FrontendClientListWorkflowExecutions"
	// FrontendClientScanWorkflowExecutionsScope tracks RPC calls to frontend service
	FrontendClientScanWorkflowExecutionsScope = "FrontendClientScanWorkflowExecutions"
	// FrontendClientCountWorkflowExecutionsScope tracks RPC calls to frontend service
	FrontendClientCountWorkflowExecutionsScope = "FrontendClientCountWorkflowExecutions"
	// FrontendClientGetSearchAttributesScope tracks RPC calls to frontend service
	FrontendClientGetSearchAttributesScope = "FrontendClientGetSearchAttributes"
	// FrontendClientGetClusterInfoScope tracks RPC calls to frontend
	FrontendClientGetClusterInfoScope = "FrontendClientGetClusterInfo"
	// FrontendClientGetSystemInfoScope tracks RPC calls to frontend
	FrontendClientGetSystemInfoScope = "FrontendClientGetSystemInfo"
	// FrontendClientListTaskQueuePartitionsScope tracks RPC calls to frontend service
	FrontendClientListTaskQueuePartitionsScope = "FrontendClientListTaskQueuePartitions"
	// FrontendClientCreateScheduleScope tracks RPC calls to frontend service
	FrontendClientCreateScheduleScope = "FrontendClientCreateSchedule"
	// FrontendClientDescribeScheduleScope tracks RPC calls to frontend service
	FrontendClientDescribeScheduleScope = "FrontendClientDescribeSchedule"
	// FrontendClientUpdateScheduleScope tracks RPC calls to frontend service
	FrontendClientUpdateScheduleScope = "FrontendClientUpdateSchedule"
	// FrontendClientPatchScheduleScope tracks RPC calls to frontend service
	FrontendClientPatchScheduleScope = "FrontendClientPatchSchedule"
	// FrontendClientListScheduleMatchingTimesScope tracks RPC calls to frontend service
	FrontendClientListScheduleMatchingTimesScope = "FrontendClientListScheduleMatchingTimes"
	// FrontendClientDeleteScheduleScope tracks RPC calls to frontend service
	FrontendClientDeleteScheduleScope = "FrontendClientDeleteSchedule"
	// FrontendClientListSchedulesScope tracks RPC calls to frontend service
	FrontendClientListSchedulesScope = "FrontendClientListSchedules"
	// FrontendClientUpdateWorkerBuildIdOrderingScope tracks RPC calls to frontend service
	FrontendClientUpdateWorkerBuildIdOrderingScope = "FrontendClientUpdateWorkerBuildIdOrdering"
	// FrontendClientUpdateWorkflowScope tracks RPC calls to frontend service
	FrontendClientUpdateWorkflowScope = "FrontendClientUpdateWorkflow"
	// FrontendClientGetWorkerBuildIdOrderingScope tracks RPC calls to frontend service
	FrontendClientGetWorkerBuildIdOrderingScope = "FrontendClientGetWorkerBuildIdOrdering"
	// FrontendClientDescribeBatchOperationScope tracks RPC calls to frontend service
	FrontendClientDescribeBatchOperationScope = "FrontendClientDescribeBatchOperation"
	// FrontendClientListBatchOperationsScope tracks RPC calls to frontend service
	FrontendClientListBatchOperationsScope = "FrontendClientListBatchOperations"
	// FrontendClientStartBatchOperationScope tracks RPC calls to frontend service
	FrontendClientStartBatchOperationScope = "FrontendClientStartBatchOperation"
	// FrontendClientStopBatchOperationScope tracks RPC calls to frontend service
	FrontendClientStopBatchOperationScope = "FrontendClientStopBatchOperation"
)

// History Client Operations
const (
	// HistoryClientStartWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientStartWorkflowExecutionScope = "HistoryClientStartWorkflowExecution"
	// HistoryClientRecordActivityTaskHeartbeatScope tracks RPC calls to history service
	HistoryClientRecordActivityTaskHeartbeatScope = "HistoryClientRecordActivityTaskHeartbeat"
	// HistoryClientRespondWorkflowTaskCompletedScope tracks RPC calls to history service
	HistoryClientRespondWorkflowTaskCompletedScope = "HistoryClientRespondWorkflowTaskCompleted"
	// HistoryClientRespondWorkflowTaskFailedScope tracks RPC calls to history service
	HistoryClientRespondWorkflowTaskFailedScope = "HistoryClientRespondWorkflowTaskFailed"
	// HistoryClientRespondActivityTaskCompletedScope tracks RPC calls to history service
	HistoryClientRespondActivityTaskCompletedScope = "HistoryClientRespondActivityTaskCompleted"
	// HistoryClientRespondActivityTaskFailedScope tracks RPC calls to history service
	HistoryClientRespondActivityTaskFailedScope = "HistoryClientRespondActivityTaskFailed"
	// HistoryClientRespondActivityTaskCanceledScope tracks RPC calls to history service
	HistoryClientRespondActivityTaskCanceledScope = "HistoryClientRespondActivityTaskCanceled"
	// HistoryClientGetMutableStateScope tracks RPC calls to history service
	HistoryClientGetMutableStateScope = "HistoryClientGetMutableState"
	// HistoryClientPollMutableStateScope tracks RPC calls to history service
	HistoryClientPollMutableStateScope = "HistoryClientPollMutableState"
	// HistoryClientResetStickyTaskQueueScope tracks RPC calls to history service
	HistoryClientResetStickyTaskQueueScope = "HistoryClientResetStickyTaskQueue"
	// HistoryClientDescribeWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientDescribeWorkflowExecutionScope = "HistoryClientDescribeWorkflowExecution"
	// HistoryClientRecordWorkflowTaskStartedScope tracks RPC calls to history service
	HistoryClientRecordWorkflowTaskStartedScope = "HistoryClientRecordWorkflowTaskStarted"
	// HistoryClientRecordActivityTaskStartedScope tracks RPC calls to history service
	HistoryClientRecordActivityTaskStartedScope = "HistoryClientRecordActivityTaskStarted"
	// HistoryClientRequestCancelWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientRequestCancelWorkflowExecutionScope = "HistoryClientRequestCancelWorkflowExecution"
	// HistoryClientSignalWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientSignalWorkflowExecutionScope = "HistoryClientSignalWorkflowExecution"
	// HistoryClientSignalWithStartWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientSignalWithStartWorkflowExecutionScope = "HistoryClientSignalWithStartWorkflowExecution"
	// HistoryClientRemoveSignalMutableStateScope tracks RPC calls to history service
	HistoryClientRemoveSignalMutableStateScope = "HistoryClientRemoveSignalMutableState"
	// HistoryClientTerminateWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientTerminateWorkflowExecutionScope = "HistoryClientTerminateWorkflowExecution"
	// HistoryClientUpdateWorkflowScope tracks RPC calls to history service
	HistoryClientUpdateWorkflowScope = "HistoryClientUpdateWorkflow"
	// HistoryClientDeleteWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientDeleteWorkflowExecutionScope = "HistoryClientDeleteWorkflowExecution"
	// HistoryClientResetWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientResetWorkflowExecutionScope = "HistoryClientResetWorkflowExecution"
	// HistoryClientScheduleWorkflowTaskScope tracks RPC calls to history service
	HistoryClientScheduleWorkflowTaskScope = "HistoryClientScheduleWorkflowTask"
	// HistoryClientVerifyFirstWorkflowTaskScheduled tracks RPC calls to history service
	HistoryClientVerifyFirstWorkflowTaskScheduled
	// HistoryClientRecordChildExecutionCompletedScope tracks RPC calls to history service
	HistoryClientRecordChildExecutionCompletedScope = "HistoryClientRecordChildExecutionCompleted"
	// HistoryClientVerifyChildExecutionCompletionRecordedScope tracks RPC calls to history service
	HistoryClientVerifyChildExecutionCompletionRecordedScope = "HistoryClientVerifyChildExecutionCompletionRecorded"
	// HistoryClientReplicateEventsV2Scope tracks RPC calls to history service
	HistoryClientReplicateEventsV2Scope = "HistoryClientReplicateEventsV2"
	// HistoryClientSyncShardStatusScope tracks RPC calls to history service
	HistoryClientSyncShardStatusScope = "HistoryClientSyncShardStatus"
	// HistoryClientSyncActivityScope tracks RPC calls to history service
	HistoryClientSyncActivityScope = "HistoryClientSyncActivity"
	// HistoryClientGetReplicationTasksScope tracks RPC calls to history service
	HistoryClientGetReplicationTasksScope = "HistoryClientGetReplicationTasks"
	// HistoryClientGetDLQReplicationTasksScope tracks RPC calls to history service
	HistoryClientGetDLQReplicationTasksScope = "HistoryClientGetDLQReplicationTasks"
	// HistoryClientQueryWorkflowScope tracks RPC calls to history service
	HistoryClientQueryWorkflowScope = "HistoryClientQueryWorkflow"
	// HistoryClientReapplyEventsScope tracks RPC calls to history service
	HistoryClientReapplyEventsScope = "HistoryClientReapplyEvents"
	// HistoryClientGetDLQMessagesScope tracks RPC calls to history service
	HistoryClientGetDLQMessagesScope = "HistoryClientGetDLQMessages"
	// HistoryClientPurgeDLQMessagesScope tracks RPC calls to history service
	HistoryClientPurgeDLQMessagesScope = "HistoryClientPurgeDLQMessages"
	// HistoryClientMergeDLQMessagesScope tracks RPC calls to history service
	HistoryClientMergeDLQMessagesScope = "HistoryClientMergeDLQMessages"
	// HistoryClientRefreshWorkflowTasksScope tracks RPC calls to history service
	HistoryClientRefreshWorkflowTasksScope = "HistoryClientRefreshWorkflowTasks"
	// HistoryClientGenerateLastHistoryReplicationTasksScope tracks RPC calls to history service
	HistoryClientGenerateLastHistoryReplicationTasksScope = "HistoryClientGenerateLastHistoryReplicationTasks"
	// HistoryClientGetReplicationStatusScope tracks RPC calls to history service
	HistoryClientGetReplicationStatusScope = "HistoryClientGetReplicationStatus"
	// HistoryClientDeleteWorkflowVisibilityRecordScope tracks RPC calls to history service
	HistoryClientDeleteWorkflowVisibilityRecordScope = "HistoryClientDeleteWorkflowVisibilityRecord"
	// HistoryClientCloseShardScope tracks RPC calls to history service
	HistoryClientCloseShardScope = "HistoryClientCloseShard"
	// HistoryClientDescribeMutableStateScope tracks RPC calls to history service
	HistoryClientDescribeMutableStateScope = "HistoryClientDescribeMutableState"
	// HistoryClientGetDLQReplicationMessagesScope tracks RPC calls to history service
	HistoryClientGetDLQReplicationMessagesScope = "HistoryClientGetDLQReplicationMessages"
	// HistoryClientGetShardScope tracks RPC calls to history service
	HistoryClientGetShardScope = "HistoryClientGetShard"
	// HistoryClientRebuildMutableStateScope tracks RPC calls to history service
	HistoryClientRebuildMutableStateScope = "HistoryClientRebuildMutableState"
	// HistoryClientRemoveTaskScope tracks RPC calls to history service
	HistoryClientRemoveTaskScope = "HistoryClientRemoveTask"
	// HistoryClientVerifyFirstWorkflowTaskScheduledScope tracks RPC calls to history service
	HistoryClientVerifyFirstWorkflowTaskScheduledScope = "HistoryClientVerifyFirstWorkflowTaskScheduled"
	// HistoryClientDescribeHistoryHostScope tracks RPC calls to history service
	HistoryClientDescribeHistoryHostScope = "HistoryClientDescribeHistoryHost"
	// HistoryClientGetReplicationMessagesScope tracks RPC calls to history service
	HistoryClientGetReplicationMessagesScope = "HistoryClientGetReplicationMessages"
)

// Matching Client Operations
const (
	// MatchingClientPollWorkflowTaskQueueScope tracks RPC calls to matching service
	MatchingClientPollWorkflowTaskQueueScope = "MatchingClientPollWorkflowTaskQueue"
	// MatchingClientPollActivityTaskQueueScope tracks RPC calls to matching service
	MatchingClientPollActivityTaskQueueScope = "MatchingClientPollActivityTaskQueue"
	// MatchingClientAddActivityTaskScope tracks RPC calls to matching service
	MatchingClientAddActivityTaskScope = "MatchingClientAddActivityTask"
	// MatchingClientAddWorkflowTaskScope tracks RPC calls to matching service
	MatchingClientAddWorkflowTaskScope = "MatchingClientAddWorkflowTask"
	// MatchingClientQueryWorkflowScope tracks RPC calls to matching service
	MatchingClientQueryWorkflowScope = "MatchingClientQueryWorkflow"
	// MatchingClientRespondQueryTaskCompletedScope tracks RPC calls to matching service
	MatchingClientRespondQueryTaskCompletedScope = "MatchingClientRespondQueryTaskCompleted"
	// MatchingClientCancelOutstandingPollScope tracks RPC calls to matching service
	MatchingClientCancelOutstandingPollScope = "MatchingClientCancelOutstandingPoll"
	// MatchingClientDescribeTaskQueueScope tracks RPC calls to matching service
	MatchingClientDescribeTaskQueueScope = "MatchingClientDescribeTaskQueue"
	// MatchingClientListTaskQueuePartitionsScope tracks RPC calls to matching service
	MatchingClientListTaskQueuePartitionsScope = "MatchingClientListTaskQueuePartitions"
	// MatchingClientUpdateWorkerBuildIdOrderingScope tracks RPC calls to matching service
	MatchingClientUpdateWorkerBuildIdOrderingScope = "MatchingClientUpdateWorkerBuildIdOrdering"
	// MatchingClientGetWorkerBuildIdOrderingScope tracks RPC calls to matching service
	MatchingClientGetWorkerBuildIdOrderingScope = "MatchingClientGetWorkerBuildIdOrdering"
	// MatchingClientInvalidateTaskQueueMetadataScope tracks RPC calls to matching service
	MatchingClientInvalidateTaskQueueMetadataScope = "MatchingClientInvalidateTaskQueueMetadata"
	// MatchingClientGetTaskQueueMetadataScope tracks RPC calls to matching service
	MatchingClientGetTaskQueueMetadataScope = "MatchingClientGetTaskQueueMetadata"
)

// Worker
const (
	// TaskQueueScavengerScope is scope used by all metrics emitted by worker.taskqueue.Scavenger module
	TaskQueueScavengerScope = "TaskQueueScavenger"
	// ExecutionsScavengerScope is scope used by all metrics emitted by worker.executions.Scavenger module
	ExecutionsScavengerScope = "ExecutionsScavenger"
)

const (
	// PersistenceAppendHistoryNodesScope tracks AppendHistoryNodes calls made by service to persistence layer
	PersistenceAppendHistoryNodesScope = "AppendHistoryNodes"
	// PersistenceAppendRawHistoryNodesScope tracks AppendRawHistoryNodes calls made by service to persistence layer
	PersistenceAppendRawHistoryNodesScope = "AppendRawHistoryNodes"
	// PersistenceDeleteHistoryNodesScope tracks DeleteHistoryNodes calls made by service to persistence layer
	PersistenceDeleteHistoryNodesScope = "DeleteHistoryNodes"
	// PersistenceParseHistoryBranchInfoScope tracks NewHistoryBranch calls made by service to persistence layer
	PersistenceParseHistoryBranchInfoScope = "ParseHistoryBranchInfo"
	// PersistenceUpdateHistoryBranchInfoScope tracks NewHistoryBranch calls made by service to persistence layer
	PersistenceUpdateHistoryBranchInfoScope = "UpdateHistoryBranchInfo"
	// PersistenceNewHistoryBranchScope tracks NewHistoryBranch calls made by service to persistence layer
	PersistenceNewHistoryBranchScope = "NewHistoryBranch"
	// PersistenceReadHistoryBranchScope tracks ReadHistoryBranch calls made by service to persistence layer
	PersistenceReadHistoryBranchScope = "ReadHistoryBranch"
	// PersistenceReadHistoryBranchReverseScope tracks ReadHistoryBranchReverse calls made by service to persistence layer
	PersistenceReadHistoryBranchReverseScope = "ReadHistoryBranchReverse"
	// PersistenceForkHistoryBranchScope tracks ForkHistoryBranch calls made by service to persistence layer
	PersistenceForkHistoryBranchScope = "ForkHistoryBranch"
	// PersistenceDeleteHistoryBranchScope tracks DeleteHistoryBranch calls made by service to persistence layer
	PersistenceDeleteHistoryBranchScope = "DeleteHistoryBranch"
	// PersistenceTrimHistoryBranchScope tracks TrimHistoryBranch calls made by service to persistence layer
	PersistenceTrimHistoryBranchScope = "TrimHistoryBranch"
	// PersistenceCompleteForkBranchScope tracks CompleteForkBranch calls made by service to persistence layer
	PersistenceCompleteForkBranchScope = "CompleteForkBranch"
	// PersistenceGetHistoryTreeScope tracks GetHistoryTree calls made by service to persistence layer
	PersistenceGetHistoryTreeScope = "GetHistoryTree"
	// PersistenceGetAllHistoryTreeBranchesScope tracks GetHistoryTree calls made by service to persistence layer
	PersistenceGetAllHistoryTreeBranchesScope = "GetAllHistoryTreeBranches"
	// PersistenceNamespaceReplicationQueueScope is the metrics scope for namespace replication queue
	PersistenceNamespaceReplicationQueueScope = "NamespaceReplicationQueue"
	// PersistenceEnqueueMessageScope tracks Enqueue calls made by service to persistence layer
	PersistenceEnqueueMessageScope = "EnqueueMessage"
	// PersistenceEnqueueMessageToDLQScope tracks Enqueue DLQ calls made by service to persistence layer
	PersistenceEnqueueMessageToDLQScope = "EnqueueMessageToDLQ"
	// PersistenceReadQueueMessagesScope tracks ReadMessages calls made by service to persistence layer
	PersistenceReadQueueMessagesScope = "ReadQueueMessages"
	// PersistenceReadMessagesFromDLQScope tracks ReadMessagesFromDLQ calls made by service to persistence layer
	PersistenceReadMessagesFromDLQScope = "ReadMessagesFromDLQ"
	// PersistenceDeleteMessagesBeforeScope tracks DeleteMessagesBefore calls made by service to persistence layer
	PersistenceDeleteMessagesBeforeScope = "DeleteMessagesBefore"
	// PersistenceDeleteMessageFromDLQScope tracks DeleteMessageFromDLQ calls made by service to persistence layer
	PersistenceDeleteMessageFromDLQScope = "DeleteMessageFromDLQ"
	// PersistenceRangeDeleteMessagesFromDLQScope tracks RangeDeleteMessagesFromDLQ calls made by service to persistence layer
	PersistenceRangeDeleteMessagesFromDLQScope = "RangeDeleteMessagesFromDLQ"
	// PersistenceUpdateAckLevelScope tracks UpdateAckLevel calls made by service to persistence layer
	PersistenceUpdateAckLevelScope = "UpdateAckLevel"
	// PersistenceGetAckLevelScope tracks GetAckLevel calls made by service to persistence layer
	PersistenceGetAckLevelScope = "GetAckLevel"
	// PersistenceUpdateDLQAckLevelScope tracks UpdateDLQAckLevel calls made by service to persistence layer
	PersistenceUpdateDLQAckLevelScope = "UpdateDLQAckLevel"
	// PersistenceGetDLQAckLevelScope tracks GetDLQAckLevel calls made by service to persistence layer
	PersistenceGetDLQAckLevelScope = "GetDLQAckLevel"
	// PersistenceListClusterMetadataScope tracks ListClusterMetadata calls made by service to persistence layer
	PersistenceListClusterMetadataScope = "ListClusterMetadata"
	// PersistenceGetClusterMetadataScope tracks GetClusterMetadata calls made by service to persistence layer
	PersistenceGetClusterMetadataScope = "GetClusterMetadata"
	// PersistenceGetCurrentClusterMetadataScope tracks GetCurrentClusterMetadata calls made by service to persistence layer
	PersistenceGetCurrentClusterMetadataScope = "GetCurrentClusterMetadata"
	// PersistenceSaveClusterMetadataScope tracks SaveClusterMetadata calls made by service to persistence layer
	PersistenceSaveClusterMetadataScope = "SaveClusterMetadata"
	// PersistenceDeleteClusterMetadataScope tracks DeleteClusterMetadata calls made by service to persistence layer
	PersistenceDeleteClusterMetadataScope = "DeleteClusterMetadata"
	// PersistenceUpsertClusterMembershipScope tracks UpsertClusterMembership calls made by service to persistence layer
	PersistenceUpsertClusterMembershipScope = "UpsertClusterMembership"
	// PersistencePruneClusterMembershipScope tracks PruneClusterMembership calls made by service to persistence layer
	PersistencePruneClusterMembershipScope = "PruneClusterMembership"
	// PersistenceGetClusterMembersScope tracks GetClusterMembers calls made by service to persistence layer
	PersistenceGetClusterMembersScope = "GetClusterMembers"
	// PersistenceGetOrCreateShardScope tracks GetOrCreateShard calls made by service to persistence layer
	PersistenceGetOrCreateShardScope = "GetOrCreateShard"
	// PersistenceUpdateShardScope tracks UpdateShard calls made by service to persistence layer
	PersistenceUpdateShardScope = "UpdateShard"
	// PersistenceAssertShardOwnershipScope tracks UpdateShard calls made by service to persistence layer
	PersistenceAssertShardOwnershipScope = "AssertShardOwnership"
	// PersistenceCreateWorkflowExecutionScope tracks CreateWorkflowExecution calls made by service to persistence layer
	PersistenceCreateWorkflowExecutionScope = "CreateWorkflowExecution"
	// PersistenceGetWorkflowExecutionScope tracks GetWorkflowExecution calls made by service to persistence layer
	PersistenceGetWorkflowExecutionScope = "GetWorkflowExecution"
	// PersistenceSetWorkflowExecutionScope tracks SetWorkflowExecution calls made by service to persistence layer
	PersistenceSetWorkflowExecutionScope = "SetWorkflowExecution"
	// PersistenceUpdateWorkflowExecutionScope tracks UpdateWorkflowExecution calls made by service to persistence layer
	PersistenceUpdateWorkflowExecutionScope = "UpdateWorkflowExecution"
	// PersistenceConflictResolveWorkflowExecutionScope tracks ConflictResolveWorkflowExecution calls made by service to persistence layer
	PersistenceConflictResolveWorkflowExecutionScope = "ConflictResolveWorkflowExecution"
	// PersistenceResetWorkflowExecutionScope tracks ResetWorkflowExecution calls made by service to persistence layer
	PersistenceResetWorkflowExecutionScope = "ResetWorkflowExecution"
	// PersistenceDeleteWorkflowExecutionScope tracks DeleteWorkflowExecution calls made by service to persistence layer
	PersistenceDeleteWorkflowExecutionScope = "DeleteWorkflowExecution"
	// PersistenceDeleteCurrentWorkflowExecutionScope tracks DeleteCurrentWorkflowExecution calls made by service to persistence layer
	PersistenceDeleteCurrentWorkflowExecutionScope = "DeleteCurrentWorkflowExecution"
	// PersistenceGetCurrentExecutionScope tracks GetCurrentExecution calls made by service to persistence layer
	PersistenceGetCurrentExecutionScope = "GetCurrentExecution"
	// PersistenceListConcreteExecutionsScope tracks ListConcreteExecutions calls made by service to persistence layer
	PersistenceListConcreteExecutionsScope = "ListConcreteExecutions"
	// PersistenceAddTasksScope tracks AddTasks calls made by service to persistence layer
	PersistenceAddTasksScope = "AddTasks"
	// PersistenceGetTransferTaskScope tracks GetTransferTask calls made by service to persistence layer
	PersistenceGetTransferTaskScope = "GetTransferTask"
	// PersistenceGetTransferTasksScope tracks GetTransferTasks calls made by service to persistence layer
	PersistenceGetTransferTasksScope = "GetTransferTasks"
	// PersistenceCompleteTransferTaskScope tracks CompleteTransferTasks calls made by service to persistence layer
	PersistenceCompleteTransferTaskScope = "CompleteTransferTask"
	// PersistenceRangeCompleteTransferTasksScope tracks CompleteTransferTasks calls made by service to persistence layer
	PersistenceRangeCompleteTransferTasksScope = "RangeCompleteTransferTasks"
	// PersistenceGetVisibilityTaskScope tracks GetVisibilityTask calls made by service to persistence layer
	PersistenceGetVisibilityTaskScope = "GetVisibilityTask"
	// PersistenceGetVisibilityTasksScope tracks GetVisibilityTasks calls made by service to persistence layer
	PersistenceGetVisibilityTasksScope = "GetVisibilityTasks"
	// PersistenceCompleteVisibilityTaskScope tracks CompleteVisibilityTasks calls made by service to persistence layer
	PersistenceCompleteVisibilityTaskScope = "CompleteVisibilityTask"
	// PersistenceRangeCompleteVisibilityTasksScope tracks CompleteVisibilityTasks calls made by service to persistence layer
	PersistenceRangeCompleteVisibilityTasksScope = "RangeCompleteVisibilityTasks"
	// PersistenceGetReplicationTaskScope tracks GetReplicationTask calls made by service to persistence layer
	PersistenceGetReplicationTaskScope = "GetReplicationTask"
	// PersistenceGetReplicationTasksScope tracks GetReplicationTasks calls made by service to persistence layer
	PersistenceGetReplicationTasksScope = "GetReplicationTasks"
	// PersistenceCompleteReplicationTaskScope tracks CompleteReplicationTasks calls made by service to persistence layer
	PersistenceCompleteReplicationTaskScope = "CompleteReplicationTask"
	// PersistenceRangeCompleteReplicationTasksScope tracks RangeCompleteReplicationTasks calls made by service to persistence layer
	PersistenceRangeCompleteReplicationTasksScope = "RangeCompleteReplicationTasks"
	// PersistencePutReplicationTaskToDLQScope tracks PersistencePutReplicationTaskToDLQScope calls made by service to persistence layer
	PersistencePutReplicationTaskToDLQScope = "PutReplicationTaskToDLQ"
	// PersistenceGetReplicationTasksFromDLQScope tracks PersistenceGetReplicationTasksFromDLQScope calls made by service to persistence layer
	PersistenceGetReplicationTasksFromDLQScope = "GetReplicationTasksFromDLQ"
	// PersistenceDeleteReplicationTaskFromDLQScope tracks PersistenceDeleteReplicationTaskFromDLQScope calls made by service to persistence layer
	PersistenceDeleteReplicationTaskFromDLQScope = "DeleteReplicationTaskFromDLQ"
	// PersistenceRangeDeleteReplicationTaskFromDLQScope tracks PersistenceRangeDeleteReplicationTaskFromDLQScope calls made by service to persistence layer
	PersistenceRangeDeleteReplicationTaskFromDLQScope = "RangeDeleteReplicationTaskFromDLQ"
	// PersistenceGetTimerTaskScope tracks GetTimerTask calls made by service to persistence layer
	PersistenceGetTimerTaskScope = "GetTimerTask"
	// PersistenceGetTimerTasksScope tracks GetTimerTasks calls made by service to persistence layer
	PersistenceGetTimerTasksScope = "GetTimerTasks"
	// PersistenceCompleteTimerTaskScope tracks CompleteTimerTasks calls made by service to persistence layer
	PersistenceCompleteTimerTaskScope = "CompleteTimerTask"
	// PersistenceRangeCompleteTimerTasksScope tracks CompleteTimerTasks calls made by service to persistence layer
	PersistenceRangeCompleteTimerTasksScope = "RangeCompleteTimerTasks"
	// PersistenceCreateTasksScope tracks CreateTasks calls made by service to persistence layer
	PersistenceCreateTasksScope = "CreateTasks"
	// PersistenceGetTasksScope tracks GetTasks calls made by service to persistence layer
	PersistenceGetTasksScope = "GetTasks"
	// PersistenceCompleteTaskScope tracks CompleteTask calls made by service to persistence layer
	PersistenceCompleteTaskScope = "CompleteTask"
	// PersistenceCompleteTasksLessThanScope is the metric scope for persistence.TaskManager.PersistenceCompleteTasksLessThan API
	PersistenceCompleteTasksLessThanScope = "CompleteTasksLessThan"
	// PersistenceCreateTaskQueueScope tracks PersistenceCreateTaskQueueScope calls made by service to persistence layer
	PersistenceCreateTaskQueueScope = "CreateTaskQueue"
	// PersistenceUpdateTaskQueueScope tracks PersistenceUpdateTaskQueueScope calls made by service to persistence layer
	PersistenceUpdateTaskQueueScope = "UpdateTaskQueue"
	// PersistenceGetTaskQueueScope tracks PersistenceGetTaskQueueScope calls made by service to persistence layer
	PersistenceGetTaskQueueScope = "GetTaskQueue"
	// PersistenceListTaskQueueScope is the metric scope for persistence.TaskManager.ListTaskQueue API
	PersistenceListTaskQueueScope = "ListTaskQueue"
	// PersistenceDeleteTaskQueueScope is the metric scope for persistence.TaskManager.DeleteTaskQueue API
	PersistenceDeleteTaskQueueScope = "DeleteTaskQueue"
	// PersistenceAppendHistoryEventsScope tracks AppendHistoryEvents calls made by service to persistence layer
	PersistenceAppendHistoryEventsScope = "AppendHistoryEvents"
	// PersistenceGetWorkflowExecutionHistoryScope tracks GetWorkflowExecutionHistory calls made by service to persistence layer
	PersistenceGetWorkflowExecutionHistoryScope = "GetWorkflowExecutionHistory"
	// PersistenceDeleteWorkflowExecutionHistoryScope tracks DeleteWorkflowExecutionHistory calls made by service to persistence layer
	PersistenceDeleteWorkflowExecutionHistoryScope = "DeleteWorkflowExecutionHistory"
	// PersistenceInitializeSystemNamespaceScope tracks InitializeSystemNamespaceScope calls made by service to persistence layer
	PersistenceInitializeSystemNamespaceScope = "InitializeSystemNamespace"
	// PersistenceCreateNamespaceScope tracks CreateNamespace calls made by service to persistence layer
	PersistenceCreateNamespaceScope = "CreateNamespace"
	// PersistenceGetNamespaceScope tracks GetNamespace calls made by service to persistence layer
	PersistenceGetNamespaceScope = "GetNamespace"
	// PersistenceUpdateNamespaceScope tracks UpdateNamespace calls made by service to persistence layer
	PersistenceUpdateNamespaceScope = "UpdateNamespace"
	// PersistenceDeleteNamespaceScope tracks DeleteNamespace calls made by service to persistence layer
	PersistenceDeleteNamespaceScope = "DeleteNamespace"
	// PersistenceRenameNamespaceScope tracks RenameNamespace calls made by service to persistence layer
	PersistenceRenameNamespaceScope = "RenameNamespace"
	// PersistenceDeleteNamespaceByNameScope tracks DeleteNamespaceByName calls made by service to persistence layer
	PersistenceDeleteNamespaceByNameScope = "DeleteNamespaceByName"
	// PersistenceListNamespacesScope tracks ListNamespaces calls made by service to persistence layer
	PersistenceListNamespacesScope = "ListNamespaces"
	// PersistenceGetMetadataScope tracks DeleteNamespaceByName calls made by service to persistence layer
	PersistenceGetMetadataScope = "GetMetadata"

	// VisibilityPersistenceRecordWorkflowExecutionStartedScope tracks RecordWorkflowExecutionStarted calls made by service to visibility persistence layer
	VisibilityPersistenceRecordWorkflowExecutionStartedScope = "RecordWorkflowExecutionStarted"
	// VisibilityPersistenceRecordWorkflowExecutionClosedScope tracks RecordWorkflowExecutionClosed calls made by service to visibility persistence layer
	VisibilityPersistenceRecordWorkflowExecutionClosedScope = "RecordWorkflowExecutionClosed"
	// VisibilityPersistenceUpsertWorkflowExecutionScope tracks UpsertWorkflowExecution calls made by service to persistence visibility layer
	VisibilityPersistenceUpsertWorkflowExecutionScope = "UpsertWorkflowExecution"
	// VisibilityPersistenceListOpenWorkflowExecutionsScope tracks ListOpenWorkflowExecutions calls made by service to visibility persistence layer
	VisibilityPersistenceListOpenWorkflowExecutionsScope = "ListOpenWorkflowExecutions"
	// VisibilityPersistenceListClosedWorkflowExecutionsScope tracks ListClosedWorkflowExecutions calls made by service to visibility persistence layer
	VisibilityPersistenceListClosedWorkflowExecutionsScope = "ListClosedWorkflowExecutions"
	// VisibilityPersistenceListOpenWorkflowExecutionsByTypeScope tracks ListOpenWorkflowExecutionsByType calls made by service to visibility persistence layer
	VisibilityPersistenceListOpenWorkflowExecutionsByTypeScope = "ListOpenWorkflowExecutionsByType"
	// VisibilityPersistenceListClosedWorkflowExecutionsByTypeScope tracks ListClosedWorkflowExecutionsByType calls made by service to visibility persistence layer
	VisibilityPersistenceListClosedWorkflowExecutionsByTypeScope = "ListClosedWorkflowExecutionsByType"
	// VisibilityPersistenceListOpenWorkflowExecutionsByWorkflowIDScope tracks ListOpenWorkflowExecutionsByWorkflowID calls made by service to visibility persistence layer
	VisibilityPersistenceListOpenWorkflowExecutionsByWorkflowIDScope = "ListOpenWorkflowExecutionsByWorkflowID"
	// VisibilityPersistenceListClosedWorkflowExecutionsByWorkflowIDScope tracks ListClosedWorkflowExecutionsByWorkflowID calls made by service to visibility persistence layer
	VisibilityPersistenceListClosedWorkflowExecutionsByWorkflowIDScope = "ListClosedWorkflowExecutionsByWorkflowID"
	// VisibilityPersistenceListClosedWorkflowExecutionsByStatusScope tracks ListClosedWorkflowExecutionsByStatus calls made by service to visibility persistence layer
	VisibilityPersistenceListClosedWorkflowExecutionsByStatusScope = "ListClosedWorkflowExecutionsByStatus"
	// VisibilityPersistenceDeleteWorkflowExecutionScope tracks DeleteWorkflowExecutions calls made by service to visibility persistence layer
	VisibilityPersistenceDeleteWorkflowExecutionScope = "DeleteWorkflowExecution"
	// VisibilityPersistenceListWorkflowExecutionsScope tracks ListWorkflowExecutions calls made by service to visibility persistence layer
	VisibilityPersistenceListWorkflowExecutionsScope = "ListWorkflowExecutions"
	// VisibilityPersistenceScanWorkflowExecutionsScope tracks ScanWorkflowExecutions calls made by service to visibility persistence layer
	VisibilityPersistenceScanWorkflowExecutionsScope = "ScanWorkflowExecutions"
	// VisibilityPersistenceCountWorkflowExecutionsScope tracks CountWorkflowExecutions calls made by service to visibility persistence layer
	VisibilityPersistenceCountWorkflowExecutionsScope = "CountWorkflowExecutions"
	// VisibilityPersistenceGetWorkflowExecutionScope tracks GetWorkflowExecution calls made by service to visibility persistence layer
	VisibilityPersistenceGetWorkflowExecutionScope = "GetWorkflowExecution"
)

// Common
const (
	ServerTlsScope = "ServerTls"
	// AuthorizationScope is the scope used by all metric emitted by authorization code
	AuthorizationScope = "Authorization"
	// NamespaceCacheScope tracks namespace cache callbacks
	NamespaceCacheScope = "NamespaceCache"
)

// Frontend Scope
const (
	// FrontendStartWorkflowExecutionScope is the metric scope for frontend.StartWorkflowExecution
	FrontendStartWorkflowExecutionScope = "StartWorkflowExecution"
	// FrontendPollWorkflowTaskQueueScope is the metric scope for frontend.PollWorkflowTaskQueue
	FrontendPollWorkflowTaskQueueScope = "PollWorkflowTaskQueue"
	// FrontendPollActivityTaskQueueScope is the metric scope for frontend.PollActivityTaskQueue
	FrontendPollActivityTaskQueueScope = "PollActivityTaskQueue"
	// FrontendRecordActivityTaskHeartbeatScope is the metric scope for frontend.RecordActivityTaskHeartbeat
	FrontendRecordActivityTaskHeartbeatScope = "RecordActivityTaskHeartbeat"
	// FrontendRecordActivityTaskHeartbeatByIdScope is the metric scope for frontend.RecordActivityTaskHeartbeatById
	FrontendRecordActivityTaskHeartbeatByIdScope = "RecordActivityTaskHeartbeatById"
	// FrontendRespondWorkflowTaskCompletedScope is the metric scope for frontend.RespondWorkflowTaskCompleted
	FrontendRespondWorkflowTaskCompletedScope = "RespondWorkflowTaskCompleted"
	// FrontendRespondWorkflowTaskFailedScope is the metric scope for frontend.RespondWorkflowTaskFailed
	FrontendRespondWorkflowTaskFailedScope = "RespondWorkflowTaskFailed"
	// FrontendRespondQueryTaskCompletedScope is the metric scope for frontend.RespondQueryTaskCompleted
	FrontendRespondQueryTaskCompletedScope = "RespondQueryTaskCompleted"
	// FrontendRespondActivityTaskCompletedScope is the metric scope for frontend.RespondActivityTaskCompleted
	FrontendRespondActivityTaskCompletedScope = "RespondActivityTaskCompleted"
	// FrontendRespondActivityTaskFailedScope is the metric scope for frontend.RespondActivityTaskFailed
	FrontendRespondActivityTaskFailedScope = "RespondActivityTaskFailed"
	// FrontendRespondActivityTaskCanceledScope is the metric scope for frontend.RespondActivityTaskCanceled
	FrontendRespondActivityTaskCanceledScope = "RespondActivityTaskCanceled"
	// FrontendRespondActivityTaskCompletedByIdScope is the metric scope for frontend.RespondActivityTaskCompletedById
	FrontendRespondActivityTaskCompletedByIdScope = "RespondActivityTaskCompletedById"
	// FrontendRespondActivityTaskFailedByIdScope is the metric scope for frontend.RespondActivityTaskFailedById
	FrontendRespondActivityTaskFailedByIdScope = "RespondActivityTaskFailedById"
	// FrontendRespondActivityTaskCanceledByIdScope is the metric scope for frontend.RespondActivityTaskCanceledById
	FrontendRespondActivityTaskCanceledByIdScope = "RespondActivityTaskCanceledById"
	// FrontendGetWorkflowExecutionHistoryScope is the metric scope for non-long-poll frontend.GetWorkflowExecutionHistory
	FrontendGetWorkflowExecutionHistoryScope = "GetWorkflowExecutionHistory"
	// FrontendGetWorkflowExecutionHistoryReverseScope is the metric for frontend.GetWorkflowExecutionHistoryReverse
	FrontendGetWorkflowExecutionHistoryReverseScope = "GetWorkflowExecutionHistoryReverse"
	// FrontendPollWorkflowExecutionHistoryScope is the metric scope for long poll case of frontend.GetWorkflowExecutionHistory
	FrontendPollWorkflowExecutionHistoryScope = "PollWorkflowExecutionHistory"
	// FrontendGetWorkflowExecutionRawHistoryScope is the metric scope for frontend.GetWorkflowExecutionRawHistory
	FrontendGetWorkflowExecutionRawHistoryScope = "GetWorkflowExecutionRawHistory"
	// FrontendPollForWorkflowExecutionRawHistoryScope is the metric scope for frontend.GetWorkflowExecutionRawHistory
	FrontendPollForWorkflowExecutionRawHistoryScope = "PollForWorkflowExecutionRawHistory"
	// FrontendSignalWorkflowExecutionScope is the metric scope for frontend.SignalWorkflowExecution
	FrontendSignalWorkflowExecutionScope = "SignalWorkflowExecution"
	// FrontendSignalWithStartWorkflowExecutionScope is the metric scope for frontend.SignalWithStartWorkflowExecution
	FrontendSignalWithStartWorkflowExecutionScope = "SignalWithStartWorkflowExecution"
	// FrontendTerminateWorkflowExecutionScope is the metric scope for frontend.TerminateWorkflowExecution
	FrontendTerminateWorkflowExecutionScope = "TerminateWorkflowExecution"
	// FrontendDeleteWorkflowExecutionScope is the metric scope for frontend.DeleteWorkflowExecution
	FrontendDeleteWorkflowExecutionScope = "DeleteWorkflowExecution"
	// FrontendRequestCancelWorkflowExecutionScope is the metric scope for frontend.RequestCancelWorkflowExecution
	FrontendRequestCancelWorkflowExecutionScope = "RequestCancelWorkflowExecution"
	// FrontendListArchivedWorkflowExecutionsScope is the metric scope for frontend.ListArchivedWorkflowExecutions
	FrontendListArchivedWorkflowExecutionsScope = "ListArchivedWorkflowExecutions"
	// FrontendListOpenWorkflowExecutionsScope is the metric scope for frontend.ListOpenWorkflowExecutions
	FrontendListOpenWorkflowExecutionsScope = "ListOpenWorkflowExecutions"
	// FrontendListClosedWorkflowExecutionsScope is the metric scope for frontend.ListClosedWorkflowExecutions
	FrontendListClosedWorkflowExecutionsScope = "ListClosedWorkflowExecutions"
	// FrontendListWorkflowExecutionsScope is the metric scope for frontend.ListWorkflowExecutions
	FrontendListWorkflowExecutionsScope = "ListWorkflowExecutions"
	// FrontendScanWorkflowExecutionsScope is the metric scope for frontend.ListWorkflowExecutions
	FrontendScanWorkflowExecutionsScope = "ScanWorkflowExecutions"
	// FrontendCountWorkflowExecutionsScope is the metric scope for frontend.CountWorkflowExecutions
	FrontendCountWorkflowExecutionsScope = "CountWorkflowExecutions"
	// FrontendRegisterNamespaceScope is the metric scope for frontend.RegisterNamespace
	FrontendRegisterNamespaceScope = "RegisterNamespace"
	// FrontendDescribeNamespaceScope is the metric scope for frontend.DescribeNamespace
	FrontendDescribeNamespaceScope = "DescribeNamespace"
	// FrontendUpdateNamespaceScope is the metric scope for frontend.DescribeNamespace
	FrontendUpdateNamespaceScope = "UpdateNamespace"
	// FrontendDeprecateNamespaceScope is the metric scope for frontend.DeprecateNamespace
	FrontendDeprecateNamespaceScope = "DeprecateNamespace"
	// FrontendQueryWorkflowScope is the metric scope for frontend.QueryWorkflow
	FrontendQueryWorkflowScope = "QueryWorkflow"
	// FrontendDescribeWorkflowExecutionScope is the metric scope for frontend.DescribeWorkflowExecution
	FrontendDescribeWorkflowExecutionScope = "DescribeWorkflowExecution"
	// FrontendDescribeTaskQueueScope is the metric scope for frontend.DescribeTaskQueue
	FrontendDescribeTaskQueueScope = "DescribeTaskQueue"
	// FrontendListTaskQueuePartitionsScope is the metric scope for frontend.ResetStickyTaskQueue
	FrontendListTaskQueuePartitionsScope = "ListTaskQueuePartitions"
	// FrontendResetStickyTaskQueueScope is the metric scope for frontend.ResetStickyTaskQueue
	FrontendResetStickyTaskQueueScope = "ResetStickyTaskQueue"
	// FrontendListNamespacesScope is the metric scope for frontend.ListNamespace
	FrontendListNamespacesScope = "ListNamespaces"
	// FrontendResetWorkflowExecutionScope is the metric scope for frontend.ResetWorkflowExecution
	FrontendResetWorkflowExecutionScope = "ResetWorkflowExecution"
	// FrontendGetSearchAttributesScope is the metric scope for frontend.GetSearchAttributes
	FrontendGetSearchAttributesScope = "GetSearchAttributes"
	// FrontendGetClusterInfoScope is the metric scope for frontend.GetClusterInfo
	FrontendGetClusterInfoScope = "GetClusterInfo"
	// FrontendGetSystemInfoScope is the metric scope for frontend.GetSystemInfo
	FrontendGetSystemInfoScope = "GetSystemInfo"
	// FrontendCreateScheduleScope is the metric scope for frontend.CreateScheduleScope = "CreateScheduleScope is the metric scope for frontend.CreateSchedule"
	FrontendCreateScheduleScope = "CreateSchedule"
	// FrontendDescribeScheduleScope is the metric scope for frontend.DescribeScheduleScope = "DescribeScheduleScope is the metric scope for frontend.DescribeSchedule"
	FrontendDescribeScheduleScope = "DescribeSchedule"
	// FrontendUpdateScheduleScope is the metric scope for frontend.UpdateScheduleScope = "UpdateScheduleScope is the metric scope for frontend.UpdateSchedule"
	FrontendUpdateScheduleScope = "UpdateSchedule"
	// FrontendPatchScheduleScope is the metric scope for frontend.PatchScheduleScope = "PatchScheduleScope is the metric scope for frontend.PatchSchedule"
	FrontendPatchScheduleScope = "PatchSchedule"
	// FrontendListScheduleMatchingTimesScope is the metric scope for frontend.ListScheduleMatchingTimesScope = "ListScheduleMatchingTimesScope is the metric scope for frontend.ListScheduleMatchingTimes"
	FrontendListScheduleMatchingTimesScope = "ListScheduleMatchingTimes"
	// FrontendDeleteScheduleScope is the metric scope for frontend.DeleteScheduleScope = "DeleteScheduleScope is the metric scope for frontend.DeleteSchedule"
	FrontendDeleteScheduleScope = "DeleteSchedule"
	// FrontendListSchedulesScope is the metric scope for frontend.ListSchedulesScope = "ListSchedulesScope is the metric scope for frontend.ListSchedules"
	FrontendListSchedulesScope = "ListSchedules"
	// FrontendUpdateWorkerBuildIdOrderingScope is the metric scope for frontend.UpdateWorkerBuildIdOrderingScope = "UpdateWorkerBuildIdOrderingScope is the metric scope for frontend.UpdateWorkerBuildIdOrdering"
	FrontendUpdateWorkerBuildIdOrderingScope = "UpdateWorkerBuildIdOrdering"
	// FrontendGetWorkerBuildIdOrderingScope is the metric scope for frontend.GetWorkerBuildIdOrderingScope = "GetWorkerBuildIdOrderingScope is the metric scope for frontend.GetWorkerBuildIdOrdering"
	FrontendGetWorkerBuildIdOrderingScope = "GetWorkerBuildIdOrdering"
	// FrontendUpdateWorkflowScope is the metric scope for frontend.UpdateWorkflow
	FrontendUpdateWorkflowScope = "UpdateWorkflow"
	// FrontendDescribeBatchOperationScope is the metric scope for frontend.DescribeBatchOperation
	FrontendDescribeBatchOperationScope = "DescribeBatchOperation"
	// FrontendListBatchOperationsScope is the metric scope for frontend.ListBatchOperations
	FrontendListBatchOperationsScope = "ListBatchOperations"
	// FrontendStartBatchOperationScope is the metric scope for frontend.StartBatchOperation
	FrontendStartBatchOperationScope = "StartBatchOperation"
	// FrontendStopBatchOperationScope is the metric scope for frontend.StopBatchOperation
	FrontendStopBatchOperationScope = "StopBatchOperation"
	// VersionCheckScope is scope used by version checker
	VersionCheckScope = "VersionCheck"
)

// History Scope
const (
	// HistoryStartWorkflowExecutionScope tracks StartWorkflowExecution API calls received by service
	HistoryStartWorkflowExecutionScope = "StartWorkflowExecution"
	// HistoryRecordActivityTaskHeartbeatScope tracks RecordActivityTaskHeartbeat API calls received by service
	HistoryRecordActivityTaskHeartbeatScope = "RecordActivityTaskHeartbeat"
	// HistoryRespondWorkflowTaskCompletedScope tracks RespondWorkflowTaskCompleted API calls received by service
	HistoryRespondWorkflowTaskCompletedScope = "RespondWorkflowTaskCompleted"
	// HistoryRespondWorkflowTaskFailedScope tracks RespondWorkflowTaskFailed API calls received by service
	HistoryRespondWorkflowTaskFailedScope = "RespondWorkflowTaskFailed"
	// HistoryRespondActivityTaskCompletedScope tracks RespondActivityTaskCompleted API calls received by service
	HistoryRespondActivityTaskCompletedScope = "RespondActivityTaskCompleted"
	// HistoryRespondActivityTaskFailedScope tracks RespondActivityTaskFailed API calls received by service
	HistoryRespondActivityTaskFailedScope = "RespondActivityTaskFailed"
	// HistoryRespondActivityTaskCanceledScope tracks RespondActivityTaskCanceled API calls received by service
	HistoryRespondActivityTaskCanceledScope = "RespondActivityTaskCanceled"
	// HistoryGetMutableStateScope tracks GetMutableStateScope API calls received by service
	HistoryGetMutableStateScope = "GetMutableState"
	// HistoryPollMutableStateScope tracks PollMutableStateScope API calls received by service
	HistoryPollMutableStateScope = "PollMutableState"
	// HistoryResetStickyTaskQueueScope tracks ResetStickyTaskQueueScope API calls received by service
	HistoryResetStickyTaskQueueScope = "ResetStickyTaskQueue"
	// HistoryDescribeWorkflowExecutionScope tracks DescribeWorkflowExecution API calls received by service
	HistoryDescribeWorkflowExecutionScope = "DescribeWorkflowExecution"
	// HistoryRecordWorkflowTaskStartedScope tracks RecordWorkflowTaskStarted API calls received by service
	HistoryRecordWorkflowTaskStartedScope = "RecordWorkflowTaskStarted"
	// HistoryRecordActivityTaskStartedScope tracks RecordActivityTaskStarted API calls received by service
	HistoryRecordActivityTaskStartedScope = "RecordActivityTaskStarted"
	// HistorySignalWorkflowExecutionScope tracks SignalWorkflowExecution API calls received by service
	HistorySignalWorkflowExecutionScope = "SignalWorkflowExecution"
	// HistorySignalWithStartWorkflowExecutionScope tracks SignalWithStartWorkflowExecution API calls received by service
	HistorySignalWithStartWorkflowExecutionScope = "SignalWithStartWorkflowExecution"
	// HistoryRemoveSignalMutableStateScope tracks RemoveSignalMutableState API calls received by service
	HistoryRemoveSignalMutableStateScope = "RemoveSignalMutableState"
	// HistoryTerminateWorkflowExecutionScope tracks TerminateWorkflowExecution API calls received by service
	HistoryTerminateWorkflowExecutionScope = "TerminateWorkflowExecution"
	// HistoryScheduleWorkflowTaskScope tracks ScheduleWorkflowTask API calls received by service
	HistoryScheduleWorkflowTaskScope = "ScheduleWorkflowTask"
	// HistoryVerifyFirstWorkflowTaskScheduled tracks VerifyFirstWorkflowTaskScheduled API calls received by service
	HistoryVerifyFirstWorkflowTaskScheduledScope = "VerifyFirstWorkflowTaskScheduled"
	// HistoryRecordChildExecutionCompletedScope tracks RecordChildExecutionCompleted API calls received by service
	HistoryRecordChildExecutionCompletedScope = "RecordChildExecutionCompleted"
	// HistoryVerifyChildExecutionCompletionRecordedScope tracks VerifyChildExecutionCompletionRecorded API calls received by service
	HistoryVerifyChildExecutionCompletionRecordedScope = "VerifyChildExecutionCompletionRecorded"
	// HistoryRequestCancelWorkflowExecutionScope tracks RequestCancelWorkflowExecution API calls received by service
	HistoryRequestCancelWorkflowExecutionScope = "RequestCancelWorkflowExecution"
	// HistorySyncShardStatusScope tracks HistorySyncShardStatus API calls received by service
	HistorySyncShardStatusScope = "SyncShardStatus"
	// HistorySyncActivityScope tracks HistoryActivity API calls received by service
	HistorySyncActivityScope = "SyncActivity"
	// HistoryRebuildMutableStateScope tracks RebuildMutable API calls received by service
	HistoryRebuildMutableStateScope = "RebuildMutableState"
	// HistoryDescribeMutableStateScope tracks DescribeMutableState API calls received by service
	HistoryDescribeMutableStateScope = "DescribeMutableState"
	// HistoryGetReplicationMessagesScope tracks GetReplicationMessages API calls received by service
	HistoryGetReplicationMessagesScope = "GetReplicationMessages"
	// HistoryGetDLQReplicationMessagesScope tracks GetReplicationMessages API calls received by service
	HistoryGetDLQReplicationMessagesScope = "GetDLQReplicationMessages"
	// HistoryReadDLQMessagesScope tracks GetDLQMessages API calls received by service
	HistoryReadDLQMessagesScope = "ReadDLQMessages"
	// HistoryPurgeDLQMessagesScope tracks PurgeDLQMessages API calls received by service
	HistoryPurgeDLQMessagesScope = "PurgeDLQMessages"
	// HistoryMergeDLQMessagesScope tracks MergeDLQMessages API calls received by service
	HistoryMergeDLQMessagesScope = "MergeDLQMessages"
	// HistoryShardControllerScope is the scope used by shard controller
	HistoryShardControllerScope = "ShardController"
	// HistoryReapplyEventsScope is the scope used by event reapplication
	HistoryReapplyEventsScope = "ReapplyEvents"
	// HistoryRefreshWorkflowTasksScope is the scope used by refresh workflow tasks API
	HistoryRefreshWorkflowTasksScope = "RefreshWorkflowTasks"
	// HistoryGenerateLastHistoryReplicationTasksScope is the scope used by generate last replication tasks API
	HistoryGenerateLastHistoryReplicationTasksScope = "GenerateLastHistoryReplicationTasks"
	// HistoryGetReplicationStatusScope is the scope used by GetReplicationStatus API
	HistoryGetReplicationStatusScope = "GetReplicationStatus"
	// HistoryHistoryRemoveTaskScope is the scope used by remove task API
	HistoryHistoryRemoveTaskScope = "HistoryRemoveTask"
	// HistoryCloseShard is the scope used by close shard API
	HistoryCloseShardScope = "CloseShard"
	// HistoryGetShard is the scope used by get shard API
	HistoryGetShardScope = "GetShard"
	// HistoryReplicateEventsV2 is the scope used by replicate events API
	HistoryReplicateEventsV2Scope = "ReplicateEventsV2"
	// HistoryDescribeHistoryHost is the scope used by describe history host API
	HistoryDescribeHistoryHostScope = "DescribeHistoryHost"
	// HistoryDeleteWorkflowVisibilityRecordScope is the scope used by delete workflow visibility record API
	HistoryDeleteWorkflowVisibilityRecordScope = "DeleteWorkflowVisibilityRecord"
	// HistoryUpdateWorkflowScope is the scope used by update workflow API
	HistoryUpdateWorkflowScope = "UpdateWorkflow"
	// HistoryResetWorkflowExecutionScope tracks ResetWorkflowExecution API calls received by service
	HistoryResetWorkflowExecutionScope = "ResetWorkflowExecution"
	// HistoryQueryWorkflowScope tracks QueryWorkflow API calls received by service
	HistoryQueryWorkflowScope = "QueryWorkflow"
	// HistoryProcessDeleteHistoryEventScope tracks ProcessDeleteHistoryEvent processing calls
	HistoryProcessDeleteHistoryEventScope = "ProcessDeleteHistoryEvent"
	// HistoryDeleteWorkflowExecutionScope tracks DeleteWorkflowExecutions API calls
	HistoryDeleteWorkflowExecutionScope = "DeleteWorkflowExecution"
	// HistoryCacheGetOrCreateScope is the scope used by history cache
	HistoryCacheGetOrCreateScope = "HistoryCacheGetOrCreate"
	// HistoryCacheGetOrCreateCurrentScope is the scope used by history cache
	HistoryCacheGetOrCreateCurrentScope = "CacheGetOrCreateCurrent"
	// TaskPriorityAssignerScope is the scope used by all metric emitted by task priority assigner
	TaskPriorityAssignerScope = "TaskPriorityAssigner"
	// TransferQueueProcessorScope is the scope used by all metric emitted by transfer queue processor
	TransferQueueProcessorScope = "TransferQueueProcessor"
	// TransferActiveQueueProcessorScope is the scope used by all metric emitted by transfer queue processor
	TransferActiveQueueProcessorScope = "TransferActiveQueueProcessor"
	// TransferStandbyQueueProcessorScope is the scope used by all metric emitted by transfer queue processor
	TransferStandbyQueueProcessorScope = "TransferStandbyQueueProcessor"
	// TransferActiveTaskActivityScope is the scope used for activity task processing by transfer queue processor
	TransferActiveTaskActivityScope = "TransferActiveTaskActivity"
	// TransferActiveTaskWorkflowTaskScope is the scope used for workflow task processing by transfer queue processor
	TransferActiveTaskWorkflowTaskScope = "TransferActiveTaskWorkflowTask"
	// TransferActiveTaskCloseExecutionScope is the scope used for close execution task processing by transfer queue processor
	TransferActiveTaskCloseExecutionScope = "TransferActiveTaskCloseExecution"
	// TransferActiveTaskCancelExecutionScope is the scope used for cancel execution task processing by transfer queue processor
	TransferActiveTaskCancelExecutionScope = "TransferActiveTaskCancelExecution"
	// TransferActiveTaskSignalExecutionScope is the scope used for signal execution task processing by transfer queue processor
	TransferActiveTaskSignalExecutionScope = "TransferActiveTaskSignalExecution"
	// TransferActiveTaskStartChildExecutionScope is the scope used for start child execution task processing by transfer queue processor
	TransferActiveTaskStartChildExecutionScope = "TransferActiveTaskStartChildExecution"
	// TransferActiveTaskResetWorkflowScope is the scope used for record workflow started task processing by transfer queue processor
	TransferActiveTaskResetWorkflowScope = "TransferActiveTaskResetWorkflow"
	// TransferStandbyTaskResetWorkflowScope is the scope used for record workflow started task processing by transfer queue processor
	TransferStandbyTaskResetWorkflowScope = "TransferStandbyTaskResetWorkflow"
	// TransferStandbyTaskActivityScope is the scope used for activity task processing by transfer queue processor
	TransferStandbyTaskActivityScope = "TransferStandbyTaskActivity"
	// TransferStandbyTaskWorkflowTaskScope is the scope used for workflow task processing by transfer queue processor
	TransferStandbyTaskWorkflowTaskScope = "TransferStandbyTaskWorkflowTask"
	// TransferStandbyTaskCloseExecutionScope is the scope used for close execution task processing by transfer queue processor
	TransferStandbyTaskCloseExecutionScope = "TransferStandbyTaskCloseExecution"
	// TransferStandbyTaskCancelExecutionScope is the scope used for cancel execution task processing by transfer queue processor
	TransferStandbyTaskCancelExecutionScope = "TransferStandbyTaskCancelExecution"
	// TransferStandbyTaskSignalExecutionScope is the scope used for signal execution task processing by transfer queue processor
	TransferStandbyTaskSignalExecutionScope = "TransferStandbyTaskSignalExecution"
	// TransferStandbyTaskStartChildExecutionScope is the scope used for start child execution task processing by transfer queue processor
	TransferStandbyTaskStartChildExecutionScope = "TransferStandbyTaskStartChildExecution"

	// VisibilityQueueProcessorScope is the scope used by all metric emitted by visibility queue processor
	VisibilityQueueProcessorScope = "VisibilityQueueProcessor"
	// VisibilityTaskStartExecutionScope is the scope used for start execution processing by visibility queue processor
	VisibilityTaskStartExecutionScope = "VisibilityTaskStartExecution"
	// VisibilityTaskUpsertExecutionScope is the scope used for upsert execution processing by visibility queue processor
	VisibilityTaskUpsertExecutionScope = "VisibilityTaskUpsertExecution"
	// VisibilityTaskCloseExecutionScope is the scope used for close execution attributes processing by visibility queue processor
	VisibilityTaskCloseExecutionScope = "VisibilityTaskCloseExecution"
	// VisibilityTaskDeleteExecutionScope is the scope used for delete by visibility queue processor
	VisibilityTaskDeleteExecutionScope = "VisibilityTaskDeleteExecution"

	// TimerQueueProcessorScope is the scope used by all metric emitted by timer queue processor
	TimerQueueProcessorScope = "TimerQueueProcessor"
	// TimerActiveQueueProcessorScope is the scope used by all metric emitted by timer queue processor
	TimerActiveQueueProcessorScope = "TimerActiveQueueProcessor"
	// TimerStandbyQueueProcessorScope is the scope used by all metric emitted by timer queue processor
	TimerStandbyQueueProcessorScope = "TimerStandbyQueueProcessor"
	// TimerActiveTaskActivityTimeoutScope is the scope used by metric emitted by timer queue processor for processing activity timeouts
	TimerActiveTaskActivityTimeoutScope = "TimerActiveTaskActivityTimeout"
	// TimerActiveTaskWorkflowTaskTimeoutScope is the scope used by metric emitted by timer queue processor for processing workflow task timeouts
	TimerActiveTaskWorkflowTaskTimeoutScope = "TimerActiveTaskWorkflowTaskTimeout"
	// TimerActiveTaskUserTimerScope is the scope used by metric emitted by timer queue processor for processing user timers
	TimerActiveTaskUserTimerScope = "TimerActiveTaskUserTimer"
	// TimerActiveTaskWorkflowTimeoutScope is the scope used by metric emitted by timer queue processor for processing workflow timeouts.
	TimerActiveTaskWorkflowTimeoutScope = "TimerActiveTaskWorkflowTimeout"
	// TimerActiveTaskActivityRetryTimerScope is the scope used by metric emitted by timer queue processor for processing retry task.
	TimerActiveTaskActivityRetryTimerScope = "TimerActiveTaskActivityRetryTimer"
	// TimerActiveTaskWorkflowBackoffTimerScope is the scope used by metric emitted by timer queue processor for processing retry task.
	TimerActiveTaskWorkflowBackoffTimerScope = "TimerActiveTaskWorkflowBackoffTimer"
	// TimerActiveTaskDeleteHistoryEventScope is the scope used by metric emitted by timer queue processor for processing history event cleanup
	TimerActiveTaskDeleteHistoryEventScope = "TimerActiveTaskDeleteHistoryEvent"
	// TimerStandbyTaskActivityTimeoutScope is the scope used by metric emitted by timer queue processor for processing activity timeouts
	TimerStandbyTaskActivityTimeoutScope = "TimerStandbyTaskActivityTimeout"
	// TimerStandbyTaskWorkflowTaskTimeoutScope is the scope used by metric emitted by timer queue processor for processing workflow task timeouts
	TimerStandbyTaskWorkflowTaskTimeoutScope = "TimerStandbyTaskWorkflowTaskTimeout"
	// TimerStandbyTaskUserTimerScope is the scope used by metric emitted by timer queue processor for processing user timers
	TimerStandbyTaskUserTimerScope = "TimerStandbyTaskUserTimer"
	// TimerStandbyTaskWorkflowTimeoutScope is the scope used by metric emitted by timer queue processor for processing workflow timeouts.
	TimerStandbyTaskWorkflowTimeoutScope = "TimerStandbyTaskWorkflowTimeout"
	// TimerStandbyTaskActivityRetryTimerScope is the scope used by metric emitted by timer queue processor for processing retry task.
	TimerStandbyTaskActivityRetryTimerScope = "TimerStandbyTaskActivityRetryTimer"
	// TimerStandbyTaskDeleteHistoryEventScope is the scope used by metric emitted by timer queue processor for processing history event cleanup
	TimerStandbyTaskDeleteHistoryEventScope = "TimerStandbyTaskDeleteHistoryEvent"
	// TimerStandbyTaskWorkflowBackoffTimerScope is the scope used by metric emitted by timer queue processor for processing retry task.
	TimerStandbyTaskWorkflowBackoffTimerScope = "TimerStandbyTaskWorkflowBackoffTimer"

	// ReplicatorQueueProcessorScope is the scope used by all metric emitted by replicator queue processor
	ReplicatorQueueProcessorScope = "ReplicatorQueueProcessor"
	// ReplicatorTaskHistoryScope is the scope used for history task processing by replicator queue processor
	ReplicatorTaskHistoryScope = "ReplicatorTaskHistory"
	// ReplicatorTaskSyncActivityScope is the scope used for sync activity by replicator queue processor
	ReplicatorTaskSyncActivityScope = "ReplicatorTaskSyncActivity"
	// ReplicateHistoryEventsScope is the scope used by historyReplicator API for applying events
	ReplicateHistoryEventsScope = "ReplicateHistoryEvents"
	// HistoryRereplicationByTransferTaskScope tracks history replication calls made by transfer task
	HistoryRereplicationByTransferTaskScope = "HistoryRereplicationByTransferTask"
	// HistoryRereplicationByTimerTaskScope tracks history replication calls made by timer task
	HistoryRereplicationByTimerTaskScope = "HistoryRereplicationByTimerTask"
	// HistoryRereplicationByHistoryReplicationScope tracks history replication calls made by history replication
	HistoryRereplicationByHistoryReplicationScope = "HistoryRereplicationByHistoryReplication"
	// HistoryRereplicationByHistoryMetadataReplicationScope tracks history replication calls made by history replication
	HistoryRereplicationByHistoryMetadataReplicationScope = "HistoryRereplicationByHistoryMetadataReplication"
	// HistoryRereplicationByActivityReplicationScope tracks history replication calls made by activity replication
	HistoryRereplicationByActivityReplicationScope = "HistoryRereplicationByActivityReplication"

	// ShardInfoScope is the scope used when updating shard info
	ShardInfoScope = "ShardInfo"
	// WorkflowContextScope is the scope used by WorkflowContext component
	WorkflowContextScope = "WorkflowContext"
	// ExecutionStatsScope is the scope used for emiting workflow execution related stats
	ExecutionStatsScope = "ExecutionStats"
	// SessionStatsScope is the scope used for emiting session update related stats
	SessionStatsScope = "SessionStats"
	// WorkflowCompletionStatsScope tracks workflow completion updates
	WorkflowCompletionStatsScope = "CompletionStats"
	// ReplicationTaskFetcherScope is scope used by all metrics emitted by ReplicationTaskFetcher
	ReplicationTaskFetcherScope = "ReplicationTaskFetcher"
	// ReplicationTaskCleanupScope is scope used by all metrics emitted by ReplicationTaskProcessor cleanup
	ReplicationTaskCleanupScope = "ReplicationTaskCleanup"
	// ReplicationDLQStatsScope is scope used by all metrics emitted related to replication DLQ
	ReplicationDLQStatsScope = "ReplicationDLQStats"
	// SyncWorkflowStateTaskScope is the scope used by closed workflow task replication processing
	SyncWorkflowStateTaskScope = "SyncWorkflowStateTask"
	// EventsCacheGetEventScope is the scope used by events cache
	EventsCacheGetEventScope = "EventsCacheGetEvent"
	// EventsCachePutEventScope is the scope used by events cache
	EventsCachePutEventScope = "EventsCachePutEvent"
	// EventsCacheDeleteEventScope is the scope used by events cache
	EventsCacheDeleteEventScope = "EventsCacheDeleteEvent"
	// EventsCacheGetFromStoreScope is the scope used by events cache
	EventsCacheGetFromStoreScope = "EventsCacheGetFromStore"
	// HistoryEventNotificationScope is the scope used by shard history event notification
	HistoryEventNotificationScope = "HistoryEventNotification"
	// ArchiverClientScope is scope used by all metrics emitted by archiver.Client
	ArchiverClientScope = "ArchiverClient"
	// DeadlockDetectorScope is a scope for deadlock detector
	DeadlockDetectorScope = "DeadlockDetector"
	// OperationTimerQueueProcessorScope is a scope for timer queue base processor
	OperationTimerQueueProcessorScope = "TimerQueueProcessor"
	// OperationTimerActiveQueueProcessorScope is a scope for timer queue active processor
	OperationTimerActiveQueueProcessorScope = "TimerActiveQueueProcessor"
	// OperationTimerStandbyQueueProcessorScope is a scope for timer queue standby processor
	OperationTimerStandbyQueueProcessorScope = "TimerStandbyQueueProcessor"
	// OperationTransferQueueProcessorScope is a scope for transfer queue base processor
	OperationTransferQueueProcessorScope = "TransferQueueProcessor"
	// OperationTransferActiveQueueProcessorScope is a scope for transfer queue active processor
	OperationTransferActiveQueueProcessorScope = "TransferActiveQueueProcessor"
	// OperationTransferStandbyQueueProcessorScope is a scope for transfer queue standby processor
	OperationTransferStandbyQueueProcessorScope = "TransferStandbyQueueProcessor"
	// OperationVisibilityQueueProcessorScope is a scope for visibility queue processor
	OperationVisibilityQueueProcessorScope = "VisibilityQueueProcessor"
	// OperationArchivalQueueProcessorScope is a scope for archival queue processor
	OperationArchivalQueueProcessorScope = "ArchivalQueueProcessor"
)

// Matching Scope
const (
	// MatchingPollWorkflowTaskQueueScope tracks PollWorkflowTaskQueue API calls received by service
	MatchingPollWorkflowTaskQueueScope = "PollWorkflowTaskQueue"
	// MatchingPollActivityTaskQueueScope tracks PollActivityTaskQueue API calls received by service
	MatchingPollActivityTaskQueueScope = "PollActivityTaskQueue"
	// MatchingAddActivityTaskScope tracks AddActivityTask API calls received by service
	MatchingAddActivityTaskScope = "AddActivityTask"
	// MatchingAddWorkflowTaskScope tracks AddWorkflowTask API calls received by service
	MatchingAddWorkflowTaskScope = "AddWorkflowTask"
	// MatchingTaskQueueMgrScope is the metrics scope for matching.TaskQueueManager component
	MatchingTaskQueueMgrScope = "TaskQueueMgr"
	// MatchingEngineScope is the metrics scope for matchingEngine component
	MatchingEngineScope = "MatchingEngine"
	// MatchingQueryWorkflowScope tracks AddWorkflowTask API calls received by service
	MatchingQueryWorkflowScope = "QueryWorkflow"
	// MatchingRespondQueryTaskCompletedScope tracks AddWorkflowTask API calls received by service
	MatchingRespondQueryTaskCompletedScope = "RespondQueryTaskCompleted"
	// MatchingCancelOutstandingPollScope tracks CancelOutstandingPoll API calls received by service
	MatchingCancelOutstandingPollScope = "CancelOutstandingPoll"
	// MatchingDescribeTaskQueueScope tracks DescribeTaskQueue API calls received by service
	MatchingDescribeTaskQueueScope = "DescribeTaskQueue"
	// MatchingListTaskQueuePartitionsScope tracks ListTaskQueuePartitions API calls received by service
	MatchingListTaskQueuePartitionsScope = "ListTaskQueuePartitions"
	// MatchingUpdateWorkerBuildIdOrderingScope tracks UpdateWorkerBuildIdOrdering API calls received by service
	MatchingUpdateWorkerBuildIdOrderingScope = "UpdateWorkerBuildIdOrdering"
	// MatchingGetWorkerBuildIdOrderingScope tracks GetWorkerBuildIdOrdering API calls received by service
	MatchingGetWorkerBuildIdOrderingScope = "GetWorkerBuildIdOrdering"
	// MatchingInvalidateTaskQueueMetadataScope tracks GetWorkerBuildIdOrdering API calls received by service
	MatchingInvalidateTaskQueueMetadataScope = "InvalidateTaskQueueMetadata"
	// MatchingGetTaskQueueMetadataScope tracks GetWorkerBuildIdOrdering API calls received by service
	MatchingGetTaskQueueMetadataScope = "GetTaskQueueMetadata"
)

// Worker Scope
const (
	// HistoryArchiverScope is used by history archivers
	HistoryArchiverScope = "HistoryArchiver"
	// VisibilityArchiverScope is used by visibility archivers
	VisibilityArchiverScope = "VisibilityArchiver"
	// HistoryScavengerScope is scope used by all metrics emitted by worker.history.Scavenger module
	HistoryScavengerScope = "HistoryScavenger"
	// ArchiverDeleteHistoryActivityScope is scope used by all metrics emitted by archiver.DeleteHistoryActivity
	ArchiverDeleteHistoryActivityScope = "ArchiverDeleteHistoryActivity"
	// ArchiverUploadHistoryActivityScope is scope used by all metrics emitted by archiver.UploadHistoryActivity
	ArchiverUploadHistoryActivityScope = "ArchiverUploadHistoryActivity"
	// ArchiverArchiveVisibilityActivityScope is scope used by all metrics emitted by archiver.ArchiveVisibilityActivity
	ArchiverArchiveVisibilityActivityScope = "ArchiverArchiveVisibilityActivity"
	// ArchiverScope is scope used by all metrics emitted by archiver.Archiver
	ArchiverScope = "Archiver"
	// ArchiverPumpScope is scope used by all metrics emitted by archiver.Pump
	ArchiverPumpScope = "ArchiverPump"
	// ArchiverArchivalWorkflowScope is scope used by all metrics emitted by archiver.ArchivalWorkflow
	ArchiverArchivalWorkflowScope = "ArchiverArchivalWorkflow"
	// AddSearchAttributesWorkflowScope is scope used by all metrics emitted by worker.AddSearchAttributesWorkflowScope module
	AddSearchAttributesWorkflowScope = "AddSearchAttributesWorkflow"
	// BatcherScope is scope used by all metrics emitted by worker.Batcher module
	BatcherScope = "Batcher"
	// ElasticsearchBulkProcessor is scope used by all metric emitted by Elasticsearch bulk processor
	ElasticsearchBulkProcessor = "ElasticsearchBulkProcessor"
	// ElasticsearchVisibility is scope used by all Elasticsearch visibility metrics
	ElasticsearchVisibility = "ElasticsearchVisibility"
	// MigrationWorkflowScope is scope used by metrics emitted by migration related workflows
	MigrationWorkflowScope = "MigrationWorkflow"
	// ReplicatorScope is the scope used by all metric emitted by replicator
	ReplicatorScope = "Replicator"
	// NamespaceReplicationTaskScope is the scope used by namespace task replication processing
	NamespaceReplicationTaskScope = "NamespaceReplicationTask"
	// HistoryReplicationTaskScope is the scope used by history task replication processing
	HistoryReplicationTaskScope = "HistoryReplicationTask"
	// HistoryMetadataReplicationTaskScope is the scope used by history metadata task replication processing
	HistoryMetadataReplicationTaskScope = "HistoryMetadataReplicationTask"
	// SyncShardTaskScope is the scope used by sync shrad information processing
	SyncShardTaskScope = "SyncShardTask"
	// SyncActivityTaskScope is the scope used by sync activity information processing
	SyncActivityTaskScope = "SyncActivityTask"
	// ESProcessorScope is scope used by all metric emitted by esProcessor
	ESProcessorScope = "ESProcessor"
	// IndexProcessorScope is scope used by all metric emitted by index processor
	IndexProcessorScope = "IndexProcessor"
	// ParentClosePolicyProcessorScope is scope used by all metrics emitted by worker.ParentClosePolicyProcessor
	ParentClosePolicyProcessorScope = "ParentClosePolicyProcessor"
	DeleteNamespaceWorkflowScope    = "DeleteNamespaceWorkflow"
	ReclaimResourcesWorkflowScope   = "ReclaimResourcesWorkflow"
	DeleteExecutionsWorkflowScope   = "DeleteExecutionsWorkflow"
)

// History task type
const (
	TaskTypeTransferActiveTaskActivity             = "TransferActiveTaskActivity"
	TaskTypeTransferActiveTaskWorkflowTask         = "TransferActiveTaskWorkflowTask"
	TaskTypeTransferActiveTaskCloseExecution       = "TransferActiveTaskCloseExecution"
	TaskTypeTransferActiveTaskCancelExecution      = "TransferActiveTaskCancelExecution"
	TaskTypeTransferActiveTaskSignalExecution      = "TransferActiveTaskSignalExecution"
	TaskTypeTransferActiveTaskStartChildExecution  = "TransferActiveTaskStartChildExecution"
	TaskTypeTransferActiveTaskResetWorkflow        = "TransferActiveTaskResetWorkflow"
	TaskTypeTransferStandbyTaskActivity            = "TransferStandbyTaskActivity"
	TaskTypeTransferStandbyTaskWorkflowTask        = "TransferStandbyTaskWorkflowTask"
	TaskTypeTransferStandbyTaskCloseExecution      = "TransferStandbyTaskCloseExecution"
	TaskTypeTransferStandbyTaskCancelExecution     = "TransferStandbyTaskCancelExecution"
	TaskTypeTransferStandbyTaskSignalExecution     = "TransferStandbyTaskSignalExecution"
	TaskTypeTransferStandbyTaskStartChildExecution = "TransferStandbyTaskStartChildExecution"
	TaskTypeTransferStandbyTaskResetWorkflow       = "TransferStandbyTaskResetWorkflow"
	TaskTypeVisibilityTaskStartExecution           = "VisibilityTaskStartExecution"
	TaskTypeVisibilityTaskUpsertExecution          = "VisibilityTaskUpsertExecution"
	TaskTypeVisibilityTaskCloseExecution           = "VisibilityTaskCloseExecution"
	TaskTypeVisibilityTaskDeleteExecution          = "VisibilityTaskDeleteExecution"
	TaskTypeArchivalTaskArchiveExecution           = "ArchivalTaskArchiveExecution"
	TaskTypeTimerActiveTaskActivityTimeout         = "TimerActiveTaskActivityTimeout"
	TaskTypeTimerActiveTaskWorkflowTaskTimeout     = "TimerActiveTaskWorkflowTaskTimeout"
	TaskTypeTimerActiveTaskUserTimer               = "TimerActiveTaskUserTimer"
	TaskTypeTimerActiveTaskWorkflowTimeout         = "TimerActiveTaskWorkflowTimeout"
	TaskTypeTimerActiveTaskActivityRetryTimer      = "TimerActiveTaskActivityRetryTimer"
	TaskTypeTimerActiveTaskWorkflowBackoffTimer    = "TimerActiveTaskWorkflowBackoffTimer"
	TaskTypeTimerActiveTaskDeleteHistoryEvent      = "TimerActiveTaskDeleteHistoryEvent"
	TaskTypeTimerStandbyTaskActivityTimeout        = "TimerStandbyTaskActivityTimeout"
	TaskTypeTimerStandbyTaskWorkflowTaskTimeout    = "TimerStandbyTaskWorkflowTaskTimeout"
	TaskTypeTimerStandbyTaskUserTimer              = "TimerStandbyTaskUserTimer"
	TaskTypeTimerStandbyTaskWorkflowTimeout        = "TimerStandbyTaskWorkflowTimeout"
	TaskTypeTimerStandbyTaskActivityRetryTimer     = "TimerStandbyTaskActivityRetryTimer"
	TaskTypeTimerStandbyTaskWorkflowBackoffTimer   = "TimerStandbyTaskWorkflowBackoffTimer"
	TaskTypeTimerStandbyTaskDeleteHistoryEvent     = "TimerStandbyTaskDeleteHistoryEvent"
)

var (
	ServiceRequests                               = NewCounterDef("service_requests")
	ServicePendingRequests                        = NewGaugeDef("service_pending_requests")
	ServiceFailures                               = NewCounterDef("service_errors")
	ServiceErrorWithType                          = NewCounterDef("service_error_with_type")
	ServiceCriticalFailures                       = NewCounterDef("service_errors_critical")
	ServiceLatency                                = NewTimerDef("service_latency")
	ServiceLatencyNoUserLatency                   = NewTimerDef("service_latency_nouserlatency")
	ServiceLatencyUserLatency                     = NewTimerDef("service_latency_userlatency")
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
	ServiceErrUnauthorizedCounter                 = NewCounterDef("service_errors_unauthorized")
	ServiceErrAuthorizeFailedCounter              = NewCounterDef("service_errors_authorize_failed")
	ActionCounter                                 = NewCounterDef("action")
	TlsCertsExpired                               = NewGaugeDef("certificates_expired")
	TlsCertsExpiring                              = NewGaugeDef("certificates_expiring")
	ServiceAuthorizationLatency                   = NewTimerDef("service_authorization_latency")
	EventBlobSize                                 = NewBytesHistogramDef("event_blob_size")
	NamespaceCachePrepareCallbacksLatency         = NewTimerDef("namespace_cache_prepare_callbacks_latency")
	NamespaceCacheCallbacksLatency                = NewTimerDef("namespace_cache_callbacks_latency")
	LockRequests                                  = NewCounterDef("lock_requests")
	LockFailures                                  = NewCounterDef("lock_failures")
	LockLatency                                   = NewTimerDef("lock_latency")
	ClientRequests                                = NewCounterDef("client_requests")
	ClientFailures                                = NewCounterDef("client_errors")
	ClientLatency                                 = NewTimerDef("client_latency")
	ClientRedirectionRequests                     = NewCounterDef("client_redirection_requests")
	ClientRedirectionFailures                     = NewCounterDef("client_redirection_errors")
	ClientRedirectionLatency                      = NewTimerDef("client_redirection_latency")
	StateTransitionCount                          = NewDimensionlessHistogramDef("state_transition_count")
	HistorySize                                   = NewBytesHistogramDef("history_size")
	HistoryCount                                  = NewDimensionlessHistogramDef("history_count")
	SearchAttributesSize                          = NewBytesHistogramDef("search_attributes_size")
	MemoSize                                      = NewBytesHistogramDef("memo_size")
	TooManyPendingChildWorkflows                  = NewCounterDef("wf_too_many_pending_child_workflows")
	TooManyPendingActivities                      = NewCounterDef("wf_too_many_pending_activities")
	TooManyPendingCancelRequests                  = NewCounterDef("wf_too_many_pending_cancel_requests")
	TooManyPendingSignalsToExternalWorkflows      = NewCounterDef("wf_too_many_pending_external_workflow_signals")

	// Frontend
	AddSearchAttributesWorkflowSuccessCount  = NewCounterDef("add_search_attributes_workflow_success")
	AddSearchAttributesWorkflowFailuresCount = NewCounterDef("add_search_attributes_workflow_failure")
	DeleteNamespaceWorkflowSuccessCount      = NewCounterDef("delete_namespace_workflow_success")
	DeleteNamespaceWorkflowFailuresCount     = NewCounterDef("delete_namespace_workflow_failure")
	VersionCheckSuccessCount                 = NewCounterDef("version_check_success")
	VersionCheckFailedCount                  = NewCounterDef("version_check_failed")
	VersionCheckRequestFailedCount           = NewCounterDef("version_check_request_failed")
	VersionCheckLatency                      = NewTimerDef("version_check_latency")

	// History
	CacheRequests                                     = NewCounterDef("cache_requests")
	CacheFailures                                     = NewCounterDef("cache_errors")
	CacheLatency                                      = NewTimerDef("cache_latency")
	CacheMissCounter                                  = NewCounterDef("cache_miss")
	HistoryEventNotificationQueueingLatency           = NewTimerDef("history_event_notification_queueing_latency")
	HistoryEventNotificationFanoutLatency             = NewTimerDef("history_event_notification_fanout_latency")
	HistoryEventNotificationInFlightMessageGauge      = NewGaugeDef("history_event_notification_inflight_message_gauge")
	HistoryEventNotificationFailDeliveryCount         = NewCounterDef("history_event_notification_fail_delivery_count")
	ArchiverClientSendSignalCount                     = NewCounterDef("archiver_client_sent_signal")
	ArchiverClientSendSignalFailureCount              = NewCounterDef("archiver_client_send_signal_error")
	ArchiverClientHistoryRequestCount                 = NewCounterDef("archiver_client_history_request")
	ArchiverClientHistoryInlineArchiveAttemptCount    = NewCounterDef("archiver_client_history_inline_archive_attempt")
	ArchiverClientHistoryInlineArchiveFailureCount    = NewCounterDef("archiver_client_history_inline_archive_failure")
	ArchiverClientVisibilityRequestCount              = NewCounterDef("archiver_client_visibility_request")
	ArchiverClientVisibilityInlineArchiveAttemptCount = NewCounterDef("archiver_client_visibility_inline_archive_attempt")
	ArchiverClientVisibilityInlineArchiveFailureCount = NewCounterDef("archiver_client_visibility_inline_archive_failure")
	ArchiverArchiveLatency                            = NewTimerDef("archiver_archive_latency")
	ArchiverArchiveTargetLatency                      = NewTimerDef("archiver_archive_target_latency")
	ShardContextClosedCounter                         = NewCounterDef("shard_closed_count")
	ShardContextCreatedCounter                        = NewCounterDef("sharditem_created_count")
	ShardContextRemovedCounter                        = NewCounterDef("sharditem_removed_count")
	ShardContextAcquisitionLatency                    = NewTimerDef("sharditem_acquisition_latency")
	ShardInfoReplicationPendingTasksTimer             = NewDimensionlessHistogramDef("shardinfo_replication_pending_task")
	ShardInfoTransferActivePendingTasksTimer          = NewDimensionlessHistogramDef("shardinfo_transfer_active_pending_task")
	ShardInfoTransferStandbyPendingTasksTimer         = NewDimensionlessHistogramDef("shardinfo_transfer_standby_pending_task")
	ShardInfoTimerActivePendingTasksTimer             = NewDimensionlessHistogramDef("shardinfo_timer_active_pending_task")
	ShardInfoTimerStandbyPendingTasksTimer            = NewDimensionlessHistogramDef("shardinfo_timer_standby_pending_task")
	ShardInfoVisibilityPendingTasksTimer              = NewDimensionlessHistogramDef("shardinfo_visibility_pending_task")
	ShardInfoReplicationLagHistogram                  = NewDimensionlessHistogramDef("shardinfo_replication_lag")
	ShardInfoTransferLagHistogram                     = NewDimensionlessHistogramDef("shardinfo_transfer_lag")
	ShardInfoTimerLagTimer                            = NewTimerDef("shardinfo_timer_lag")
	ShardInfoVisibilityLagHistogram                   = NewDimensionlessHistogramDef("shardinfo_visibility_lag")
	ShardInfoTransferDiffHistogram                    = NewDimensionlessHistogramDef("shardinfo_transfer_diff")
	ShardInfoTimerDiffTimer                           = NewTimerDef("shardinfo_timer_diff")
	ShardInfoTransferFailoverInProgressHistogram      = NewDimensionlessHistogramDef("shardinfo_transfer_failover_in_progress")
	ShardInfoTimerFailoverInProgressHistogram         = NewDimensionlessHistogramDef("shardinfo_timer_failover_in_progress")
	ShardInfoTransferFailoverLatencyTimer             = NewTimerDef("shardinfo_transfer_failover_latency")
	ShardInfoTimerFailoverLatencyTimer                = NewTimerDef("shardinfo_timer_failover_latency")
	SyncShardFromRemoteCounter                        = NewCounterDef("syncshard_remote_count")
	SyncShardFromRemoteFailure                        = NewCounterDef("syncshard_remote_failed")
	TaskRequests                                      = NewCounterDef("task_requests")
	TaskLoadLatency                                   = NewTimerDef("task_latency_load")       // latency from task generation to task loading (persistence scheduleToStart)
	TaskScheduleLatency                               = NewTimerDef("task_latency_schedule")   // latency from task submission to in-memory queue to processing (in-memory scheduleToStart)
	TaskProcessingLatency                             = NewTimerDef("task_latency_processing") // latency for processing task one time
	TaskProcessingUserLatency                         = NewTimerDef("task_latency_user")       // latency for locking workflow execution
	TaskLatency                                       = NewTimerDef("task_latency")            // task in-memory latency across multiple attempts
	TaskQueueLatency                                  = NewTimerDef("task_latency_queue")      // task e2e latency
	TaskAttempt                                       = NewDimensionlessHistogramDef("task_attempt")
	TaskFailures                                      = NewCounterDef("task_errors")
	TaskDiscarded                                     = NewCounterDef("task_errors_discarded")
	TaskSkipped                                       = NewCounterDef("task_skipped")
	TaskVersionMisMatch                               = NewCounterDef("task_errors_version_mismatch")
	TasksDependencyTaskNotCompleted                   = NewCounterDef("task_dependency_task_not_completed")
	TaskStandbyRetryCounter                           = NewCounterDef("task_errors_standby_retry_counter")
	TaskWorkflowBusyCounter                           = NewCounterDef("task_errors_workflow_busy")
	TaskNotActiveCounter                              = NewCounterDef("task_errors_not_active_counter")
	TaskLimitExceededCounter                          = NewCounterDef("task_errors_limit_exceeded_counter")
	TaskScheduleToStartLatency                        = NewTimerDef("task_schedule_to_start_latency")
	TransferTaskMissingEventCounter                   = NewCounterDef("transfer_task_missing_event_counter")
	TaskBatchCompleteCounter                          = NewCounterDef("task_batch_complete_counter")
	TaskReschedulerPendingTasks                       = NewDimensionlessHistogramDef("task_rescheduler_pending_tasks")
	TaskThrottledCounter                              = NewCounterDef("task_throttled_counter")
	PendingTasksCounter                               = NewDimensionlessHistogramDef("pending_tasks")
	QueueScheduleLatency                              = NewTimerDef("queue_latency_schedule") // latency for scheduling 100 tasks in one task channel
	QueueReaderCountHistogram                         = NewDimensionlessHistogramDef("queue_reader_count")
	QueueSliceCountHistogram                          = NewDimensionlessHistogramDef("queue_slice_count")
	QueueActionCounter                                = NewCounterDef("queue_actions")
	ActivityE2ELatency                                = NewTimerDef("activity_end_to_end_latency")
	AckLevelUpdateCounter                             = NewCounterDef("ack_level_update")
	AckLevelUpdateFailedCounter                       = NewCounterDef("ack_level_update_failed")
	CommandTypeScheduleActivityCounter                = NewCounterDef("schedule_activity_command")
	CommandTypeCompleteWorkflowCounter                = NewCounterDef("complete_workflow_command")
	CommandTypeFailWorkflowCounter                    = NewCounterDef("fail_workflow_command")
	CommandTypeCancelWorkflowCounter                  = NewCounterDef("cancel_workflow_command")
	CommandTypeStartTimerCounter                      = NewCounterDef("start_timer_command")
	CommandTypeCancelActivityCounter                  = NewCounterDef("cancel_activity_command")
	CommandTypeCancelTimerCounter                     = NewCounterDef("cancel_timer_command")
	CommandTypeRecordMarkerCounter                    = NewCounterDef("record_marker_command")
	CommandTypeCancelExternalWorkflowCounter          = NewCounterDef("cancel_external_workflow_command")
	CommandTypeContinueAsNewCounter                   = NewCounterDef("continue_as_new_command")
	CommandTypeSignalExternalWorkflowCounter          = NewCounterDef("signal_external_workflow_command")
	CommandTypeUpsertWorkflowSearchAttributesCounter  = NewCounterDef("upsert_workflow_search_attributes_command")
	CommandTypeModifyWorkflowPropertiesCounter        = NewCounterDef("modify_workflow_properties_command")
	CommandTypeChildWorkflowCounter                   = NewCounterDef("child_workflow_command")
	ActivityEagerExecutionCounter                     = NewCounterDef("activity_eager_execution")
	EmptyCompletionCommandsCounter                    = NewCounterDef("empty_completion_commands")
	MultipleCompletionCommandsCounter                 = NewCounterDef("multiple_completion_commands")
	FailedWorkflowTasksCounter                        = NewCounterDef("failed_workflow_tasks")
	WorkflowTaskAttempt                               = NewDimensionlessHistogramDef("workflow_task_attempt")
	StaleMutableStateCounter                          = NewCounterDef("stale_mutable_state")
	AutoResetPointsLimitExceededCounter               = NewCounterDef("auto_reset_points_exceed_limit")
	AutoResetPointCorruptionCounter                   = NewCounterDef("auto_reset_point_corruption")
	ConcurrencyUpdateFailureCounter                   = NewCounterDef("concurrency_update_failure")
	ServiceErrShardOwnershipLostCounter               = NewCounterDef("service_errors_shard_ownership_lost")
	ServiceErrTaskAlreadyStartedCounter               = NewCounterDef("service_errors_task_already_started")
	HeartbeatTimeoutCounter                           = NewCounterDef("heartbeat_timeout")
	ScheduleToStartTimeoutCounter                     = NewCounterDef("schedule_to_start_timeout")
	StartToCloseTimeoutCounter                        = NewCounterDef("start_to_close_timeout")
	ScheduleToCloseTimeoutCounter                     = NewCounterDef("schedule_to_close_timeout")
	NewTimerNotifyCounter                             = NewCounterDef("new_timer_notifications")
	AcquireShardsCounter                              = NewCounterDef("acquire_shards_count")
	AcquireShardsLatency                              = NewTimerDef("acquire_shards_latency")
	MembershipChangedCounter                          = NewCounterDef("membership_changed_count")
	NumShardsGauge                                    = NewGaugeDef("numshards_gauge")
	GetEngineForShardErrorCounter                     = NewCounterDef("get_engine_for_shard_errors")
	GetEngineForShardLatency                          = NewTimerDef("get_engine_for_shard_latency")
	RemoveEngineForShardLatency                       = NewTimerDef("remove_engine_for_shard_latency")
	CompleteWorkflowTaskWithStickyEnabledCounter      = NewCounterDef("complete_workflow_task_sticky_enabled_count")
	CompleteWorkflowTaskWithStickyDisabledCounter     = NewCounterDef("complete_workflow_task_sticky_disabled_count")
	WorkflowTaskHeartbeatTimeoutCounter               = NewCounterDef("workflow_task_heartbeat_timeout_count")
	EmptyReplicationEventsCounter                     = NewCounterDef("empty_replication_events")
	DuplicateReplicationEventsCounter                 = NewCounterDef("duplicate_replication_events")
	StaleReplicationEventsCounter                     = NewCounterDef("stale_replication_events")
	ReplicationEventsSizeTimer                        = NewTimerDef("replication_events_size")
	BufferReplicationTaskTimer                        = NewTimerDef("buffer_replication_tasks")
	UnbufferReplicationTaskTimer                      = NewTimerDef("unbuffer_replication_tasks")
	HistoryConflictsCounter                           = NewCounterDef("history_conflicts")
	CompleteTaskFailedCounter                         = NewCounterDef("complete_task_fail_count")
	AcquireLockFailedCounter                          = NewCounterDef("acquire_lock_failed")
	WorkflowContextCleared                            = NewCounterDef("workflow_context_cleared")
	MutableStateSize                                  = NewBytesHistogramDef("mutable_state_size")
	ExecutionInfoSize                                 = NewBytesHistogramDef("execution_info_size")
	ExecutionStateSize                                = NewBytesHistogramDef("execution_state_size")
	ActivityInfoSize                                  = NewBytesHistogramDef("activity_info_size")
	TimerInfoSize                                     = NewBytesHistogramDef("timer_info_size")
	ChildInfoSize                                     = NewBytesHistogramDef("child_info_size")
	RequestCancelInfoSize                             = NewBytesHistogramDef("request_cancel_info_size")
	SignalInfoSize                                    = NewBytesHistogramDef("signal_info_size")
	BufferedEventsSize                                = NewBytesHistogramDef("buffered_events_size")
	ActivityInfoCount                                 = NewDimensionlessHistogramDef("activity_info_count")
	TimerInfoCount                                    = NewDimensionlessHistogramDef("timer_info_count")
	ChildInfoCount                                    = NewDimensionlessHistogramDef("child_info_count")
	SignalInfoCount                                   = NewDimensionlessHistogramDef("signal_info_count")
	RequestCancelInfoCount                            = NewDimensionlessHistogramDef("request_cancel_info_count")
	BufferedEventsCount                               = NewDimensionlessHistogramDef("buffered_events_count")
	TaskCount                                         = NewDimensionlessHistogramDef("task_count")
	WorkflowRetryBackoffTimerCount                    = NewCounterDef("workflow_retry_backoff_timer")
	WorkflowCronBackoffTimerCount                     = NewCounterDef("workflow_cron_backoff_timer")
	WorkflowCleanupDeleteCount                        = NewCounterDef("workflow_cleanup_delete")
	WorkflowCleanupArchiveCount                       = NewCounterDef("workflow_cleanup_archive")
	WorkflowCleanupNopCount                           = NewCounterDef("workflow_cleanup_nop")
	WorkflowCleanupDeleteHistoryInlineCount           = NewCounterDef("workflow_cleanup_delete_history_inline")
	WorkflowSuccessCount                              = NewCounterDef("workflow_success")
	WorkflowCancelCount                               = NewCounterDef("workflow_cancel")
	WorkflowFailedCount                               = NewCounterDef("workflow_failed")
	WorkflowTimeoutCount                              = NewCounterDef("workflow_timeout")
	WorkflowTerminateCount                            = NewCounterDef("workflow_terminate")
	WorkflowContinuedAsNewCount                       = NewCounterDef("workflow_continued_as_new")
	LastRetrievedMessageID                            = NewGaugeDef("last_retrieved_message_id")
	LastProcessedMessageID                            = NewGaugeDef("last_processed_message_id")
	ReplicationTasksApplied                           = NewCounterDef("replication_tasks_applied")
	ReplicationTasksFailed                            = NewCounterDef("replication_tasks_failed")
	ReplicationTasksLag                               = NewTimerDef("replication_tasks_lag")
	ReplicationLatency                                = NewTimerDef("replication_latency")
	ReplicationTasksFetched                           = NewTimerDef("replication_tasks_fetched")
	ReplicationTasksReturned                          = NewTimerDef("replication_tasks_returned")
	ReplicationTasksAppliedLatency                    = NewTimerDef("replication_tasks_applied_latency")
	ReplicationDLQFailed                              = NewCounterDef("replication_dlq_enqueue_failed")
	ReplicationDLQMaxLevelGauge                       = NewGaugeDef("replication_dlq_max_level")
	ReplicationDLQAckLevelGauge                       = NewGaugeDef("replication_dlq_ack_level")
	GetReplicationMessagesForShardLatency             = NewTimerDef("get_replication_messages_for_shard")
	GetDLQReplicationMessagesLatency                  = NewTimerDef("get_dlq_replication_messages")
	EventReapplySkippedCount                          = NewCounterDef("event_reapply_skipped_count")
	DirectQueryDispatchLatency                        = NewTimerDef("direct_query_dispatch_latency")
	DirectQueryDispatchStickyLatency                  = NewTimerDef("direct_query_dispatch_sticky_latency")
	DirectQueryDispatchNonStickyLatency               = NewTimerDef("direct_query_dispatch_non_sticky_latency")
	DirectQueryDispatchStickySuccessCount             = NewCounterDef("direct_query_dispatch_sticky_success")
	DirectQueryDispatchNonStickySuccessCount          = NewCounterDef("direct_query_dispatch_non_sticky_success")
	DirectQueryDispatchClearStickinessLatency         = NewTimerDef("direct_query_dispatch_clear_stickiness_latency")
	DirectQueryDispatchClearStickinessSuccessCount    = NewCounterDef("direct_query_dispatch_clear_stickiness_success")
	DirectQueryDispatchTimeoutBeforeNonStickyCount    = NewCounterDef("direct_query_dispatch_timeout_before_non_sticky")
	WorkflowTaskQueryLatency                          = NewTimerDef("workflow_task_query_latency")
	ConsistentQueryTimeoutCount                       = NewCounterDef("consistent_query_timeout")
	QueryBeforeFirstWorkflowTaskCount                 = NewCounterDef("query_before_first_workflow_task")
	QueryBufferExceededCount                          = NewCounterDef("query_buffer_exceeded")
	QueryRegistryInvalidStateCount                    = NewCounterDef("query_registry_invalid_state")
	WorkerNotSupportsConsistentQueryCount             = NewCounterDef("worker_not_supports_consistent_query")
	WorkflowTaskTimeoutOverrideCount                  = NewCounterDef("workflow_task_timeout_overrides")
	WorkflowRunTimeoutOverrideCount                   = NewCounterDef("workflow_run_timeout_overrides")
	ReplicationTaskCleanupCount                       = NewCounterDef("replication_task_cleanup_count")
	ReplicationTaskCleanupFailure                     = NewCounterDef("replication_task_cleanup_failed")
	MutableStateChecksumMismatch                      = NewCounterDef("mutable_state_checksum_mismatch")
	MutableStateChecksumInvalidated                   = NewCounterDef("mutable_state_checksum_invalidated")
	ClusterMetadataLockLatency                        = NewTimerDef("cluster_metadata_lock_latency")
	ClusterMetadataCallbackLockLatency                = NewTimerDef("cluster_metadata_callback_lock_latency")
	ShardControllerLockLatency                        = NewTimerDef("shard_controller_lock_latency")
	ShardLockLatency                                  = NewTimerDef("shard_lock_latency")
	NamespaceRegistryLockLatency                      = NewTimerDef("namespace_registry_lock_latency")
	NamespaceRegistryCallbackLockLatency              = NewTimerDef("namespace_registry_callback_lock_latency")
	ClosedWorkflowBufferEventCount                    = NewCounterDef("closed_workflow_buffer_event_counter")

	// Matching
	MatchingClientForwardedCounter            = NewCounterDef("forwarded")
	MatchingClientInvalidTaskQueueName        = NewCounterDef("invalid_task_queue_name")
	SyncMatchLatencyPerTaskQueue              = NewTimerDef("syncmatch_latency")
	AsyncMatchLatencyPerTaskQueue             = NewTimerDef("asyncmatch_latency")
	PollSuccessPerTaskQueueCounter            = NewCounterDef("poll_success")
	PollTimeoutPerTaskQueueCounter            = NewCounterDef("poll_timeouts")
	PollSuccessWithSyncPerTaskQueueCounter    = NewCounterDef("poll_success_sync")
	LeaseRequestPerTaskQueueCounter           = NewCounterDef("lease_requests")
	LeaseFailurePerTaskQueueCounter           = NewCounterDef("lease_failures")
	ConditionFailedErrorPerTaskQueueCounter   = NewCounterDef("condition_failed_errors")
	RespondQueryTaskFailedPerTaskQueueCounter = NewCounterDef("respond_query_failed")
	SyncThrottlePerTaskQueueCounter           = NewCounterDef("sync_throttle_count")
	BufferThrottlePerTaskQueueCounter         = NewCounterDef("buffer_throttle_count")
	ExpiredTasksPerTaskQueueCounter           = NewCounterDef("tasks_expired")
	ForwardedPerTaskQueueCounter              = NewCounterDef("forwarded_per_tl")
	ForwardTaskCallsPerTaskQueue              = NewCounterDef("forward_task_calls")
	ForwardTaskErrorsPerTaskQueue             = NewCounterDef("forward_task_errors")
	ForwardQueryCallsPerTaskQueue             = NewCounterDef("forward_query_calls")
	ForwardQueryErrorsPerTaskQueue            = NewCounterDef("forward_query_errors")
	ForwardPollCallsPerTaskQueue              = NewCounterDef("forward_poll_calls")
	ForwardPollErrorsPerTaskQueue             = NewCounterDef("forward_poll_errors")
	ForwardTaskLatencyPerTaskQueue            = NewTimerDef("forward_task_latency")
	ForwardQueryLatencyPerTaskQueue           = NewTimerDef("forward_query_latency")
	ForwardPollLatencyPerTaskQueue            = NewTimerDef("forward_poll_latency")
	LocalToLocalMatchPerTaskQueueCounter      = NewCounterDef("local_to_local_matches")
	LocalToRemoteMatchPerTaskQueueCounter     = NewCounterDef("local_to_remote_matches")
	RemoteToLocalMatchPerTaskQueueCounter     = NewCounterDef("remote_to_local_matches")
	RemoteToRemoteMatchPerTaskQueueCounter    = NewCounterDef("remote_to_remote_matches")
	LoadedTaskQueueGauge                      = NewGaugeDef("loaded_task_queue_count")
	TaskQueueStartedCounter                   = NewCounterDef("task_queue_started")
	TaskQueueStoppedCounter                   = NewCounterDef("task_queue_stopped")
	TaskWriteThrottlePerTaskQueueCounter      = NewCounterDef("task_write_throttle_count")
	TaskWriteLatencyPerTaskQueue              = NewTimerDef("task_write_latency")
	TaskLagPerTaskQueueGauge                  = NewGaugeDef("task_lag_per_tl")
	NoRecentPollerTasksPerTaskQueueCounter    = NewCounterDef("no_poller_tasks")

	// Worker
	ExecutorTasksDoneCount                                    = NewCounterDef("executor_done")
	ExecutorTasksErrCount                                     = NewCounterDef("executor_err")
	ExecutorTasksDeferredCount                                = NewCounterDef("executor_deferred")
	ExecutorTasksDroppedCount                                 = NewCounterDef("executor_dropped")
	StartedCount                                              = NewCounterDef("started")
	StoppedCount                                              = NewCounterDef("stopped")
	ScanDuration                                              = NewTimerDef("scan_duration")
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
	HistoryWorkflowExecutionCacheLatency                      = NewTimerDef("history_workflow_execution_cache_latency")
	VisibilityArchiverArchiveNonRetryableErrorCount           = NewCounterDef("visibility_archiver_archive_non_retryable_error")
	VisibilityArchiverArchiveTransientErrorCount              = NewCounterDef("visibility_archiver_archive_transient_error")
	VisibilityArchiveSuccessCount                             = NewCounterDef("visibility_archiver_archive_success")
	HistoryScavengerSuccessCount                              = NewCounterDef("scavenger_success")
	HistoryScavengerErrorCount                                = NewCounterDef("scavenger_errors")
	HistoryScavengerSkipCount                                 = NewCounterDef("scavenger_skips")
	ExecutionsOutstandingCount                                = NewGaugeDef("executions_outstanding")
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
	ScavengerValidationRequestsCount                          = NewCounterDef("scavenger_validation_requests")
	ScavengerValidationFailuresCount                          = NewCounterDef("scavenger_validation_failures")
	ScavengerValidationSkipsCount                             = NewCounterDef("scavenger_validation_skips")
	AddSearchAttributesFailuresCount                          = NewCounterDef("add_search_attributes_failures")
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
	BatcherProcessorSuccess                                   = NewCounterDef("batcher_processor_requests")
	BatcherProcessorFailures                                  = NewCounterDef("batcher_processor_errors")
	BatcherOperationFailures                                  = NewCounterDef("batcher_operation_errors")
	ElasticsearchBulkProcessorRequests                        = NewCounterDef("elasticsearch_bulk_processor_requests")
	ElasticsearchBulkProcessorQueuedRequests                  = NewDimensionlessHistogramDef("elasticsearch_bulk_processor_queued_requests")
	ElasticsearchBulkProcessorRetries                         = NewCounterDef("elasticsearch_bulk_processor_retries")
	ElasticsearchBulkProcessorFailures                        = NewCounterDef("elasticsearch_bulk_processor_errors")
	ElasticsearchBulkProcessorCorruptedData                   = NewCounterDef("elasticsearch_bulk_processor_corrupted_data")
	ElasticsearchBulkProcessorDuplicateRequest                = NewCounterDef("elasticsearch_bulk_processor_duplicate_request")
	ElasticsearchBulkProcessorRequestLatency                  = NewTimerDef("elasticsearch_bulk_processor_request_latency")
	ElasticsearchBulkProcessorCommitLatency                   = NewTimerDef("elasticsearch_bulk_processor_commit_latency")
	ElasticsearchBulkProcessorWaitAddLatency                  = NewTimerDef("elasticsearch_bulk_processor_wait_add_latency")
	ElasticsearchBulkProcessorWaitStartLatency                = NewTimerDef("elasticsearch_bulk_processor_wait_start_latency")
	ElasticsearchBulkProcessorBulkSize                        = NewDimensionlessHistogramDef("elasticsearch_bulk_processor_bulk_size")
	ElasticsearchDocumentParseFailuresCount                   = NewCounterDef("elasticsearch_document_parse_failures_counter")
	ElasticsearchDocumentGenerateFailuresCount                = NewCounterDef("elasticsearch_document_generate_failures_counter")
	CatchUpReadyShardCountGauge                               = NewGaugeDef("catchup_ready_shard_count")
	HandoverReadyShardCountGauge                              = NewGaugeDef("handover_ready_shard_count")
	ReplicatorMessages                                        = NewCounterDef("replicator_messages")
	ReplicatorFailures                                        = NewCounterDef("replicator_errors")
	ReplicatorLatency                                         = NewTimerDef("replicator_latency")
	ReplicatorDLQFailures                                     = NewCounterDef("replicator_dlq_enqueue_fails")
	NamespaceReplicationEnqueueDLQCount                       = NewCounterDef("namespace_replication_dlq_enqueue_requests")
	ParentClosePolicyProcessorSuccess                         = NewCounterDef("parent_close_policy_processor_requests")
	ParentClosePolicyProcessorFailures                        = NewCounterDef("parent_close_policy_processor_errors")

	// Replication
	NamespaceReplicationTaskAckLevelGauge = NewGaugeDef("namespace_replication_task_ack_level")
	NamespaceReplicationDLQAckLevelGauge  = NewGaugeDef("namespace_dlq_ack_level")
	NamespaceReplicationDLQMaxLevelGauge  = NewGaugeDef("namespace_dlq_max_level")

	// Persistence
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
	VisibilityPersistenceRequests                       = NewCounterDef("visibility_persistence_requests")
	VisibilityPersistenceErrorWithType                  = NewCounterDef("visibility_persistence_error_with_type")
	VisibilityPersistenceFailures                       = NewCounterDef("visibility_persistence_errors")
	VisibilityPersistenceResourceExhausted              = NewCounterDef("visibility_persistence_resource_exhausted")
	VisibilityPersistenceLatency                        = NewTimerDef("visibility_persistence_latency")
)
