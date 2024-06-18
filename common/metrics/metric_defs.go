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

// Common tags for all services
const (
	OperationTagName            = "operation"
	ServiceRoleTagName          = "service_role"
	CacheTypeTagName            = "cache_type"
	FailureTagName              = "failure"
	TaskCategoryTagName         = "task_category"
	TaskTypeTagName             = "task_type"
	TaskPriorityTagName         = "task_priority"
	QueueReaderIDTagName        = "queue_reader_id"
	QueueActionTagName          = "queue_action"
	QueueTypeTagName            = "queue_type"
	visibilityPluginNameTagName = "visibility_plugin_name"
	ErrorTypeTagName            = "error_type"
	httpStatusTagName           = "http_status"
	nexusMethodTagName          = "method"
	nexusEndpointTagName        = "nexus_endpoint"
	nexusOutcomeTagName         = "outcome"
	versionedTagName            = "versioned"
	resourceExhaustedTag        = "resource_exhausted_cause"
	resourceExhaustedScopeTag   = "resource_exhausted_scope"
	PartitionTypeName           = "partition_type"
	PriorityTagName             = "priority"
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

	InvalidHistoryURITagValue    = "invalid_history_uri"
	InvalidVisibilityURITagValue = "invalid_visibility_uri"
)

// Admin Client Operations
const (
	// AdminClientStreamWorkflowReplicationMessagesScope tracks RPC calls to admin service
	AdminClientStreamWorkflowReplicationMessagesScope = "AdminClientStreamWorkflowReplicationMessages"
)

// History Client Operations
const (
	// HistoryClientStreamWorkflowReplicationMessagesScope tracks RPC calls to history service
	HistoryClientStreamWorkflowReplicationMessagesScope = "HistoryClientStreamWorkflowReplicationMessages"
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
	// PersistenceReadHistoryBranchScope tracks ReadHistoryBranch calls made by service to persistence layer
	PersistenceReadHistoryBranchScope = "ReadHistoryBranch"
	// PersistenceReadHistoryBranchReverseScope tracks ReadHistoryBranchReverse calls made by service to persistence layer
	PersistenceReadHistoryBranchReverseScope = "ReadHistoryBranchReverse"
	// PersistenceReadRawHistoryBranchScope tracks ReadRawHistoryBranch calls made by service to persistence layer
	PersistenceReadRawHistoryBranchScope = "ReadRawHistoryBranch"
	// PersistenceForkHistoryBranchScope tracks ForkHistoryBranch calls made by service to persistence layer
	PersistenceForkHistoryBranchScope = "ForkHistoryBranch"
	// PersistenceDeleteHistoryBranchScope tracks DeleteHistoryBranch calls made by service to persistence layer
	PersistenceDeleteHistoryBranchScope = "DeleteHistoryBranch"
	// PersistenceTrimHistoryBranchScope tracks TrimHistoryBranch calls made by service to persistence layer
	PersistenceTrimHistoryBranchScope = "TrimHistoryBranch"
	// PersistenceGetAllHistoryTreeBranchesScope tracks GetAllHistoryTreeBranches calls made by service to persistence layer
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
	// PersistenceGetTransferTasksScope tracks GetTransferTasks calls made by service to persistence layer
	PersistenceGetTransferTasksScope = "GetTransferTasks"
	// PersistenceCompleteTransferTaskScope tracks CompleteTransferTasks calls made by service to persistence layer
	PersistenceCompleteTransferTaskScope = "CompleteTransferTask"
	// PersistenceRangeCompleteTransferTasksScope tracks CompleteTransferTasks calls made by service to persistence layer
	PersistenceRangeCompleteTransferTasksScope = "RangeCompleteTransferTasks"
	// PersistenceGetVisibilityTasksScope tracks GetVisibilityTasks calls made by service to persistence layer
	PersistenceGetVisibilityTasksScope = "GetVisibilityTasks"
	// PersistenceCompleteVisibilityTaskScope tracks CompleteVisibilityTasks calls made by service to persistence layer
	PersistenceCompleteVisibilityTaskScope = "CompleteVisibilityTask"
	// PersistenceRangeCompleteVisibilityTasksScope tracks CompleteVisibilityTasks calls made by service to persistence layer
	PersistenceRangeCompleteVisibilityTasksScope = "RangeCompleteVisibilityTasks"
	// PersistenceGetReplicationTaskScope tracks GetReplicationTask calls made by service to persistence layer
	PersistenceGetArchivalTasksScope = "GetArchivalTasks"
	// PersistenceGetOutboundTasksScope tracks GetOutboundTasks calls made by service to persistence layer
	PersistenceGetOutboundTasksScope = "GetOutboundTasks"
	// PersistenceCompleteOutboundTasksScope tracks CompleteOutboundTasks calls made by service to persistence layer
	PersistenceCompleteOutboundTasksScope = "CompleteOutboundTasks"
	// PersistenceRangeCompleteOutboundTasksScope tracks RangeCompleteOutboundTasks calls made by service to persistence layer
	PersistenceRangeCompleteOutboundTasksScope = "RangeCompleteOutboundTasks"
	// PersistenceCompleteArchivalTaskScope tracks CompleteArchivalTasks calls made by service to persistence layer
	PersistenceCompleteArchivalTaskScope = "CompleteArchivalTask"
	// PersistenceRangeCompleteArchivalTasksScope tracks CompleteArchivalTasks calls made by service to persistence layer
	PersistenceRangeCompleteArchivalTasksScope = "RangeCompleteArchivalTasks"
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
	// PersistenceGetTaskQueueUserDataScope is the metric scope for persistence.TaskManager.GetTaskQueueUserData API
	PersistenceGetTaskQueueUserDataScope = "GetTaskQueueUserData"
	// PersistenceUpdateTaskQueueUserDataScope is the metric scope for persistence.TaskManager.UpdateTaskQueueUserData API
	PersistenceUpdateTaskQueueUserDataScope = "UpdateTaskQueueUserData"
	// PersistenceListTaskQueueUserDataEntriesScope is the metric scope for persistence.TaskManager.ListTaskQueueUserDataEntries API
	PersistenceListTaskQueueUserDataEntriesScope = "ListTaskQueueUserDataEntries"
	// PersistenceGetTaskQueuesByBuildIdScope is the metric scope for persistence.TaskManager.GetTaskQueuesByBuildId API
	PersistenceGetTaskQueuesByBuildIdScope = "GetTaskQueuesByBuildId"
	// PersistenceCountTaskQueuesByBuildIdScope is the metric scope for persistence.TaskManager.CountTaskQueuesByBuildId API
	PersistenceCountTaskQueuesByBuildIdScope = "CountTaskQueuesByBuildId"
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
	// PersistenceGetNexusEndpointScope tracks GetNexusEndpoint calls made by service to persistence layer
	PersistenceGetNexusEndpointScope = "GetNexusEndpoint"
	// PersistenceListNexusEndpointsScope tracks ListNexusEndpoint calls made by service to persistence layer
	PersistenceListNexusEndpointsScope = "ListNexusEndpoints"
	// PersistenceCreateOrUpdateNexusEndpointScope tracks CreateOrUpdateNexusEndpoint calls made by service to persistence layer
	PersistenceCreateOrUpdateNexusEndpointScope = "CreateOrUpdateNexusEndpoint"
	// PersistenceDeleteNexusEndpointScope tracks DeleteNexusEndpoint calls made by service to persistence layer
	PersistenceDeleteNexusEndpointScope = "DeleteNexusEndpoint"

	// VisibilityPersistenceRecordWorkflowExecutionStartedScope tracks RecordWorkflowExecutionStarted calls made by service to visibility persistence layer
	VisibilityPersistenceRecordWorkflowExecutionStartedScope = "RecordWorkflowExecutionStarted"
	// VisibilityPersistenceRecordWorkflowExecutionClosedScope tracks RecordWorkflowExecutionClosed calls made by service to visibility persistence layer
	VisibilityPersistenceRecordWorkflowExecutionClosedScope = "RecordWorkflowExecutionClosed"
	// VisibilityPersistenceUpsertWorkflowExecutionScope tracks UpsertWorkflowExecution calls made by service to persistence visibility layer
	VisibilityPersistenceUpsertWorkflowExecutionScope = "UpsertWorkflowExecution"
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
	// AdminGetWorkflowExecutionRawHistoryV2Scope is the metric scope for admin.GetWorkflowExecutionRawHistoryScope
	AdminGetWorkflowExecutionRawHistoryV2Scope = "AdminGetWorkflowExecutionRawHistoryV2"
	// AdminGetWorkflowExecutionRawHistoryScope is the metric scope for admin.GetWorkflowExecutionRawHistoryScope
	AdminGetWorkflowExecutionRawHistoryScope = "AdminGetWorkflowExecutionRawHistory"
	// OperatorAddSearchAttributesScope is the metric scope for operator.AddSearchAttributes
	OperatorAddSearchAttributesScope = "OperatorAddSearchAttributes"
	// OperatorDeleteNamespaceScope is the metric scope for operator.OperatorDeleteNamespace
	OperatorDeleteNamespaceScope = "OperatorDeleteNamespace"

	// FrontendGetWorkflowExecutionHistoryScope is the metric scope for non-long-poll frontend.GetWorkflowExecutionHistory
	FrontendGetWorkflowExecutionHistoryScope = "GetWorkflowExecutionHistory"
	// FrontendPollWorkflowExecutionHistoryScope is the metric scope for long poll case of frontend.GetWorkflowExecutionHistory
	FrontendPollWorkflowExecutionHistoryScope = "PollWorkflowExecutionHistory"

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
	// HistoryRespondActivityTaskCompletedScope tracks RespondActivityTaskCompleted API calls received by service
	HistoryRespondActivityTaskCompletedScope = "RespondActivityTaskCompleted"
	// HistoryRespondActivityTaskFailedScope tracks RespondActivityTaskFailed API calls received by service
	HistoryRespondActivityTaskFailedScope = "RespondActivityTaskFailed"
	// HistoryRespondActivityTaskCanceledScope tracks RespondActivityTaskCanceled API calls received by service
	HistoryRespondActivityTaskCanceledScope = "RespondActivityTaskCanceled"
	// HistoryGetWorkflowExecutionHistoryScope is the metric scope for non-long-poll frontend.GetWorkflowExecutionHistory
	HistoryGetWorkflowExecutionHistoryScope = "GetWorkflowExecutionHistory"
	// HistoryPollWorkflowExecutionHistoryScope is the metric scope for long poll case of frontend.GetWorkflowExecutionHistory
	HistoryPollWorkflowExecutionHistoryScope = "PollWorkflowExecutionHistory"
	// HistoryGetWorkflowExecutionRawHistoryScope tracks GetWorkflowExecutionRawHistoryV2Scope API calls received by service
	HistoryGetWorkflowExecutionRawHistoryScope = "GetWorkflowExecutionRawHistory"
	// HistoryGetWorkflowExecutionRawHistoryV2Scope tracks GetWorkflowExecutionRawHistoryV2Scope API calls received by service
	HistoryGetWorkflowExecutionRawHistoryV2Scope = "GetWorkflowExecutionRawHistoryV2"
	// HistoryGetHistoryScope tracks GetHistoryScope API calls received by service
	HistoryGetHistoryScope = "GetHistory"
	// HistoryGetRawHistoryScope tracks GetRawHistoryScope API calls received by service
	HistoryGetRawHistoryScope = "GetRawHistory"
	// HistoryGetHistoryReverseScope tracks GetHistoryReverseScope API calls received by service
	HistoryGetHistoryReverseScope = "GetHistoryReverse"
	// HistoryRecordWorkflowTaskStartedScope tracks RecordWorkflowTaskStarted API calls received by service
	HistoryRecordWorkflowTaskStartedScope = "RecordWorkflowTaskStarted"
	// HistoryRecordActivityTaskStartedScope tracks RecordActivityTaskStarted API calls received by service
	HistoryRecordActivityTaskStartedScope = "RecordActivityTaskStarted"
	// HistorySignalWithStartWorkflowExecutionScope tracks SignalWithStartWorkflowExecution API calls received by service
	HistorySignalWithStartWorkflowExecutionScope = "SignalWithStartWorkflowExecution"
	// HistorySyncShardStatusScope tracks HistorySyncShardStatus API calls received by service
	HistorySyncShardStatusScope = "SyncShardStatus"
	// HistoryShardControllerScope is the scope used by shard controller
	HistoryShardControllerScope = "ShardController"
	// HistoryReapplyEventsScope is the scope used by event reapplication
	HistoryReapplyEventsScope = "ReapplyEvents"
	// HistoryQueryWorkflowScope tracks QueryWorkflow API calls received by service
	HistoryQueryWorkflowScope = "QueryWorkflow"
	// HistoryProcessDeleteHistoryEventScope tracks ProcessDeleteHistoryEvent processing calls
	HistoryProcessDeleteHistoryEventScope = "ProcessDeleteHistoryEvent"
	// HistoryDeleteWorkflowExecutionScope tracks DeleteWorkflowExecutions API calls
	HistoryDeleteWorkflowExecutionScope = "DeleteWorkflowExecution"
	// HistoryCacheGetOrCreateScope is the scope used by history cache
	HistoryCacheGetOrCreateScope = "HistoryCacheGetOrCreate"
	// HistoryCacheGetOrCreateCurrentScope is the scope used by history cache
	HistoryCacheGetOrCreateCurrentScope = "HistoryCacheGetOrCreateCurrent"

	// TransferActiveTaskCloseExecutionScope is the scope used for close execution task processing by transfer queue processor
	TransferActiveTaskCloseExecutionScope = "TransferActiveTaskCloseExecution"

	// TimerActiveTaskActivityTimeoutScope is the scope used by metric emitted by timer queue processor for processing activity timeouts
	TimerActiveTaskActivityTimeoutScope = "TimerActiveTaskActivityTimeout"
	// TimerActiveTaskWorkflowTaskTimeoutScope is the scope used by metric emitted by timer queue processor for processing workflow task timeouts
	TimerActiveTaskWorkflowTaskTimeoutScope = "TimerActiveTaskWorkflowTaskTimeout"
	// TimerActiveTaskWorkflowBackoffTimerScope is the scope used by metric emitted by timer queue processor for processing retry task.
	TimerActiveTaskWorkflowBackoffTimerScope = "TimerActiveTaskWorkflowBackoffTimer"

	// ReplicatorQueueProcessorScope is the scope used by all metric emitted by replicator queue processor
	ReplicatorQueueProcessorScope = "ReplicatorQueueProcessor"
	// ReplicateHistoryEventsScope is the scope used by historyReplicator API for applying events
	ReplicateHistoryEventsScope = "ReplicateHistoryEvents"
	// HistoryRereplicationByTransferTaskScope tracks history replication calls made by transfer task
	HistoryRereplicationByTransferTaskScope = "HistoryRereplicationByTransferTask"
	// HistoryRereplicationByTimerTaskScope tracks history replication calls made by timer task
	HistoryRereplicationByTimerTaskScope = "HistoryRereplicationByTimerTask"
	// HistoryRereplicationByHistoryReplicationScope tracks history replication calls made by history replication
	HistoryRereplicationByHistoryReplicationScope = "HistoryRereplicationByHistoryReplication"
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
	// ReplicationTaskTrackerScope is scope used by all metrics emitted by ExecutableTaskTracker
	ReplicationTaskTrackerScope = "ReplicationTaskTracker"
	// ReplicationTaskCleanupScope is scope used by all metrics emitted by ReplicationTaskProcessor cleanup
	ReplicationTaskCleanupScope = "ReplicationTaskCleanup"
	// ReplicationDLQStatsScope is scope used by all metrics emitted related to replication DLQ
	ReplicationDLQStatsScope = "ReplicationDLQStats"
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
	// OperationTransferQueueProcessorScope is a scope for transfer queue base processor
	OperationTransferQueueProcessorScope = "TransferQueueProcessor"
	// OperationVisibilityQueueProcessorScope is a scope for visibility queue processor
	OperationVisibilityQueueProcessorScope = "VisibilityQueueProcessor"
	// OperationArchivalQueueProcessorScope is a scope for archival queue processor
	OperationArchivalQueueProcessorScope = "ArchivalQueueProcessor"
	// OperationMemoryScheduledQueueProcessorScope is a scope for memory scheduled queue processor.
	OperationMemoryScheduledQueueProcessorScope = "MemoryScheduledQueueProcessor"
	// OperationOutboundQueueProcessorScope is a scope for the outbound queue processor.
	OperationOutboundQueueProcessorScope = "OutboundQueueProcessor"
)

// Matching Scope
const (
	// MatchingPollWorkflowTaskQueueScope tracks PollWorkflowTaskQueue API calls received by service
	MatchingPollWorkflowTaskQueueScope = "PollWorkflowTaskQueue"
	// MatchingPollActivityTaskQueueScope tracks PollActivityTaskQueue API calls received by service
	MatchingPollActivityTaskQueueScope = "PollActivityTaskQueue"
	// MatchingPollNexusTaskQueueScope tracks PollNexusTaskQueue API calls received by service
	MatchingPollNexusTaskQueueScope = "PollNexusTaskQueue"
	// MatchingAddActivityTaskScope tracks AddActivityTask API calls received by service
	MatchingAddActivityTaskScope = "AddActivityTask"
	// MatchingAddWorkflowTaskScope tracks AddWorkflowTask API calls received by service
	MatchingAddWorkflowTaskScope = "AddWorkflowTask"
	// MatchingTaskQueueMgrScope is the metrics scope for matching.TaskQueueManager component
	MatchingTaskQueueMgrScope = "TaskQueueMgr"
	// MatchingTaskQueuePartitionManagerScope is the metrics scope for matching.TaskQueuePartitionManager component
	MatchingTaskQueuePartitionManagerScope = "TaskQueuePartitionManager"
	// MatchingEngineScope is the metrics scope for matchingEngine component
	MatchingEngineScope = "MatchingEngine"
	// MatchingQueryWorkflowScope tracks AddWorkflowTask API calls received by service
	MatchingQueryWorkflowScope = "QueryWorkflow"
	// MatchingRespondQueryTaskCompletedScope tracks RespondQueryTaskCompleted API calls received by service
	MatchingRespondQueryTaskCompletedScope = "RespondQueryTaskCompleted"
	// MatchingRespondNexusTaskCompletedScope tracks RespondNexusTaskCompleted API calls received by service
	MatchingRespondNexusTaskCompletedScope = "RespondNexusTaskCompleted"
	// MatchingRespondNexusTaskFailedScope tracks RespondNexusTaskFailed API calls received by service
	MatchingRespondNexusTaskFailedScope = "RespondNexusTaskFailed"
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
	// SyncActivityTaskScope is the scope used by sync activity
	SyncActivityTaskScope = "SyncActivityTask"
	// SyncWorkflowStateTaskScope is the scope used by closed workflow task replication processing
	SyncWorkflowStateTaskScope = "SyncWorkflowStateTask"
	// SyncWatermarkScope is the scope used by closed workflow task replication processing
	SyncWatermarkScope = "SyncWatermark"
	// NoopTaskScope is the scope used by noop task
	NoopTaskScope = "NoopTask"
	// UnknownTaskScope is the scope used by unknown task
	UnknownTaskScope = "UnknownTask"
	// ParentClosePolicyProcessorScope is scope used by all metrics emitted by worker.ParentClosePolicyProcessor
	ParentClosePolicyProcessorScope = "ParentClosePolicyProcessor"
	DeleteNamespaceWorkflowScope    = "DeleteNamespaceWorkflow"
	ReclaimResourcesWorkflowScope   = "ReclaimResourcesWorkflow"
	DeleteExecutionsWorkflowScope   = "DeleteExecutionsWorkflow"
)

// History task type
const (
	TaskTypeTransferActiveTaskActivity               = "TransferActiveTaskActivity"
	TaskTypeTransferActiveTaskWorkflowTask           = "TransferActiveTaskWorkflowTask"
	TaskTypeTransferActiveTaskCloseExecution         = "TransferActiveTaskCloseExecution"
	TaskTypeTransferActiveTaskCancelExecution        = "TransferActiveTaskCancelExecution"
	TaskTypeTransferActiveTaskSignalExecution        = "TransferActiveTaskSignalExecution"
	TaskTypeTransferActiveTaskStartChildExecution    = "TransferActiveTaskStartChildExecution"
	TaskTypeTransferActiveTaskResetWorkflow          = "TransferActiveTaskResetWorkflow"
	TaskTypeTransferActiveTaskDeleteExecution        = "TransferActiveTaskDeleteExecution"
	TaskTypeTransferStandbyTaskActivity              = "TransferStandbyTaskActivity"
	TaskTypeTransferStandbyTaskWorkflowTask          = "TransferStandbyTaskWorkflowTask"
	TaskTypeTransferStandbyTaskCloseExecution        = "TransferStandbyTaskCloseExecution"
	TaskTypeTransferStandbyTaskCancelExecution       = "TransferStandbyTaskCancelExecution"
	TaskTypeTransferStandbyTaskSignalExecution       = "TransferStandbyTaskSignalExecution"
	TaskTypeTransferStandbyTaskStartChildExecution   = "TransferStandbyTaskStartChildExecution"
	TaskTypeTransferStandbyTaskResetWorkflow         = "TransferStandbyTaskResetWorkflow"
	TaskTypeTransferStandbyTaskDeleteExecution       = "TransferStandbyTaskDeleteExecution"
	TaskTypeVisibilityTaskStartExecution             = "VisibilityTaskStartExecution"
	TaskTypeVisibilityTaskUpsertExecution            = "VisibilityTaskUpsertExecution"
	TaskTypeVisibilityTaskCloseExecution             = "VisibilityTaskCloseExecution"
	TaskTypeVisibilityTaskDeleteExecution            = "VisibilityTaskDeleteExecution"
	TaskTypeArchivalTaskArchiveExecution             = "ArchivalTaskArchiveExecution"
	TaskTypeTimerActiveTaskActivityTimeout           = "TimerActiveTaskActivityTimeout"
	TaskTypeTimerActiveTaskWorkflowTaskTimeout       = "TimerActiveTaskWorkflowTaskTimeout"
	TaskTypeTimerActiveTaskUserTimer                 = "TimerActiveTaskUserTimer"
	TaskTypeTimerActiveTaskWorkflowRunTimeout        = "TimerActiveTaskWorkflowRunTimeout"
	TaskTypeTimerActiveTaskWorkflowExecutionTimeout  = "TimerActiveTaskWorkflowExecutionTimeout"
	TaskTypeTimerActiveTaskActivityRetryTimer        = "TimerActiveTaskActivityRetryTimer"
	TaskTypeTimerActiveTaskWorkflowBackoffTimer      = "TimerActiveTaskWorkflowBackoffTimer"
	TaskTypeTimerActiveTaskDeleteHistoryEvent        = "TimerActiveTaskDeleteHistoryEvent"
	TaskTypeTimerStandbyTaskActivityTimeout          = "TimerStandbyTaskActivityTimeout"
	TaskTypeTimerStandbyTaskWorkflowTaskTimeout      = "TimerStandbyTaskWorkflowTaskTimeout"
	TaskTypeTimerStandbyTaskUserTimer                = "TimerStandbyTaskUserTimer"
	TaskTypeTimerStandbyTaskWorkflowRunTimeout       = "TimerStandbyTaskWorkflowRunTimeout"
	TaskTypeTimerStandbyTaskWorkflowExecutionTimeout = "TimerStandbyTaskWorkflowExecutionTimeout"
	TaskTypeTimerStandbyTaskActivityRetryTimer       = "TimerStandbyTaskActivityRetryTimer"
	TaskTypeTimerStandbyTaskWorkflowBackoffTimer     = "TimerStandbyTaskWorkflowBackoffTimer"
	TaskTypeTimerStandbyTaskDeleteHistoryEvent       = "TimerStandbyTaskDeleteHistoryEvent"
	TaskTypeMemoryScheduledTaskWorkflowTaskTimeout   = "MemoryScheduledTaskWorkflowTaskTimeout"
)

// Schedule action types
const (
	ScheduleActionTypeTag       = "schedule_action"
	ScheduleActionStartWorkflow = "start_workflow"
)

var (
	ServiceRequests = NewCounterDef(
		"service_requests",
		WithDescription("The number of RPC requests received by the service."),
	)
	ServicePendingRequests                   = NewGaugeDef("service_pending_requests")
	ServiceFailures                          = NewCounterDef("service_errors")
	ServicePanic                             = NewCounterDef("service_panics")
	ServiceErrorWithType                     = NewCounterDef("service_error_with_type")
	ServiceLatency                           = NewTimerDef("service_latency")
	ServiceLatencyNoUserLatency              = NewTimerDef("service_latency_nouserlatency")
	ServiceLatencyUserLatency                = NewTimerDef("service_latency_userlatency")
	ServiceErrInvalidArgumentCounter         = NewCounterDef("service_errors_invalid_argument")
	ServiceErrNamespaceNotActiveCounter      = NewCounterDef("service_errors_namespace_not_active")
	ServiceErrResourceExhaustedCounter       = NewCounterDef("service_errors_resource_exhausted")
	ServiceErrNotFoundCounter                = NewCounterDef("service_errors_entity_not_found")
	ServiceErrExecutionAlreadyStartedCounter = NewCounterDef("service_errors_execution_already_started")
	ServiceErrContextTimeoutCounter          = NewCounterDef("service_errors_context_timeout")
	ServiceErrRetryTaskCounter               = NewCounterDef("service_errors_retry_task")
	ServiceErrIncompleteHistoryCounter       = NewCounterDef("service_errors_incomplete_history")
	ServiceErrNonDeterministicCounter        = NewCounterDef("service_errors_nondeterministic")
	ServiceErrUnauthorizedCounter            = NewCounterDef("service_errors_unauthorized")
	ServiceErrAuthorizeFailedCounter         = NewCounterDef("service_errors_authorize_failed")
	ActionCounter                            = NewCounterDef("action")
	TlsCertsExpired                          = NewGaugeDef("certificates_expired")
	TlsCertsExpiring                         = NewGaugeDef("certificates_expiring")
	ServiceAuthorizationLatency              = NewTimerDef("service_authorization_latency")
	EventBlobSize                            = NewBytesHistogramDef("event_blob_size")
	LockRequests                             = NewCounterDef("lock_requests")
	LockLatency                              = NewTimerDef("lock_latency")
	SemaphoreRequests                        = NewCounterDef("semaphore_requests")
	SemaphoreFailures                        = NewCounterDef("semaphore_failures")
	SemaphoreLatency                         = NewTimerDef("semaphore_latency")
	ClientRequests                           = NewCounterDef(
		"client_requests",
		WithDescription("The number of requests sent by the client to an individual service, keyed by `service_role` and `operation`."),
	)
	ClientFailures                   = NewCounterDef("client_errors")
	ClientLatency                    = NewTimerDef("client_latency")
	ClientRedirectionRequests        = NewCounterDef("client_redirection_requests")
	ClientRedirectionFailures        = NewCounterDef("client_redirection_errors")
	ClientRedirectionLatency         = NewTimerDef("client_redirection_latency")
	StateTransitionCount             = NewDimensionlessHistogramDef("state_transition_count")
	HistorySize                      = NewBytesHistogramDef("history_size")
	HistoryCount                     = NewDimensionlessHistogramDef("history_count")
	TasksCompletedPerShardInfoUpdate = NewDimensionlessHistogramDef("tasks_per_shardinfo_update")
	TimeBetweenShardInfoUpdates      = NewTimerDef("time_between_shardinfo_update")
	SearchAttributesSize             = NewBytesHistogramDef("search_attributes_size")
	MemoSize                         = NewBytesHistogramDef("memo_size")
	TooManyPendingChildWorkflows     = NewCounterDef(
		"wf_too_many_pending_child_workflows",
		WithDescription("The number of Workflow Tasks failed because they would cause the limit on the number of pending child workflows to be exceeded. See https://t.mp/limits for more information."),
	)
	TooManyPendingActivities = NewCounterDef(
		"wf_too_many_pending_activities",
		WithDescription("The number of Workflow Tasks failed because they would cause the limit on the number of pending activities to be exceeded. See https://t.mp/limits for more information."),
	)
	TooManyPendingCancelRequests = NewCounterDef(
		"wf_too_many_pending_cancel_requests",
		WithDescription("The number of Workflow Tasks failed because they would cause the limit on the number of pending cancel requests to be exceeded. See https://t.mp/limits for more information."),
	)
	TooManyPendingSignalsToExternalWorkflows = NewCounterDef(
		"wf_too_many_pending_external_workflow_signals",
		WithDescription("The number of Workflow Tasks failed because they would cause the limit on the number of pending signals to external workflows to be exceeded. See https://t.mp/limits for more information."),
	)
	UTF8ValidationErrors = NewCounterDef(
		"utf8_validation_errors",
		WithDescription("Number of times the service encountered a proto message with invalid UTF-8 in a string field"),
	)

	// Frontend
	AddSearchAttributesWorkflowSuccessCount  = NewCounterDef("add_search_attributes_workflow_success")
	AddSearchAttributesWorkflowFailuresCount = NewCounterDef("add_search_attributes_workflow_failure")
	DeleteNamespaceWorkflowSuccessCount      = NewCounterDef("delete_namespace_workflow_success")
	DeleteNamespaceWorkflowFailuresCount     = NewCounterDef("delete_namespace_workflow_failure")
	VersionCheckSuccessCount                 = NewCounterDef("version_check_success")
	VersionCheckFailedCount                  = NewCounterDef("version_check_failed")
	VersionCheckRequestFailedCount           = NewCounterDef("version_check_request_failed")
	VersionCheckLatency                      = NewTimerDef("version_check_latency")
	HTTPServiceRequests                      = NewCounterDef(
		"http_service_requests",
		WithDescription("The number of HTTP requests received by the service."),
	)
	NexusRequests = NewCounterDef(
		"nexus_requests",
		WithDescription("The number of Nexus requests received by the service."),
	)
	NexusRequestPreProcessErrors = NewCounterDef(
		"nexus_request_preprocess_errors",
		WithDescription("The number of Nexus requests for which pre-processing failed."),
	)
	NexusLatencyHistogram = NewTimerDef(
		"nexus_latency",
		WithDescription("Latency histogram of Nexus requests."),
	)
	NexusCompletionRequests = NewCounterDef(
		"nexus_completion_requests",
		WithDescription("The number of Nexus completion (callback) requests received by the service."),
	)
	NexusCompletionLatencyHistogram = NewTimerDef(
		"nexus_completion_latency",
		WithDescription("Latency histogram of Nexus completion (callback) requests."),
	)
	NexusCompletionRequestPreProcessErrors = NewCounterDef(
		"nexus_completion_request_preprocess_errors",
		WithDescription("The number of Nexus completion requests for which pre-processing failed."),
	)
	HostRPSLimit          = NewGaugeDef("host_rps_limit")
	NamespaceHostRPSLimit = NewGaugeDef("namespace_host_rps_limit")

	// History
	CacheRequests                                = NewCounterDef("cache_requests")
	CacheFailures                                = NewCounterDef("cache_errors")
	CacheLatency                                 = NewTimerDef("cache_latency")
	CacheMissCounter                             = NewCounterDef("cache_miss")
	CacheSize                                    = NewGaugeDef("cache_size")
	CacheUsage                                   = NewGaugeDef("cache_usage")
	CachePinnedUsage                             = NewGaugeDef("cache_pinned_usage")
	CacheTtl                                     = NewTimerDef("cache_ttl")
	CacheEntryAgeOnGet                           = NewTimerDef("cache_entry_age_on_get")
	CacheEntryAgeOnEviction                      = NewTimerDef("cache_entry_age_on_eviction")
	HistoryEventNotificationQueueingLatency      = NewTimerDef("history_event_notification_queueing_latency")
	HistoryEventNotificationFanoutLatency        = NewTimerDef("history_event_notification_fanout_latency")
	HistoryEventNotificationInFlightMessageGauge = NewGaugeDef("history_event_notification_inflight_message_gauge")
	HistoryEventNotificationFailDeliveryCount    = NewCounterDef("history_event_notification_fail_delivery_count")
	// ArchivalTaskInvalidURI is emitted by the archival queue task executor when the history or visibility URI for an
	// archival task is not a valid URI.
	// We may emit this metric several times for a single task if the task is retried.
	ArchivalTaskInvalidURI              = NewCounterDef("archival_task_invalid_uri")
	ArchiverArchiveLatency              = NewTimerDef("archiver_archive_latency")
	ArchiverArchiveTargetLatency        = NewTimerDef("archiver_archive_target_latency")
	ShardContextClosedCounter           = NewCounterDef("shard_closed_count")
	ShardContextCreatedCounter          = NewCounterDef("sharditem_created_count")
	ShardContextRemovedCounter          = NewCounterDef("sharditem_removed_count")
	ShardContextAcquisitionLatency      = NewTimerDef("sharditem_acquisition_latency")
	ShardInfoImmediateQueueLagHistogram = NewDimensionlessHistogramDef(
		"shardinfo_immediate_queue_lag",
		WithDescription("A histogram across history shards for the difference between the smallest taskID of pending history tasks and the last generated history task ID."),
	)
	ShardInfoScheduledQueueLagTimer = NewTimerDef(
		"shardinfo_scheduled_queue_lag",
		WithDescription("A histogram across history shards for the difference between the earliest scheduled time of pending history tasks and current time."),
	)
	SyncShardFromRemoteCounter = NewCounterDef("syncshard_remote_count")
	SyncShardFromRemoteFailure = NewCounterDef("syncshard_remote_failed")
	TaskRequests               = NewCounterDef(
		"task_requests",
		WithDescription("The number of history tasks processed."),
	)
	TaskLoadLatency = NewTimerDef(
		"task_latency_load",
		WithDescription("Latency from history task generation to loading into memory (persistence schedule to start latency)."),
	)
	TaskScheduleLatency = NewTimerDef(
		"task_latency_schedule",
		WithDescription("Latency from history task loading to start processing (in-memory schedule to start latency)."),
	)
	TaskProcessingLatency = NewTimerDef(
		"task_latency_processing",
		WithDescription("Latency for processing a history task one time."),
	)
	TaskLatency = NewTimerDef(
		"task_latency",
		WithDescription("Latency for procsssing and completing a history task. This latency is across all attempts but excludes any latencies related to workflow lock or user qutoa limit."),
	)
	TaskQueueLatency = NewTimerDef(
		"task_latency_queue",
		WithDescription("End-to-end latency for processing and completing a history task, from task generation to completion."),
	)
	TaskAttempt = NewDimensionlessHistogramDef(
		"task_attempt",
		WithDescription("The number of attempts took to complete a history task."),
	)
	TaskFailures = NewCounterDef(
		"task_errors",
		WithDescription("The number of unexpected history task processing errors."),
	)
	TaskTerminalFailures = NewCounterDef(
		"task_terminal_failures",
		WithDescription("The number of times a history task failed with a terminal failure, causing it to be sent to the DLQ."),
	)
	TaskDLQFailures = NewCounterDef(
		"task_dlq_failures",
		WithDescription("The number of times we failed to send a history task to the DLQ."),
	)
	TaskDLQSendLatency = NewTimerDef(
		"task_dlq_latency",
		WithDescription("The amount of time it took to successfully send a task to the DLQ. This only records the"+
			" latency of the final attempt to send the task to the DLQ, not the cumulative latency of all attempts."),
	)
	TaskDiscarded                   = NewCounterDef("task_errors_discarded")
	TaskSkipped                     = NewCounterDef("task_skipped")
	TaskVersionMisMatch             = NewCounterDef("task_errors_version_mismatch")
	TasksDependencyTaskNotCompleted = NewCounterDef("task_dependency_task_not_completed")
	TaskStandbyRetryCounter         = NewCounterDef("task_errors_standby_retry_counter")
	TaskWorkflowBusyCounter         = NewCounterDef(
		"task_errors_workflow_busy",
		WithDescription("The number of history task processing errors caused by failing to acquire workflow lock within the configured timeout (history.cacheNonUserContextLockTimeout)."),
	)
	TaskNotActiveCounter         = NewCounterDef("task_errors_not_active_counter")
	TaskNamespaceHandoverCounter = NewCounterDef("task_errors_namespace_handover")
	TaskInternalErrorCounter     = NewCounterDef("task_errors_internal")
	TaskThrottledCounter         = NewCounterDef(
		"task_errors_throttled",
		WithDescription("The number of history task processing errors caused by resource exhausted errors, excluding workflow busy case."),
	)
	TaskCorruptionCounter       = NewCounterDef("task_errors_corruption")
	TaskScheduleToStartLatency  = NewTimerDef("task_schedule_to_start_latency")
	TaskBatchCompleteCounter    = NewCounterDef("task_batch_complete_counter")
	TaskReschedulerPendingTasks = NewDimensionlessHistogramDef("task_rescheduler_pending_tasks")
	PendingTasksCounter         = NewDimensionlessHistogramDef(
		"pending_tasks",
		WithDescription("A histogram across history shards for the number of in-memory pending history tasks."),
	)
	TaskSchedulerThrottled                               = NewCounterDef("task_scheduler_throttled")
	QueueScheduleLatency                                 = NewTimerDef("queue_latency_schedule") // latency for scheduling 100 tasks in one task channel
	QueueReaderCountHistogram                            = NewDimensionlessHistogramDef("queue_reader_count")
	QueueSliceCountHistogram                             = NewDimensionlessHistogramDef("queue_slice_count")
	QueueActionCounter                                   = NewCounterDef("queue_actions")
	ActivityE2ELatency                                   = NewTimerDef("activity_end_to_end_latency")
	AckLevelUpdateCounter                                = NewCounterDef("ack_level_update")
	AckLevelUpdateFailedCounter                          = NewCounterDef("ack_level_update_failed")
	CommandCounter                                       = NewCounterDef("command")
	MessageTypeRequestWorkflowExecutionUpdateCounter     = NewCounterDef("request_workflow_update_message")
	MessageTypeAcceptWorkflowExecutionUpdateCounter      = NewCounterDef("accept_workflow_update_message")
	MessageTypeRespondWorkflowExecutionUpdateCounter     = NewCounterDef("respond_workflow_update_message")
	MessageTypeRejectWorkflowExecutionUpdateCounter      = NewCounterDef("reject_workflow_update_message")
	InvalidStateTransitionWorkflowExecutionUpdateCounter = NewCounterDef("invalid_state_transition_workflow_update_message")
	WorkflowExecutionUpdateRegistrySize                  = NewBytesHistogramDef("workflow_update_registry_size")
	WorkflowExecutionUpdateRequestRateLimited            = NewCounterDef("workflow_update_request_rate_limited")
	WorkflowExecutionUpdateTooMany                       = NewCounterDef("workflow_update_request_too_many")
	WorkflowExecutionUpdateAborted                       = NewCounterDef("workflow_update_aborted")
	WorkflowExecutionUpdateSentToWorker                  = NewCounterDef("workflow_update_sent_to_worker")
	WorkflowExecutionUpdateSentToWorkerAgain             = NewCounterDef("workflow_update_sent_to_worker_again")
	WorkflowExecutionUpdateWaitStageAccepted             = NewCounterDef("workflow_update_wait_stage_accepted")
	WorkflowExecutionUpdateWaitStageCompleted            = NewCounterDef("workflow_update_wait_stage_completed")
	WorkflowExecutionUpdateSpeculativeWorkflowTask       = NewCounterDef("workflow_update_speculative_workflow_task")
	WorkflowExecutionUpdateNormalWorkflowTask            = NewCounterDef("workflow_update_normal_workflow_task")
	WorkflowExecutionUpdateClientTimeout                 = NewCounterDef("workflow_update_client_timeout")
	WorkflowExecutionUpdateServerTimeout                 = NewCounterDef("workflow_update_server_timeout")
	ConvertSpeculativeWorkflowTask                       = NewCounterDef(
		"workflow_task_convert_speculative_to_normal",
		WithDescription("The number of speculative workflow tasks converted to normal workflow tasks."))

	ActivityEagerExecutionCounter = NewCounterDef("activity_eager_execution")
	// WorkflowEagerExecutionCounter is emitted any time eager workflow start is requested.
	WorkflowEagerExecutionCounter = NewCounterDef("workflow_eager_execution")
	// WorkflowEagerExecutionDeniedCounter is emitted any time eager workflow start is requested and the serer fell back
	// to standard dispatch.
	// Timeouts and failures are not counted in this metric.
	// This metric has a "reason" tag attached to it to understand why eager start was denied.
	WorkflowEagerExecutionDeniedCounter           = NewCounterDef("workflow_eager_execution_denied")
	EmptyCompletionCommandsCounter                = NewCounterDef("empty_completion_commands")
	MultipleCompletionCommandsCounter             = NewCounterDef("multiple_completion_commands")
	FailedWorkflowTasksCounter                    = NewCounterDef("failed_workflow_tasks")
	WorkflowTaskAttempt                           = NewDimensionlessHistogramDef("workflow_task_attempt")
	StaleMutableStateCounter                      = NewCounterDef("stale_mutable_state")
	AutoResetPointsLimitExceededCounter           = NewCounterDef("auto_reset_points_exceed_limit")
	AutoResetPointCorruptionCounter               = NewCounterDef("auto_reset_point_corruption")
	BatchableTaskBatchCount                       = NewGaugeDef("batchable_task_batch_count")
	ConcurrencyUpdateFailureCounter               = NewCounterDef("concurrency_update_failure")
	ServiceErrShardOwnershipLostCounter           = NewCounterDef("service_errors_shard_ownership_lost")
	HeartbeatTimeoutCounter                       = NewCounterDef("heartbeat_timeout")
	ScheduleToStartTimeoutCounter                 = NewCounterDef("schedule_to_start_timeout")
	StartToCloseTimeoutCounter                    = NewCounterDef("start_to_close_timeout")
	ScheduleToCloseTimeoutCounter                 = NewCounterDef("schedule_to_close_timeout")
	NewTimerNotifyCounter                         = NewCounterDef("new_timer_notifications")
	AcquireShardsCounter                          = NewCounterDef("acquire_shards_count")
	AcquireShardsLatency                          = NewTimerDef("acquire_shards_latency")
	MembershipChangedCounter                      = NewCounterDef("membership_changed_count")
	NumShardsGauge                                = NewGaugeDef("numshards_gauge")
	GetEngineForShardErrorCounter                 = NewCounterDef("get_engine_for_shard_errors")
	GetEngineForShardLatency                      = NewTimerDef("get_engine_for_shard_latency")
	RemoveEngineForShardLatency                   = NewTimerDef("remove_engine_for_shard_latency")
	CompleteWorkflowTaskWithStickyEnabledCounter  = NewCounterDef("complete_workflow_task_sticky_enabled_count")
	CompleteWorkflowTaskWithStickyDisabledCounter = NewCounterDef("complete_workflow_task_sticky_disabled_count")
	WorkflowTaskHeartbeatTimeoutCounter           = NewCounterDef("workflow_task_heartbeat_timeout_count")
	DuplicateReplicationEventsCounter             = NewCounterDef("duplicate_replication_events")
	AcquireLockFailedCounter                      = NewCounterDef("acquire_lock_failed")
	WorkflowContextCleared                        = NewCounterDef("workflow_context_cleared")
	MutableStateSize                              = NewBytesHistogramDef(
		"mutable_state_size",
		WithDescription("The size of an individual Workflow Execution's state, emitted each time a workflow execution is retrieved or updated."),
	)
	PersistedMutableStateSize = NewBytesHistogramDef(
		"persisted_mutable_state_size",
		WithDescription("Size of the persisted Workflow Execution's state in DB, emitted each time a workflow execution is updated."),
	)
	ExecutionInfoSize                     = NewBytesHistogramDef("execution_info_size")
	ExecutionStateSize                    = NewBytesHistogramDef("execution_state_size")
	ActivityInfoSize                      = NewBytesHistogramDef("activity_info_size")
	TimerInfoSize                         = NewBytesHistogramDef("timer_info_size")
	ChildInfoSize                         = NewBytesHistogramDef("child_info_size")
	RequestCancelInfoSize                 = NewBytesHistogramDef("request_cancel_info_size")
	SignalInfoSize                        = NewBytesHistogramDef("signal_info_size")
	SignalRequestIDSize                   = NewBytesHistogramDef("signal_request_id_size")
	BufferedEventsSize                    = NewBytesHistogramDef("buffered_events_size")
	ActivityInfoCount                     = NewDimensionlessHistogramDef("activity_info_count")
	TimerInfoCount                        = NewDimensionlessHistogramDef("timer_info_count")
	ChildInfoCount                        = NewDimensionlessHistogramDef("child_info_count")
	SignalInfoCount                       = NewDimensionlessHistogramDef("signal_info_count")
	RequestCancelInfoCount                = NewDimensionlessHistogramDef("request_cancel_info_count")
	SignalRequestIDCount                  = NewDimensionlessHistogramDef("signal_request_id_count")
	BufferedEventsCount                   = NewDimensionlessHistogramDef("buffered_events_count")
	TaskCount                             = NewDimensionlessHistogramDef("task_count")
	TotalActivityCount                    = NewDimensionlessHistogramDef("total_activity_count")
	TotalUserTimerCount                   = NewDimensionlessHistogramDef("total_user_timer_count")
	TotalChildExecutionCount              = NewDimensionlessHistogramDef("total_child_execution_count")
	TotalRequestCancelExternalCount       = NewDimensionlessHistogramDef("total_request_cancel_external_count")
	TotalSignalExternalCount              = NewDimensionlessHistogramDef("total_signal_external_count")
	TotalSignalCount                      = NewDimensionlessHistogramDef("total_signal_count")
	WorkflowBackoffCount                  = NewCounterDef("workflow_backoff_timer")
	WorkflowRetryBackoffTimerCount        = NewCounterDef("workflow_retry_backoff_timer")
	WorkflowCronBackoffTimerCount         = NewCounterDef("workflow_cron_backoff_timer")
	WorkflowDelayedStartBackoffTimerCount = NewCounterDef("workflow_delayed_start_backoff_timer")
	WorkflowCleanupDeleteCount            = NewCounterDef("workflow_cleanup_delete")
	WorkflowSuccessCount                  = NewCounterDef("workflow_success")
	WorkflowCancelCount                   = NewCounterDef("workflow_cancel")
	WorkflowFailedCount                   = NewCounterDef("workflow_failed")
	WorkflowTimeoutCount                  = NewCounterDef("workflow_timeout")
	WorkflowTerminateCount                = NewCounterDef("workflow_terminate")
	WorkflowContinuedAsNewCount           = NewCounterDef("workflow_continued_as_new")
	ReplicationStreamPanic                = NewCounterDef("replication_stream_panic")
	ReplicationStreamError                = NewCounterDef("replication_stream_error")
	ReplicationServiceError               = NewCounterDef("replication_service_error")
	ReplicationTasksSend                  = NewCounterDef("replication_tasks_send")
	ReplicationTasksRecv                  = NewCounterDef("replication_tasks_recv")
	ReplicationTasksRecvBacklog           = NewDimensionlessHistogramDef("replication_tasks_recv_backlog")
	ReplicationTasksSkipped               = NewCounterDef("replication_tasks_skipped")
	ReplicationTasksApplied               = NewCounterDef("replication_tasks_applied")
	ReplicationTasksFailed                = NewCounterDef("replication_tasks_failed")
	// ReplicationTasksLag is a heuristic for how far behind the remote DC is for a given cluster. It measures the
	// difference between task IDs so its unit should be "tasks".
	ReplicationTasksLag = NewDimensionlessHistogramDef("replication_tasks_lag")
	// ReplicationTasksFetched records the number of tasks fetched by the poller.
	ReplicationTasksFetched                        = NewDimensionlessHistogramDef("replication_tasks_fetched")
	ReplicationLatency                             = NewTimerDef("replication_latency")
	ReplicationTaskTransmissionLatency             = NewTimerDef("replication_task_transmission_latency")
	ReplicationDLQFailed                           = NewCounterDef("replication_dlq_enqueue_failed")
	ReplicationDLQMaxLevelGauge                    = NewGaugeDef("replication_dlq_max_level")
	ReplicationDLQAckLevelGauge                    = NewGaugeDef("replication_dlq_ack_level")
	ReplicationNonEmptyDLQCount                    = NewCounterDef("replication_dlq_non_empty")
	ReplicationOutlierNamespace                    = NewCounterDef("replication_outlier_namespace")
	EventReapplySkippedCount                       = NewCounterDef("event_reapply_skipped_count")
	DirectQueryDispatchLatency                     = NewTimerDef("direct_query_dispatch_latency")
	DirectQueryDispatchStickyLatency               = NewTimerDef("direct_query_dispatch_sticky_latency")
	DirectQueryDispatchNonStickyLatency            = NewTimerDef("direct_query_dispatch_non_sticky_latency")
	DirectQueryDispatchStickySuccessCount          = NewCounterDef("direct_query_dispatch_sticky_success")
	DirectQueryDispatchNonStickySuccessCount       = NewCounterDef("direct_query_dispatch_non_sticky_success")
	DirectQueryDispatchClearStickinessLatency      = NewTimerDef("direct_query_dispatch_clear_stickiness_latency")
	DirectQueryDispatchClearStickinessSuccessCount = NewCounterDef("direct_query_dispatch_clear_stickiness_success")
	DirectQueryDispatchTimeoutBeforeNonStickyCount = NewCounterDef("direct_query_dispatch_timeout_before_non_sticky")
	WorkflowTaskQueryLatency                       = NewTimerDef("workflow_task_query_latency")
	ConsistentQueryTimeoutCount                    = NewCounterDef("consistent_query_timeout")
	QueryBufferExceededCount                       = NewCounterDef("query_buffer_exceeded")
	QueryRegistryInvalidStateCount                 = NewCounterDef("query_registry_invalid_state")
	WorkflowTaskTimeoutOverrideCount               = NewCounterDef("workflow_task_timeout_overrides")
	WorkflowRunTimeoutOverrideCount                = NewCounterDef("workflow_run_timeout_overrides")
	ReplicationTaskCleanupCount                    = NewCounterDef("replication_task_cleanup_count")
	ReplicationTaskCleanupFailure                  = NewCounterDef("replication_task_cleanup_failed")
	MutableStateDirty                              = NewCounterDef("mutable_state_dirty")
	MutableStateChecksumMismatch                   = NewCounterDef("mutable_state_checksum_mismatch")
	MutableStateChecksumInvalidated                = NewCounterDef("mutable_state_checksum_invalidated")
	ClosedWorkflowBufferEventCount                 = NewCounterDef("closed_workflow_buffer_event_counter")
	OutOfOrderBufferedEventsCounter                = NewCounterDef("out_of_order_buffered_events")
	ShardLingerSuccess                             = NewTimerDef("shard_linger_success")
	ShardLingerTimeouts                            = NewCounterDef("shard_linger_timeouts")
	DynamicRateLimiterMultiplier                   = NewGaugeDef("dynamic_rate_limit_multiplier")
	DLQWrites                                      = NewCounterDef(
		"dlq_writes",
		WithDescription("The number of times a message is enqueued to DLQ. DLQ can be inspected using tdbg dlq command."),
	)
	ReadNamespaceErrors             = NewCounterDef("read_namespace_errors")
	RateLimitedTaskRunnableWaitTime = NewTimerDef("rate_limited_task_runnable_wait_time")

	// Deadlock detector latency metrics
	DDClusterMetadataLockLatency         = NewTimerDef("dd_cluster_metadata_lock_latency")
	DDClusterMetadataCallbackLockLatency = NewTimerDef("dd_cluster_metadata_callback_lock_latency")
	DDShardControllerLockLatency         = NewTimerDef("dd_shard_controller_lock_latency")
	DDShardLockLatency                   = NewTimerDef("dd_shard_lock_latency")
	DDShardIOSemaphoreLatency            = NewTimerDef("dd_shard_io_semaphore_latency")
	DDNamespaceRegistryLockLatency       = NewTimerDef("dd_namespace_registry_lock_latency")

	// Matching
	MatchingClientForwardedCounter            = NewCounterDef("forwarded")
	MatchingClientInvalidTaskQueueName        = NewCounterDef("invalid_task_queue_name")
	MatchingClientInvalidTaskQueuePartition   = NewCounterDef("invalid_task_queue_partition")
	SyncMatchLatencyPerTaskQueue              = NewTimerDef("syncmatch_latency")
	AsyncMatchLatencyPerTaskQueue             = NewTimerDef("asyncmatch_latency")
	PollSuccessPerTaskQueueCounter            = NewCounterDef("poll_success")
	PollTimeoutPerTaskQueueCounter            = NewCounterDef("poll_timeouts")
	PollSuccessWithSyncPerTaskQueueCounter    = NewCounterDef("poll_success_sync")
	PollLatencyPerTaskQueue                   = NewTimerDef("poll_latency")
	LeaseRequestPerTaskQueueCounter           = NewCounterDef("lease_requests")
	LeaseFailurePerTaskQueueCounter           = NewCounterDef("lease_failures")
	ConditionFailedErrorPerTaskQueueCounter   = NewCounterDef("condition_failed_errors")
	RespondQueryTaskFailedPerTaskQueueCounter = NewCounterDef("respond_query_failed")
	RespondNexusTaskFailedPerTaskQueueCounter = NewCounterDef("respond_nexus_failed")
	SyncThrottlePerTaskQueueCounter           = NewCounterDef("sync_throttle_count")
	BufferThrottlePerTaskQueueCounter         = NewCounterDef("buffer_throttle_count")
	ExpiredTasksPerTaskQueueCounter           = NewCounterDef("tasks_expired")
	ForwardedPerTaskQueueCounter              = NewCounterDef("forwarded_per_tl")
	ForwardTaskErrorsPerTaskQueue             = NewCounterDef("forward_task_errors")
	LocalToLocalMatchPerTaskQueueCounter      = NewCounterDef("local_to_local_matches")
	LocalToRemoteMatchPerTaskQueueCounter     = NewCounterDef("local_to_remote_matches")
	RemoteToLocalMatchPerTaskQueueCounter     = NewCounterDef("remote_to_local_matches")
	RemoteToRemoteMatchPerTaskQueueCounter    = NewCounterDef("remote_to_remote_matches")
	LoadedTaskQueueFamilyGauge                = NewGaugeDef("loaded_task_queue_family_count")
	LoadedTaskQueueGauge                      = NewGaugeDef("loaded_task_queue_count")
	LoadedTaskQueuePartitionGauge             = NewGaugeDef("loaded_task_queue_partition_count")
	LoadedPhysicalTaskQueueGauge              = NewGaugeDef("loaded_physical_task_queue_count")
	TaskQueueStartedCounter                   = NewCounterDef("task_queue_started")
	TaskQueueStoppedCounter                   = NewCounterDef("task_queue_stopped")
	TaskWriteThrottlePerTaskQueueCounter      = NewCounterDef("task_write_throttle_count")
	TaskWriteLatencyPerTaskQueue              = NewTimerDef("task_write_latency")
	TaskLagPerTaskQueueGauge                  = NewGaugeDef("task_lag_per_tl")
	NoRecentPollerTasksPerTaskQueueCounter    = NewCounterDef("no_poller_tasks")
	UnknownBuildPollsCounter                  = NewCounterDef("unknown_build_polls")
	UnknownBuildTasksCounter                  = NewCounterDef("unknown_build_tasks")
	TaskDispatchLatencyPerTaskQueue           = NewTimerDef("task_dispatch_latency")
	ApproximateBacklogCount                   = NewGaugeDef("approximate_backlog_count")

	// Versioning and Reachability
	ReachabilityExitPointCounter = NewCounterDef("reachability_exit_point_count")

	// Worker
	ExecutorTasksDoneCount                          = NewCounterDef("executor_done")
	ExecutorTasksErrCount                           = NewCounterDef("executor_err")
	ExecutorTasksDeferredCount                      = NewCounterDef("executor_deferred")
	ExecutorTasksDroppedCount                       = NewCounterDef("executor_dropped")
	StartedCount                                    = NewCounterDef("started")
	StoppedCount                                    = NewCounterDef("stopped")
	TaskProcessedCount                              = NewGaugeDef("task_processed")
	TaskDeletedCount                                = NewGaugeDef("task_deleted")
	TaskQueueProcessedCount                         = NewGaugeDef("taskqueue_processed")
	TaskQueueDeletedCount                           = NewGaugeDef("taskqueue_deleted")
	TaskQueueOutstandingCount                       = NewGaugeDef("taskqueue_outstanding")
	HistoryArchiverArchiveNonRetryableErrorCount    = NewCounterDef("history_archiver_archive_non_retryable_error")
	HistoryArchiverArchiveTransientErrorCount       = NewCounterDef("history_archiver_archive_transient_error")
	HistoryArchiverArchiveSuccessCount              = NewCounterDef("history_archiver_archive_success")
	HistoryArchiverTotalUploadSize                  = NewBytesHistogramDef("history_archiver_total_upload_size")
	HistoryArchiverHistorySize                      = NewBytesHistogramDef("history_archiver_history_size")
	HistoryArchiverDuplicateArchivalsCount          = NewCounterDef("history_archiver_duplicate_archivals")
	HistoryArchiverBlobExistsCount                  = NewCounterDef("history_archiver_blob_exists")
	HistoryArchiverBlobSize                         = NewBytesHistogramDef("history_archiver_blob_size")
	HistoryWorkflowExecutionCacheLatency            = NewTimerDef("history_workflow_execution_cache_latency")
	VisibilityArchiverArchiveNonRetryableErrorCount = NewCounterDef("visibility_archiver_archive_non_retryable_error")
	VisibilityArchiverArchiveTransientErrorCount    = NewCounterDef("visibility_archiver_archive_transient_error")
	VisibilityArchiveSuccessCount                   = NewCounterDef("visibility_archiver_archive_success")
	HistoryScavengerSuccessCount                    = NewCounterDef("scavenger_success")
	HistoryScavengerErrorCount                      = NewCounterDef("scavenger_errors")
	HistoryScavengerSkipCount                       = NewCounterDef("scavenger_skips")
	ExecutionsOutstandingCount                      = NewGaugeDef("executions_outstanding")
	ScavengerValidationRequestsCount                = NewCounterDef("scavenger_validation_requests")
	ScavengerValidationFailuresCount                = NewCounterDef("scavenger_validation_failures")
	ScavengerValidationSkipsCount                   = NewCounterDef("scavenger_validation_skips")
	AddSearchAttributesFailuresCount                = NewCounterDef("add_search_attributes_failures")
	DeleteNamespaceSuccessCount                     = NewCounterDef("delete_namespace_success")
	RenameNamespaceSuccessCount                     = NewCounterDef("rename_namespace_success")
	DeleteExecutionsSuccessCount                    = NewCounterDef("delete_executions_success")
	DeleteNamespaceFailuresCount                    = NewCounterDef("delete_namespace_failures")
	UpdateNamespaceFailuresCount                    = NewCounterDef("update_namespace_failures")
	RenameNamespaceFailuresCount                    = NewCounterDef("rename_namespace_failures")
	ReadNamespaceFailuresCount                      = NewCounterDef("read_namespace_failures")
	ListExecutionsFailuresCount                     = NewCounterDef("list_executions_failures")
	CountExecutionsFailuresCount                    = NewCounterDef("count_executions_failures")
	DeleteExecutionFailuresCount                    = NewCounterDef("delete_execution_failures")
	DeleteExecutionNotFoundCount                    = NewCounterDef("delete_execution_not_found")
	RateLimiterFailuresCount                        = NewCounterDef("rate_limiter_failures")
	BatcherProcessorSuccess                         = NewCounterDef(
		"batcher_processor_requests",
		WithDescription("The number of individual workflow execution tasks successfully processed by the batch request processor"),
	)
	BatcherProcessorFailures                          = NewCounterDef("batcher_processor_errors")
	BatcherOperationFailures                          = NewCounterDef("batcher_operation_errors")
	ElasticsearchBulkProcessorRequests                = NewCounterDef("elasticsearch_bulk_processor_requests")
	ElasticsearchBulkProcessorQueuedRequests          = NewDimensionlessHistogramDef("elasticsearch_bulk_processor_queued_requests")
	ElasticsearchBulkProcessorFailures                = NewCounterDef("elasticsearch_bulk_processor_errors")
	ElasticsearchBulkProcessorCorruptedData           = NewCounterDef("elasticsearch_bulk_processor_corrupted_data")
	ElasticsearchBulkProcessorDuplicateRequest        = NewCounterDef("elasticsearch_bulk_processor_duplicate_request")
	ElasticsearchBulkProcessorRequestLatency          = NewTimerDef("elasticsearch_bulk_processor_request_latency")
	ElasticsearchBulkProcessorCommitLatency           = NewTimerDef("elasticsearch_bulk_processor_commit_latency")
	ElasticsearchBulkProcessorWaitAddLatency          = NewTimerDef("elasticsearch_bulk_processor_wait_add_latency")
	ElasticsearchBulkProcessorWaitStartLatency        = NewTimerDef("elasticsearch_bulk_processor_wait_start_latency")
	ElasticsearchBulkProcessorBulkSize                = NewDimensionlessHistogramDef("elasticsearch_bulk_processor_bulk_size")
	ElasticsearchBulkProcessorBulkResquestTookLatency = NewTimerDef("elasticsearch_bulk_processor_bulk_request_took_latency")
	ElasticsearchDocumentParseFailuresCount           = NewCounterDef("elasticsearch_document_parse_failures_counter")
	ElasticsearchDocumentGenerateFailuresCount        = NewCounterDef("elasticsearch_document_generate_failures_counter")
	ElasticsearchCustomOrderByClauseCount             = NewCounterDef("elasticsearch_custom_order_by_clause_counter")
	CatchUpReadyShardCountGauge                       = NewGaugeDef("catchup_ready_shard_count")
	HandoverReadyShardCountGauge                      = NewGaugeDef("handover_ready_shard_count")
	ReplicatorMessages                                = NewCounterDef("replicator_messages")
	ReplicatorFailures                                = NewCounterDef("replicator_errors")
	ReplicatorLatency                                 = NewTimerDef("replicator_latency")
	ReplicatorDLQFailures                             = NewCounterDef("replicator_dlq_enqueue_fails")
	NamespaceReplicationEnqueueDLQCount               = NewCounterDef("namespace_replication_dlq_enqueue_requests")
	ParentClosePolicyProcessorSuccess                 = NewCounterDef("parent_close_policy_processor_requests")
	ParentClosePolicyProcessorFailures                = NewCounterDef("parent_close_policy_processor_errors")
	ScheduleMissedCatchupWindow                       = NewCounterDef(
		"schedule_missed_catchup_window",
		WithDescription("The number of times a schedule missed an action due to the configured catchup window"),
	)
	ScheduleRateLimited = NewCounterDef(
		"schedule_rate_limited",
		WithDescription("The number of times a schedule action was delayed by more than 1s due to rate limiting"),
	)
	ScheduleBufferOverruns = NewCounterDef(
		"schedule_buffer_overruns",
		WithDescription("The number of schedule actions that were dropped due to the action buffer being full"),
	)
	ScheduleActionSuccess = NewCounterDef(
		"schedule_action_success",
		WithDescription("The number of schedule actions that were successfully taken by a schedule"),
	)
	ScheduleActionErrors = NewCounterDef(
		"schedule_action_errors",
		WithDescription("The number of schedule actions that failed to start"),
	)
	ScheduleCancelWorkflowErrors = NewCounterDef(
		"schedule_cancel_workflow_errors",
		WithDescription("The number of times a schedule got an error trying to cancel a previous run"),
	)
	ScheduleTerminateWorkflowErrors = NewCounterDef(
		"schedule_terminate_workflow_errors",
		WithDescription("The number of times a schedule got an error trying to terminate a previous run"),
	)
	ScheduleActionDelay = NewTimerDef(
		"schedule_action_delay",
		WithDescription("Delay between when scheduled actions should/actually happen"),
	)

	// Force replication
	EncounterZombieWorkflowCount        = NewCounterDef("encounter_zombie_workflow_count")
	EncounterNotFoundWorkflowCount      = NewCounterDef("encounter_not_found_workflow_count")
	EncounterPassRetentionWorkflowCount = NewCounterDef("encounter_pass_retention_workflow_count")
	GenerateReplicationTasksLatency     = NewTimerDef("generate_replication_tasks_latency")
	VerifyReplicationTaskSuccess        = NewCounterDef("verify_replication_task_success")
	VerifyReplicationTaskNotFound       = NewCounterDef("verify_replication_task_not_found")
	VerifyReplicationTaskFailed         = NewCounterDef("verify_replication_task_failed")
	VerifyReplicationTasksLatency       = NewTimerDef("verify_replication_tasks_latency")
	VerifyDescribeMutableStateLatency   = NewTimerDef("verify_describe_mutable_state_latency")

	// Replication
	NamespaceReplicationTaskAckLevelGauge = NewGaugeDef("namespace_replication_task_ack_level")
	NamespaceReplicationDLQAckLevelGauge  = NewGaugeDef("namespace_dlq_ack_level")
	NamespaceReplicationDLQMaxLevelGauge  = NewGaugeDef("namespace_dlq_max_level")

	// Persistence
	PersistenceRequests = NewCounterDef(
		"persistence_requests",
		WithDescription("Persistence requests, keyed by `operation`"),
	)
	PersistenceFailures      = NewCounterDef("persistence_errors")
	PersistenceErrorWithType = NewCounterDef(
		"persistence_error_with_type",
		WithDescription("Persistence errors, keyed by `error_type`"),
	)
	PersistenceLatency = NewTimerDef(
		"persistence_latency",
		WithDescription("Persistence latency, keyed by `operation`"),
	)
	PersistenceShardRPS                    = NewDimensionlessHistogramDef("persistence_shard_rps")
	PersistenceErrResourceExhaustedCounter = NewCounterDef("persistence_errors_resource_exhausted")
	VisibilityPersistenceRequests          = NewCounterDef("visibility_persistence_requests")
	VisibilityPersistenceErrorWithType     = NewCounterDef("visibility_persistence_error_with_type")
	VisibilityPersistenceFailures          = NewCounterDef("visibility_persistence_errors")
	VisibilityPersistenceResourceExhausted = NewCounterDef("visibility_persistence_resource_exhausted")
	VisibilityPersistenceLatency           = NewTimerDef("visibility_persistence_latency")
	CassandraInitSessionLatency            = NewTimerDef("cassandra_init_session_latency")
	CassandraSessionRefreshFailures        = NewCounterDef("cassandra_session_refresh_failures")
	PersistenceSessionRefreshFailures      = NewCounterDef("persistence_session_refresh_failures")
	PersistenceSessionRefreshAttempts      = NewCounterDef("persistence_session_refresh_attempts")

	// Common service base metrics
	RestartCount         = NewCounterDef("restarts")
	NumGoRoutinesGauge   = NewGaugeDef("num_goroutines")
	GoMaxProcsGauge      = NewGaugeDef("gomaxprocs")
	MemoryAllocatedGauge = NewGaugeDef("memory_allocated")
	MemoryHeapGauge      = NewGaugeDef("memory_heap")
	MemoryHeapIdleGauge  = NewGaugeDef("memory_heapidle")
	MemoryHeapInuseGauge = NewGaugeDef("memory_heapinuse")
	MemoryStackGauge     = NewGaugeDef("memory_stack")
	NumGCCounter         = NewBytesHistogramDef("memory_num_gc")
	GcPauseMsTimer       = NewTimerDef("memory_gc_pause_ms")
)
