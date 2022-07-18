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

// types used/defined by the package
type (
	// MetricName is the name of the metric
	MetricName string

	// MetricType is the type of the metric
	MetricType int

	MetricUnit string

	// metricDefinition contains the definition for a metric
	metricDefinition struct {
		metricType       MetricType // metric type
		metricName       MetricName // metric name
		metricRollupName MetricName // optional. if non-empty, this name must be used for rolled-up version of this metric
		unit             MetricUnit
	}

	// scopeDefinition holds the tag definitions for a scope
	scopeDefinition struct {
		operation string            // 'operation' tag for scope
		tags      map[string]string // additional tags for scope
	}

	// ServiceIdx is an index that uniquely identifies the service
	ServiceIdx int
)

// MetricUnit supported values
// Values are pulled from https://pkg.go.dev/golang.org/x/exp/event#Unit
const (
	Dimensionless = "1"
	Milliseconds  = "ms"
	Bytes         = "By"
)

// MetricTypes which are supported
const (
	Counter MetricType = iota
	Timer
	Gauge
	Histogram
)

// Service names for all services that emit metrics.
const (
	Common ServiceIdx = iota
	Frontend
	History
	Matching
	Worker
	Server
	UnitTestService
	NumServices
)

// Values used for metrics propagation
const (
	HistoryWorkflowExecutionCacheLatency = "history_workflow_execution_cache_latency"
)

// Common tags for all services
const (
	OperationTagName      = "operation"
	ServiceRoleTagName    = "service_role"
	CacheTypeTagName      = "cache_type"
	FailureTagName        = "failure"
	TaskCategoryTagName   = "task_category"
	TaskTypeTagName       = "task_type"
	TaskPriorityTagName   = "task_priority"
	QueueTypeTagName      = "queue_type"
	visibilityTypeTagName = "visibility_type"
	ErrorTypeTagName      = "error_type"
	httpStatusTagName     = "http_status"
	resourceExhaustedTag  = "resource_exhausted_cause"
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

	standardVisibilityTagValue = "standard_visibility"
	advancedVisibilityTagValue = "advanced_visibility"
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

// ServiceMetrics are types for common service base metrics
var ServiceMetrics = map[MetricName]MetricType{
	RestartCount: Counter,
}

// GoRuntimeMetrics represent the runtime stats from go runtime
var GoRuntimeMetrics = map[MetricName]MetricType{
	NumGoRoutinesGauge:   Gauge,
	GoMaxProcsGauge:      Gauge,
	MemoryAllocatedGauge: Gauge,
	MemoryHeapGauge:      Gauge,
	MemoryHeapIdleGauge:  Gauge,
	MemoryHeapInuseGauge: Gauge,
	MemoryStackGauge:     Gauge,
	NumGCCounter:         Counter,
	GcPauseMsTimer:       Timer,
}

// Scopes enum
const (
	UnknownScope = iota

	// -- Common Operation scopes --

	// PersistenceGetOrCreateShardScope tracks GetOrCreateShard calls made by service to persistence layer
	PersistenceGetOrCreateShardScope
	// PersistenceUpdateShardScope tracks UpdateShard calls made by service to persistence layer
	PersistenceUpdateShardScope
	// PersistenceAssertShardOwnershipScope tracks UpdateShard calls made by service to persistence layer
	PersistenceAssertShardOwnershipScope
	// PersistenceCreateWorkflowExecutionScope tracks CreateWorkflowExecution calls made by service to persistence layer
	PersistenceCreateWorkflowExecutionScope
	// PersistenceGetWorkflowExecutionScope tracks GetWorkflowExecution calls made by service to persistence layer
	PersistenceGetWorkflowExecutionScope
	// PersistenceSetWorkflowExecutionScope tracks SetWorkflowExecution calls made by service to persistence layer
	PersistenceSetWorkflowExecutionScope
	// PersistenceUpdateWorkflowExecutionScope tracks UpdateWorkflowExecution calls made by service to persistence layer
	PersistenceUpdateWorkflowExecutionScope
	// PersistenceConflictResolveWorkflowExecutionScope tracks ConflictResolveWorkflowExecution calls made by service to persistence layer
	PersistenceConflictResolveWorkflowExecutionScope
	// PersistenceResetWorkflowExecutionScope tracks ResetWorkflowExecution calls made by service to persistence layer
	PersistenceResetWorkflowExecutionScope
	// PersistenceDeleteWorkflowExecutionScope tracks DeleteWorkflowExecution calls made by service to persistence layer
	PersistenceDeleteWorkflowExecutionScope
	// PersistenceDeleteCurrentWorkflowExecutionScope tracks DeleteCurrentWorkflowExecution calls made by service to persistence layer
	PersistenceDeleteCurrentWorkflowExecutionScope
	// PersistenceGetCurrentExecutionScope tracks GetCurrentExecution calls made by service to persistence layer
	PersistenceGetCurrentExecutionScope
	// PersistenceListConcreteExecutionsScope tracks ListConcreteExecutions calls made by service to persistence layer
	PersistenceListConcreteExecutionsScope
	// PersistenceAddTasksScope tracks AddTasks calls made by service to persistence layer
	PersistenceAddTasksScope
	// PersistenceGetTransferTaskScope tracks GetTransferTask calls made by service to persistence layer
	PersistenceGetTransferTaskScope
	// PersistenceGetTransferTasksScope tracks GetTransferTasks calls made by service to persistence layer
	PersistenceGetTransferTasksScope
	// PersistenceCompleteTransferTaskScope tracks CompleteTransferTasks calls made by service to persistence layer
	PersistenceCompleteTransferTaskScope
	// PersistenceRangeCompleteTransferTasksScope tracks CompleteTransferTasks calls made by service to persistence layer
	PersistenceRangeCompleteTransferTasksScope

	// PersistenceGetVisibilityTaskScope tracks GetVisibilityTask calls made by service to persistence layer
	PersistenceGetVisibilityTaskScope
	// PersistenceGetVisibilityTasksScope tracks GetVisibilityTasks calls made by service to persistence layer
	PersistenceGetVisibilityTasksScope
	// PersistenceCompleteVisibilityTaskScope tracks CompleteVisibilityTasks calls made by service to persistence layer
	PersistenceCompleteVisibilityTaskScope
	// PersistenceRangeCompleteVisibilityTasksScope tracks CompleteVisibilityTasks calls made by service to persistence layer
	PersistenceRangeCompleteVisibilityTasksScope

	// PersistenceGetReplicationTaskScope tracks GetReplicationTask calls made by service to persistence layer
	PersistenceGetReplicationTaskScope
	// PersistenceGetReplicationTasksScope tracks GetReplicationTasks calls made by service to persistence layer
	PersistenceGetReplicationTasksScope
	// PersistenceCompleteReplicationTaskScope tracks CompleteReplicationTasks calls made by service to persistence layer
	PersistenceCompleteReplicationTaskScope
	// PersistenceRangeCompleteReplicationTasksScope tracks RangeCompleteReplicationTasks calls made by service to persistence layer
	PersistenceRangeCompleteReplicationTasksScope
	// PersistencePutReplicationTaskToDLQScope tracks PersistencePutReplicationTaskToDLQScope calls made by service to persistence layer
	PersistencePutReplicationTaskToDLQScope
	// PersistenceGetReplicationTasksFromDLQScope tracks PersistenceGetReplicationTasksFromDLQScope calls made by service to persistence layer
	PersistenceGetReplicationTasksFromDLQScope
	// PersistenceDeleteReplicationTaskFromDLQScope tracks PersistenceDeleteReplicationTaskFromDLQScope calls made by service to persistence layer
	PersistenceDeleteReplicationTaskFromDLQScope
	// PersistenceRangeDeleteReplicationTaskFromDLQScope tracks PersistenceRangeDeleteReplicationTaskFromDLQScope calls made by service to persistence layer
	PersistenceRangeDeleteReplicationTaskFromDLQScope
	// PersistenceGetTimerTaskScope tracks GetTimerTask calls made by service to persistence layer
	PersistenceGetTimerTaskScope
	// PersistenceGetTimerTasksScope tracks GetTimerTasks calls made by service to persistence layer
	PersistenceGetTimerTasksScope
	// PersistenceCompleteTimerTaskScope tracks CompleteTimerTasks calls made by service to persistence layer
	PersistenceCompleteTimerTaskScope
	// PersistenceRangeCompleteTimerTasksScope tracks CompleteTimerTasks calls made by service to persistence layer
	PersistenceRangeCompleteTimerTasksScope
	// PersistenceCreateTaskScope tracks CreateTask calls made by service to persistence layer
	PersistenceCreateTaskScope
	// PersistenceGetTasksScope tracks GetTasks calls made by service to persistence layer
	PersistenceGetTasksScope
	// PersistenceCompleteTaskScope tracks CompleteTask calls made by service to persistence layer
	PersistenceCompleteTaskScope
	// PersistenceCompleteTasksLessThanScope is the metric scope for persistence.TaskManager.PersistenceCompleteTasksLessThan API
	PersistenceCompleteTasksLessThanScope
	// PersistenceCreateTaskQueueScope tracks PersistenceCreateTaskQueueScope calls made by service to persistence layer
	PersistenceCreateTaskQueueScope
	// PersistenceUpdateTaskQueueScope tracks PersistenceUpdateTaskQueueScope calls made by service to persistence layer
	PersistenceUpdateTaskQueueScope
	// PersistenceGetTaskQueueScope tracks PersistenceGetTaskQueueScope calls made by service to persistence layer
	PersistenceGetTaskQueueScope
	// PersistenceListTaskQueueScope is the metric scope for persistence.TaskManager.ListTaskQueue API
	PersistenceListTaskQueueScope
	// PersistenceDeleteTaskQueueScope is the metric scope for persistence.TaskManager.DeleteTaskQueue API
	PersistenceDeleteTaskQueueScope
	// PersistenceAppendHistoryEventsScope tracks AppendHistoryEvents calls made by service to persistence layer
	PersistenceAppendHistoryEventsScope
	// PersistenceGetWorkflowExecutionHistoryScope tracks GetWorkflowExecutionHistory calls made by service to persistence layer
	PersistenceGetWorkflowExecutionHistoryScope
	// PersistenceDeleteWorkflowExecutionHistoryScope tracks DeleteWorkflowExecutionHistory calls made by service to persistence layer
	PersistenceDeleteWorkflowExecutionHistoryScope
	// PersistenceInitializeSystemNamespaceScope tracks InitializeSystemNamespaceScope calls made by service to persistence layer
	PersistenceInitializeSystemNamespaceScope
	// PersistenceCreateNamespaceScope tracks CreateNamespace calls made by service to persistence layer
	PersistenceCreateNamespaceScope
	// PersistenceGetNamespaceScope tracks GetNamespace calls made by service to persistence layer
	PersistenceGetNamespaceScope
	// PersistenceUpdateNamespaceScope tracks UpdateNamespace calls made by service to persistence layer
	PersistenceUpdateNamespaceScope
	// PersistenceDeleteNamespaceScope tracks DeleteNamespace calls made by service to persistence layer
	PersistenceDeleteNamespaceScope
	// PersistenceRenameNamespaceScope tracks RenameNamespace calls made by service to persistence layer
	PersistenceRenameNamespaceScope
	// PersistenceDeleteNamespaceByNameScope tracks DeleteNamespaceByName calls made by service to persistence layer
	PersistenceDeleteNamespaceByNameScope
	// PersistenceListNamespaceScope tracks DeleteNamespaceByName calls made by service to persistence layer
	PersistenceListNamespaceScope
	// PersistenceGetMetadataScope tracks DeleteNamespaceByName calls made by service to persistence layer
	PersistenceGetMetadataScope

	// VisibilityPersistenceRecordWorkflowExecutionStartedScope tracks RecordWorkflowExecutionStarted calls made by service to visibility persistence layer
	VisibilityPersistenceRecordWorkflowExecutionStartedScope
	// VisibilityPersistenceRecordWorkflowExecutionClosedScope tracks RecordWorkflowExecutionClosed calls made by service to visibility persistence layer
	VisibilityPersistenceRecordWorkflowExecutionClosedScope
	// VisibilityPersistenceUpsertWorkflowExecutionScope tracks UpsertWorkflowExecution calls made by service to persistence visibility layer
	VisibilityPersistenceUpsertWorkflowExecutionScope
	// VisibilityPersistenceListOpenWorkflowExecutionsScope tracks ListOpenWorkflowExecutions calls made by service to visibility persistence layer
	VisibilityPersistenceListOpenWorkflowExecutionsScope
	// VisibilityPersistenceListClosedWorkflowExecutionsScope tracks ListClosedWorkflowExecutions calls made by service to visibility persistence layer
	VisibilityPersistenceListClosedWorkflowExecutionsScope
	// VisibilityPersistenceListOpenWorkflowExecutionsByTypeScope tracks ListOpenWorkflowExecutionsByType calls made by service to visibility persistence layer
	VisibilityPersistenceListOpenWorkflowExecutionsByTypeScope
	// VisibilityPersistenceListClosedWorkflowExecutionsByTypeScope tracks ListClosedWorkflowExecutionsByType calls made by service to visibility persistence layer
	VisibilityPersistenceListClosedWorkflowExecutionsByTypeScope
	// VisibilityPersistenceListOpenWorkflowExecutionsByWorkflowIDScope tracks ListOpenWorkflowExecutionsByWorkflowID calls made by service to visibility persistence layer
	VisibilityPersistenceListOpenWorkflowExecutionsByWorkflowIDScope
	// VisibilityPersistenceListClosedWorkflowExecutionsByWorkflowIDScope tracks ListClosedWorkflowExecutionsByWorkflowID calls made by service to visibility persistence layer
	VisibilityPersistenceListClosedWorkflowExecutionsByWorkflowIDScope
	// VisibilityPersistenceListClosedWorkflowExecutionsByStatusScope tracks ListClosedWorkflowExecutionsByStatus calls made by service to visibility persistence layer
	VisibilityPersistenceListClosedWorkflowExecutionsByStatusScope
	// VisibilityPersistenceDeleteWorkflowExecutionScope tracks DeleteWorkflowExecutions calls made by service to visibility persistence layer
	VisibilityPersistenceDeleteWorkflowExecutionScope
	// VisibilityPersistenceListWorkflowExecutionsScope tracks ListWorkflowExecutions calls made by service to visibility persistence layer
	VisibilityPersistenceListWorkflowExecutionsScope
	// VisibilityPersistenceScanWorkflowExecutionsScope tracks ScanWorkflowExecutions calls made by service to visibility persistence layer
	VisibilityPersistenceScanWorkflowExecutionsScope
	// VisibilityPersistenceCountWorkflowExecutionsScope tracks CountWorkflowExecutions calls made by service to visibility persistence layer
	VisibilityPersistenceCountWorkflowExecutionsScope

	// PersistenceEnqueueMessageScope tracks Enqueue calls made by service to persistence layer
	PersistenceEnqueueMessageScope
	// PersistenceEnqueueMessageToDLQScope tracks Enqueue DLQ calls made by service to persistence layer
	PersistenceEnqueueMessageToDLQScope
	// PersistenceReadQueueMessagesScope tracks ReadMessages calls made by service to persistence layer
	PersistenceReadQueueMessagesScope
	// PersistenceReadQueueMessagesFromDLQScope tracks ReadMessagesFromDLQ calls made by service to persistence layer
	PersistenceReadQueueMessagesFromDLQScope
	// PersistenceDeleteQueueMessagesScope tracks DeleteMessages calls made by service to persistence layer
	PersistenceDeleteQueueMessagesScope
	// PersistenceDeleteQueueMessageFromDLQScope tracks DeleteMessageFromDLQ calls made by service to persistence layer
	PersistenceDeleteQueueMessageFromDLQScope
	// PersistenceRangeDeleteMessagesFromDLQScope tracks RangeDeleteMessagesFromDLQ calls made by service to persistence layer
	PersistenceRangeDeleteMessagesFromDLQScope
	// PersistenceUpdateAckLevelScope tracks UpdateAckLevel calls made by service to persistence layer
	PersistenceUpdateAckLevelScope
	// PersistenceGetAckLevelScope tracks GetAckLevel calls made by service to persistence layer
	PersistenceGetAckLevelScope
	// PersistenceUpdateDLQAckLevelScope tracks UpdateDLQAckLevel calls made by service to persistence layer
	PersistenceUpdateDLQAckLevelScope
	// PersistenceGetDLQAckLevelScope tracks GetDLQAckLevel calls made by service to persistence layer
	PersistenceGetDLQAckLevelScope
	// PersistenceListClusterMetadataScope tracks ListClusterMetadata calls made by service to persistence layer
	PersistenceListClusterMetadataScope
	// PersistenceGetClusterMetadataScope tracks GetClusterMetadata calls made by service to persistence layer
	PersistenceGetClusterMetadataScope
	// PersistenceSaveClusterMetadataScope tracks SaveClusterMetadata calls made by service to persistence layer
	PersistenceSaveClusterMetadataScope
	// PersistenceDeleteClusterMetadataScope tracks DeleteClusterMetadata calls made by service to persistence layer
	PersistenceDeleteClusterMetadataScope
	// PersistenceUpsertClusterMembershipScope tracks UpsertClusterMembership calls made by service to persistence layer
	PersistenceUpsertClusterMembershipScope
	// PersistencePruneClusterMembershipScope tracks PruneClusterMembership calls made by service to persistence layer
	PersistencePruneClusterMembershipScope
	// PersistenceGetClusterMembersScope tracks GetClusterMembers calls made by service to persistence layer
	PersistenceGetClusterMembersScope
	// HistoryClientStartWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientStartWorkflowExecutionScope
	// HistoryClientRecordActivityTaskHeartbeatScope tracks RPC calls to history service
	HistoryClientRecordActivityTaskHeartbeatScope
	// HistoryClientRespondWorkflowTaskCompletedScope tracks RPC calls to history service
	HistoryClientRespondWorkflowTaskCompletedScope
	// HistoryClientRespondWorkflowTaskFailedScope tracks RPC calls to history service
	HistoryClientRespondWorkflowTaskFailedScope
	// HistoryClientRespondActivityTaskCompletedScope tracks RPC calls to history service
	HistoryClientRespondActivityTaskCompletedScope
	// HistoryClientRespondActivityTaskFailedScope tracks RPC calls to history service
	HistoryClientRespondActivityTaskFailedScope
	// HistoryClientRespondActivityTaskCanceledScope tracks RPC calls to history service
	HistoryClientRespondActivityTaskCanceledScope
	// HistoryClientGetMutableStateScope tracks RPC calls to history service
	HistoryClientGetMutableStateScope
	// HistoryClientPollMutableStateScope tracks RPC calls to history service
	HistoryClientPollMutableStateScope
	// HistoryClientResetStickyTaskQueueScope tracks RPC calls to history service
	HistoryClientResetStickyTaskQueueScope
	// HistoryClientDescribeWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientDescribeWorkflowExecutionScope
	// HistoryClientRecordWorkflowTaskStartedScope tracks RPC calls to history service
	HistoryClientRecordWorkflowTaskStartedScope
	// HistoryClientRecordActivityTaskStartedScope tracks RPC calls to history service
	HistoryClientRecordActivityTaskStartedScope
	// HistoryClientRequestCancelWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientRequestCancelWorkflowExecutionScope
	// HistoryClientSignalWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientSignalWorkflowExecutionScope
	// HistoryClientSignalWithStartWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientSignalWithStartWorkflowExecutionScope
	// HistoryClientRemoveSignalMutableStateScope tracks RPC calls to history service
	HistoryClientRemoveSignalMutableStateScope
	// HistoryClientTerminateWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientTerminateWorkflowExecutionScope
	// HistoryClientUpdateWorkflowScope tracks RPC calls to history service
	HistoryClientUpdateWorkflowScope
	// HistoryClientDeleteWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientDeleteWorkflowExecutionScope
	// HistoryClientResetWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientResetWorkflowExecutionScope
	// HistoryClientScheduleWorkflowTaskScope tracks RPC calls to history service
	HistoryClientScheduleWorkflowTaskScope
	// HistoryClientVerifyFirstWorkflowTaskScheduled tracks RPC calls to history service
	HistoryClientVerifyFirstWorkflowTaskScheduled
	// HistoryClientRecordChildExecutionCompletedScope tracks RPC calls to history service
	HistoryClientRecordChildExecutionCompletedScope
	// HistoryClientVerifyChildExecutionCompletionRecordedScope tracks RPC calls to history service
	HistoryClientVerifyChildExecutionCompletionRecordedScope
	// HistoryClientReplicateEventsV2Scope tracks RPC calls to history service
	HistoryClientReplicateEventsV2Scope
	// HistoryClientSyncShardStatusScope tracks RPC calls to history service
	HistoryClientSyncShardStatusScope
	// HistoryClientSyncActivityScope tracks RPC calls to history service
	HistoryClientSyncActivityScope
	// HistoryClientGetReplicationTasksScope tracks RPC calls to history service
	HistoryClientGetReplicationTasksScope
	// HistoryClientGetDLQReplicationTasksScope tracks RPC calls to history service
	HistoryClientGetDLQReplicationTasksScope
	// HistoryClientQueryWorkflowScope tracks RPC calls to history service
	HistoryClientQueryWorkflowScope
	// HistoryClientReapplyEventsScope tracks RPC calls to history service
	HistoryClientReapplyEventsScope
	// HistoryClientGetDLQMessagesScope tracks RPC calls to history service
	HistoryClientGetDLQMessagesScope
	// HistoryClientPurgeDLQMessagesScope tracks RPC calls to history service
	HistoryClientPurgeDLQMessagesScope
	// HistoryClientMergeDLQMessagesScope tracks RPC calls to history service
	HistoryClientMergeDLQMessagesScope
	// HistoryClientRefreshWorkflowTasksScope tracks RPC calls to history service
	HistoryClientRefreshWorkflowTasksScope
	// HistoryClientGenerateLastHistoryReplicationTasksScope tracks RPC calls to history service
	HistoryClientGenerateLastHistoryReplicationTasksScope
	// HistoryClientGetReplicationStatusScope tracks RPC calls to history service
	HistoryClientGetReplicationStatusScope
	// HistoryClientDeleteWorkflowVisibilityRecordScope tracks RPC calls to history service
	HistoryClientDeleteWorkflowVisibilityRecordScope
	// HistoryClientCloseShardScope tracks RPC calls to history service
	HistoryClientCloseShardScope
	// HistoryClientDescribeMutableStateScope tracks RPC calls to history service
	HistoryClientDescribeMutableStateScope
	// HistoryClientGetDLQReplicationMessagesScope tracks RPC calls to history service
	HistoryClientGetDLQReplicationMessagesScope
	// HistoryClientGetShardScope tracks RPC calls to history service
	HistoryClientGetShardScope
	// HistoryClientRebuildMutableStateScope tracks RPC calls to history service
	HistoryClientRebuildMutableStateScope
	// HistoryClientRemoveTaskScope tracks RPC calls to history service
	HistoryClientRemoveTaskScope
	// HistoryClientVerifyFirstWorkflowTaskScheduledScope tracks RPC calls to history service
	HistoryClientVerifyFirstWorkflowTaskScheduledScope
	// HistoryClientDescribeHistoryHostScope tracks RPC calls to history service
	HistoryClientDescribeHistoryHostScope
	// HistoryClientGetReplicationMessagesScope tracks RPC calls to history service
	HistoryClientGetReplicationMessagesScope
	// MatchingClientPollWorkflowTaskQueueScope tracks RPC calls to matching service
	MatchingClientPollWorkflowTaskQueueScope
	// MatchingClientPollActivityTaskQueueScope tracks RPC calls to matching service
	MatchingClientPollActivityTaskQueueScope
	// MatchingClientAddActivityTaskScope tracks RPC calls to matching service
	MatchingClientAddActivityTaskScope
	// MatchingClientAddWorkflowTaskScope tracks RPC calls to matching service
	MatchingClientAddWorkflowTaskScope
	// MatchingClientQueryWorkflowScope tracks RPC calls to matching service
	MatchingClientQueryWorkflowScope
	// MatchingClientRespondQueryTaskCompletedScope tracks RPC calls to matching service
	MatchingClientRespondQueryTaskCompletedScope
	// MatchingClientCancelOutstandingPollScope tracks RPC calls to matching service
	MatchingClientCancelOutstandingPollScope
	// MatchingClientDescribeTaskQueueScope tracks RPC calls to matching service
	MatchingClientDescribeTaskQueueScope
	// MatchingClientListTaskQueuePartitionsScope tracks RPC calls to matching service
	MatchingClientListTaskQueuePartitionsScope
	// MatchingClientUpdateWorkerBuildIdOrderingScope tracks RPC calls to matching service
	MatchingClientUpdateWorkerBuildIdOrderingScope
	// MatchingGetBuildIdOrdering tracks RPC calls to matching service
	MatchingClientGetWorkerBuildIdOrderingScope
	// FrontendClientDeprecateNamespaceScope tracks RPC calls to frontend service
	FrontendClientDeprecateNamespaceScope
	// FrontendClientDescribeNamespaceScope tracks RPC calls to frontend service
	FrontendClientDescribeNamespaceScope
	// FrontendClientDescribeTaskQueueScope tracks RPC calls to frontend service
	FrontendClientDescribeTaskQueueScope
	// FrontendClientDescribeWorkflowExecutionScope tracks RPC calls to frontend service
	FrontendClientDescribeWorkflowExecutionScope
	// FrontendClientGetWorkflowExecutionHistoryScope tracks RPC calls to frontend service
	FrontendClientGetWorkflowExecutionHistoryScope
	// FrontendClientGetWorkflowExecutionHistoryReverseScope tracks RPC calls to frontend service
	FrontendClientGetWorkflowExecutionHistoryReverseScope
	// FrontendClientGetWorkflowExecutionRawHistoryScope tracks RPC calls to frontend service
	FrontendClientGetWorkflowExecutionRawHistoryScope
	// FrontendClientPollForWorkflowExecutionRawHistoryScope tracks RPC calls to frontend service
	FrontendClientPollForWorkflowExecutionRawHistoryScope
	// FrontendClientListArchivedWorkflowExecutionsScope tracks RPC calls to frontend service
	FrontendClientListArchivedWorkflowExecutionsScope
	// FrontendClientListClosedWorkflowExecutionsScope tracks RPC calls to frontend service
	FrontendClientListClosedWorkflowExecutionsScope
	// FrontendClientListNamespacesScope tracks RPC calls to frontend service
	FrontendClientListNamespacesScope
	// FrontendClientListOpenWorkflowExecutionsScope tracks RPC calls to frontend service
	FrontendClientListOpenWorkflowExecutionsScope
	// FrontendClientPollActivityTaskQueueScope tracks RPC calls to frontend service
	FrontendClientPollActivityTaskQueueScope
	// FrontendClientPollWorkflowTaskQueueScope tracks RPC calls to frontend service
	FrontendClientPollWorkflowTaskQueueScope
	// FrontendClientQueryWorkflowScope tracks RPC calls to frontend service
	FrontendClientQueryWorkflowScope
	// FrontendClientRecordActivityTaskHeartbeatScope tracks RPC calls to frontend service
	FrontendClientRecordActivityTaskHeartbeatScope
	// FrontendClientRecordActivityTaskHeartbeatByIdScope tracks RPC calls to frontend service
	FrontendClientRecordActivityTaskHeartbeatByIdScope
	// FrontendClientRegisterNamespaceScope tracks RPC calls to frontend service
	FrontendClientRegisterNamespaceScope
	// FrontendClientRequestCancelWorkflowExecutionScope tracks RPC calls to frontend service
	FrontendClientRequestCancelWorkflowExecutionScope
	// FrontendClientResetStickyTaskQueueScope tracks RPC calls to frontend service
	FrontendClientResetStickyTaskQueueScope
	// FrontendClientResetWorkflowExecutionScope tracks RPC calls to frontend service
	FrontendClientResetWorkflowExecutionScope
	// FrontendClientRespondActivityTaskCanceledScope tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCanceledScope
	// FrontendClientRespondActivityTaskCanceledByIdScope tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCanceledByIdScope
	// FrontendClientRespondActivityTaskCompletedScope tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCompletedScope
	// FrontendClientRespondActivityTaskCompletedByIdScope tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCompletedByIdScope
	// FrontendClientRespondActivityTaskFailedScope tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskFailedScope
	// FrontendClientRespondActivityTaskFailedByIdScope tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskFailedByIdScope
	// FrontendClientRespondWorkflowTaskCompletedScope tracks RPC calls to frontend service
	FrontendClientRespondWorkflowTaskCompletedScope
	// FrontendClientRespondWorkflowTaskFailedScope tracks RPC calls to frontend service
	FrontendClientRespondWorkflowTaskFailedScope
	// FrontendClientRespondQueryTaskCompletedScope tracks RPC calls to frontend service
	FrontendClientRespondQueryTaskCompletedScope
	// FrontendClientSignalWithStartWorkflowExecutionScope tracks RPC calls to frontend service
	FrontendClientSignalWithStartWorkflowExecutionScope
	// FrontendClientSignalWorkflowExecutionScope tracks RPC calls to frontend service
	FrontendClientSignalWorkflowExecutionScope
	// FrontendClientStartWorkflowExecutionScope tracks RPC calls to frontend service
	FrontendClientStartWorkflowExecutionScope
	// FrontendClientTerminateWorkflowExecutionScope tracks RPC calls to frontend service
	FrontendClientTerminateWorkflowExecutionScope
	// FrontendClientUpdateNamespaceScope tracks RPC calls to frontend service
	FrontendClientUpdateNamespaceScope
	// FrontendClientListWorkflowExecutionsScope tracks RPC calls to frontend service
	FrontendClientListWorkflowExecutionsScope
	// FrontendClientScanWorkflowExecutionsScope tracks RPC calls to frontend service
	FrontendClientScanWorkflowExecutionsScope
	// FrontendClientCountWorkflowExecutionsScope tracks RPC calls to frontend service
	FrontendClientCountWorkflowExecutionsScope
	// FrontendClientGetSearchAttributesScope tracks RPC calls to frontend service
	FrontendClientGetSearchAttributesScope
	// FrontendClientGetClusterInfoScope tracks RPC calls to frontend
	FrontendClientGetClusterInfoScope
	// FrontendClientGetSystemInfoScope tracks RPC calls to frontend
	FrontendClientGetSystemInfoScope
	// FrontendClientListTaskQueuePartitionsScope tracks RPC calls to frontend service
	FrontendClientListTaskQueuePartitionsScope
	// FrontendClientCreateScheduleScope tracks RPC calls to frontend service
	FrontendClientCreateScheduleScope
	// FrontendClientDescribeScheduleScope tracks RPC calls to frontend service
	FrontendClientDescribeScheduleScope
	// FrontendClientUpdateScheduleScope tracks RPC calls to frontend service
	FrontendClientUpdateScheduleScope
	// FrontendClientPatchScheduleScope tracks RPC calls to frontend service
	FrontendClientPatchScheduleScope
	// FrontendClientListScheduleMatchingTimesScope tracks RPC calls to frontend service
	FrontendClientListScheduleMatchingTimesScope
	// FrontendClientDeleteScheduleScope tracks RPC calls to frontend service
	FrontendClientDeleteScheduleScope
	// FrontendClientListSchedulesScope tracks RPC calls to frontend service
	FrontendClientListSchedulesScope
	// FrontendClientUpdateWorkerBuildIdOrderingScope tracks RPC calls to frontend service
	FrontendClientUpdateWorkerBuildIdOrderingScope
	// FrontendClientUpdateWorkflowScope tracks RPC calls to frontend service
	FrontendClientUpdateWorkflowScope
	// FrontendClientGetWorkerBuildIdOrderingScope tracks RPC calls to frontend service
	FrontendClientGetWorkerBuildIdOrderingScope
	// AdminClientAddSearchAttributesScope tracks RPC calls to admin service
	AdminClientAddSearchAttributesScope
	// AdminClientRemoveSearchAttributesScope tracks RPC calls to admin service
	AdminClientRemoveSearchAttributesScope
	// AdminClientGetSearchAttributesScope tracks RPC calls to admin service
	AdminClientGetSearchAttributesScope
	// AdminClientCloseShardScope tracks RPC calls to admin service
	AdminClientCloseShardScope
	// AdminClientGetShardScope tracks RPC calls to admin service
	AdminClientGetShardScope
	// AdminClientListHistoryTasksScope tracks RPC calls to admin service
	AdminClientListHistoryTasksScope
	// AdminClientRemoveTaskScope tracks RPC calls to admin service
	AdminClientRemoveTaskScope
	// AdminClientDescribeHistoryHostScope tracks RPC calls to admin service
	AdminClientDescribeHistoryHostScope
	// AdminClientRebuildMutableStateScope tracks RPC calls to admin service
	AdminClientRebuildMutableStateScope
	// AdminClientDescribeMutableStateScope tracks RPC calls to admin service
	AdminClientDescribeMutableStateScope
	// AdminClientGetWorkflowExecutionRawHistoryScope tracks RPC calls to admin service
	AdminClientGetWorkflowExecutionRawHistoryScope
	// AdminClientGetWorkflowExecutionRawHistoryV2Scope tracks RPC calls to admin service
	AdminClientGetWorkflowExecutionRawHistoryV2Scope
	// AdminClientDescribeClusterScope tracks RPC calls to admin service
	AdminClientDescribeClusterScope
	// AdminClientListClustersScope tracks RPC calls to admin service
	AdminClientListClustersScope
	// AdminClientListClusterMembersScope tracks RPC calls to admin service
	AdminClientListClusterMembersScope
	// AdminClientAddOrUpdateRemoteClusterScope tracks RPC calls to admin service
	AdminClientAddOrUpdateRemoteClusterScope
	// AdminClientRemoveRemoteClusterScope tracks RPC calls to admin service
	AdminClientRemoveRemoteClusterScope
	// AdminClientGetReplicationMessagesScope tracks RPC calls to admin service
	AdminClientGetReplicationMessagesScope
	// AdminClientGetNamespaceReplicationMessagesScope tracks RPC calls to admin service
	AdminClientGetNamespaceReplicationMessagesScope
	// AdminClientGetDLQReplicationMessagesScope tracks RPC calls to admin service
	AdminClientGetDLQReplicationMessagesScope
	// AdminClientReapplyEventsScope tracks RPC calls to admin service
	AdminClientReapplyEventsScope
	// AdminClientGetDLQMessagesScope tracks RPC calls to admin service
	AdminClientGetDLQMessagesScope
	// AdminClientPurgeDLQMessagesScope tracks RPC calls to admin service
	AdminClientPurgeDLQMessagesScope
	// AdminClientMergeDLQMessagesScope tracks RPC calls to admin service
	AdminClientMergeDLQMessagesScope
	// AdminClientRefreshWorkflowTasksScope tracks RPC calls to admin service
	AdminClientRefreshWorkflowTasksScope
	// AdminClientResendReplicationTasksScope tracks RPC calls to admin service
	AdminClientResendReplicationTasksScope
	// AdminClientGetTaskQueueTasksScope tracks RPC calls to admin service
	AdminClientGetTaskQueueTasksScope
	// AdminClientDeleteWorkflowExecutionScope tracks RPC calls to admin service
	AdminClientDeleteWorkflowExecutionScope
	// DCRedirectionDeprecateNamespaceScope tracks RPC calls for dc redirection
	DCRedirectionDeprecateNamespaceScope
	// DCRedirectionDescribeNamespaceScope tracks RPC calls for dc redirection
	DCRedirectionDescribeNamespaceScope
	// DCRedirectionDescribeTaskQueueScope tracks RPC calls for dc redirection
	DCRedirectionDescribeTaskQueueScope
	// DCRedirectionDescribeWorkflowExecutionScope tracks RPC calls for dc redirection
	DCRedirectionDescribeWorkflowExecutionScope
	// DCRedirectionGetWorkflowExecutionHistoryScope tracks RPC calls for dc redirection
	DCRedirectionGetWorkflowExecutionHistoryScope
	// DCRedirectionGetWorkflowExecutionHistoryReverseScope tracks RPC calls for dc redirection
	DCRedirectionGetWorkflowExecutionHistoryReverseScope
	// DCRedirectionGetWorkflowExecutionRawHistoryScope tracks RPC calls for dc redirection
	DCRedirectionGetWorkflowExecutionRawHistoryScope
	// DCRedirectionPollForWorkflowExecutionRawHistoryScope tracks RPC calls for dc redirection
	DCRedirectionPollForWorkflowExecutionRawHistoryScope
	// DCRedirectionListArchivedWorkflowExecutionsScope tracks RPC calls for dc redirection
	DCRedirectionListArchivedWorkflowExecutionsScope
	// DCRedirectionListClosedWorkflowExecutionsScope tracks RPC calls for dc redirection
	DCRedirectionListClosedWorkflowExecutionsScope
	// DCRedirectionListNamespacesScope tracks RPC calls for dc redirection
	DCRedirectionListNamespacesScope
	// DCRedirectionListOpenWorkflowExecutionsScope tracks RPC calls for dc redirection
	DCRedirectionListOpenWorkflowExecutionsScope
	// DCRedirectionListWorkflowExecutionsScope tracks RPC calls for dc redirection
	DCRedirectionListWorkflowExecutionsScope
	// DCRedirectionScanWorkflowExecutionsScope tracks RPC calls for dc redirection
	DCRedirectionScanWorkflowExecutionsScope
	// DCRedirectionCountWorkflowExecutionsScope tracks RPC calls for dc redirection
	DCRedirectionCountWorkflowExecutionsScope
	// DCRedirectionGetSearchAttributesScope tracks RPC calls for dc redirection
	DCRedirectionGetSearchAttributesScope
	// DCRedirectionPollActivityTaskQueueScope tracks RPC calls for dc redirection
	DCRedirectionPollActivityTaskQueueScope
	// DCRedirectionPollWorkflowTaskQueueScope tracks RPC calls for dc redirection
	DCRedirectionPollWorkflowTaskQueueScope
	// DCRedirectionQueryWorkflowScope tracks RPC calls for dc redirection
	DCRedirectionQueryWorkflowScope
	// DCRedirectionRecordActivityTaskHeartbeatScope tracks RPC calls for dc redirection
	DCRedirectionRecordActivityTaskHeartbeatScope
	// DCRedirectionRecordActivityTaskHeartbeatByIdScope tracks RPC calls for dc redirection
	DCRedirectionRecordActivityTaskHeartbeatByIdScope
	// DCRedirectionRegisterNamespaceScope tracks RPC calls for dc redirection
	DCRedirectionRegisterNamespaceScope
	// DCRedirectionRequestCancelWorkflowExecutionScope tracks RPC calls for dc redirection
	DCRedirectionRequestCancelWorkflowExecutionScope
	// DCRedirectionResetStickyTaskQueueScope tracks RPC calls for dc redirection
	DCRedirectionResetStickyTaskQueueScope
	// DCRedirectionResetWorkflowExecutionScope tracks RPC calls for dc redirection
	DCRedirectionResetWorkflowExecutionScope
	// DCRedirectionRespondActivityTaskCanceledScope tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskCanceledScope
	// DCRedirectionRespondActivityTaskCanceledByIdScope tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskCanceledByIdScope
	// DCRedirectionRespondActivityTaskCompletedScope tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskCompletedScope
	// DCRedirectionRespondActivityTaskCompletedByIdScope tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskCompletedByIdScope
	// DCRedirectionRespondActivityTaskFailedScope tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskFailedScope
	// DCRedirectionRespondActivityTaskFailedByIdScope tracks RPC calls for dc redirection
	DCRedirectionRespondActivityTaskFailedByIdScope
	// DCRedirectionRespondWorkflowTaskCompletedScope tracks RPC calls for dc redirection
	DCRedirectionRespondWorkflowTaskCompletedScope
	// DCRedirectionRespondWorkflowTaskFailedScope tracks RPC calls for dc redirection
	DCRedirectionRespondWorkflowTaskFailedScope
	// DCRedirectionRespondQueryTaskCompletedScope tracks RPC calls for dc redirection
	DCRedirectionRespondQueryTaskCompletedScope
	// DCRedirectionSignalWithStartWorkflowExecutionScope tracks RPC calls for dc redirection
	DCRedirectionSignalWithStartWorkflowExecutionScope
	// DCRedirectionSignalWorkflowExecutionScope tracks RPC calls for dc redirection
	DCRedirectionSignalWorkflowExecutionScope
	// DCRedirectionStartWorkflowExecutionScope tracks RPC calls for dc redirection
	DCRedirectionStartWorkflowExecutionScope
	// DCRedirectionTerminateWorkflowExecutionScope tracks RPC calls for dc redirection
	DCRedirectionTerminateWorkflowExecutionScope
	// DCRedirectionUpdateNamespaceScope tracks RPC calls for dc redirection
	DCRedirectionUpdateNamespaceScope
	// DCRedirectionListTaskQueuePartitionsScope tracks RPC calls for dc redirection
	DCRedirectionListTaskQueuePartitionsScope
	// DCRedirectionCreateScheduleScope tracks RPC calls for dc redirection
	DCRedirectionCreateScheduleScope
	// DCRedirectionDescribeScheduleScope tracks RPC calls for dc redirection
	DCRedirectionDescribeScheduleScope
	// DCRedirectionUpdateScheduleScope tracks RPC calls for dc redirection
	DCRedirectionUpdateScheduleScope
	// DCRedirectionPatchScheduleScope tracks RPC calls for dc redirection
	DCRedirectionPatchScheduleScope
	// DCRedirectionListScheduleMatchingTimesScope tracks RPC calls for dc redirection
	DCRedirectionListScheduleMatchingTimesScope
	// DCRedirectionDeleteScheduleScope tracks RPC calls for dc redirection
	DCRedirectionDeleteScheduleScope
	// DCRedirectionListSchedulesScope tracks RPC calls for dc redirection
	DCRedirectionListSchedulesScope
	// DCRedirectionUpdateWorkerBuildIdOrderingScope tracks RPC calls for dc redirection
	DCRedirectionUpdateWorkerBuildIdOrderingScope
	// DCRedirectionGetWorkerBuildIdOrderingScope tracks RPC calls for dc redirection
	DCRedirectionGetWorkerBuildIdOrderingScope
	// DCRedirectionUpdateWorkflowScope tracks RPC calls for dc redirection
	DCRedirectionUpdateWorkflowScope

	// MessagingClientPublishScope tracks Publish calls made by service to messaging layer
	MessagingClientPublishScope
	// MessagingClientPublishBatchScope tracks Publish calls made by service to messaging layer
	MessagingClientPublishBatchScope

	// NamespaceCacheScope tracks namespace cache callbacks
	NamespaceCacheScope
	// HistoryRereplicationByTransferTaskScope tracks history replication calls made by transfer task
	HistoryRereplicationByTransferTaskScope
	// HistoryRereplicationByTimerTaskScope tracks history replication calls made by timer task
	HistoryRereplicationByTimerTaskScope
	// HistoryRereplicationByHistoryReplicationScope tracks history replication calls made by history replication
	HistoryRereplicationByHistoryReplicationScope
	// HistoryRereplicationByHistoryMetadataReplicationScope tracks history replication calls made by history replication
	HistoryRereplicationByHistoryMetadataReplicationScope
	// HistoryRereplicationByActivityReplicationScope tracks history replication calls made by activity replication
	HistoryRereplicationByActivityReplicationScope

	// PersistenceAppendHistoryNodesScope tracks AppendHistoryNodes calls made by service to persistence layer
	PersistenceAppendHistoryNodesScope
	// PersistenceAppendRawHistoryNodesScope tracks AppendRawHistoryNodes calls made by service to persistence layer
	PersistenceAppendRawHistoryNodesScope
	// PersistenceDeleteHistoryNodesScope tracks DeleteHistoryNodes calls made by service to persistence layer
	PersistenceDeleteHistoryNodesScope
	// PersistenceReadHistoryBranchScope tracks ReadHistoryBranch calls made by service to persistence layer
	PersistenceReadHistoryBranchScope
	// PersistenceReadHistoryBranchReverseScope tracks ReadHistoryBranchReverse calls made by service to persistence layer
	PersistenceReadHistoryBranchReverseScope
	// PersistenceForkHistoryBranchScope tracks ForkHistoryBranch calls made by service to persistence layer
	PersistenceForkHistoryBranchScope
	// PersistenceDeleteHistoryBranchScope tracks DeleteHistoryBranch calls made by service to persistence layer
	PersistenceDeleteHistoryBranchScope
	// PersistenceTrimHistoryBranchScope tracks TrimHistoryBranch calls made by service to persistence layer
	PersistenceTrimHistoryBranchScope
	// PersistenceCompleteForkBranchScope tracks CompleteForkBranch calls made by service to persistence layer
	PersistenceCompleteForkBranchScope
	// PersistenceGetHistoryTreeScope tracks GetHistoryTree calls made by service to persistence layer
	PersistenceGetHistoryTreeScope
	// PersistenceGetAllHistoryTreeBranchesScope tracks GetHistoryTree calls made by service to persistence layer
	PersistenceGetAllHistoryTreeBranchesScope
	// PersistenceNamespaceReplicationQueueScope is the metrics scope for namespace replication queue
	PersistenceNamespaceReplicationQueueScope

	// ClusterMetadataArchivalConfigScope tracks ArchivalConfig calls to ClusterMetadata
	ClusterMetadataArchivalConfigScope

	// ElasticsearchBulkProcessor is scope used by all metric emitted by Elasticsearch bulk processor
	ElasticsearchBulkProcessor

	// ElasticsearchVisibility is scope used by all Elasticsearch visibility metrics
	ElasticsearchVisibility

	// SequentialTaskProcessingScope is used by sequential task processing logic
	SequentialTaskProcessingScope
	// ParallelTaskProcessingScope is used by parallel task processing logic
	ParallelTaskProcessingScope
	// TaskSchedulerScope is used by task scheduler logic
	TaskSchedulerScope

	// HistoryArchiverScope is used by history archivers
	HistoryArchiverScope
	// VisibilityArchiverScope is used by visibility archivers
	VisibilityArchiverScope

	// The following metrics are only used by internal archiver implemention.
	// TODO: move them to internal repo once temporal plugin model is in place.

	// BlobstoreClientUploadScope tracks Upload calls to blobstore
	BlobstoreClientUploadScope
	// BlobstoreClientDownloadScope tracks Download calls to blobstore
	BlobstoreClientDownloadScope
	// BlobstoreClientGetMetadataScope tracks GetMetadata calls to blobstore
	BlobstoreClientGetMetadataScope
	// BlobstoreClientExistsScope tracks Exists calls to blobstore
	BlobstoreClientExistsScope
	// BlobstoreClientDeleteScope tracks Delete calls to blobstore
	BlobstoreClientDeleteScope
	// BlobstoreClientDirectoryExistsScope tracks DirectoryExists calls to blobstore
	BlobstoreClientDirectoryExistsScope

	DynamicConfigScope

	NumCommonScopes
)

// -- Operation scopes for Admin service --
const (
	// AdminDescribeHistoryHostScope is the metric scope for admin.AdminDescribeHistoryHostScope
	AdminDescribeHistoryHostScope = iota + NumCommonScopes
	// AdminAddSearchAttributesScope is the metric scope for admin.AdminAddSearchAttributesScope
	AdminAddSearchAttributesScope
	// AdminRemoveSearchAttributesScope is the metric scope for admin.AdminRemoveSearchAttributesScope
	AdminRemoveSearchAttributesScope
	// AdminGetSearchAttributesScope is the metric scope for admin.AdminGetSearchAttributesScope
	AdminGetSearchAttributesScope
	// AdminRebuildMutableStateScope is the metric scope for admin.AdminRebuildMutableStateScope
	AdminRebuildMutableStateScope
	// AdminDescribeWorkflowExecutionScope is the metric scope for admin.AdminDescribeWorkflowExecutionScope
	AdminDescribeWorkflowExecutionScope
	// AdminGetWorkflowExecutionRawHistoryScope is the metric scope for admin.GetWorkflowExecutionRawHistoryScope
	AdminGetWorkflowExecutionRawHistoryScope
	// AdminGetWorkflowExecutionRawHistoryV2Scope is the metric scope for admin.GetWorkflowExecutionRawHistoryScope
	AdminGetWorkflowExecutionRawHistoryV2Scope
	// AdminGetReplicationMessagesScope is the metric scope for admin.GetReplicationMessages
	AdminGetReplicationMessagesScope
	// AdminGetNamespaceReplicationMessagesScope is the metric scope for admin.GetNamespaceReplicationMessages
	AdminGetNamespaceReplicationMessagesScope
	// AdminGetDLQReplicationMessagesScope is the metric scope for admin.GetDLQReplicationMessages
	AdminGetDLQReplicationMessagesScope
	// AdminReapplyEventsScope is the metric scope for admin.ReapplyEvents
	AdminReapplyEventsScope
	// AdminRefreshWorkflowTasksScope is the metric scope for admin.RefreshWorkflowTasks
	AdminRefreshWorkflowTasksScope
	// AdminResendReplicationTasksScope is the metric scope for admin.ResendReplicationTasks
	AdminResendReplicationTasksScope
	// AdminGetTaskQueueTasksScope is the metric scope for admin.GetTaskQueueTasks
	AdminGetTaskQueueTasksScope
	// AdminRemoveTaskScope is the metric scope for admin.AdminRemoveTaskScope
	AdminRemoveTaskScope
	// AdminCloseShardScope is the metric scope for admin.AdminCloseShardScope
	AdminCloseShardScope
	// AdminGetShardScope is the metric scope for admin.AdminGetShardScope
	AdminGetShardScope
	// AdminListHistoryTasksScope is the metric scope for admin.ListHistoryTasksScope
	AdminListHistoryTasksScope
	// AdminReadDLQMessagesScope is the metric scope for admin.AdminReadDLQMessagesScope
	AdminReadDLQMessagesScope
	// AdminPurgeDLQMessagesScope is the metric scope for admin.AdminPurgeDLQMessagesScope
	AdminPurgeDLQMessagesScope
	// AdminMergeDLQMessagesScope is the metric scope for admin.AdminMergeDLQMessagesScope
	AdminMergeDLQMessagesScope
	// AdminListClusterMembersScope is the metric scope for admin.AdminListClusterMembersScope
	AdminListClusterMembersScope
	// AdminDescribeClusterScope is the metric scope for admin.AdminDescribeClusterScope
	AdminDescribeClusterScope
	// AdminListClustersScope is the metric scope for admin.AdminListClustersScope
	AdminListClustersScope
	// AdminAddOrUpdateRemoteClusterScope is the metric scope for admin.AdminAddOrUpdateRemoteClusterScope
	AdminAddOrUpdateRemoteClusterScope
	// AdminRemoveRemoteClusterScope is the metric scope for admin.AdminRemoveRemoteClusterScope
	AdminRemoveRemoteClusterScope
	// AdminDeleteWorkflowExecutionScope is the metric scope for admin.AdminDeleteWorkflowExecutionScope
	AdminDeleteWorkflowExecutionScope

	NumAdminScopes
)

// -- Operation scopes for Admin service --
const (
	// OperatorAddSearchAttributesScope is the metric scope for operator.AddSearchAttributes
	OperatorAddSearchAttributesScope = iota + NumAdminScopes
	// OperatorRemoveSearchAttributesScope is the metric scope for operator.RemoveSearchAttributes
	OperatorRemoveSearchAttributesScope
	// OperatorListSearchAttributesScope is the metric scope for operator.ListSearchAttributes
	OperatorListSearchAttributesScope
	OperatorDeleteNamespaceScope

	NumOperatorScopes
)

// -- Operation scopes for Frontend service --
const (
	// FrontendStartWorkflowExecutionScope is the metric scope for frontend.StartWorkflowExecution
	FrontendStartWorkflowExecutionScope = iota + NumOperatorScopes
	// FrontendPollWorkflowTaskQueueScope is the metric scope for frontend.PollWorkflowTaskQueue
	FrontendPollWorkflowTaskQueueScope
	// FrontendPollActivityTaskQueueScope is the metric scope for frontend.PollActivityTaskQueue
	FrontendPollActivityTaskQueueScope
	// FrontendRecordActivityTaskHeartbeatScope is the metric scope for frontend.RecordActivityTaskHeartbeat
	FrontendRecordActivityTaskHeartbeatScope
	// FrontendRecordActivityTaskHeartbeatByIdScope is the metric scope for frontend.RespondWorkflowTaskCompleted
	FrontendRecordActivityTaskHeartbeatByIdScope
	// FrontendRespondWorkflowTaskCompletedScope is the metric scope for frontend.RespondWorkflowTaskCompleted
	FrontendRespondWorkflowTaskCompletedScope
	// FrontendRespondWorkflowTaskFailedScope is the metric scope for frontend.RespondWorkflowTaskFailed
	FrontendRespondWorkflowTaskFailedScope
	// FrontendRespondQueryTaskCompletedScope is the metric scope for frontend.RespondQueryTaskCompleted
	FrontendRespondQueryTaskCompletedScope
	// FrontendRespondActivityTaskCompletedScope is the metric scope for frontend.RespondActivityTaskCompleted
	FrontendRespondActivityTaskCompletedScope
	// FrontendRespondActivityTaskFailedScope is the metric scope for frontend.RespondActivityTaskFailed
	FrontendRespondActivityTaskFailedScope
	// FrontendRespondActivityTaskCanceledScope is the metric scope for frontend.RespondActivityTaskCanceled
	FrontendRespondActivityTaskCanceledScope
	// FrontendRespondActivityTaskCompletedByIdScope is the metric scope for frontend.RespondActivityTaskCompletedById
	FrontendRespondActivityTaskCompletedByIdScope
	// FrontendRespondActivityTaskFailedByIdScope is the metric scope for frontend.RespondActivityTaskFailedById
	FrontendRespondActivityTaskFailedByIdScope
	// FrontendRespondActivityTaskCanceledByIdScope is the metric scope for frontend.RespondActivityTaskCanceledById
	FrontendRespondActivityTaskCanceledByIdScope
	// FrontendGetWorkflowExecutionHistoryScope is the metric scope for non-long-poll frontend.GetWorkflowExecutionHistory
	FrontendGetWorkflowExecutionHistoryScope
	// FrontendGetWorkflowExecutionHistoryReverseScope is the metric for frontend.GetWorkflowExecutionHistoryReverse
	FrontendGetWorkflowExecutionHistoryReverseScope
	// FrontendPollWorkflowExecutionHistoryScope is the metric scope for long poll case of frontend.GetWorkflowExecutionHistory
	FrontendPollWorkflowExecutionHistoryScope
	// FrontendGetWorkflowExecutionRawHistoryScope is the metric scope for frontend.GetWorkflowExecutionRawHistory
	FrontendGetWorkflowExecutionRawHistoryScope
	// FrontendPollForWorkflowExecutionRawHistoryScope is the metric scope for frontend.GetWorkflowExecutionRawHistory
	FrontendPollForWorkflowExecutionRawHistoryScope
	// FrontendSignalWorkflowExecutionScope is the metric scope for frontend.SignalWorkflowExecution
	FrontendSignalWorkflowExecutionScope
	// FrontendSignalWithStartWorkflowExecutionScope is the metric scope for frontend.SignalWithStartWorkflowExecution
	FrontendSignalWithStartWorkflowExecutionScope
	// FrontendTerminateWorkflowExecutionScope is the metric scope for frontend.TerminateWorkflowExecution
	FrontendTerminateWorkflowExecutionScope
	// FrontendRequestCancelWorkflowExecutionScope is the metric scope for frontend.RequestCancelWorkflowExecution
	FrontendRequestCancelWorkflowExecutionScope
	// FrontendListArchivedWorkflowExecutionsScope is the metric scope for frontend.ListArchivedWorkflowExecutions
	FrontendListArchivedWorkflowExecutionsScope
	// FrontendListOpenWorkflowExecutionsScope is the metric scope for frontend.ListOpenWorkflowExecutions
	FrontendListOpenWorkflowExecutionsScope
	// FrontendListClosedWorkflowExecutionsScope is the metric scope for frontend.ListClosedWorkflowExecutions
	FrontendListClosedWorkflowExecutionsScope
	// FrontendListWorkflowExecutionsScope is the metric scope for frontend.ListWorkflowExecutions
	FrontendListWorkflowExecutionsScope
	// FrontendScanWorkflowExecutionsScope is the metric scope for frontend.ListWorkflowExecutions
	FrontendScanWorkflowExecutionsScope
	// FrontendCountWorkflowExecutionsScope is the metric scope for frontend.CountWorkflowExecutions
	FrontendCountWorkflowExecutionsScope
	// FrontendRegisterNamespaceScope is the metric scope for frontend.RegisterNamespace
	FrontendRegisterNamespaceScope
	// FrontendDescribeNamespaceScope is the metric scope for frontend.DescribeNamespace
	FrontendDescribeNamespaceScope
	// FrontendUpdateNamespaceScope is the metric scope for frontend.DescribeNamespace
	FrontendUpdateNamespaceScope
	// FrontendDeprecateNamespaceScope is the metric scope for frontend.DeprecateNamespace
	FrontendDeprecateNamespaceScope
	// FrontendQueryWorkflowScope is the metric scope for frontend.QueryWorkflow
	FrontendQueryWorkflowScope
	// FrontendDescribeWorkflowExecutionScope is the metric scope for frontend.DescribeWorkflowExecution
	FrontendDescribeWorkflowExecutionScope
	// FrontendDescribeTaskQueueScope is the metric scope for frontend.DescribeTaskQueue
	FrontendDescribeTaskQueueScope
	// FrontendListTaskQueuePartitionsScope is the metric scope for frontend.ResetStickyTaskQueue
	FrontendListTaskQueuePartitionsScope
	// FrontendResetStickyTaskQueueScope is the metric scope for frontend.ResetStickyTaskQueue
	FrontendResetStickyTaskQueueScope
	// FrontendListNamespacesScope is the metric scope for frontend.ListNamespace
	FrontendListNamespacesScope
	// FrontendResetWorkflowExecutionScope is the metric scope for frontend.ResetWorkflowExecution
	FrontendResetWorkflowExecutionScope
	// FrontendGetSearchAttributesScope is the metric scope for frontend.GetSearchAttributes
	FrontendGetSearchAttributesScope
	// FrontendGetClusterInfoScope is the metric scope for frontend.GetClusterInfo
	FrontendGetClusterInfoScope
	// FrontendGetSystemInfoScope is the metric scope for frontend.GetSystemInfo
	FrontendGetSystemInfoScope
	// FrontendCreateScheduleScope is the metric scope for frontend.CreateScheduleScope
	FrontendCreateScheduleScope
	// FrontendDescribeScheduleScope is the metric scope for frontend.DescribeScheduleScope
	FrontendDescribeScheduleScope
	// FrontendUpdateScheduleScope is the metric scope for frontend.UpdateScheduleScope
	FrontendUpdateScheduleScope
	// FrontendPatchScheduleScope is the metric scope for frontend.PatchScheduleScope
	FrontendPatchScheduleScope
	// FrontendListScheduleMatchingTimesScope is the metric scope for frontend.ListScheduleMatchingTimesScope
	FrontendListScheduleMatchingTimesScope
	// FrontendDeleteScheduleScope is the metric scope for frontend.DeleteScheduleScope
	FrontendDeleteScheduleScope
	// FrontendListSchedulesScope is the metric scope for frontend.ListSchedulesScope
	FrontendListSchedulesScope
	// FrontendUpdateWorkerBuildIdOrderingScope is the metric scope for frontend.UpdateWorkerBuildIdOrderingScope
	FrontendUpdateWorkerBuildIdOrderingScope
	// FrontendGetWorkerBuildIdOrderingScope is the metric scope for frontend.GetWorkerBuildIdOrderingScope
	FrontendGetWorkerBuildIdOrderingScope
	// FrontendUpdateWorkflowScope is the metric scope for frontend.UpdateWorkflow
	FrontendUpdateWorkflowScope

	// VersionCheckScope is scope used by version checker
	VersionCheckScope
	// AuthorizationScope is the scope used by all metric emitted by authorization code
	AuthorizationScope

	NumFrontendScopes
)

// -- Operation scopes for History service --
const (
	// HistoryStartWorkflowExecutionScope tracks StartWorkflowExecution API calls received by service
	HistoryStartWorkflowExecutionScope = iota + NumFrontendScopes
	// HistoryRecordActivityTaskHeartbeatScope tracks RecordActivityTaskHeartbeat API calls received by service
	HistoryRecordActivityTaskHeartbeatScope
	// HistoryRespondWorkflowTaskCompletedScope tracks RespondWorkflowTaskCompleted API calls received by service
	HistoryRespondWorkflowTaskCompletedScope
	// HistoryRespondWorkflowTaskFailedScope tracks RespondWorkflowTaskFailed API calls received by service
	HistoryRespondWorkflowTaskFailedScope
	// HistoryRespondActivityTaskCompletedScope tracks RespondActivityTaskCompleted API calls received by service
	HistoryRespondActivityTaskCompletedScope
	// HistoryRespondActivityTaskFailedScope tracks RespondActivityTaskFailed API calls received by service
	HistoryRespondActivityTaskFailedScope
	// HistoryRespondActivityTaskCanceledScope tracks RespondActivityTaskCanceled API calls received by service
	HistoryRespondActivityTaskCanceledScope
	// HistoryGetMutableStateScope tracks GetMutableStateScope API calls received by service
	HistoryGetMutableStateScope
	// HistoryPollMutableStateScope tracks PollMutableStateScope API calls received by service
	HistoryPollMutableStateScope
	// HistoryResetStickyTaskQueueScope tracks ResetStickyTaskQueueScope API calls received by service
	HistoryResetStickyTaskQueueScope
	// HistoryDescribeWorkflowExecutionScope tracks DescribeWorkflowExecution API calls received by service
	HistoryDescribeWorkflowExecutionScope
	// HistoryRecordWorkflowTaskStartedScope tracks RecordWorkflowTaskStarted API calls received by service
	HistoryRecordWorkflowTaskStartedScope
	// HistoryRecordActivityTaskStartedScope tracks RecordActivityTaskStarted API calls received by service
	HistoryRecordActivityTaskStartedScope
	// HistorySignalWorkflowExecutionScope tracks SignalWorkflowExecution API calls received by service
	HistorySignalWorkflowExecutionScope
	// HistorySignalWithStartWorkflowExecutionScope tracks SignalWithStartWorkflowExecution API calls received by service
	HistorySignalWithStartWorkflowExecutionScope
	// HistoryRemoveSignalMutableStateScope tracks RemoveSignalMutableState API calls received by service
	HistoryRemoveSignalMutableStateScope
	// HistoryTerminateWorkflowExecutionScope tracks TerminateWorkflowExecution API calls received by service
	HistoryTerminateWorkflowExecutionScope
	// HistoryScheduleWorkflowTaskScope tracks ScheduleWorkflowTask API calls received by service
	HistoryScheduleWorkflowTaskScope
	// HistoryVerifyFirstWorkflowTaskScheduled tracks VerifyFirstWorkflowTaskScheduled API calls received by service
	HistoryVerifyFirstWorkflowTaskScheduled
	// HistoryRecordChildExecutionCompletedScope tracks RecordChildExecutionCompleted API calls received by service
	HistoryRecordChildExecutionCompletedScope
	// HistoryVerifyChildExecutionCompletionRecordedScope tracks VerifyChildExecutionCompletionRecorded API calls received by service
	HistoryVerifyChildExecutionCompletionRecordedScope
	// HistoryRequestCancelWorkflowExecutionScope tracks RequestCancelWorkflowExecution API calls received by service
	HistoryRequestCancelWorkflowExecutionScope
	// HistorySyncShardStatusScope tracks HistorySyncShardStatus API calls received by service
	HistorySyncShardStatusScope
	// HistorySyncActivityScope tracks HistoryActivity API calls received by service
	HistorySyncActivityScope
	// HistoryRebuildMutableStateScope tracks RebuildMutable API calls received by service
	HistoryRebuildMutableStateScope
	// HistoryDescribeMutableStateScope tracks DescribeMutableState API calls received by service
	HistoryDescribeMutableStateScope
	// HistoryGetReplicationMessagesScope tracks GetReplicationMessages API calls received by service
	HistoryGetReplicationMessagesScope
	// HistoryGetDLQReplicationMessagesScope tracks GetReplicationMessages API calls received by service
	HistoryGetDLQReplicationMessagesScope
	// HistoryReadDLQMessagesScope tracks GetDLQMessages API calls received by service
	HistoryReadDLQMessagesScope
	// HistoryPurgeDLQMessagesScope tracks PurgeDLQMessages API calls received by service
	HistoryPurgeDLQMessagesScope
	// HistoryMergeDLQMessagesScope tracks MergeDLQMessages API calls received by service
	HistoryMergeDLQMessagesScope
	// HistoryShardControllerScope is the scope used by shard controller
	HistoryShardControllerScope
	// HistoryReapplyEventsScope is the scope used by event reapplication
	HistoryReapplyEventsScope
	// HistoryRefreshWorkflowTasksScope is the scope used by refresh workflow tasks API
	HistoryRefreshWorkflowTasksScope
	// HistoryGenerateLastHistoryReplicationTasksScope is the scope used by generate last replication tasks API
	HistoryGenerateLastHistoryReplicationTasksScope
	// HistoryGetReplicationStatusScope is the scope used by GetReplicationStatus API
	HistoryGetReplicationStatusScope
	// HistoryHistoryRemoveTaskScope is the scope used by remove task API
	HistoryHistoryRemoveTaskScope
	// HistoryCloseShard is the scope used by close shard API
	HistoryCloseShard
	// HistoryGetShard is the scope used by get shard API
	HistoryGetShard
	// HistoryReplicateEventsV2 is the scope used by replicate events API
	HistoryReplicateEventsV2
	// HistoryResetStickyTaskQueue is the scope used by reset sticky task queue API
	HistoryResetStickyTaskQueue
	// HistoryReapplyEvents is the scope used by reapply events API
	HistoryReapplyEvents
	// HistoryDescribeHistoryHost is the scope used by describe history host API
	HistoryDescribeHistoryHost
	// HistoryDeleteWorkflowVisibilityRecordScope is the scope used by delete workflow visibility record API
	HistoryDeleteWorkflowVisibilityRecordScope
	// HistoryUpdateWorkflowScope is the scope used by update workflow API
	HistoryUpdateWorkflowScope
	// TaskPriorityAssignerScope is the scope used by all metric emitted by task priority assigner
	TaskPriorityAssignerScope
	// TransferQueueProcessorScope is the scope used by all metric emitted by transfer queue processor
	TransferQueueProcessorScope
	// TransferActiveQueueProcessorScope is the scope used by all metric emitted by transfer queue processor
	TransferActiveQueueProcessorScope
	// TransferStandbyQueueProcessorScope is the scope used by all metric emitted by transfer queue processor
	TransferStandbyQueueProcessorScope
	// TransferActiveTaskActivityScope is the scope used for activity task processing by transfer queue processor
	TransferActiveTaskActivityScope
	// TransferActiveTaskWorkflowTaskScope is the scope used for workflow task processing by transfer queue processor
	TransferActiveTaskWorkflowTaskScope
	// TransferActiveTaskCloseExecutionScope is the scope used for close execution task processing by transfer queue processor
	TransferActiveTaskCloseExecutionScope
	// TransferActiveTaskCancelExecutionScope is the scope used for cancel execution task processing by transfer queue processor
	TransferActiveTaskCancelExecutionScope
	// TransferActiveTaskSignalExecutionScope is the scope used for signal execution task processing by transfer queue processor
	TransferActiveTaskSignalExecutionScope
	// TransferActiveTaskStartChildExecutionScope is the scope used for start child execution task processing by transfer queue processor
	TransferActiveTaskStartChildExecutionScope
	// TransferActiveTaskResetWorkflowScope is the scope used for record workflow started task processing by transfer queue processor
	TransferActiveTaskResetWorkflowScope
	// TransferStandbyTaskResetWorkflowScope is the scope used for record workflow started task processing by transfer queue processor
	TransferStandbyTaskResetWorkflowScope
	// TransferStandbyTaskActivityScope is the scope used for activity task processing by transfer queue processor
	TransferStandbyTaskActivityScope
	// TransferStandbyTaskWorkflowTaskScope is the scope used for workflow task processing by transfer queue processor
	TransferStandbyTaskWorkflowTaskScope
	// TransferStandbyTaskCloseExecutionScope is the scope used for close execution task processing by transfer queue processor
	TransferStandbyTaskCloseExecutionScope
	// TransferStandbyTaskCancelExecutionScope is the scope used for cancel execution task processing by transfer queue processor
	TransferStandbyTaskCancelExecutionScope
	// TransferStandbyTaskSignalExecutionScope is the scope used for signal execution task processing by transfer queue processor
	TransferStandbyTaskSignalExecutionScope
	// TransferStandbyTaskStartChildExecutionScope is the scope used for start child execution task processing by transfer queue processor
	TransferStandbyTaskStartChildExecutionScope

	// VisibilityQueueProcessorScope is the scope used by all metric emitted by visibility queue processor
	VisibilityQueueProcessorScope
	// VisibilityTaskStartExecutionScope is the scope used for start execution processing by visibility queue processor
	VisibilityTaskStartExecutionScope
	// VisibilityTaskUpsertExecutionScope is the scope used for upsert execution processing by visibility queue processor
	VisibilityTaskUpsertExecutionScope
	// VisibilityTaskCloseExecutionScope is the scope used for close execution attributes processing by visibility queue processor
	VisibilityTaskCloseExecutionScope
	// VisibilityTaskDeleteExecutionScope is the scope used for delete by visibility queue processor
	VisibilityTaskDeleteExecutionScope

	// TimerQueueProcessorScope is the scope used by all metric emitted by timer queue processor
	TimerQueueProcessorScope
	// TimerActiveQueueProcessorScope is the scope used by all metric emitted by timer queue processor
	TimerActiveQueueProcessorScope
	// TimerStandbyQueueProcessorScope is the scope used by all metric emitted by timer queue processor
	TimerStandbyQueueProcessorScope
	// TimerActiveTaskActivityTimeoutScope is the scope used by metric emitted by timer queue processor for processing activity timeouts
	TimerActiveTaskActivityTimeoutScope
	// TimerActiveTaskWorkflowTaskTimeoutScope is the scope used by metric emitted by timer queue processor for processing workflow task timeouts
	TimerActiveTaskWorkflowTaskTimeoutScope
	// TimerActiveTaskUserTimerScope is the scope used by metric emitted by timer queue processor for processing user timers
	TimerActiveTaskUserTimerScope
	// TimerActiveTaskWorkflowTimeoutScope is the scope used by metric emitted by timer queue processor for processing workflow timeouts.
	TimerActiveTaskWorkflowTimeoutScope
	// TimerActiveTaskActivityRetryTimerScope is the scope used by metric emitted by timer queue processor for processing retry task.
	TimerActiveTaskActivityRetryTimerScope
	// TimerActiveTaskWorkflowBackoffTimerScope is the scope used by metric emitted by timer queue processor for processing retry task.
	TimerActiveTaskWorkflowBackoffTimerScope
	// TimerActiveTaskDeleteHistoryEventScope is the scope used by metric emitted by timer queue processor for processing history event cleanup
	TimerActiveTaskDeleteHistoryEventScope
	// TimerStandbyTaskActivityTimeoutScope is the scope used by metric emitted by timer queue processor for processing activity timeouts
	TimerStandbyTaskActivityTimeoutScope
	// TimerStandbyTaskWorkflowTaskTimeoutScope is the scope used by metric emitted by timer queue processor for processing workflow task timeouts
	TimerStandbyTaskWorkflowTaskTimeoutScope
	// TimerStandbyTaskUserTimerScope is the scope used by metric emitted by timer queue processor for processing user timers
	TimerStandbyTaskUserTimerScope
	// TimerStandbyTaskWorkflowTimeoutScope is the scope used by metric emitted by timer queue processor for processing workflow timeouts.
	TimerStandbyTaskWorkflowTimeoutScope
	// TimerStandbyTaskActivityRetryTimerScope is the scope used by metric emitted by timer queue processor for processing retry task.
	TimerStandbyTaskActivityRetryTimerScope
	// TimerStandbyTaskDeleteHistoryEventScope is the scope used by metric emitted by timer queue processor for processing history event cleanup
	TimerStandbyTaskDeleteHistoryEventScope
	// TimerStandbyTaskWorkflowBackoffTimerScope is the scope used by metric emitted by timer queue processor for processing retry task.
	TimerStandbyTaskWorkflowBackoffTimerScope
	// HistoryEventNotificationScope is the scope used by shard history event nitification
	HistoryEventNotificationScope
	// ReplicatorQueueProcessorScope is the scope used by all metric emitted by replicator queue processor
	ReplicatorQueueProcessorScope
	// ReplicatorTaskHistoryScope is the scope used for history task processing by replicator queue processor
	ReplicatorTaskHistoryScope
	// ReplicatorTaskSyncActivityScope is the scope used for sync activity by replicator queue processor
	ReplicatorTaskSyncActivityScope
	// ReplicateHistoryEventsScope is the scope used by historyReplicator API for applying events
	ReplicateHistoryEventsScope
	// ShardInfoScope is the scope used when updating shard info
	ShardInfoScope
	// WorkflowContextScope is the scope used by WorkflowContext component
	WorkflowContextScope
	// HistoryCacheGetOrCreateScope is the scope used by history cache
	HistoryCacheGetOrCreateScope
	// HistoryCacheGetOrCreateCurrentScope is the scope used by history cache
	HistoryCacheGetOrCreateCurrentScope
	// EventsCacheGetEventScope is the scope used by events cache
	EventsCacheGetEventScope
	// EventsCachePutEventScope is the scope used by events cache
	EventsCachePutEventScope
	// EventsCacheDeleteEventScope is the scope used by events cache
	EventsCacheDeleteEventScope
	// EventsCacheGetFromStoreScope is the scope used by events cache
	EventsCacheGetFromStoreScope
	// ExecutionStatsScope is the scope used for emiting workflow execution related stats
	ExecutionStatsScope
	// SessionStatsScope is the scope used for emiting session update related stats
	SessionStatsScope
	// HistoryResetWorkflowExecutionScope tracks ResetWorkflowExecution API calls received by service
	HistoryResetWorkflowExecutionScope
	// HistoryQueryWorkflowScope tracks QueryWorkflow API calls received by service
	HistoryQueryWorkflowScope
	// HistoryProcessDeleteHistoryEventScope tracks ProcessDeleteHistoryEvent processing calls
	HistoryProcessDeleteHistoryEventScope
	// HistoryDeleteWorkflowExecutionScope tracks DeleteWorkflowExecutions API calls
	HistoryDeleteWorkflowExecutionScope
	// WorkflowCompletionStatsScope tracks workflow completion updates
	WorkflowCompletionStatsScope
	// ArchiverClientScope is scope used by all metrics emitted by archiver.Client
	ArchiverClientScope
	// ReplicationTaskFetcherScope is scope used by all metrics emitted by ReplicationTaskFetcher
	ReplicationTaskFetcherScope
	// ReplicationTaskCleanupScope is scope used by all metrics emitted by ReplicationTaskProcessor cleanup
	ReplicationTaskCleanupScope
	// ReplicationDLQStatsScope is scope used by all metrics emitted related to replication DLQ
	ReplicationDLQStatsScope
	// SyncWorkflowStateTaskScope is the scope used by closed workflow task replication processing
	SyncWorkflowStateTaskScope

	NumHistoryScopes
)

// -- Operation scopes for Matching service --
const (
	// MatchingPollWorkflowTaskQueueScope tracks PollWorkflowTaskQueue API calls received by service
	MatchingPollWorkflowTaskQueueScope = iota + NumHistoryScopes
	// MatchingPollActivityTaskQueueScope tracks PollActivityTaskQueue API calls received by service
	MatchingPollActivityTaskQueueScope
	// MatchingAddActivityTaskScope tracks AddActivityTask API calls received by service
	MatchingAddActivityTaskScope
	// MatchingAddWorkflowTaskScope tracks AddWorkflowTask API calls received by service
	MatchingAddWorkflowTaskScope
	// MatchingTaskQueueMgrScope is the metrics scope for matching.TaskQueueManager component
	MatchingTaskQueueMgrScope
	// MatchingEngineScope is the metrics scope for matchingEngine component
	MatchingEngineScope
	// MatchingQueryWorkflowScope tracks AddWorkflowTask API calls received by service
	MatchingQueryWorkflowScope
	// MatchingRespondQueryTaskCompletedScope tracks AddWorkflowTask API calls received by service
	MatchingRespondQueryTaskCompletedScope
	// MatchingCancelOutstandingPollScope tracks CancelOutstandingPoll API calls received by service
	MatchingCancelOutstandingPollScope
	// MatchingDescribeTaskQueueScope tracks DescribeTaskQueue API calls received by service
	MatchingDescribeTaskQueueScope
	// MatchingListTaskQueuePartitionsScope tracks ListTaskQueuePartitions API calls received by service
	MatchingListTaskQueuePartitionsScope
	// MatchingUpdateWorkerBuildIdOrderingScope tracks UpdateWorkerBuildIdOrdering API calls received by service
	MatchingUpdateWorkerBuildIdOrderingScope
	// MatchingGetWorkerBuildIdOrderingScope tracks GetWorkerBuildIdOrdering API calls received by service
	MatchingGetWorkerBuildIdOrderingScope

	NumMatchingScopes
)

// -- Operation scopes for Worker service --
const (
	// ReplicatorScope is the scope used by all metric emitted by replicator
	ReplicatorScope = iota + NumMatchingScopes
	// NamespaceReplicationTaskScope is the scope used by namespace task replication processing
	NamespaceReplicationTaskScope
	// HistoryReplicationTaskScope is the scope used by history task replication processing
	HistoryReplicationTaskScope
	// HistoryMetadataReplicationTaskScope is the scope used by history metadata task replication processing
	HistoryMetadataReplicationTaskScope
	// SyncShardTaskScope is the scope used by sync shrad information processing
	SyncShardTaskScope
	// SyncActivityTaskScope is the scope used by sync activity information processing
	SyncActivityTaskScope
	// ESProcessorScope is scope used by all metric emitted by esProcessor
	ESProcessorScope
	// IndexProcessorScope is scope used by all metric emitted by index processor
	IndexProcessorScope
	// ArchiverDeleteHistoryActivityScope is scope used by all metrics emitted by archiver.DeleteHistoryActivity
	ArchiverDeleteHistoryActivityScope
	// ArchiverUploadHistoryActivityScope is scope used by all metrics emitted by archiver.UploadHistoryActivity
	ArchiverUploadHistoryActivityScope
	// ArchiverArchiveVisibilityActivityScope is scope used by all metrics emitted by archiver.ArchiveVisibilityActivity
	ArchiverArchiveVisibilityActivityScope
	// ArchiverScope is scope used by all metrics emitted by archiver.Archiver
	ArchiverScope
	// ArchiverPumpScope is scope used by all metrics emitted by archiver.Pump
	ArchiverPumpScope
	// ArchiverArchivalWorkflowScope is scope used by all metrics emitted by archiver.ArchivalWorkflow
	ArchiverArchivalWorkflowScope
	// TaskQueueScavengerScope is scope used by all metrics emitted by worker.taskqueue.Scavenger module
	TaskQueueScavengerScope
	// ExecutionsScavengerScope is scope used by all metrics emitted by worker.executions.Scavenger module
	ExecutionsScavengerScope
	// BatcherScope is scope used by all metrics emitted by worker.Batcher module
	BatcherScope
	// HistoryScavengerScope is scope used by all metrics emitted by worker.history.Scavenger module
	HistoryScavengerScope
	// ParentClosePolicyProcessorScope is scope used by all metrics emitted by worker.ParentClosePolicyProcessor
	ParentClosePolicyProcessorScope
	// AddSearchAttributesWorkflowScope is scope used by all metrics emitted by worker.AddSearchAttributesWorkflowScope module
	AddSearchAttributesWorkflowScope
	// MigrationWorkflowScope is scope used by metrics emitted by migration related workflows
	MigrationWorkflowScope

	DeleteNamespaceWorkflowScope
	ReclaimResourcesWorkflowScope
	DeleteExecutionsWorkflowScope

	NumWorkerScopes
)

// -- Scopes for Server --
const (
	ServerTlsScope = iota + NumWorkerScopes

	NumServerScopes
)

// -- Scopes for UnitTestService --
const (
	TestScope1 = iota + NumServerScopes
	TestScope2

	NumUnitTestServiceScopes
)

// ScopeDefs record the scopes for all services
var ScopeDefs = map[ServiceIdx]map[int]scopeDefinition{
	// common scope Names
	Common: {
		UnknownScope:                                      {operation: "Unknown"},
		PersistenceGetOrCreateShardScope:                  {operation: "GetOrCreateShard"},
		PersistenceUpdateShardScope:                       {operation: "UpdateShard"},
		PersistenceAssertShardOwnershipScope:              {operation: "AssertShardOwnership"},
		PersistenceCreateWorkflowExecutionScope:           {operation: "CreateWorkflowExecution"},
		PersistenceGetWorkflowExecutionScope:              {operation: "GetWorkflowExecution"},
		PersistenceSetWorkflowExecutionScope:              {operation: "SetWorkflowExecution"},
		PersistenceUpdateWorkflowExecutionScope:           {operation: "UpdateWorkflowExecution"},
		PersistenceConflictResolveWorkflowExecutionScope:  {operation: "ConflictResolveWorkflowExecution"},
		PersistenceResetWorkflowExecutionScope:            {operation: "ResetWorkflowExecution"},
		PersistenceDeleteWorkflowExecutionScope:           {operation: "DeleteWorkflowExecution"},
		PersistenceDeleteCurrentWorkflowExecutionScope:    {operation: "DeleteCurrentWorkflowExecution"},
		PersistenceGetCurrentExecutionScope:               {operation: "GetCurrentExecution"},
		PersistenceListConcreteExecutionsScope:            {operation: "ListConcreteExecutions"},
		PersistenceAddTasksScope:                          {operation: "AddTasks"},
		PersistenceGetTransferTaskScope:                   {operation: "GetTransferTask"},
		PersistenceGetTransferTasksScope:                  {operation: "GetTransferTasks"},
		PersistenceCompleteTransferTaskScope:              {operation: "CompleteTransferTask"},
		PersistenceRangeCompleteTransferTasksScope:        {operation: "RangeCompleteTransferTask"},
		PersistenceGetVisibilityTaskScope:                 {operation: "GetVisibilityTask"},
		PersistenceGetVisibilityTasksScope:                {operation: "GetVisibilityTasks"},
		PersistenceCompleteVisibilityTaskScope:            {operation: "CompleteVisibilityTask"},
		PersistenceRangeCompleteVisibilityTasksScope:      {operation: "RangeCompleteVisibilityTask"},
		PersistenceGetReplicationTaskScope:                {operation: "GetReplicationTask"},
		PersistenceGetReplicationTasksScope:               {operation: "GetReplicationTasks"},
		PersistenceCompleteReplicationTaskScope:           {operation: "CompleteReplicationTask"},
		PersistenceRangeCompleteReplicationTasksScope:     {operation: "RangeCompleteReplicationTask"},
		PersistencePutReplicationTaskToDLQScope:           {operation: "PutReplicationTaskToDLQ"},
		PersistenceGetReplicationTasksFromDLQScope:        {operation: "GetReplicationTasksFromDLQ"},
		PersistenceDeleteReplicationTaskFromDLQScope:      {operation: "DeleteReplicationTaskFromDLQ"},
		PersistenceRangeDeleteReplicationTaskFromDLQScope: {operation: "RangeDeleteReplicationTaskFromDLQ"},
		PersistenceGetTimerTaskScope:                      {operation: "GetTimerTask"},
		PersistenceGetTimerTasksScope:                     {operation: "GetTimerTasks"},
		PersistenceCompleteTimerTaskScope:                 {operation: "CompleteTimerTask"},
		PersistenceRangeCompleteTimerTasksScope:           {operation: "RangeCompleteTimerTask"},
		PersistenceCreateTaskScope:                        {operation: "CreateTask"},
		PersistenceGetTasksScope:                          {operation: "GetTasks"},
		PersistenceCompleteTaskScope:                      {operation: "CompleteTask"},
		PersistenceCompleteTasksLessThanScope:             {operation: "CompleteTasksLessThan"},
		PersistenceCreateTaskQueueScope:                   {operation: "CreateTaskQueue"},
		PersistenceUpdateTaskQueueScope:                   {operation: "UpdateTaskQueue"},
		PersistenceGetTaskQueueScope:                      {operation: "GetTaskQueue"},
		PersistenceListTaskQueueScope:                     {operation: "ListTaskQueue"},
		PersistenceDeleteTaskQueueScope:                   {operation: "DeleteTaskQueue"},
		PersistenceAppendHistoryEventsScope:               {operation: "AppendHistoryEvents"},
		PersistenceGetWorkflowExecutionHistoryScope:       {operation: "GetWorkflowExecutionHistory"},
		PersistenceDeleteWorkflowExecutionHistoryScope:    {operation: "DeleteWorkflowExecutionHistory"},
		PersistenceInitializeSystemNamespaceScope:         {operation: "InitializeSystemNamespace"},
		PersistenceCreateNamespaceScope:                   {operation: "CreateNamespace"},
		PersistenceGetNamespaceScope:                      {operation: "GetNamespace"},
		PersistenceUpdateNamespaceScope:                   {operation: "UpdateNamespace"},
		PersistenceDeleteNamespaceScope:                   {operation: "DeleteNamespace"},
		PersistenceRenameNamespaceScope:                   {operation: "RenameNamespace"},
		PersistenceDeleteNamespaceByNameScope:             {operation: "DeleteNamespaceByName"},
		PersistenceListNamespaceScope:                     {operation: "ListNamespace"},
		PersistenceGetMetadataScope:                       {operation: "GetMetadata"},

		VisibilityPersistenceRecordWorkflowExecutionStartedScope:           {operation: "RecordWorkflowExecutionStarted", tags: map[string]string{visibilityTypeTagName: unknownValue}},
		VisibilityPersistenceRecordWorkflowExecutionClosedScope:            {operation: "RecordWorkflowExecutionClosed", tags: map[string]string{visibilityTypeTagName: unknownValue}},
		VisibilityPersistenceUpsertWorkflowExecutionScope:                  {operation: "UpsertWorkflowExecution", tags: map[string]string{visibilityTypeTagName: unknownValue}},
		VisibilityPersistenceListOpenWorkflowExecutionsScope:               {operation: "ListOpenWorkflowExecutions", tags: map[string]string{visibilityTypeTagName: unknownValue}},
		VisibilityPersistenceListClosedWorkflowExecutionsScope:             {operation: "ListClosedWorkflowExecutions", tags: map[string]string{visibilityTypeTagName: unknownValue}},
		VisibilityPersistenceListOpenWorkflowExecutionsByTypeScope:         {operation: "ListOpenWorkflowExecutionsByType", tags: map[string]string{visibilityTypeTagName: unknownValue}},
		VisibilityPersistenceListClosedWorkflowExecutionsByTypeScope:       {operation: "ListClosedWorkflowExecutionsByType", tags: map[string]string{visibilityTypeTagName: unknownValue}},
		VisibilityPersistenceListOpenWorkflowExecutionsByWorkflowIDScope:   {operation: "ListOpenWorkflowExecutionsByWorkflowID", tags: map[string]string{visibilityTypeTagName: unknownValue}},
		VisibilityPersistenceListClosedWorkflowExecutionsByWorkflowIDScope: {operation: "ListClosedWorkflowExecutionsByWorkflowID", tags: map[string]string{visibilityTypeTagName: unknownValue}},
		VisibilityPersistenceListClosedWorkflowExecutionsByStatusScope:     {operation: "ListClosedWorkflowExecutionsByStatus", tags: map[string]string{visibilityTypeTagName: unknownValue}},
		VisibilityPersistenceDeleteWorkflowExecutionScope:                  {operation: "VisibilityDeleteWorkflowExecution", tags: map[string]string{visibilityTypeTagName: unknownValue}},
		VisibilityPersistenceListWorkflowExecutionsScope:                   {operation: "ListWorkflowExecutions", tags: map[string]string{visibilityTypeTagName: unknownValue}},
		VisibilityPersistenceScanWorkflowExecutionsScope:                   {operation: "ScanWorkflowExecutions", tags: map[string]string{visibilityTypeTagName: unknownValue}},
		VisibilityPersistenceCountWorkflowExecutionsScope:                  {operation: "CountWorkflowExecutions", tags: map[string]string{visibilityTypeTagName: unknownValue}},

		PersistenceAppendHistoryNodesScope:         {operation: "AppendHistoryNodes"},
		PersistenceAppendRawHistoryNodesScope:      {operation: "AppendRawHistoryNodes"},
		PersistenceDeleteHistoryNodesScope:         {operation: "DeleteHistoryNodes"},
		PersistenceReadHistoryBranchScope:          {operation: "ReadHistoryBranch"},
		PersistenceReadHistoryBranchReverseScope:   {operation: "ReadHistoryBranchReverse"},
		PersistenceForkHistoryBranchScope:          {operation: "ForkHistoryBranch"},
		PersistenceDeleteHistoryBranchScope:        {operation: "DeleteHistoryBranch"},
		PersistenceTrimHistoryBranchScope:          {operation: "TrimHistoryBranch"},
		PersistenceCompleteForkBranchScope:         {operation: "CompleteForkBranch"},
		PersistenceGetHistoryTreeScope:             {operation: "GetHistoryTree"},
		PersistenceGetAllHistoryTreeBranchesScope:  {operation: "GetAllHistoryTreeBranches"},
		PersistenceEnqueueMessageScope:             {operation: "EnqueueMessage"},
		PersistenceEnqueueMessageToDLQScope:        {operation: "EnqueueMessageToDLQ"},
		PersistenceReadQueueMessagesScope:          {operation: "ReadQueueMessages"},
		PersistenceReadQueueMessagesFromDLQScope:   {operation: "ReadQueueMessagesFromDLQ"},
		PersistenceDeleteQueueMessagesScope:        {operation: "DeleteQueueMessages"},
		PersistenceDeleteQueueMessageFromDLQScope:  {operation: "DeleteQueueMessageFromDLQ"},
		PersistenceRangeDeleteMessagesFromDLQScope: {operation: "RangeDeleteMessagesFromDLQ"},
		PersistenceUpdateAckLevelScope:             {operation: "UpdateAckLevel"},
		PersistenceGetAckLevelScope:                {operation: "GetAckLevel"},
		PersistenceUpdateDLQAckLevelScope:          {operation: "UpdateDLQAckLevel"},
		PersistenceGetDLQAckLevelScope:             {operation: "GetDLQAckLevel"},
		PersistenceNamespaceReplicationQueueScope:  {operation: "NamespaceReplicationQueue"},
		PersistenceListClusterMetadataScope:        {operation: "ListClusterMetadata"},
		PersistenceGetClusterMetadataScope:         {operation: "GetClusterMetadata"},
		PersistenceSaveClusterMetadataScope:        {operation: "SaveClusterMetadata"},
		PersistenceDeleteClusterMetadataScope:      {operation: "DeleteClusterMetadata"},
		PersistencePruneClusterMembershipScope:     {operation: "PruneClusterMembership"},
		PersistenceGetClusterMembersScope:          {operation: "GetClusterMembership"},
		PersistenceUpsertClusterMembershipScope:    {operation: "UpsertClusterMembership"},

		ClusterMetadataArchivalConfigScope: {operation: "ArchivalConfig"},

		HistoryClientStartWorkflowExecutionScope:                 {operation: "HistoryClientStartWorkflowExecution", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRecordActivityTaskHeartbeatScope:            {operation: "HistoryClientRecordActivityTaskHeartbeat", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRespondWorkflowTaskCompletedScope:           {operation: "HistoryClientRespondWorkflowTaskCompleted", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRespondWorkflowTaskFailedScope:              {operation: "HistoryClientRespondWorkflowTaskFailed", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRespondActivityTaskCompletedScope:           {operation: "HistoryClientRespondActivityTaskCompleted", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRespondActivityTaskFailedScope:              {operation: "HistoryClientRespondActivityTaskFailed", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRespondActivityTaskCanceledScope:            {operation: "HistoryClientRespondActivityTaskCanceled", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientGetMutableStateScope:                        {operation: "HistoryClientGetMutableState", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientPollMutableStateScope:                       {operation: "HistoryClientPollMutableState", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientResetStickyTaskQueueScope:                   {operation: "HistoryClientResetStickyTaskQueueScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientDescribeWorkflowExecutionScope:              {operation: "HistoryClientDescribeWorkflowExecution", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRecordWorkflowTaskStartedScope:              {operation: "HistoryClientRecordWorkflowTaskStarted", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRecordActivityTaskStartedScope:              {operation: "HistoryClientRecordActivityTaskStarted", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRequestCancelWorkflowExecutionScope:         {operation: "HistoryClientRequestCancelWorkflowExecution", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientSignalWorkflowExecutionScope:                {operation: "HistoryClientSignalWorkflowExecution", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientSignalWithStartWorkflowExecutionScope:       {operation: "HistoryClientSignalWithStartWorkflowExecution", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRemoveSignalMutableStateScope:               {operation: "HistoryClientRemoveSignalMutableStateScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientTerminateWorkflowExecutionScope:             {operation: "HistoryClientTerminateWorkflowExecution", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientUpdateWorkflowScope:                         {operation: "HistoryClientUpdateWorkflow", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientDeleteWorkflowExecutionScope:                {operation: "HistoryClientDeleteWorkflowExecution", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientResetWorkflowExecutionScope:                 {operation: "HistoryClientResetWorkflowExecution", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientScheduleWorkflowTaskScope:                   {operation: "HistoryClientScheduleWorkflowTask", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientVerifyFirstWorkflowTaskScheduled:            {operation: "HistoryClientVerifyFirstWorkflowTaskScheduled", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRecordChildExecutionCompletedScope:          {operation: "HistoryClientRecordChildExecutionCompleted", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientVerifyChildExecutionCompletionRecordedScope: {operation: "HistoryClientVerifyChildExecutionCompletionRecorded", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientReplicateEventsV2Scope:                      {operation: "HistoryClientReplicateEventsV2", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientSyncShardStatusScope:                        {operation: "HistoryClientSyncShardStatusScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientSyncActivityScope:                           {operation: "HistoryClientSyncActivityScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientGetReplicationTasksScope:                    {operation: "HistoryClientGetReplicationTasksScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientGetDLQReplicationTasksScope:                 {operation: "HistoryClientGetDLQReplicationTasksScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientQueryWorkflowScope:                          {operation: "HistoryClientQueryWorkflowScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientReapplyEventsScope:                          {operation: "HistoryClientReapplyEventsScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientGetDLQMessagesScope:                         {operation: "HistoryClientGetDLQMessagesScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientPurgeDLQMessagesScope:                       {operation: "HistoryClientPurgeDLQMessagesScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientMergeDLQMessagesScope:                       {operation: "HistoryClientMergeDLQMessagesScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRefreshWorkflowTasksScope:                   {operation: "HistoryClientRefreshWorkflowTasksScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientGenerateLastHistoryReplicationTasksScope:    {operation: "HistoryClientGenerateLastHistoryReplicationTasksScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientGetReplicationStatusScope:                   {operation: "HistoryClientGetReplicationStatusScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientDeleteWorkflowVisibilityRecordScope:         {operation: "HistoryClientDeleteWorkflowVisibilityRecordScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientCloseShardScope:                             {operation: "HistoryClientCloseShardScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientDescribeMutableStateScope:                   {operation: "HistoryClientDescribeMutableStateScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientGetDLQReplicationMessagesScope:              {operation: "HistoryClientGetDLQReplicationMessagesScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientGetShardScope:                               {operation: "HistoryClientGetShardScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRebuildMutableStateScope:                    {operation: "HistoryClientRebuildMutableStateScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRemoveTaskScope:                             {operation: "HistoryClientRemoveTaskScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientVerifyFirstWorkflowTaskScheduledScope:       {operation: "HistoryClientVerifyFirstWorkflowTaskScheduledScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientDescribeHistoryHostScope:                    {operation: "HistoryClientDescribeHistoryHostScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientGetReplicationMessagesScope:                 {operation: "HistoryClientGetReplicationMessagesScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},

		MatchingClientPollWorkflowTaskQueueScope:       {operation: "MatchingClientPollWorkflowTaskQueue", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientPollActivityTaskQueueScope:       {operation: "MatchingClientPollActivityTaskQueue", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientAddActivityTaskScope:             {operation: "MatchingClientAddActivityTask", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientAddWorkflowTaskScope:             {operation: "MatchingClientAddWorkflowTask", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientQueryWorkflowScope:               {operation: "MatchingClientQueryWorkflow", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientRespondQueryTaskCompletedScope:   {operation: "MatchingClientRespondQueryTaskCompleted", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientCancelOutstandingPollScope:       {operation: "MatchingClientCancelOutstandingPoll", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientDescribeTaskQueueScope:           {operation: "MatchingClientDescribeTaskQueue", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientListTaskQueuePartitionsScope:     {operation: "MatchingClientListTaskQueuePartitions", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientUpdateWorkerBuildIdOrderingScope: {operation: "MatchingClientUpdateWorkerBuildIdOrdering", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientGetWorkerBuildIdOrderingScope:    {operation: "MatchingClientGetWorkerBuildIdOrdering", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},

		FrontendClientDeprecateNamespaceScope:                 {operation: "FrontendClientDeprecateNamespace", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientDescribeNamespaceScope:                  {operation: "FrontendClientDescribeNamespace", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientDescribeTaskQueueScope:                  {operation: "FrontendClientDescribeTaskQueue", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientDescribeWorkflowExecutionScope:          {operation: "FrontendClientDescribeWorkflowExecution", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientGetWorkflowExecutionHistoryScope:        {operation: "FrontendClientGetWorkflowExecutionHistory", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientGetWorkflowExecutionHistoryReverseScope: {operation: "FrontendClientGetWorkflowExecutionHistoryReverse", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientGetWorkflowExecutionRawHistoryScope:     {operation: "FrontendClientGetWorkflowExecutionRawHistory", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientPollForWorkflowExecutionRawHistoryScope: {operation: "FrontendClientPollForWorkflowExecutionRawHistoryScope", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientListArchivedWorkflowExecutionsScope:     {operation: "FrontendClientListArchivedWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientListClosedWorkflowExecutionsScope:       {operation: "FrontendClientListClosedWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientListNamespacesScope:                     {operation: "FrontendClientListNamespaces", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientListOpenWorkflowExecutionsScope:         {operation: "FrontendClientListOpenWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientPollActivityTaskQueueScope:              {operation: "FrontendClientPollActivityTaskQueue", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientPollWorkflowTaskQueueScope:              {operation: "FrontendClientPollWorkflowTaskQueue", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientQueryWorkflowScope:                      {operation: "FrontendClientQueryWorkflow", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRecordActivityTaskHeartbeatScope:        {operation: "FrontendClientRecordActivityTaskHeartbeat", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRecordActivityTaskHeartbeatByIdScope:    {operation: "FrontendClientRecordActivityTaskHeartbeatById", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRegisterNamespaceScope:                  {operation: "FrontendClientRegisterNamespace", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRequestCancelWorkflowExecutionScope:     {operation: "FrontendClientRequestCancelWorkflowExecution", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientResetStickyTaskQueueScope:               {operation: "FrontendClientResetStickyTaskQueue", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientResetWorkflowExecutionScope:             {operation: "FrontendClientResetWorkflowExecution", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRespondActivityTaskCanceledScope:        {operation: "FrontendClientRespondActivityTaskCanceled", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRespondActivityTaskCanceledByIdScope:    {operation: "FrontendClientRespondActivityTaskCanceledById", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRespondActivityTaskCompletedScope:       {operation: "FrontendClientRespondActivityTaskCompleted", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRespondActivityTaskCompletedByIdScope:   {operation: "FrontendClientRespondActivityTaskCompletedById", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRespondActivityTaskFailedScope:          {operation: "FrontendClientRespondActivityTaskFailed", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRespondActivityTaskFailedByIdScope:      {operation: "FrontendClientRespondActivityTaskFailedById", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRespondWorkflowTaskCompletedScope:       {operation: "FrontendClientRespondWorkflowTaskCompleted", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRespondWorkflowTaskFailedScope:          {operation: "FrontendClientRespondWorkflowTaskFailed", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRespondQueryTaskCompletedScope:          {operation: "FrontendClientRespondQueryTaskCompleted", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientSignalWithStartWorkflowExecutionScope:   {operation: "FrontendClientSignalWithStartWorkflowExecution", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientSignalWorkflowExecutionScope:            {operation: "FrontendClientSignalWorkflowExecution", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientStartWorkflowExecutionScope:             {operation: "FrontendClientStartWorkflowExecution", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientTerminateWorkflowExecutionScope:         {operation: "FrontendClientTerminateWorkflowExecution", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientUpdateNamespaceScope:                    {operation: "FrontendClientUpdateNamespace", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientListWorkflowExecutionsScope:             {operation: "FrontendClientListWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientScanWorkflowExecutionsScope:             {operation: "FrontendClientScanWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientCountWorkflowExecutionsScope:            {operation: "FrontendClientCountWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientGetSearchAttributesScope:                {operation: "FrontendClientGetSearchAttributes", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientGetClusterInfoScope:                     {operation: "FrontendClientGetClusterInfoScope", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientGetSystemInfoScope:                      {operation: "FrontendClientGetSystemInfoScope", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientListTaskQueuePartitionsScope:            {operation: "FrontendClientListTaskQueuePartitions", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientCreateScheduleScope:                     {operation: "FrontendClientCreateSchedule", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientDescribeScheduleScope:                   {operation: "FrontendClientDescribeSchedule", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientUpdateScheduleScope:                     {operation: "FrontendClientUpdateSchedule", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientPatchScheduleScope:                      {operation: "FrontendClientPatchSchedule", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientListScheduleMatchingTimesScope:          {operation: "FrontendClientListScheduleMatchingTimes", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientDeleteScheduleScope:                     {operation: "FrontendClientDeleteSchedule", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientListSchedulesScope:                      {operation: "FrontendClientListSchedules", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientUpdateWorkerBuildIdOrderingScope:        {operation: "FrontendClientUpdateWorkerBuildIdOrdering", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientGetWorkerBuildIdOrderingScope:           {operation: "FrontendClientGetWorkerBuildIdOrdering", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientUpdateWorkflowScope:                     {operation: "FrontendClientUpdateWorkflow", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},

		AdminClientAddSearchAttributesScope:              {operation: "AdminClientAddSearchAttributes", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientRemoveSearchAttributesScope:           {operation: "AdminClientRemoveSearchAttributes", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientGetSearchAttributesScope:              {operation: "AdminClientGetSearchAttributes", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientDescribeHistoryHostScope:              {operation: "AdminClientDescribeHistoryHost", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientRebuildMutableStateScope:              {operation: "AdminClientRebuildMutableState", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientDescribeMutableStateScope:             {operation: "AdminClientDescribeMutableState", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientGetWorkflowExecutionRawHistoryScope:   {operation: "AdminClientGetWorkflowExecutionRawHistory", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientGetWorkflowExecutionRawHistoryV2Scope: {operation: "AdminClientGetWorkflowExecutionRawHistoryV2", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientDescribeClusterScope:                  {operation: "AdminClientDescribeCluster", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientListClustersScope:                     {operation: "AdminClientListClusters", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientAddOrUpdateRemoteClusterScope:         {operation: "AdminClientAddOrUpdateRemoteCluster", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientRemoveRemoteClusterScope:              {operation: "AdminClientRemoveRemoteCluster", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientRefreshWorkflowTasksScope:             {operation: "AdminClientRefreshWorkflowTasks", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientResendReplicationTasksScope:           {operation: "AdminClientResendReplicationTasks", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientGetTaskQueueTasksScope:                {operation: "AdminClientGetTaskQueueTasks", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientListClusterMembersScope:               {operation: "AdminClientListClusterMembers", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientCloseShardScope:                       {operation: "AdminClientCloseShard", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientGetShardScope:                         {operation: "AdminClientGetShard", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientListHistoryTasksScope:                 {operation: "AdminClientListHistoryTasks", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientRemoveTaskScope:                       {operation: "AdminClientRemoveTask", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientGetReplicationMessagesScope:           {operation: "AdminClientGetReplicationMessagesScope", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientGetNamespaceReplicationMessagesScope:  {operation: "AdminClientGetNamespaceReplicationMessagesScope", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientGetDLQReplicationMessagesScope:        {operation: "AdminClientGetDLQReplicationMessagesScope", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientReapplyEventsScope:                    {operation: "AdminClientReapplyEventsScope", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientGetDLQMessagesScope:                   {operation: "AdminClientGetDLQMessages", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientPurgeDLQMessagesScope:                 {operation: "AdminClientPurgeDLQMessages", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientMergeDLQMessagesScope:                 {operation: "AdminClientMergeDLQMessages", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientDeleteWorkflowExecutionScope:          {operation: "AdminClientDeleteWorkflowExecution", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},

		DCRedirectionDeprecateNamespaceScope:                 {operation: "DCRedirectionDeprecateNamespace", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionDescribeNamespaceScope:                  {operation: "DCRedirectionDescribeNamespace", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionDescribeTaskQueueScope:                  {operation: "DCRedirectionDescribeTaskQueue", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionDescribeWorkflowExecutionScope:          {operation: "DCRedirectionDescribeWorkflowExecution", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionGetWorkflowExecutionHistoryScope:        {operation: "DCRedirectionGetWorkflowExecutionHistory", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionGetWorkflowExecutionHistoryReverseScope: {operation: "DCRedirectionGetWorkflowExecutionHistoryReverse", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionGetWorkflowExecutionRawHistoryScope:     {operation: "DCRedirectionGetWorkflowExecutionRawHistoryScope", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionPollForWorkflowExecutionRawHistoryScope: {operation: "DCRedirectionPollForWorkflowExecutionRawHistoryScope", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionListArchivedWorkflowExecutionsScope:     {operation: "DCRedirectionListArchivedWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionListClosedWorkflowExecutionsScope:       {operation: "DCRedirectionListClosedWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionListNamespacesScope:                     {operation: "DCRedirectionListNamespaces", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionListOpenWorkflowExecutionsScope:         {operation: "DCRedirectionListOpenWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionListWorkflowExecutionsScope:             {operation: "DCRedirectionListWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionScanWorkflowExecutionsScope:             {operation: "DCRedirectionScanWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionCountWorkflowExecutionsScope:            {operation: "DCRedirectionCountWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionGetSearchAttributesScope:                {operation: "DCRedirectionGetSearchAttributes", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionPollActivityTaskQueueScope:              {operation: "DCRedirectionPollActivityTaskQueue", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionPollWorkflowTaskQueueScope:              {operation: "DCRedirectionPollWorkflowTaskQueue", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionQueryWorkflowScope:                      {operation: "DCRedirectionQueryWorkflow", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRecordActivityTaskHeartbeatScope:        {operation: "DCRedirectionRecordActivityTaskHeartbeat", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRecordActivityTaskHeartbeatByIdScope:    {operation: "DCRedirectionRecordActivityTaskHeartbeatById", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRegisterNamespaceScope:                  {operation: "DCRedirectionRegisterNamespace", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRequestCancelWorkflowExecutionScope:     {operation: "DCRedirectionRequestCancelWorkflowExecution", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionResetStickyTaskQueueScope:               {operation: "DCRedirectionResetStickyTaskQueue", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionResetWorkflowExecutionScope:             {operation: "DCRedirectionResetWorkflowExecution", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRespondActivityTaskCanceledScope:        {operation: "DCRedirectionRespondActivityTaskCanceled", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRespondActivityTaskCanceledByIdScope:    {operation: "DCRedirectionRespondActivityTaskCanceledById", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRespondActivityTaskCompletedScope:       {operation: "DCRedirectionRespondActivityTaskCompleted", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRespondActivityTaskCompletedByIdScope:   {operation: "DCRedirectionRespondActivityTaskCompletedById", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRespondActivityTaskFailedScope:          {operation: "DCRedirectionRespondActivityTaskFailed", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRespondActivityTaskFailedByIdScope:      {operation: "DCRedirectionRespondActivityTaskFailedById", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRespondWorkflowTaskCompletedScope:       {operation: "DCRedirectionRespondWorkflowTaskCompleted", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRespondWorkflowTaskFailedScope:          {operation: "DCRedirectionRespondWorkflowTaskFailed", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRespondQueryTaskCompletedScope:          {operation: "DCRedirectionRespondQueryTaskCompleted", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionSignalWithStartWorkflowExecutionScope:   {operation: "DCRedirectionSignalWithStartWorkflowExecution", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionSignalWorkflowExecutionScope:            {operation: "DCRedirectionSignalWorkflowExecution", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionStartWorkflowExecutionScope:             {operation: "DCRedirectionStartWorkflowExecution", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionTerminateWorkflowExecutionScope:         {operation: "DCRedirectionTerminateWorkflowExecution", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionUpdateNamespaceScope:                    {operation: "DCRedirectionUpdateNamespace", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionListTaskQueuePartitionsScope:            {operation: "DCRedirectionListTaskQueuePartitions", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionCreateScheduleScope:                     {operation: "DCRedirectionCreateSchedule", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionDescribeScheduleScope:                   {operation: "DCRedirectionDescribeSchedule", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionUpdateScheduleScope:                     {operation: "DCRedirectionUpdateSchedule", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionPatchScheduleScope:                      {operation: "DCRedirectionPatchSchedule", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionListScheduleMatchingTimesScope:          {operation: "DCRedirectionListScheduleMatchingTimes", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionDeleteScheduleScope:                     {operation: "DCRedirectionDeleteSchedule", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionListSchedulesScope:                      {operation: "DCRedirectionListSchedules", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionUpdateWorkerBuildIdOrderingScope:        {operation: "DCRedirectionUpdateWorkerBuildIdOrdering", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionGetWorkerBuildIdOrderingScope:           {operation: "DCRedirectionGetWorkerBuildIdOrdering", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionUpdateWorkflowScope:                     {operation: "DCRedirectionUpdateWorkflow", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},

		MessagingClientPublishScope:      {operation: "MessagingClientPublish"},
		MessagingClientPublishBatchScope: {operation: "MessagingClientPublishBatch"},

		NamespaceCacheScope:                                   {operation: "NamespaceCache"},
		HistoryRereplicationByTransferTaskScope:               {operation: "HistoryRereplicationByTransferTask"},
		HistoryRereplicationByTimerTaskScope:                  {operation: "HistoryRereplicationByTimerTask"},
		HistoryRereplicationByHistoryReplicationScope:         {operation: "HistoryRereplicationByHistoryReplication"},
		HistoryRereplicationByHistoryMetadataReplicationScope: {operation: "HistoryRereplicationByHistoryMetadataReplication"},
		HistoryRereplicationByActivityReplicationScope:        {operation: "HistoryRereplicationByActivityReplication"},

		ElasticsearchBulkProcessor: {operation: "ElasticsearchBulkProcessor"},
		ElasticsearchVisibility:    {operation: "ElasticsearchVisibility"},

		SequentialTaskProcessingScope: {operation: "SequentialTaskProcessing"},
		ParallelTaskProcessingScope:   {operation: "ParallelTaskProcessing"},
		TaskSchedulerScope:            {operation: "TaskScheduler"},

		HistoryArchiverScope:    {operation: "HistoryArchiver"},
		VisibilityArchiverScope: {operation: "VisibilityArchiver"},

		BlobstoreClientUploadScope:          {operation: "BlobstoreClientUpload", tags: map[string]string{ServiceRoleTagName: BlobstoreRoleTagValue}},
		BlobstoreClientDownloadScope:        {operation: "BlobstoreClientDownload", tags: map[string]string{ServiceRoleTagName: BlobstoreRoleTagValue}},
		BlobstoreClientGetMetadataScope:     {operation: "BlobstoreClientGetMetadata", tags: map[string]string{ServiceRoleTagName: BlobstoreRoleTagValue}},
		BlobstoreClientExistsScope:          {operation: "BlobstoreClientExists", tags: map[string]string{ServiceRoleTagName: BlobstoreRoleTagValue}},
		BlobstoreClientDeleteScope:          {operation: "BlobstoreClientDelete", tags: map[string]string{ServiceRoleTagName: BlobstoreRoleTagValue}},
		BlobstoreClientDirectoryExistsScope: {operation: "BlobstoreClientDirectoryExists", tags: map[string]string{ServiceRoleTagName: BlobstoreRoleTagValue}},

		DynamicConfigScope: {operation: "DynamicConfig"},
	},
	// Frontend Scope Names
	Frontend: {
		// Admin API scope co-locates with frontend
		AdminRemoveTaskScope:                            {operation: "AdminRemoveTask"},
		AdminCloseShardScope:                            {operation: "AdminCloseShard"},
		AdminGetShardScope:                              {operation: "AdminGetShard"},
		AdminListHistoryTasksScope:                      {operation: "AdminListHistoryTasks"},
		AdminReadDLQMessagesScope:                       {operation: "AdminReadDLQMessages"},
		AdminPurgeDLQMessagesScope:                      {operation: "AdminPurgeDLQMessages"},
		AdminMergeDLQMessagesScope:                      {operation: "AdminMergeDLQMessages"},
		AdminDescribeHistoryHostScope:                   {operation: "DescribeHistoryHost"},
		AdminAddSearchAttributesScope:                   {operation: "AdminAddSearchAttributes"},
		AdminRemoveSearchAttributesScope:                {operation: "AdminRemoveSearchAttributes"},
		AdminGetSearchAttributesScope:                   {operation: "AdminGetSearchAttributes"},
		AdminRebuildMutableStateScope:                   {operation: "AdminRebuildMutableState"},
		AdminDescribeWorkflowExecutionScope:             {operation: "DescribeWorkflowExecution"},
		AdminGetWorkflowExecutionRawHistoryScope:        {operation: "GetWorkflowExecutionRawHistory"},
		AdminGetWorkflowExecutionRawHistoryV2Scope:      {operation: "GetWorkflowExecutionRawHistoryV2"},
		AdminGetReplicationMessagesScope:                {operation: "GetReplicationMessages"},
		AdminListClusterMembersScope:                    {operation: "AdminListClusterMembers"},
		AdminGetNamespaceReplicationMessagesScope:       {operation: "GetNamespaceReplicationMessages"},
		AdminGetDLQReplicationMessagesScope:             {operation: "AdminGetDLQReplicationMessages"},
		AdminReapplyEventsScope:                         {operation: "ReapplyEvents"},
		AdminRefreshWorkflowTasksScope:                  {operation: "RefreshWorkflowTasks"},
		AdminResendReplicationTasksScope:                {operation: "ResendReplicationTasks"},
		AdminGetTaskQueueTasksScope:                     {operation: "GetTaskQueueTasks"},
		AdminDescribeClusterScope:                       {operation: "AdminDescribeCluster"},
		AdminListClustersScope:                          {operation: "AdminListClusters"},
		AdminAddOrUpdateRemoteClusterScope:              {operation: "AdminAddOrUpdateRemoteCluster"},
		AdminRemoveRemoteClusterScope:                   {operation: "AdminRemoveRemoteCluster"},
		AdminDeleteWorkflowExecutionScope:               {operation: "AdminDeleteWorkflowExecution"},
		OperatorAddSearchAttributesScope:                {operation: "OperatorAddSearchAttributes"},
		OperatorRemoveSearchAttributesScope:             {operation: "OperatorRemoveSearchAttributes"},
		OperatorListSearchAttributesScope:               {operation: "OperatorListSearchAttributes"},
		OperatorDeleteNamespaceScope:                    {operation: "OperatorDeleteNamespace"},
		FrontendStartWorkflowExecutionScope:             {operation: "StartWorkflowExecution"},
		FrontendPollWorkflowTaskQueueScope:              {operation: "PollWorkflowTaskQueue"},
		FrontendPollActivityTaskQueueScope:              {operation: "PollActivityTaskQueue"},
		FrontendRecordActivityTaskHeartbeatScope:        {operation: "RecordActivityTaskHeartbeat"},
		FrontendRecordActivityTaskHeartbeatByIdScope:    {operation: "RecordActivityTaskHeartbeatById"},
		FrontendRespondWorkflowTaskCompletedScope:       {operation: "RespondWorkflowTaskCompleted"},
		FrontendRespondWorkflowTaskFailedScope:          {operation: "RespondWorkflowTaskFailed"},
		FrontendRespondQueryTaskCompletedScope:          {operation: "RespondQueryTaskCompleted"},
		FrontendRespondActivityTaskCompletedScope:       {operation: "RespondActivityTaskCompleted"},
		FrontendRespondActivityTaskFailedScope:          {operation: "RespondActivityTaskFailed"},
		FrontendRespondActivityTaskCanceledScope:        {operation: "RespondActivityTaskCanceled"},
		FrontendRespondActivityTaskCompletedByIdScope:   {operation: "RespondActivityTaskCompletedById"},
		FrontendRespondActivityTaskFailedByIdScope:      {operation: "RespondActivityTaskFailedById"},
		FrontendRespondActivityTaskCanceledByIdScope:    {operation: "RespondActivityTaskCanceledById"},
		FrontendGetWorkflowExecutionHistoryScope:        {operation: "GetWorkflowExecutionHistory"},
		FrontendGetWorkflowExecutionHistoryReverseScope: {operation: "GetWorkflowExecutionHistoryReverse"},
		FrontendPollWorkflowExecutionHistoryScope:       {operation: "PollWorkflowExecutionHistory"},
		FrontendGetWorkflowExecutionRawHistoryScope:     {operation: "GetWorkflowExecutionRawHistory"},
		FrontendPollForWorkflowExecutionRawHistoryScope: {operation: "PollForWorkflowExecutionRawHistory"},
		FrontendSignalWorkflowExecutionScope:            {operation: "SignalWorkflowExecution"},
		FrontendSignalWithStartWorkflowExecutionScope:   {operation: "SignalWithStartWorkflowExecution"},
		FrontendTerminateWorkflowExecutionScope:         {operation: "TerminateWorkflowExecution"},
		FrontendResetWorkflowExecutionScope:             {operation: "ResetWorkflowExecution"},
		FrontendRequestCancelWorkflowExecutionScope:     {operation: "RequestCancelWorkflowExecution"},
		FrontendListArchivedWorkflowExecutionsScope:     {operation: "ListArchivedWorkflowExecutions"},
		FrontendListOpenWorkflowExecutionsScope:         {operation: "ListOpenWorkflowExecutions"},
		FrontendListClosedWorkflowExecutionsScope:       {operation: "ListClosedWorkflowExecutions"},
		FrontendListWorkflowExecutionsScope:             {operation: "ListWorkflowExecutions"},
		FrontendScanWorkflowExecutionsScope:             {operation: "ScanWorkflowExecutions"},
		FrontendCountWorkflowExecutionsScope:            {operation: "CountWorkflowExecutions"},
		FrontendRegisterNamespaceScope:                  {operation: "RegisterNamespace"},
		FrontendDescribeNamespaceScope:                  {operation: "DescribeNamespace"},
		FrontendListNamespacesScope:                     {operation: "ListNamespaces"},
		FrontendUpdateNamespaceScope:                    {operation: "UpdateNamespace"},
		FrontendDeprecateNamespaceScope:                 {operation: "DeprecateNamespace"},
		FrontendQueryWorkflowScope:                      {operation: "QueryWorkflow"},
		FrontendDescribeWorkflowExecutionScope:          {operation: "DescribeWorkflowExecution"},
		FrontendListTaskQueuePartitionsScope:            {operation: "ListTaskQueuePartitions"},
		FrontendDescribeTaskQueueScope:                  {operation: "DescribeTaskQueue"},
		FrontendResetStickyTaskQueueScope:               {operation: "ResetStickyTaskQueue"},
		FrontendGetSearchAttributesScope:                {operation: "GetSearchAttributes"},
		FrontendGetClusterInfoScope:                     {operation: "GetClusterInfo"},
		FrontendGetSystemInfoScope:                      {operation: "GetSystemInfo"},
		FrontendCreateScheduleScope:                     {operation: "CreateSchedule"},
		FrontendDescribeScheduleScope:                   {operation: "DescribeSchedule"},
		FrontendUpdateScheduleScope:                     {operation: "UpdateSchedule"},
		FrontendPatchScheduleScope:                      {operation: "PatchSchedule"},
		FrontendListScheduleMatchingTimesScope:          {operation: "ListScheduleMatchingTimes"},
		FrontendDeleteScheduleScope:                     {operation: "DeleteSchedule"},
		FrontendListSchedulesScope:                      {operation: "ListSchedules"},
		FrontendUpdateWorkerBuildIdOrderingScope:        {operation: "UpdateWorkerBuildIdOrdering"},
		FrontendGetWorkerBuildIdOrderingScope:           {operation: "GetWorkerBuildIdOrdering"},
		VersionCheckScope:                               {operation: "VersionCheck"},
		AuthorizationScope:                              {operation: "Authorization"},
		FrontendUpdateWorkflowScope:                     {operation: "UpdateWorkflow"},
	},
	// History Scope Names
	History: {
		HistoryStartWorkflowExecutionScope:                 {operation: "StartWorkflowExecution"},
		HistoryRecordActivityTaskHeartbeatScope:            {operation: "RecordActivityTaskHeartbeat"},
		HistoryRespondWorkflowTaskCompletedScope:           {operation: "RespondWorkflowTaskCompleted"},
		HistoryRespondWorkflowTaskFailedScope:              {operation: "RespondWorkflowTaskFailed"},
		HistoryRespondActivityTaskCompletedScope:           {operation: "RespondActivityTaskCompleted"},
		HistoryRespondActivityTaskFailedScope:              {operation: "RespondActivityTaskFailed"},
		HistoryRespondActivityTaskCanceledScope:            {operation: "RespondActivityTaskCanceled"},
		HistoryGetMutableStateScope:                        {operation: "GetMutableState"},
		HistoryPollMutableStateScope:                       {operation: "PollMutableState"},
		HistoryResetStickyTaskQueueScope:                   {operation: "ResetStickyTaskQueueScope"},
		HistoryDescribeWorkflowExecutionScope:              {operation: "DescribeWorkflowExecution"},
		HistoryRecordWorkflowTaskStartedScope:              {operation: "RecordWorkflowTaskStarted"},
		HistoryRecordActivityTaskStartedScope:              {operation: "RecordActivityTaskStarted"},
		HistorySignalWorkflowExecutionScope:                {operation: "SignalWorkflowExecution"},
		HistorySignalWithStartWorkflowExecutionScope:       {operation: "SignalWithStartWorkflowExecution"},
		HistoryRemoveSignalMutableStateScope:               {operation: "RemoveSignalMutableState"},
		HistoryTerminateWorkflowExecutionScope:             {operation: "TerminateWorkflowExecution"},
		HistoryResetWorkflowExecutionScope:                 {operation: "ResetWorkflowExecution"},
		HistoryQueryWorkflowScope:                          {operation: "QueryWorkflow"},
		HistoryProcessDeleteHistoryEventScope:              {operation: "ProcessDeleteHistoryEvent"},
		HistoryDeleteWorkflowExecutionScope:                {operation: "DeleteWorkflowExecution"},
		HistoryScheduleWorkflowTaskScope:                   {operation: "ScheduleWorkflowTask"},
		HistoryVerifyFirstWorkflowTaskScheduled:            {operation: "VerifyFirstWorkflowTaskScheduled"},
		HistoryRecordChildExecutionCompletedScope:          {operation: "RecordChildExecutionCompleted"},
		HistoryVerifyChildExecutionCompletionRecordedScope: {operation: "VerifyChildExecutionCompletionRecorded"},
		HistoryRequestCancelWorkflowExecutionScope:         {operation: "RequestCancelWorkflowExecution"},
		HistorySyncShardStatusScope:                        {operation: "SyncShardStatus"},
		HistorySyncActivityScope:                           {operation: "SyncActivity"},
		HistoryRebuildMutableStateScope:                    {operation: "RebuildMutableState"},
		HistoryDescribeMutableStateScope:                   {operation: "DescribeMutableState"},
		HistoryGetReplicationMessagesScope:                 {operation: "GetReplicationMessages"},
		HistoryGetDLQReplicationMessagesScope:              {operation: "GetDLQReplicationMessages"},
		HistoryReadDLQMessagesScope:                        {operation: "GetDLQMessages"},
		HistoryPurgeDLQMessagesScope:                       {operation: "PurgeDLQMessages"},
		HistoryMergeDLQMessagesScope:                       {operation: "MergeDLQMessages"},
		HistoryShardControllerScope:                        {operation: "ShardController"},
		HistoryReapplyEventsScope:                          {operation: "EventReapplication"},
		HistoryRefreshWorkflowTasksScope:                   {operation: "RefreshWorkflowTasks"},
		HistoryGenerateLastHistoryReplicationTasksScope:    {operation: "GenerateLastHistoryReplicationTasks"},
		HistoryGetReplicationStatusScope:                   {operation: "GetReplicationStatus"},
		HistoryHistoryRemoveTaskScope:                      {operation: "RemoveTask"},
		HistoryCloseShard:                                  {operation: "CloseShard"},
		HistoryGetShard:                                    {operation: "GetShard"},
		HistoryReplicateEventsV2:                           {operation: "ReplicateEventsV2"},
		HistoryResetStickyTaskQueue:                        {operation: "ResetStickyTaskQueue"},
		HistoryReapplyEvents:                               {operation: "ReapplyEvents"},
		HistoryDescribeHistoryHost:                         {operation: "DescribeHistoryHost"},
		HistoryDeleteWorkflowVisibilityRecordScope:         {operation: "DeleteWorkflowVisibilityRecord"},
		HistoryUpdateWorkflowScope:                         {operation: "UpdateWorkflow"},

		TaskPriorityAssignerScope:                   {operation: "TaskPriorityAssigner"},
		TransferQueueProcessorScope:                 {operation: "TransferQueueProcessor"},
		TransferActiveQueueProcessorScope:           {operation: "TransferActiveQueueProcessor"},
		TransferStandbyQueueProcessorScope:          {operation: "TransferStandbyQueueProcessor"},
		TransferActiveTaskActivityScope:             {operation: "TransferActiveTaskActivity"},
		TransferActiveTaskWorkflowTaskScope:         {operation: "TransferActiveTaskWorkflowTask"},
		TransferActiveTaskCloseExecutionScope:       {operation: "TransferActiveTaskCloseExecution"},
		TransferActiveTaskCancelExecutionScope:      {operation: "TransferActiveTaskCancelExecution"},
		TransferActiveTaskSignalExecutionScope:      {operation: "TransferActiveTaskSignalExecution"},
		TransferActiveTaskStartChildExecutionScope:  {operation: "TransferActiveTaskStartChildExecution"},
		TransferActiveTaskResetWorkflowScope:        {operation: "TransferActiveTaskResetWorkflow"},
		TransferStandbyTaskActivityScope:            {operation: "TransferStandbyTaskActivity"},
		TransferStandbyTaskWorkflowTaskScope:        {operation: "TransferStandbyTaskWorkflowTask"},
		TransferStandbyTaskCloseExecutionScope:      {operation: "TransferStandbyTaskCloseExecution"},
		TransferStandbyTaskCancelExecutionScope:     {operation: "TransferStandbyTaskCancelExecution"},
		TransferStandbyTaskSignalExecutionScope:     {operation: "TransferStandbyTaskSignalExecution"},
		TransferStandbyTaskStartChildExecutionScope: {operation: "TransferStandbyTaskStartChildExecution"},
		TransferStandbyTaskResetWorkflowScope:       {operation: "TransferStandbyTaskResetWorkflow"},

		VisibilityQueueProcessorScope:      {operation: "VisibilityQueueProcessor"},
		VisibilityTaskStartExecutionScope:  {operation: "VisibilityTaskStartExecution"},
		VisibilityTaskUpsertExecutionScope: {operation: "VisibilityTaskUpsertExecution"},
		VisibilityTaskCloseExecutionScope:  {operation: "VisibilityTaskCloseExecution"},
		VisibilityTaskDeleteExecutionScope: {operation: "VisibilityTaskDeleteExecution"},

		TimerQueueProcessorScope:                  {operation: "TimerQueueProcessor"},
		TimerActiveQueueProcessorScope:            {operation: "TimerActiveQueueProcessor"},
		TimerStandbyQueueProcessorScope:           {operation: "TimerStandbyQueueProcessor"},
		TimerActiveTaskActivityTimeoutScope:       {operation: "TimerActiveTaskActivityTimeout"},
		TimerActiveTaskWorkflowTaskTimeoutScope:   {operation: "TimerActiveTaskWorkflowTaskTimeout"},
		TimerActiveTaskUserTimerScope:             {operation: "TimerActiveTaskUserTimer"},
		TimerActiveTaskWorkflowTimeoutScope:       {operation: "TimerActiveTaskWorkflowTimeout"},
		TimerActiveTaskActivityRetryTimerScope:    {operation: "TimerActiveTaskActivityRetryTimer"},
		TimerActiveTaskWorkflowBackoffTimerScope:  {operation: "TimerActiveTaskWorkflowBackoffTimer"},
		TimerActiveTaskDeleteHistoryEventScope:    {operation: "TimerActiveTaskDeleteHistoryEvent"},
		TimerStandbyTaskActivityTimeoutScope:      {operation: "TimerStandbyTaskActivityTimeout"},
		TimerStandbyTaskWorkflowTaskTimeoutScope:  {operation: "TimerStandbyTaskWorkflowTaskTimeout"},
		TimerStandbyTaskUserTimerScope:            {operation: "TimerStandbyTaskUserTimer"},
		TimerStandbyTaskWorkflowTimeoutScope:      {operation: "TimerStandbyTaskWorkflowTimeout"},
		TimerStandbyTaskActivityRetryTimerScope:   {operation: "TimerStandbyTaskActivityRetryTimer"},
		TimerStandbyTaskWorkflowBackoffTimerScope: {operation: "TimerStandbyTaskWorkflowBackoffTimer"},
		TimerStandbyTaskDeleteHistoryEventScope:   {operation: "TimerStandbyTaskDeleteHistoryEvent"},
		HistoryEventNotificationScope:             {operation: "HistoryEventNotification"},
		ReplicatorQueueProcessorScope:             {operation: "ReplicatorQueueProcessor"},
		ReplicatorTaskHistoryScope:                {operation: "ReplicatorTaskHistory"},
		ReplicatorTaskSyncActivityScope:           {operation: "ReplicatorTaskSyncActivity"},
		ReplicateHistoryEventsScope:               {operation: "ReplicateHistoryEvents"},
		ShardInfoScope:                            {operation: "ShardInfo"},
		WorkflowContextScope:                      {operation: "WorkflowContext"},
		HistoryCacheGetOrCreateScope:              {operation: "HistoryCacheGetOrCreate", tags: map[string]string{CacheTypeTagName: MutableStateCacheTypeTagValue}},
		HistoryCacheGetOrCreateCurrentScope:       {operation: "HistoryCacheGetOrCreateCurrent", tags: map[string]string{CacheTypeTagName: MutableStateCacheTypeTagValue}},
		EventsCacheGetEventScope:                  {operation: "EventsCacheGetEvent", tags: map[string]string{CacheTypeTagName: EventsCacheTypeTagValue}},
		EventsCachePutEventScope:                  {operation: "EventsCachePutEvent", tags: map[string]string{CacheTypeTagName: EventsCacheTypeTagValue}},
		EventsCacheDeleteEventScope:               {operation: "EventsCacheDeleteEvent", tags: map[string]string{CacheTypeTagName: EventsCacheTypeTagValue}},
		EventsCacheGetFromStoreScope:              {operation: "EventsCacheGetFromStore", tags: map[string]string{CacheTypeTagName: EventsCacheTypeTagValue}},
		ExecutionStatsScope:                       {operation: "ExecutionStats"},
		SessionStatsScope:                         {operation: "SessionStats"},
		WorkflowCompletionStatsScope:              {operation: "CompletionStats"},
		ArchiverClientScope:                       {operation: "ArchiverClient"},
		ReplicationTaskFetcherScope:               {operation: "ReplicationTaskFetcher"},
		ReplicationTaskCleanupScope:               {operation: "ReplicationTaskCleanup"},
		ReplicationDLQStatsScope:                  {operation: "ReplicationDLQStats"},
		SyncShardTaskScope:                        {operation: "SyncShardTask"},
		SyncActivityTaskScope:                     {operation: "SyncActivityTask"},
		HistoryMetadataReplicationTaskScope:       {operation: "HistoryMetadataReplicationTask"},
		HistoryReplicationTaskScope:               {operation: "HistoryReplicationTask"},
		SyncWorkflowStateTaskScope:                {operation: "SyncWorkflowStateTask"},
		ReplicatorScope:                           {operation: "Replicator"},
	},
	// Matching Scope Names
	Matching: {
		MatchingPollWorkflowTaskQueueScope:       {operation: "PollWorkflowTaskQueue"},
		MatchingPollActivityTaskQueueScope:       {operation: "PollActivityTaskQueue"},
		MatchingAddActivityTaskScope:             {operation: "AddActivityTask"},
		MatchingAddWorkflowTaskScope:             {operation: "AddWorkflowTask"},
		MatchingTaskQueueMgrScope:                {operation: "TaskQueueMgr"},
		MatchingEngineScope:                      {operation: "MatchingEngine"},
		MatchingQueryWorkflowScope:               {operation: "QueryWorkflow"},
		MatchingRespondQueryTaskCompletedScope:   {operation: "RespondQueryTaskCompleted"},
		MatchingCancelOutstandingPollScope:       {operation: "CancelOutstandingPoll"},
		MatchingDescribeTaskQueueScope:           {operation: "DescribeTaskQueue"},
		MatchingListTaskQueuePartitionsScope:     {operation: "ListTaskQueuePartitions"},
		MatchingUpdateWorkerBuildIdOrderingScope: {operation: "UpdateWorkerBuildIdOrdering"},
		MatchingGetWorkerBuildIdOrderingScope:    {operation: "GetWorkerBuildIdOrdering"},
	},
	// Worker Scope Names
	Worker: {
		ReplicatorScope:                        {operation: "Replicator"},
		NamespaceReplicationTaskScope:          {operation: "NamespaceReplicationTask"},
		HistoryReplicationTaskScope:            {operation: "HistoryReplicationTask"},
		HistoryMetadataReplicationTaskScope:    {operation: "HistoryMetadataReplicationTask"},
		SyncShardTaskScope:                     {operation: "SyncShardTask"},
		SyncActivityTaskScope:                  {operation: "SyncActivityTask"},
		ESProcessorScope:                       {operation: "ESProcessor"},
		IndexProcessorScope:                    {operation: "IndexProcessor"},
		ArchiverDeleteHistoryActivityScope:     {operation: "ArchiverDeleteHistoryActivity"},
		ArchiverUploadHistoryActivityScope:     {operation: "ArchiverUploadHistoryActivity"},
		ArchiverArchiveVisibilityActivityScope: {operation: "ArchiverArchiveVisibilityActivity"},
		ArchiverScope:                          {operation: "Archiver"},
		ArchiverPumpScope:                      {operation: "ArchiverPump"},
		ArchiverArchivalWorkflowScope:          {operation: "ArchiverArchivalWorkflow"},
		TaskQueueScavengerScope:                {operation: "taskqueuescavenger"},
		ExecutionsScavengerScope:               {operation: "executionsscavenger"},
		HistoryScavengerScope:                  {operation: "historyscavenger"},
		BatcherScope:                           {operation: "batcher"},
		ParentClosePolicyProcessorScope:        {operation: "ParentClosePolicyProcessor"},
		AddSearchAttributesWorkflowScope:       {operation: "AddSearchAttributesWorkflow"},
		MigrationWorkflowScope:                 {operation: "MigrationWorkflow"},
		DeleteNamespaceWorkflowScope:           {operation: "DeleteNamespaceWorkflow"},
		ReclaimResourcesWorkflowScope:          {operation: "ReclaimResourcesWorkflow"},
		DeleteExecutionsWorkflowScope:          {operation: "DeleteExecutionsWorkflow"},
	},
	Server: {
		ServerTlsScope: {operation: "ServerTls"},
	},
	UnitTestService: {
		TestScope1: {operation: "test_scope_1_operation"},
		TestScope2: {operation: "test_scope_2_operation"},
	},
}

// Common Metrics enum
const (
	ServiceRequests = iota
	ServicePendingRequests
	ServiceFailures
	ServiceErrorWithType
	ServiceCriticalFailures
	ServiceLatency
	ServiceLatencyNoUserLatency
	ServiceLatencyUserLatency
	ServiceErrInvalidArgumentCounter
	ServiceErrNamespaceNotActiveCounter
	ServiceErrResourceExhaustedCounter
	ServiceErrNotFoundCounter
	ServiceErrExecutionAlreadyStartedCounter
	ServiceErrNamespaceAlreadyExistsCounter
	ServiceErrCancellationAlreadyRequestedCounter
	ServiceErrQueryFailedCounter
	ServiceErrContextCancelledCounter
	ServiceErrContextTimeoutCounter
	ServiceErrRetryTaskCounter
	ServiceErrBadBinaryCounter
	ServiceErrClientVersionNotSupportedCounter
	ServiceErrIncompleteHistoryCounter
	ServiceErrNonDeterministicCounter
	ServiceErrUnauthorizedCounter
	ServiceErrAuthorizeFailedCounter

	ActionCounter

	PersistenceRequests
	PersistenceFailures
	PersistenceErrorWithType
	PersistenceLatency
	PersistenceErrShardExistsCounter
	PersistenceErrShardOwnershipLostCounter
	PersistenceErrConditionFailedCounter
	PersistenceErrCurrentWorkflowConditionFailedCounter
	PersistenceErrWorkflowConditionFailedCounter
	PersistenceErrTimeoutCounter
	PersistenceErrBusyCounter
	PersistenceErrEntityNotExistsCounter
	PersistenceErrNamespaceAlreadyExistsCounter
	PersistenceErrBadRequestCounter

	ClientRequests
	ClientFailures
	ClientLatency

	ClientRedirectionRequests
	ClientRedirectionFailures
	ClientRedirectionLatency

	ServiceAuthorizationLatency

	NamespaceCachePrepareCallbacksLatency
	NamespaceCacheCallbacksLatency

	StateTransitionCount
	HistorySize
	HistoryCount
	EventBlobSize
	SearchAttributesSize

	LockRequests
	LockFailures
	LockLatency

	ArchivalConfigFailures

	VisibilityPersistenceRequests
	VisibilityPersistenceFailures
	VisibilityPersistenceLatency
	VisibilityPersistenceInvalidArgument
	VisibilityPersistenceResourceExhausted
	VisibilityPersistenceConditionFailed
	VisibilityPersistenceTimeout
	VisibilityPersistenceNotFound
	VisibilityPersistenceInternal
	VisibilityPersistenceUnavailable

	SequentialTaskSubmitRequest
	SequentialTaskSubmitRequestTaskQueueExist
	SequentialTaskSubmitRequestTaskQueueMissing
	SequentialTaskSubmitLatency
	SequentialTaskQueueSize
	SequentialTaskQueueProcessingLatency
	SequentialTaskTaskProcessingLatency

	ParallelTaskSubmitRequest
	ParallelTaskSubmitLatency
	ParallelTaskTaskProcessingLatency

	PriorityTaskSubmitRequest
	PriorityTaskSubmitLatency

	HistoryArchiverArchiveNonRetryableErrorCount
	HistoryArchiverArchiveTransientErrorCount
	HistoryArchiverArchiveSuccessCount
	HistoryArchiverHistoryMutatedCount
	HistoryArchiverTotalUploadSize
	HistoryArchiverHistorySize
	HistoryArchiverDuplicateArchivalsCount

	// The following metrics are only used by internal history archiver implemention.
	// TODO: move them to internal repo once temporal plugin model is in place.

	HistoryArchiverBlobExistsCount
	HistoryArchiverBlobSize
	HistoryArchiverRunningDeterministicConstructionCheckCount
	HistoryArchiverDeterministicConstructionCheckFailedCount
	HistoryArchiverRunningBlobIntegrityCheckCount
	HistoryArchiverBlobIntegrityCheckFailedCount

	VisibilityArchiverArchiveNonRetryableErrorCount
	VisibilityArchiverArchiveTransientErrorCount
	VisibilityArchiveSuccessCount

	MatchingClientForwardedCounter
	MatchingClientInvalidTaskQueueName

	NamespaceReplicationTaskAckLevelGauge
	NamespaceReplicationDLQAckLevelGauge
	NamespaceReplicationDLQMaxLevelGauge

	// common metrics that are emitted per task queue

	ServiceRequestsPerTaskQueue
	ServiceFailuresPerTaskQueue
	ServiceLatencyPerTaskQueue
	ServiceErrInvalidArgumentPerTaskQueueCounter
	ServiceErrNamespaceNotActivePerTaskQueueCounter
	ServiceErrResourceExhaustedPerTaskQueueCounter
	ServiceErrNotFoundPerTaskQueueCounter
	ServiceErrExecutionAlreadyStartedPerTaskQueueCounter
	ServiceErrNamespaceAlreadyExistsPerTaskQueueCounter
	ServiceErrCancellationAlreadyRequestedPerTaskQueueCounter
	ServiceErrQueryFailedPerTaskQueueCounter
	ServiceErrContextTimeoutPerTaskQueueCounter
	ServiceErrRetryTaskPerTaskQueueCounter
	ServiceErrBadBinaryPerTaskQueueCounter
	ServiceErrClientVersionNotSupportedPerTaskQueueCounter
	ServiceErrIncompleteHistoryPerTaskQueueCounter
	ServiceErrNonDeterministicPerTaskQueueCounter
	ServiceErrUnauthorizedPerTaskQueueCounter
	ServiceErrAuthorizeFailedPerTaskQueueCounter
	VersionCheckSuccessCount
	VersionCheckRequestFailedCount
	VersionCheckFailedCount
	VersionCheckLatency

	ParentClosePolicyProcessorSuccess
	ParentClosePolicyProcessorFailures

	AddSearchAttributesWorkflowSuccessCount
	AddSearchAttributesWorkflowFailuresCount

	ElasticsearchDocumentParseFailuresCount
	ElasticsearchDocumentGenerateFailuresCount

	DeleteNamespaceWorkflowSuccessCount
	DeleteNamespaceWorkflowFailuresCount

	NoopImplementationIsUsed

	NumCommonMetrics // Needs to be last on this list for iota numbering
)

// History Metrics enum
const (
	TaskRequests = iota + NumCommonMetrics
	TaskLatency
	TaskFailures
	TaskDiscarded
	TaskSkipped
	TaskAttemptTimer
	TaskStandbyRetryCounter
	TaskWorkflowBusyCounter
	TaskNotActiveCounter
	TaskLimitExceededCounter
	TaskBatchCompleteCounter
	TaskProcessingLatency
	TaskNoUserProcessingLatency
	TaskQueueLatency
	TaskUserLatency
	TaskNoUserLatency
	TaskNoUserQueueLatency
	TaskReschedulerPendingTasks
	TaskScheduleToStartLatency
	TaskThrottledCounter
	TransferTaskMissingEventCounter

	ActivityE2ELatency
	AckLevelUpdateCounter
	AckLevelUpdateFailedCounter
	CommandTypeScheduleActivityCounter
	CommandTypeCompleteWorkflowCounter
	CommandTypeFailWorkflowCounter
	CommandTypeCancelWorkflowCounter
	CommandTypeStartTimerCounter
	CommandTypeCancelActivityCounter
	CommandTypeCancelTimerCounter
	CommandTypeRecordMarkerCounter
	CommandTypeCancelExternalWorkflowCounter
	CommandTypeChildWorkflowCounter
	CommandTypeContinueAsNewCounter
	CommandTypeSignalExternalWorkflowCounter
	CommandTypeUpsertWorkflowSearchAttributesCounter
	ActivityEagerExecutionCounter
	EmptyCompletionCommandsCounter
	MultipleCompletionCommandsCounter
	FailedWorkflowTasksCounter
	WorkflowTaskAttempt
	StaleMutableStateCounter
	AutoResetPointsLimitExceededCounter
	AutoResetPointCorruptionCounter
	ConcurrencyUpdateFailureCounter
	ServiceErrTaskAlreadyStartedCounter
	ServiceErrShardOwnershipLostCounter
	HeartbeatTimeoutCounter
	ScheduleToStartTimeoutCounter
	StartToCloseTimeoutCounter
	ScheduleToCloseTimeoutCounter
	NewTimerNotifyCounter
	AcquireShardsCounter
	AcquireShardsLatency
	ShardContextClosedCounter
	ShardContextCreatedCounter
	ShardContextRemovedCounter
	ShardContextAcquisitionLatency
	ShardInfoReplicationPendingTasksTimer
	ShardInfoTransferActivePendingTasksTimer
	ShardInfoTransferStandbyPendingTasksTimer
	ShardInfoTimerActivePendingTasksTimer
	ShardInfoTimerStandbyPendingTasksTimer
	ShardInfoVisibilityPendingTasksTimer
	ShardInfoReplicationLagHistogram
	ShardInfoTransferLagHistogram
	ShardInfoTimerLagTimer
	ShardInfoVisibilityLagHistogram
	ShardInfoTransferDiffHistogram
	ShardInfoTimerDiffTimer
	ShardInfoTransferFailoverInProgressHistogram
	ShardInfoTimerFailoverInProgressHistogram
	ShardInfoTransferFailoverLatencyTimer
	ShardInfoTimerFailoverLatencyTimer
	SyncShardFromRemoteCounter
	SyncShardFromRemoteFailure
	MembershipChangedCounter
	NumShardsGauge
	GetEngineForShardErrorCounter
	GetEngineForShardLatency
	RemoveEngineForShardLatency
	CompleteWorkflowTaskWithStickyEnabledCounter
	CompleteWorkflowTaskWithStickyDisabledCounter
	WorkflowTaskHeartbeatTimeoutCounter
	HistoryEventNotificationQueueingLatency
	HistoryEventNotificationFanoutLatency
	HistoryEventNotificationInFlightMessageGauge
	HistoryEventNotificationFailDeliveryCount
	EmptyReplicationEventsCounter
	DuplicateReplicationEventsCounter
	StaleReplicationEventsCounter
	ReplicationEventsSizeTimer
	BufferReplicationTaskTimer
	UnbufferReplicationTaskTimer
	HistoryConflictsCounter
	CompleteTaskFailedCounter
	CacheRequests
	CacheFailures
	CacheLatency
	CacheMissCounter
	AcquireLockFailedCounter
	WorkflowContextCleared
	MutableStateSize
	ExecutionInfoSize
	ExecutionStateSize
	ActivityInfoSize
	TimerInfoSize
	ChildInfoSize
	RequestCancelInfoSize
	SignalInfoSize
	BufferedEventsSize
	ActivityInfoCount
	TimerInfoCount
	ChildInfoCount
	SignalInfoCount
	RequestCancelInfoCount
	BufferedEventsCount
	TaskCount
	WorkflowRetryBackoffTimerCount
	WorkflowCronBackoffTimerCount
	WorkflowCleanupDeleteCount
	WorkflowCleanupArchiveCount
	WorkflowCleanupNopCount
	WorkflowCleanupDeleteHistoryInlineCount
	WorkflowSuccessCount
	WorkflowCancelCount
	WorkflowFailedCount
	WorkflowTimeoutCount
	WorkflowTerminateCount
	WorkflowContinuedAsNewCount
	ArchiverClientSendSignalCount
	ArchiverClientSendSignalFailureCount
	ArchiverClientHistoryRequestCount
	ArchiverClientHistoryInlineArchiveAttemptCount
	ArchiverClientHistoryInlineArchiveFailureCount
	ArchiverClientVisibilityRequestCount
	ArchiverClientVisibilityInlineArchiveAttemptCount
	ArchiverClientVisibilityInlineArchiveFailureCount
	LastRetrievedMessageID
	LastProcessedMessageID
	ReplicationTasksApplied
	ReplicationTasksFailed
	ReplicationTasksLag
	ReplicationLatency
	ReplicationTasksFetched
	ReplicationTasksReturned
	ReplicationTasksAppliedLatency
	ReplicationDLQFailed
	ReplicationDLQMaxLevelGauge
	ReplicationDLQAckLevelGauge
	GetReplicationMessagesForShardLatency
	GetDLQReplicationMessagesLatency
	EventReapplySkippedCount
	DirectQueryDispatchLatency
	DirectQueryDispatchStickyLatency
	DirectQueryDispatchNonStickyLatency
	DirectQueryDispatchStickySuccessCount
	DirectQueryDispatchNonStickySuccessCount
	DirectQueryDispatchClearStickinessLatency
	DirectQueryDispatchClearStickinessSuccessCount
	DirectQueryDispatchTimeoutBeforeNonStickyCount
	WorkflowTaskQueryLatency
	ConsistentQueryTimeoutCount
	QueryBeforeFirstWorkflowTaskCount
	QueryBufferExceededCount
	QueryRegistryInvalidStateCount
	WorkerNotSupportsConsistentQueryCount
	WorkflowTaskTimeoutOverrideCount
	WorkflowRunTimeoutOverrideCount
	ReplicationTaskCleanupCount
	ReplicationTaskCleanupFailure
	MutableStateChecksumMismatch
	MutableStateChecksumInvalidated

	ElasticsearchBulkProcessorRequests
	ElasticsearchBulkProcessorQueuedRequests
	ElasticsearchBulkProcessorRetries
	ElasticsearchBulkProcessorFailures
	ElasticsearchBulkProcessorCorruptedData
	ElasticsearchBulkProcessorDuplicateRequest

	ElasticsearchBulkProcessorRequestLatency
	ElasticsearchBulkProcessorCommitLatency
	ElasticsearchBulkProcessorWaitAddLatency
	ElasticsearchBulkProcessorWaitStartLatency

	ElasticsearchBulkProcessorBulkSize

	NumHistoryMetrics
)

// Matching metrics enum
const (
	PollSuccessPerTaskQueueCounter = iota + NumHistoryMetrics
	PollTimeoutPerTaskQueueCounter
	PollSuccessWithSyncPerTaskQueueCounter
	LeaseRequestPerTaskQueueCounter
	LeaseFailurePerTaskQueueCounter
	ConditionFailedErrorPerTaskQueueCounter
	RespondQueryTaskFailedPerTaskQueueCounter
	SyncThrottlePerTaskQueueCounter
	BufferThrottlePerTaskQueueCounter
	SyncMatchLatencyPerTaskQueue
	AsyncMatchLatencyPerTaskQueue
	ExpiredTasksPerTaskQueueCounter
	ForwardedPerTaskQueueCounter
	ForwardTaskCallsPerTaskQueue // Deprecated todo not used
	ForwardTaskErrorsPerTaskQueue
	ForwardTaskLatencyPerTaskQueue
	ForwardQueryCallsPerTaskQueue  // Deprecated todo not used
	ForwardQueryErrorsPerTaskQueue // Deprecated todo not used
	ForwardQueryLatencyPerTaskQueue
	ForwardPollCallsPerTaskQueue  // Deprecated todo not used
	ForwardPollErrorsPerTaskQueue // Deprecated todo not used
	ForwardPollLatencyPerTaskQueue
	LocalToLocalMatchPerTaskQueueCounter
	LocalToRemoteMatchPerTaskQueueCounter
	RemoteToLocalMatchPerTaskQueueCounter
	RemoteToRemoteMatchPerTaskQueueCounter
	LoadedTaskQueueGauge
	TaskQueueStartedCounter
	TaskQueueStoppedCounter
	TaskWriteThrottlePerTaskQueueCounter
	TaskWriteLatencyPerTaskQueue
	TaskLagPerTaskQueueGauge

	NumMatchingMetrics
)

// Worker metrics enum
const (
	ReplicatorMessages = iota + NumMatchingMetrics
	ReplicatorFailures
	ReplicatorLatency
	ReplicatorDLQFailures
	ArchiverNonRetryableErrorCount
	ArchiverStartedCount
	ArchiverStoppedCount
	ArchiverCoroutineStartedCount
	ArchiverCoroutineStoppedCount
	ArchiverHandleHistoryRequestLatency
	ArchiverHandleVisibilityRequestLatency
	ArchiverUploadWithRetriesLatency
	ArchiverDeleteWithRetriesLatency
	ArchiverUploadFailedAllRetriesCount
	ArchiverUploadSuccessCount
	ArchiverDeleteFailedAllRetriesCount
	ArchiverDeleteSuccessCount
	ArchiverHandleVisibilityFailedAllRetiresCount
	ArchiverHandleVisibilitySuccessCount
	ArchiverBacklogSizeGauge
	ArchiverPumpTimeoutCount
	ArchiverPumpSignalThresholdCount
	ArchiverPumpTimeoutWithoutSignalsCount
	ArchiverPumpSignalChannelClosedCount
	ArchiverWorkflowStartedCount
	ArchiverNumPumpedRequestsCount
	ArchiverNumHandledRequestsCount
	ArchiverPumpedNotEqualHandledCount
	ArchiverHandleAllRequestsLatency
	ArchiverWorkflowStoppingCount
	TaskProcessedCount
	TaskDeletedCount
	TaskQueueProcessedCount
	TaskQueueDeletedCount
	TaskQueueOutstandingCount
	ExecutionsOutstandingCount
	StartedCount
	StoppedCount
	ScanDuration
	ExecutorTasksDoneCount
	ExecutorTasksErrCount
	ExecutorTasksDeferredCount
	ExecutorTasksDroppedCount
	BatcherProcessorSuccess
	BatcherProcessorFailures
	HistoryScavengerSuccessCount
	HistoryScavengerErrorCount
	HistoryScavengerSkipCount
	NamespaceReplicationEnqueueDLQCount
	ScavengerValidationRequestsCount
	ScavengerValidationFailuresCount
	AddSearchAttributesFailuresCount
	CatchUpReadyShardCountGauge
	HandoverReadyShardCountGauge

	DeleteNamespaceSuccessCount
	RenameNamespaceSuccessCount
	DeleteExecutionsSuccessCount
	DeleteNamespaceFailuresCount
	UpdateNamespaceFailuresCount
	RenameNamespaceFailuresCount
	ReadNamespaceFailuresCount
	ListExecutionsFailuresCount
	CountExecutionsFailuresCount
	DeleteExecutionFailuresCount
	DeleteExecutionNotFoundCount
	RateLimiterFailuresCount

	NumWorkerMetrics
)

// Server metrics enum
const (
	TlsCertsExpired = iota + NumWorkerMetrics
	TlsCertsExpiring

	NumServerMetrics
)

// UnitTestService metrics enum
const (
	TestCounterMetric1 = iota + NumServerMetrics
	TestCounterMetric2
	TestCounterRollupMetric1
	TestTimerMetric1
	TestTimerMetric2
	TestGaugeMetric1
	TestGaugeMetric2
	TestBytesHistogramMetric1
	TestBytesHistogramMetric2
	TestDimensionlessHistogramMetric1
	TestDimensionlessHistogramMetric2

	NumUnitTestServiceMetrics
)

// MetricDefs record the metrics for all services
var MetricDefs = map[ServiceIdx]map[int]metricDefinition{
	Common: {
		ServiceRequests:                                     NewCounterDef("service_requests"),
		ServicePendingRequests:                              NewGaugeDef("service_pending_requests"),
		ServiceFailures:                                     NewCounterDef("service_errors"),
		ServiceErrorWithType:                                NewCounterDef("service_error_with_type"),
		ServiceCriticalFailures:                             NewCounterDef("service_errors_critical"),
		ServiceLatency:                                      NewTimerDef("service_latency"),
		ServiceLatencyNoUserLatency:                         NewTimerDef("service_latency_nouserlatency"),
		ServiceLatencyUserLatency:                           NewTimerDef("service_latency_userlatency"),
		ServiceErrInvalidArgumentCounter:                    NewCounterDef("service_errors_invalid_argument"),
		ServiceErrNamespaceNotActiveCounter:                 NewCounterDef("service_errors_namespace_not_active"),
		ServiceErrResourceExhaustedCounter:                  NewCounterDef("service_errors_resource_exhausted"),
		ServiceErrNotFoundCounter:                           NewCounterDef("service_errors_entity_not_found"),
		ServiceErrExecutionAlreadyStartedCounter:            NewCounterDef("service_errors_execution_already_started"),
		ServiceErrNamespaceAlreadyExistsCounter:             NewCounterDef("service_errors_namespace_already_exists"),
		ServiceErrCancellationAlreadyRequestedCounter:       NewCounterDef("service_errors_cancellation_already_requested"),
		ServiceErrQueryFailedCounter:                        NewCounterDef("service_errors_query_failed"),
		ServiceErrContextCancelledCounter:                   NewCounterDef("service_errors_context_cancelled"),
		ServiceErrContextTimeoutCounter:                     NewCounterDef("service_errors_context_timeout"),
		ServiceErrRetryTaskCounter:                          NewCounterDef("service_errors_retry_task"),
		ServiceErrBadBinaryCounter:                          NewCounterDef("service_errors_bad_binary"),
		ServiceErrClientVersionNotSupportedCounter:          NewCounterDef("service_errors_client_version_not_supported"),
		ServiceErrIncompleteHistoryCounter:                  NewCounterDef("service_errors_incomplete_history"),
		ServiceErrNonDeterministicCounter:                   NewCounterDef("service_errors_nondeterministic"),
		ServiceErrUnauthorizedCounter:                       NewCounterDef("service_errors_unauthorized"),
		ServiceErrAuthorizeFailedCounter:                    NewCounterDef("service_errors_authorize_failed"),
		ActionCounter:                                       NewCounterDef("action"),
		PersistenceRequests:                                 NewCounterDef("persistence_requests"),
		PersistenceFailures:                                 NewCounterDef("persistence_errors"),
		PersistenceErrorWithType:                            NewCounterDef("persistence_error_with_type"),
		PersistenceLatency:                                  NewTimerDef("persistence_latency"),
		PersistenceErrShardExistsCounter:                    NewCounterDef("persistence_errors_shard_exists"),
		PersistenceErrShardOwnershipLostCounter:             NewCounterDef("persistence_errors_shard_ownership_lost"),
		PersistenceErrConditionFailedCounter:                NewCounterDef("persistence_errors_condition_failed"),
		PersistenceErrCurrentWorkflowConditionFailedCounter: NewCounterDef("persistence_errors_current_workflow_condition_failed"),
		PersistenceErrWorkflowConditionFailedCounter:        NewCounterDef("persistence_errors_workflow_condition_failed"),
		PersistenceErrTimeoutCounter:                        NewCounterDef("persistence_errors_timeout"),
		PersistenceErrBusyCounter:                           NewCounterDef("persistence_errors_busy"),
		PersistenceErrEntityNotExistsCounter:                NewCounterDef("persistence_errors_entity_not_exists"),
		PersistenceErrNamespaceAlreadyExistsCounter:         NewCounterDef("persistence_errors_namespace_already_exists"),
		PersistenceErrBadRequestCounter:                     NewCounterDef("persistence_errors_bad_request"),
		ClientRequests:                                      NewCounterDef("client_requests"),
		ClientFailures:                                      NewCounterDef("client_errors"),
		ClientLatency:                                       NewTimerDef("client_latency"),
		ClientRedirectionRequests:                           NewCounterDef("client_redirection_requests"),
		ClientRedirectionFailures:                           NewCounterDef("client_redirection_errors"),
		ClientRedirectionLatency:                            NewTimerDef("client_redirection_latency"),
		ServiceAuthorizationLatency:                         NewTimerDef("service_authorization_latency"),
		NamespaceCachePrepareCallbacksLatency:               NewTimerDef("namespace_cache_prepare_callbacks_latency"),
		NamespaceCacheCallbacksLatency:                      NewTimerDef("namespace_cache_callbacks_latency"),
		StateTransitionCount:                                NewDimensionlessHistogramDef("state_transition_count"),
		HistorySize:                                         NewBytesHistogramDef("history_size"),
		HistoryCount:                                        NewDimensionlessHistogramDef("history_count"),
		EventBlobSize:                                       NewBytesHistogramDef("event_blob_size"),
		SearchAttributesSize:                                NewBytesHistogramDef("search_attributes_size"),
		LockRequests:                                        NewCounterDef("lock_requests"),
		LockFailures:                                        NewCounterDef("lock_failures"),
		LockLatency:                                         NewTimerDef("lock_latency"),
		ArchivalConfigFailures:                              NewCounterDef("archivalconfig_failures"),

		VisibilityPersistenceRequests:          NewCounterDef("visibility_persistence_requests"),
		VisibilityPersistenceFailures:          NewCounterDef("visibility_persistence_errors"),
		VisibilityPersistenceLatency:           NewTimerDef("visibility_persistence_latency"),
		VisibilityPersistenceInvalidArgument:   NewCounterDef("visibility_persistence_invalid_argument"),
		VisibilityPersistenceResourceExhausted: NewCounterDef("visibility_persistence_resource_exhausted"),
		VisibilityPersistenceConditionFailed:   NewCounterDef("visibility_persistence_condition_failed"),
		VisibilityPersistenceTimeout:           NewCounterDef("visibility_persistence_timeout"),
		VisibilityPersistenceNotFound:          NewCounterDef("visibility_persistence_not_found"),
		VisibilityPersistenceInternal:          NewCounterDef("visibility_persistence_internal"),
		VisibilityPersistenceUnavailable:       NewCounterDef("visibility_persistence_unavailable"),

		SequentialTaskSubmitRequest:                 NewCounterDef("sequentialtask_submit_request"),
		SequentialTaskSubmitRequestTaskQueueExist:   NewCounterDef("sequentialtask_submit_request_taskqueue_exist"),
		SequentialTaskSubmitRequestTaskQueueMissing: NewCounterDef("sequentialtask_submit_request_taskqueue_missing"),
		SequentialTaskSubmitLatency:                 NewTimerDef("sequentialtask_submit_latency"),
		SequentialTaskQueueSize:                     NewBytesHistogramDef("sequentialtask_queue_size"),
		SequentialTaskQueueProcessingLatency:        NewTimerDef("sequentialtask_queue_processing_latency"),
		SequentialTaskTaskProcessingLatency:         NewTimerDef("sequentialtask_task_processing_latency"),
		ParallelTaskSubmitRequest:                   NewCounterDef("paralleltask_submit_request"),
		ParallelTaskSubmitLatency:                   NewTimerDef("paralleltask_submit_latency"),
		ParallelTaskTaskProcessingLatency:           NewTimerDef("paralleltask_task_processing_latency"),
		PriorityTaskSubmitRequest:                   NewCounterDef("prioritytask_submit_request"),
		PriorityTaskSubmitLatency:                   NewTimerDef("prioritytask_submit_latency"),

		HistoryArchiverArchiveNonRetryableErrorCount:              NewCounterDef("history_archiver_archive_non_retryable_error"),
		HistoryArchiverArchiveTransientErrorCount:                 NewCounterDef("history_archiver_archive_transient_error"),
		HistoryArchiverArchiveSuccessCount:                        NewCounterDef("history_archiver_archive_success"),
		HistoryArchiverHistoryMutatedCount:                        NewCounterDef("history_archiver_history_mutated"),
		HistoryArchiverTotalUploadSize:                            NewBytesHistogramDef("history_archiver_total_upload_size"),
		HistoryArchiverHistorySize:                                NewBytesHistogramDef("history_archiver_history_size"),
		HistoryArchiverDuplicateArchivalsCount:                    NewCounterDef("history_archiver_duplicate_archivals"),
		HistoryArchiverBlobExistsCount:                            NewCounterDef("history_archiver_blob_exists"),
		HistoryArchiverBlobSize:                                   NewBytesHistogramDef("history_archiver_blob_size"),
		HistoryArchiverRunningDeterministicConstructionCheckCount: NewCounterDef("history_archiver_running_deterministic_construction_check"),
		HistoryArchiverDeterministicConstructionCheckFailedCount:  NewCounterDef("history_archiver_deterministic_construction_check_failed"),
		HistoryArchiverRunningBlobIntegrityCheckCount:             NewCounterDef("history_archiver_running_blob_integrity_check"),
		HistoryArchiverBlobIntegrityCheckFailedCount:              NewCounterDef("history_archiver_blob_integrity_check_failed"),
		VisibilityArchiverArchiveNonRetryableErrorCount:           NewCounterDef("visibility_archiver_archive_non_retryable_error"),
		VisibilityArchiverArchiveTransientErrorCount:              NewCounterDef("visibility_archiver_archive_transient_error"),
		VisibilityArchiveSuccessCount:                             NewCounterDef("visibility_archiver_archive_success"),
		VersionCheckSuccessCount:                                  NewCounterDef("version_check_success"),
		VersionCheckFailedCount:                                   NewCounterDef("version_check_failed"),
		VersionCheckRequestFailedCount:                            NewCounterDef("version_check_request_failed"),
		VersionCheckLatency:                                       NewTimerDef("version_check_latency"),

		ParentClosePolicyProcessorSuccess:  NewCounterDef("parent_close_policy_processor_requests"),
		ParentClosePolicyProcessorFailures: NewCounterDef("parent_close_policy_processor_errors"),

		AddSearchAttributesWorkflowSuccessCount:  NewCounterDef("add_search_attributes_workflow_success"),
		AddSearchAttributesWorkflowFailuresCount: NewCounterDef("add_search_attributes_workflow_failure"),

		DeleteNamespaceWorkflowSuccessCount:  NewCounterDef("delete_namespace_workflow_success"),
		DeleteNamespaceWorkflowFailuresCount: NewCounterDef("delete_namespace_workflow_failure"),

		MatchingClientForwardedCounter:     NewCounterDef("forwarded"),
		MatchingClientInvalidTaskQueueName: NewCounterDef("invalid_task_queue_name"),

		NamespaceReplicationTaskAckLevelGauge: NewGaugeDef("namespace_replication_task_ack_level"),
		NamespaceReplicationDLQAckLevelGauge:  NewGaugeDef("namespace_dlq_ack_level"),
		NamespaceReplicationDLQMaxLevelGauge:  NewGaugeDef("namespace_dlq_max_level"),

		// per task queue common metrics

		ServiceRequestsPerTaskQueue:                               NewRollupCounterDef("service_requests_per_tl", "service_requests"),
		ServiceFailuresPerTaskQueue:                               NewRollupCounterDef("service_errors_per_tl", "service_errors"),
		ServiceLatencyPerTaskQueue:                                NewRollupTimerDef("service_latency_per_tl", "service_latency"),
		ServiceErrInvalidArgumentPerTaskQueueCounter:              NewRollupCounterDef("service_errors_invalid_argument_per_tl", "service_errors_invalid_argument"),
		ServiceErrNamespaceNotActivePerTaskQueueCounter:           NewRollupCounterDef("service_errors_namespace_not_active_per_tl", "service_errors_namespace_not_active"),
		ServiceErrResourceExhaustedPerTaskQueueCounter:            NewRollupCounterDef("service_errors_resource_exhausted_per_tl", "service_errors_resource_exhausted"),
		ServiceErrNotFoundPerTaskQueueCounter:                     NewRollupCounterDef("service_errors_entity_not_found_per_tl", "service_errors_entity_not_found"),
		ServiceErrExecutionAlreadyStartedPerTaskQueueCounter:      NewRollupCounterDef("service_errors_execution_already_started_per_tl", "service_errors_execution_already_started"),
		ServiceErrNamespaceAlreadyExistsPerTaskQueueCounter:       NewRollupCounterDef("service_errors_namespace_already_exists_per_tl", "service_errors_namespace_already_exists"),
		ServiceErrCancellationAlreadyRequestedPerTaskQueueCounter: NewRollupCounterDef("service_errors_cancellation_already_requested_per_tl", "service_errors_cancellation_already_requested"),
		ServiceErrQueryFailedPerTaskQueueCounter:                  NewRollupCounterDef("service_errors_query_failed_per_tl", "service_errors_query_failed"),
		ServiceErrContextTimeoutPerTaskQueueCounter:               NewRollupCounterDef("service_errors_context_timeout_per_tl", "service_errors_context_timeout"),
		ServiceErrRetryTaskPerTaskQueueCounter:                    NewRollupCounterDef("service_errors_retry_task_per_tl", "service_errors_retry_task"),
		ServiceErrBadBinaryPerTaskQueueCounter:                    NewRollupCounterDef("service_errors_bad_binary_per_tl", "service_errors_bad_binary"),
		ServiceErrClientVersionNotSupportedPerTaskQueueCounter:    NewRollupCounterDef("service_errors_client_version_not_supported_per_tl", "service_errors_client_version_not_supported"),
		ServiceErrIncompleteHistoryPerTaskQueueCounter:            NewRollupCounterDef("service_errors_incomplete_history_per_tl", "service_errors_incomplete_history"),
		ServiceErrNonDeterministicPerTaskQueueCounter:             NewRollupCounterDef("service_errors_nondeterministic_per_tl", "service_errors_nondeterministic"),
		ServiceErrUnauthorizedPerTaskQueueCounter:                 NewRollupCounterDef("service_errors_unauthorized_per_tl", "service_errors_unauthorized"),
		ServiceErrAuthorizeFailedPerTaskQueueCounter:              NewRollupCounterDef("service_errors_authorize_failed_per_tl", "service_errors_authorize_failed"),
		ElasticsearchDocumentParseFailuresCount:                   NewCounterDef("elasticsearch_document_parse_failures_counter"),
		ElasticsearchDocumentGenerateFailuresCount:                NewCounterDef("elasticsearch_document_generate_failures_counter"),

		NoopImplementationIsUsed: NewCounterDef("noop_implementation_is_used"),
	},
	History: {
		TaskRequests: NewCounterDef("task_requests"),

		TaskLatency:       NewTimerDef("task_latency"),               // overall/all attempts within single worker
		TaskUserLatency:   NewTimerDef("task_latency_userlatency"),   // from task generated to task complete
		TaskNoUserLatency: NewTimerDef("task_latency_nouserlatency"), // from task generated to task complete

		TaskAttemptTimer:         NewDimensionlessHistogramDef("task_attempt"),
		TaskFailures:             NewCounterDef("task_errors"),
		TaskDiscarded:            NewCounterDef("task_errors_discarded"),
		TaskSkipped:              NewCounterDef("task_skipped"),
		TaskStandbyRetryCounter:  NewCounterDef("task_errors_standby_retry_counter"),
		TaskWorkflowBusyCounter:  NewCounterDef("task_errors_workflow_busy"),
		TaskNotActiveCounter:     NewCounterDef("task_errors_not_active_counter"),
		TaskLimitExceededCounter: NewCounterDef("task_errors_limit_exceeded_counter"),

		TaskScheduleToStartLatency: NewTimerDef("task_schedule_to_start_latency"),

		TaskProcessingLatency:       NewTimerDef("task_latency_processing"),               // per-attempt
		TaskNoUserProcessingLatency: NewTimerDef("task_latency_processing_nouserlatency"), // per-attempt

		TaskQueueLatency:       NewTimerDef("task_latency_queue"),               // from task generated to task complete
		TaskNoUserQueueLatency: NewTimerDef("task_latency_queue_nouserlatency"), // from task generated to task complete

		TransferTaskMissingEventCounter:                   NewCounterDef("transfer_task_missing_event_counter"),
		TaskBatchCompleteCounter:                          NewCounterDef("task_batch_complete_counter"),
		TaskReschedulerPendingTasks:                       NewDimensionlessHistogramDef("task_rescheduler_pending_tasks"),
		TaskThrottledCounter:                              NewCounterDef("task_throttled_counter"),
		ActivityE2ELatency:                                NewTimerDef("activity_end_to_end_latency"),
		AckLevelUpdateCounter:                             NewCounterDef("ack_level_update"),
		AckLevelUpdateFailedCounter:                       NewCounterDef("ack_level_update_failed"),
		CommandTypeScheduleActivityCounter:                NewCounterDef("schedule_activity_command"),
		CommandTypeCompleteWorkflowCounter:                NewCounterDef("complete_workflow_command"),
		CommandTypeFailWorkflowCounter:                    NewCounterDef("fail_workflow_command"),
		CommandTypeCancelWorkflowCounter:                  NewCounterDef("cancel_workflow_command"),
		CommandTypeStartTimerCounter:                      NewCounterDef("start_timer_command"),
		CommandTypeCancelActivityCounter:                  NewCounterDef("cancel_activity_command"),
		CommandTypeCancelTimerCounter:                     NewCounterDef("cancel_timer_command"),
		CommandTypeRecordMarkerCounter:                    NewCounterDef("record_marker_command"),
		CommandTypeCancelExternalWorkflowCounter:          NewCounterDef("cancel_external_workflow_command"),
		CommandTypeContinueAsNewCounter:                   NewCounterDef("continue_as_new_command"),
		CommandTypeSignalExternalWorkflowCounter:          NewCounterDef("signal_external_workflow_command"),
		CommandTypeUpsertWorkflowSearchAttributesCounter:  NewCounterDef("upsert_workflow_search_attributes_command"),
		CommandTypeChildWorkflowCounter:                   NewCounterDef("child_workflow_command"),
		ActivityEagerExecutionCounter:                     NewCounterDef("activity_eager_execution"),
		EmptyCompletionCommandsCounter:                    NewCounterDef("empty_completion_commands"),
		MultipleCompletionCommandsCounter:                 NewCounterDef("multiple_completion_commands"),
		FailedWorkflowTasksCounter:                        NewCounterDef("failed_workflow_tasks"),
		WorkflowTaskAttempt:                               NewDimensionlessHistogramDef("workflow_task_attempt"),
		StaleMutableStateCounter:                          NewCounterDef("stale_mutable_state"),
		AutoResetPointsLimitExceededCounter:               NewCounterDef("auto_reset_points_exceed_limit"),
		AutoResetPointCorruptionCounter:                   NewCounterDef("auto_reset_point_corruption"),
		ConcurrencyUpdateFailureCounter:                   NewCounterDef("concurrency_update_failure"),
		ServiceErrShardOwnershipLostCounter:               NewCounterDef("service_errors_shard_ownership_lost"),
		ServiceErrTaskAlreadyStartedCounter:               NewCounterDef("service_errors_task_already_started"),
		HeartbeatTimeoutCounter:                           NewCounterDef("heartbeat_timeout"),
		ScheduleToStartTimeoutCounter:                     NewCounterDef("schedule_to_start_timeout"),
		StartToCloseTimeoutCounter:                        NewCounterDef("start_to_close_timeout"),
		ScheduleToCloseTimeoutCounter:                     NewCounterDef("schedule_to_close_timeout"),
		NewTimerNotifyCounter:                             NewCounterDef("new_timer_notifications"),
		AcquireShardsCounter:                              NewCounterDef("acquire_shards_count"),
		AcquireShardsLatency:                              NewTimerDef("acquire_shards_latency"),
		ShardContextClosedCounter:                         NewCounterDef("shard_closed_count"),
		ShardContextCreatedCounter:                        NewCounterDef("sharditem_created_count"),
		ShardContextRemovedCounter:                        NewCounterDef("sharditem_removed_count"),
		ShardContextAcquisitionLatency:                    NewTimerDef("sharditem_acquisition_latency"),
		ShardInfoReplicationPendingTasksTimer:             NewDimensionlessHistogramDef("shardinfo_replication_pending_task"),
		ShardInfoTransferActivePendingTasksTimer:          NewDimensionlessHistogramDef("shardinfo_transfer_active_pending_task"),
		ShardInfoTransferStandbyPendingTasksTimer:         NewDimensionlessHistogramDef("shardinfo_transfer_standby_pending_task"),
		ShardInfoTimerActivePendingTasksTimer:             NewDimensionlessHistogramDef("shardinfo_timer_active_pending_task"),
		ShardInfoTimerStandbyPendingTasksTimer:            NewDimensionlessHistogramDef("shardinfo_timer_standby_pending_task"),
		ShardInfoVisibilityPendingTasksTimer:              NewDimensionlessHistogramDef("shardinfo_visibility_pending_task"),
		ShardInfoReplicationLagHistogram:                  NewDimensionlessHistogramDef("shardinfo_replication_lag"),
		ShardInfoTransferLagHistogram:                     NewDimensionlessHistogramDef("shardinfo_transfer_lag"),
		ShardInfoTimerLagTimer:                            NewTimerDef("shardinfo_timer_lag"),
		ShardInfoVisibilityLagHistogram:                   NewDimensionlessHistogramDef("shardinfo_visibility_lag"),
		ShardInfoTransferDiffHistogram:                    NewDimensionlessHistogramDef("shardinfo_transfer_diff"),
		ShardInfoTimerDiffTimer:                           NewTimerDef("shardinfo_timer_diff"),
		ShardInfoTransferFailoverInProgressHistogram:      NewDimensionlessHistogramDef("shardinfo_transfer_failover_in_progress"),
		ShardInfoTimerFailoverInProgressHistogram:         NewDimensionlessHistogramDef("shardinfo_timer_failover_in_progress"),
		ShardInfoTransferFailoverLatencyTimer:             NewTimerDef("shardinfo_transfer_failover_latency"),
		ShardInfoTimerFailoverLatencyTimer:                NewTimerDef("shardinfo_timer_failover_latency"),
		SyncShardFromRemoteCounter:                        NewCounterDef("syncshard_remote_count"),
		SyncShardFromRemoteFailure:                        NewCounterDef("syncshard_remote_failed"),
		MembershipChangedCounter:                          NewCounterDef("membership_changed_count"),
		NumShardsGauge:                                    NewGaugeDef("numshards_gauge"),
		GetEngineForShardErrorCounter:                     NewCounterDef("get_engine_for_shard_errors"),
		GetEngineForShardLatency:                          NewTimerDef("get_engine_for_shard_latency"),
		RemoveEngineForShardLatency:                       NewTimerDef("remove_engine_for_shard_latency"),
		CompleteWorkflowTaskWithStickyEnabledCounter:      NewCounterDef("complete_workflow_task_sticky_enabled_count"),
		CompleteWorkflowTaskWithStickyDisabledCounter:     NewCounterDef("complete_workflow_task_sticky_disabled_count"),
		WorkflowTaskHeartbeatTimeoutCounter:               NewCounterDef("workflow_task_heartbeat_timeout_count"),
		HistoryEventNotificationQueueingLatency:           NewTimerDef("history_event_notification_queueing_latency"),
		HistoryEventNotificationFanoutLatency:             NewTimerDef("history_event_notification_fanout_latency"),
		HistoryEventNotificationInFlightMessageGauge:      NewGaugeDef("history_event_notification_inflight_message_gauge"),
		HistoryEventNotificationFailDeliveryCount:         NewCounterDef("history_event_notification_fail_delivery_count"),
		EmptyReplicationEventsCounter:                     NewCounterDef("empty_replication_events"),
		DuplicateReplicationEventsCounter:                 NewCounterDef("duplicate_replication_events"),
		StaleReplicationEventsCounter:                     NewCounterDef("stale_replication_events"),
		ReplicationEventsSizeTimer:                        NewTimerDef("replication_events_size"),
		BufferReplicationTaskTimer:                        NewTimerDef("buffer_replication_tasks"),
		UnbufferReplicationTaskTimer:                      NewTimerDef("unbuffer_replication_tasks"),
		HistoryConflictsCounter:                           NewCounterDef("history_conflicts"),
		CompleteTaskFailedCounter:                         NewCounterDef("complete_task_fail_count"),
		CacheRequests:                                     NewCounterDef("cache_requests"),
		CacheFailures:                                     NewCounterDef("cache_errors"),
		CacheLatency:                                      NewTimerDef("cache_latency"),
		CacheMissCounter:                                  NewCounterDef("cache_miss"),
		AcquireLockFailedCounter:                          NewCounterDef("acquire_lock_failed"),
		WorkflowContextCleared:                            NewCounterDef("workflow_context_cleared"),
		MutableStateSize:                                  NewBytesHistogramDef("mutable_state_size"),
		ExecutionInfoSize:                                 NewBytesHistogramDef("execution_info_size"),
		ExecutionStateSize:                                NewBytesHistogramDef("execution_state_size"),
		ActivityInfoSize:                                  NewBytesHistogramDef("activity_info_size"),
		TimerInfoSize:                                     NewBytesHistogramDef("timer_info_size"),
		ChildInfoSize:                                     NewBytesHistogramDef("child_info_size"),
		RequestCancelInfoSize:                             NewBytesHistogramDef("request_cancel_info_size"),
		SignalInfoSize:                                    NewBytesHistogramDef("signal_info_size"),
		BufferedEventsSize:                                NewBytesHistogramDef("buffered_events_size"),
		ActivityInfoCount:                                 NewDimensionlessHistogramDef("activity_info_count"),
		TimerInfoCount:                                    NewDimensionlessHistogramDef("timer_info_count"),
		ChildInfoCount:                                    NewDimensionlessHistogramDef("child_info_count"),
		SignalInfoCount:                                   NewDimensionlessHistogramDef("signal_info_count"),
		RequestCancelInfoCount:                            NewDimensionlessHistogramDef("request_cancel_info_count"),
		BufferedEventsCount:                               NewDimensionlessHistogramDef("buffered_events_count"),
		TaskCount:                                         NewDimensionlessHistogramDef("task_count"),
		WorkflowRetryBackoffTimerCount:                    NewCounterDef("workflow_retry_backoff_timer"),
		WorkflowCronBackoffTimerCount:                     NewCounterDef("workflow_cron_backoff_timer"),
		WorkflowCleanupDeleteCount:                        NewCounterDef("workflow_cleanup_delete"),
		WorkflowCleanupArchiveCount:                       NewCounterDef("workflow_cleanup_archive"),
		WorkflowCleanupNopCount:                           NewCounterDef("workflow_cleanup_nop"),
		WorkflowCleanupDeleteHistoryInlineCount:           NewCounterDef("workflow_cleanup_delete_history_inline"),
		WorkflowSuccessCount:                              NewCounterDef("workflow_success"),
		WorkflowCancelCount:                               NewCounterDef("workflow_cancel"),
		WorkflowFailedCount:                               NewCounterDef("workflow_failed"),
		WorkflowTimeoutCount:                              NewCounterDef("workflow_timeout"),
		WorkflowTerminateCount:                            NewCounterDef("workflow_terminate"),
		WorkflowContinuedAsNewCount:                       NewCounterDef("workflow_continued_as_new"),
		ArchiverClientSendSignalCount:                     NewCounterDef("archiver_client_sent_signal"),
		ArchiverClientSendSignalFailureCount:              NewCounterDef("archiver_client_send_signal_error"),
		ArchiverClientHistoryRequestCount:                 NewCounterDef("archiver_client_history_request"),
		ArchiverClientHistoryInlineArchiveAttemptCount:    NewCounterDef("archiver_client_history_inline_archive_attempt"),
		ArchiverClientHistoryInlineArchiveFailureCount:    NewCounterDef("archiver_client_history_inline_archive_failure"),
		ArchiverClientVisibilityRequestCount:              NewCounterDef("archiver_client_visibility_request"),
		ArchiverClientVisibilityInlineArchiveAttemptCount: NewCounterDef("archiver_client_visibility_inline_archive_attempt"),
		ArchiverClientVisibilityInlineArchiveFailureCount: NewCounterDef("archiver_client_visibility_inline_archive_failure"),
		LastRetrievedMessageID:                            NewGaugeDef("last_retrieved_message_id"),
		LastProcessedMessageID:                            NewGaugeDef("last_processed_message_id"),
		ReplicationTasksApplied:                           NewCounterDef("replication_tasks_applied"),
		ReplicationTasksFailed:                            NewCounterDef("replication_tasks_failed"),
		ReplicationTasksLag:                               NewTimerDef("replication_tasks_lag"),
		ReplicationLatency:                                NewTimerDef("replication_latency"),
		ReplicationTasksFetched:                           NewTimerDef("replication_tasks_fetched"),
		ReplicationTasksReturned:                          NewTimerDef("replication_tasks_returned"),
		ReplicationTasksAppliedLatency:                    NewTimerDef("replication_tasks_applied_latency"),
		ReplicationDLQFailed:                              NewCounterDef("replication_dlq_enqueue_failed"),
		ReplicationDLQMaxLevelGauge:                       NewGaugeDef("replication_dlq_max_level"),
		ReplicationDLQAckLevelGauge:                       NewGaugeDef("replication_dlq_ack_level"),
		GetReplicationMessagesForShardLatency:             NewTimerDef("get_replication_messages_for_shard"),
		GetDLQReplicationMessagesLatency:                  NewTimerDef("get_dlq_replication_messages"),
		EventReapplySkippedCount:                          NewCounterDef("event_reapply_skipped_count"),
		DirectQueryDispatchLatency:                        NewTimerDef("direct_query_dispatch_latency"),
		DirectQueryDispatchStickyLatency:                  NewTimerDef("direct_query_dispatch_sticky_latency"),
		DirectQueryDispatchNonStickyLatency:               NewTimerDef("direct_query_dispatch_non_sticky_latency"),
		DirectQueryDispatchStickySuccessCount:             NewCounterDef("direct_query_dispatch_sticky_success"),
		DirectQueryDispatchNonStickySuccessCount:          NewCounterDef("direct_query_dispatch_non_sticky_success"),
		DirectQueryDispatchClearStickinessLatency:         NewTimerDef("direct_query_dispatch_clear_stickiness_latency"),
		DirectQueryDispatchClearStickinessSuccessCount:    NewCounterDef("direct_query_dispatch_clear_stickiness_success"),
		DirectQueryDispatchTimeoutBeforeNonStickyCount:    NewCounterDef("direct_query_dispatch_timeout_before_non_sticky"),
		WorkflowTaskQueryLatency:                          NewTimerDef("workflow_task_query_latency"),
		ConsistentQueryTimeoutCount:                       NewCounterDef("consistent_query_timeout"),
		QueryBeforeFirstWorkflowTaskCount:                 NewCounterDef("query_before_first_workflow_task"),
		QueryBufferExceededCount:                          NewCounterDef("query_buffer_exceeded"),
		QueryRegistryInvalidStateCount:                    NewCounterDef("query_registry_invalid_state"),
		WorkerNotSupportsConsistentQueryCount:             NewCounterDef("worker_not_supports_consistent_query"),
		WorkflowTaskTimeoutOverrideCount:                  NewCounterDef("workflow_task_timeout_overrides"),
		WorkflowRunTimeoutOverrideCount:                   NewCounterDef("workflow_run_timeout_overrides"),
		ReplicationTaskCleanupCount:                       NewCounterDef("replication_task_cleanup_count"),
		ReplicationTaskCleanupFailure:                     NewCounterDef("replication_task_cleanup_failed"),
		MutableStateChecksumMismatch:                      NewCounterDef("mutable_state_checksum_mismatch"),
		MutableStateChecksumInvalidated:                   NewCounterDef("mutable_state_checksum_invalidated"),

		ElasticsearchBulkProcessorRequests:         NewCounterDef("elasticsearch_bulk_processor_requests"),
		ElasticsearchBulkProcessorQueuedRequests:   NewDimensionlessHistogramDef("elasticsearch_bulk_processor_queued_requests"),
		ElasticsearchBulkProcessorRetries:          NewCounterDef("elasticsearch_bulk_processor_retries"),
		ElasticsearchBulkProcessorFailures:         NewCounterDef("elasticsearch_bulk_processor_errors"),
		ElasticsearchBulkProcessorCorruptedData:    NewCounterDef("elasticsearch_bulk_processor_corrupted_data"),
		ElasticsearchBulkProcessorDuplicateRequest: NewCounterDef("elasticsearch_bulk_processor_duplicate_request"),
		ElasticsearchBulkProcessorRequestLatency:   NewTimerDef("elasticsearch_bulk_processor_request_latency"),
		ElasticsearchBulkProcessorCommitLatency:    NewTimerDef("elasticsearch_bulk_processor_commit_latency"),
		ElasticsearchBulkProcessorWaitAddLatency:   NewTimerDef("elasticsearch_bulk_processor_wait_add_latency"),
		ElasticsearchBulkProcessorWaitStartLatency: NewTimerDef("elasticsearch_bulk_processor_wait_start_latency"),
		ElasticsearchBulkProcessorBulkSize:         NewDimensionlessHistogramDef("elasticsearch_bulk_processor_bulk_size"),
	},
	Matching: {
		PollSuccessPerTaskQueueCounter:            NewRollupCounterDef("poll_success_per_tl", "poll_success"),
		PollTimeoutPerTaskQueueCounter:            NewRollupCounterDef("poll_timeouts_per_tl", "poll_timeouts"),
		PollSuccessWithSyncPerTaskQueueCounter:    NewRollupCounterDef("poll_success_sync_per_tl", "poll_success_sync"),
		LeaseRequestPerTaskQueueCounter:           NewRollupCounterDef("lease_requests_per_tl", "lease_requests"),
		LeaseFailurePerTaskQueueCounter:           NewRollupCounterDef("lease_failures_per_tl", "lease_failures"),
		ConditionFailedErrorPerTaskQueueCounter:   NewRollupCounterDef("condition_failed_errors_per_tl", "condition_failed_errors"),
		RespondQueryTaskFailedPerTaskQueueCounter: NewRollupCounterDef("respond_query_failed_per_tl", "respond_query_failed"),
		SyncThrottlePerTaskQueueCounter:           NewRollupCounterDef("sync_throttle_count_per_tl", "sync_throttle_count"),
		BufferThrottlePerTaskQueueCounter:         NewRollupCounterDef("buffer_throttle_count_per_tl", "buffer_throttle_count"),
		ExpiredTasksPerTaskQueueCounter:           NewRollupCounterDef("tasks_expired_per_tl", "tasks_expired"),
		ForwardedPerTaskQueueCounter:              NewCounterDef("forwarded_per_tl"),
		ForwardTaskCallsPerTaskQueue:              NewRollupCounterDef("forward_task_calls_per_tl", "forward_task_calls"),
		ForwardTaskErrorsPerTaskQueue:             NewRollupCounterDef("forward_task_errors_per_tl", "forward_task_errors"),
		ForwardQueryCallsPerTaskQueue:             NewRollupCounterDef("forward_query_calls_per_tl", "forward_query_calls"),
		ForwardQueryErrorsPerTaskQueue:            NewRollupCounterDef("forward_query_errors_per_tl", "forward_query_errors"),
		ForwardPollCallsPerTaskQueue:              NewRollupCounterDef("forward_poll_calls_per_tl", "forward_poll_calls"),
		ForwardPollErrorsPerTaskQueue:             NewRollupCounterDef("forward_poll_errors_per_tl", "forward_poll_errors"),
		SyncMatchLatencyPerTaskQueue:              NewRollupTimerDef("syncmatch_latency_per_tl", "syncmatch_latency"),
		AsyncMatchLatencyPerTaskQueue:             NewRollupTimerDef("asyncmatch_latency_per_tl", "asyncmatch_latency"),
		ForwardTaskLatencyPerTaskQueue:            NewRollupTimerDef("forward_task_latency_per_tl", "forward_task_latency"),
		ForwardQueryLatencyPerTaskQueue:           NewRollupTimerDef("forward_query_latency_per_tl", "forward_query_latency"),
		ForwardPollLatencyPerTaskQueue:            NewRollupTimerDef("forward_poll_latency_per_tl", "forward_poll_latency"),
		LocalToLocalMatchPerTaskQueueCounter:      NewRollupCounterDef("local_to_local_matches_per_tl", "local_to_local_matches"),
		LocalToRemoteMatchPerTaskQueueCounter:     NewRollupCounterDef("local_to_remote_matches_per_tl", "local_to_remote_matches"),
		RemoteToLocalMatchPerTaskQueueCounter:     NewRollupCounterDef("remote_to_local_matches_per_tl", "remote_to_local_matches"),
		RemoteToRemoteMatchPerTaskQueueCounter:    NewRollupCounterDef("remote_to_remote_matches_per_tl", "remote_to_remote_matches"),
		LoadedTaskQueueGauge:                      NewGaugeDef("loaded_task_queue_count"),
		TaskQueueStartedCounter:                   NewCounterDef("task_queue_started"),
		TaskQueueStoppedCounter:                   NewCounterDef("task_queue_stopped"),
		TaskWriteThrottlePerTaskQueueCounter:      NewRollupCounterDef("task_write_throttle_count_per_tl", "task_write_throttle_count"),
		TaskWriteLatencyPerTaskQueue:              NewRollupTimerDef("task_write_latency_per_tl", "task_write_latency"),
		TaskLagPerTaskQueueGauge:                  NewGaugeDef("task_lag_per_tl"),
	},
	Worker: {
		ReplicatorMessages:                            NewCounterDef("replicator_messages"),
		ReplicatorFailures:                            NewCounterDef("replicator_errors"),
		ReplicatorLatency:                             NewTimerDef("replicator_latency"),
		ReplicatorDLQFailures:                         NewCounterDef("replicator_dlq_enqueue_fails"),
		ArchiverNonRetryableErrorCount:                NewCounterDef("archiver_non_retryable_error"),
		ArchiverStartedCount:                          NewCounterDef("archiver_started"),
		ArchiverStoppedCount:                          NewCounterDef("archiver_stopped"),
		ArchiverCoroutineStartedCount:                 NewCounterDef("archiver_coroutine_started"),
		ArchiverCoroutineStoppedCount:                 NewCounterDef("archiver_coroutine_stopped"),
		ArchiverHandleHistoryRequestLatency:           NewTimerDef("archiver_handle_history_request_latency"),
		ArchiverHandleVisibilityRequestLatency:        NewTimerDef("archiver_handle_visibility_request_latency"),
		ArchiverUploadWithRetriesLatency:              NewTimerDef("archiver_upload_with_retries_latency"),
		ArchiverDeleteWithRetriesLatency:              NewTimerDef("archiver_delete_with_retries_latency"),
		ArchiverUploadFailedAllRetriesCount:           NewCounterDef("archiver_upload_failed_all_retries"),
		ArchiverUploadSuccessCount:                    NewCounterDef("archiver_upload_success"),
		ArchiverDeleteFailedAllRetriesCount:           NewCounterDef("archiver_delete_failed_all_retries"),
		ArchiverDeleteSuccessCount:                    NewCounterDef("archiver_delete_success"),
		ArchiverHandleVisibilityFailedAllRetiresCount: NewCounterDef("archiver_handle_visibility_failed_all_retries"),
		ArchiverHandleVisibilitySuccessCount:          NewCounterDef("archiver_handle_visibility_success"),
		ArchiverBacklogSizeGauge:                      NewCounterDef("archiver_backlog_size"),
		ArchiverPumpTimeoutCount:                      NewCounterDef("archiver_pump_timeout"),
		ArchiverPumpSignalThresholdCount:              NewCounterDef("archiver_pump_signal_threshold"),
		ArchiverPumpTimeoutWithoutSignalsCount:        NewCounterDef("archiver_pump_timeout_without_signals"),
		ArchiverPumpSignalChannelClosedCount:          NewCounterDef("archiver_pump_signal_channel_closed"),
		ArchiverWorkflowStartedCount:                  NewCounterDef("archiver_workflow_started"),
		ArchiverNumPumpedRequestsCount:                NewCounterDef("archiver_num_pumped_requests"),
		ArchiverNumHandledRequestsCount:               NewCounterDef("archiver_num_handled_requests"),
		ArchiverPumpedNotEqualHandledCount:            NewCounterDef("archiver_pumped_not_equal_handled"),
		ArchiverHandleAllRequestsLatency:              NewTimerDef("archiver_handle_all_requests_latency"),
		ArchiverWorkflowStoppingCount:                 NewCounterDef("archiver_workflow_stopping"),
		TaskProcessedCount:                            NewGaugeDef("task_processed"),
		TaskDeletedCount:                              NewGaugeDef("task_deleted"),
		TaskQueueProcessedCount:                       NewGaugeDef("taskqueue_processed"),
		TaskQueueDeletedCount:                         NewGaugeDef("taskqueue_deleted"),
		TaskQueueOutstandingCount:                     NewGaugeDef("taskqueue_outstanding"),
		ExecutionsOutstandingCount:                    NewGaugeDef("executions_outstanding"),
		StartedCount:                                  NewCounterDef("started"),
		StoppedCount:                                  NewCounterDef("stopped"),
		ScanDuration:                                  NewTimerDef("scan_duration"),
		ExecutorTasksDoneCount:                        NewCounterDef("executor_done"),
		ExecutorTasksErrCount:                         NewCounterDef("executor_err"),
		ExecutorTasksDeferredCount:                    NewCounterDef("executor_deferred"),
		ExecutorTasksDroppedCount:                     NewCounterDef("executor_dropped"),
		BatcherProcessorSuccess:                       NewCounterDef("batcher_processor_requests"),
		BatcherProcessorFailures:                      NewCounterDef("batcher_processor_errors"),
		HistoryScavengerSuccessCount:                  NewCounterDef("scavenger_success"),
		HistoryScavengerErrorCount:                    NewCounterDef("scavenger_errors"),
		HistoryScavengerSkipCount:                     NewCounterDef("scavenger_skips"),
		NamespaceReplicationEnqueueDLQCount:           NewCounterDef("namespace_replication_dlq_enqueue_requests"),
		ScavengerValidationRequestsCount:              NewCounterDef("scavenger_validation_requests"),
		ScavengerValidationFailuresCount:              NewCounterDef("scavenger_validation_failures"),
		AddSearchAttributesFailuresCount:              NewCounterDef("add_search_attributes_failures"),
		CatchUpReadyShardCountGauge:                   NewGaugeDef("catchup_ready_shard_count"),
		HandoverReadyShardCountGauge:                  NewGaugeDef("handover_ready_shard_count"),

		DeleteNamespaceSuccessCount:  NewCounterDef("delete_namespace_success"),
		RenameNamespaceSuccessCount:  NewCounterDef("rename_namespace_success"),
		DeleteExecutionsSuccessCount: NewCounterDef("delete_executions_success"),
		DeleteNamespaceFailuresCount: NewCounterDef("delete_namespace_failures"),
		UpdateNamespaceFailuresCount: NewCounterDef("update_namespace_failures"),
		RenameNamespaceFailuresCount: NewCounterDef("rename_namespace_failures"),
		ReadNamespaceFailuresCount:   NewCounterDef("read_namespace_failures"),
		ListExecutionsFailuresCount:  NewCounterDef("list_executions_failures"),
		CountExecutionsFailuresCount: NewCounterDef("count_executions_failures"),
		DeleteExecutionFailuresCount: NewCounterDef("delete_execution_failures"),
		DeleteExecutionNotFoundCount: NewCounterDef("delete_execution_not_found"),
		RateLimiterFailuresCount:     NewCounterDef("rate_limiter_failures"),
	},
	Server: {
		TlsCertsExpired:  NewGaugeDef("certificates_expired"),
		TlsCertsExpiring: NewGaugeDef("certificates_expiring"),
	},
	UnitTestService: {
		TestCounterMetric1:                NewCounterDef("test_counter_metric_a"),
		TestCounterMetric2:                NewCounterDef("test_counter_metric_b"),
		TestCounterRollupMetric1:          NewRollupCounterDef("test_counter_rollup_metric_a", "test_counter_rollup_metric_a_rollup"),
		TestTimerMetric1:                  NewTimerDef("test_timer_metric_a"),
		TestTimerMetric2:                  NewTimerDef("test_timer_metric_b"),
		TestGaugeMetric1:                  NewGaugeDef("test_gauge_metric_a"),
		TestGaugeMetric2:                  NewGaugeDef("test_gauge_metric_b"),
		TestBytesHistogramMetric1:         NewBytesHistogramDef("test_bytes_histogram_metric_a"),
		TestBytesHistogramMetric2:         NewBytesHistogramDef("test_bytes_histogram_metric_b"),
		TestDimensionlessHistogramMetric1: NewDimensionlessHistogramDef("test_bytes_histogram_metric_a"),
		TestDimensionlessHistogramMetric2: NewDimensionlessHistogramDef("test_bytes_histogram_metric_b"),
	},
}

// ErrorClass is an enum to help with classifying SLA vs. non-SLA errors (SLA = "service level agreement")
type ErrorClass uint8

const (
	// NoError indicates that there is no error (error should be nil)
	NoError = ErrorClass(iota)
	// UserError indicates that this is NOT an SLA-reportable error
	UserError
	// InternalError indicates that this is an SLA-reportable error
	InternalError
)

// Empty returns true if the metricName is an empty string
func (mn MetricName) Empty() bool {
	return mn == ""
}

// String returns string representation of this metric name
func (mn MetricName) String() string {
	return string(mn)
}

func NewTimerDef(name string) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricType: Timer, unit: Milliseconds}
}

func NewRollupTimerDef(name string, rollupName string) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricRollupName: MetricName(rollupName), metricType: Timer, unit: Milliseconds}
}

func NewBytesHistogramDef(name string) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricType: Histogram, unit: Bytes}
}

func NewDimensionlessHistogramDef(name string) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricType: Histogram, unit: Dimensionless}
}

func NewCounterDef(name string) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricType: Counter}
}

// Rollup counter name is used to report aggregated metric excluding namespace tag.
func NewRollupCounterDef(name string, rollupName string) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricRollupName: MetricName(rollupName), metricType: Counter}
}

func NewGaugeDef(name string) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricType: Gauge}
}
