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
		MetricType       MetricType // metric type
		MetricName       MetricName // metric name
		MetricRollupName MetricName // optional. if non-empty, this name must be used for rolled-up version of this metric
		Unit             MetricUnit
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
	// MatchingGetWorkerBuildIdOrderingScope tracks RPC calls to matching service
	MatchingClientGetWorkerBuildIdOrderingScope
	// MatchingClientInvalidateTaskQueueMetadataScope tracks RPC calls to matching service
	MatchingClientInvalidateTaskQueueMetadataScope
	// MatchingClientGetTaskQueueMetadataScope tracks RPC calls to matching service
	MatchingClientGetTaskQueueMetadataScope
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
	// FrontendClientDescribeBatchOperationScope tracks RPC calls to frontend service
	FrontendClientDescribeBatchOperationScope
	// FrontendClientListBatchOperationsScope tracks RPC calls to frontend service
	FrontendClientListBatchOperationsScope
	// FrontendClientStartBatchOperationScope tracks RPC calls to frontend service
	FrontendClientStartBatchOperationScope
	// FrontendClientStopBatchOperationScope tracks RPC calls to frontend service
	FrontendClientStopBatchOperationScope

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
	// DCRedirectionDescribeBatchOperationScope tracks RPC calls for dc redirection
	DCRedirectionDescribeBatchOperationScope
	// DCRedirectionListBatchOperationsScope tracks RPC calls for dc redirection
	DCRedirectionListBatchOperationsScope
	// DCRedirectionStartBatchOperationScope tracks RPC calls for dc redirection
	DCRedirectionStartBatchOperationScope
	// DCRedirectionStopBatchOperationScope tracks RPC calls for dc redirection
	DCRedirectionStopBatchOperationScope

	// MessagingClientPublishScope tracks Publish calls made by service to messaging layer
	MessagingClientPublishScope
	// MessagingClientPublishBatchScope tracks Publish calls made by service to messaging layer
	MessagingClientPublishBatchScope

	// NamespaceCacheScope tracks namespace cache callbacks
	NamespaceCacheScope

	// PersistenceAppendHistoryNodesScope tracks AppendHistoryNodes calls made by service to persistence layer
	PersistenceAppendHistoryNodesScope
	// PersistenceAppendRawHistoryNodesScope tracks AppendRawHistoryNodes calls made by service to persistence layer
	PersistenceAppendRawHistoryNodesScope
	// PersistenceDeleteHistoryNodesScope tracks DeleteHistoryNodes calls made by service to persistence layer
	PersistenceDeleteHistoryNodesScope
	// PersistenceParseHistoryBranchInfoScope tracks NewHistoryBranch calls made by service to persistence layer
	PersistenceParseHistoryBranchInfoScope
	// PersistenceUpdateHistoryBranchInfoScope tracks NewHistoryBranch calls made by service to persistence layer
	PersistenceUpdateHistoryBranchInfoScope
	// PersistenceNewHistoryBranchScope tracks NewHistoryBranch calls made by service to persistence layer
	PersistenceNewHistoryBranchScope
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

	// SequentialTaskProcessingScope is used by sequential task processing logic
	SequentialTaskProcessingScope
	// ParallelTaskProcessingScope is used by parallel task processing logic
	ParallelTaskProcessingScope
	// TaskSchedulerScope is used by task scheduler logic
	TaskSchedulerScope

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

// -- Operation scopes for Frontend service --

// -- Scopes for UnitTestService --
const (
	TestScope1 = iota + NumCommonScopes
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

		PersistenceAppendHistoryNodesScope:         {operation: "AppendHistoryNodes"},
		PersistenceAppendRawHistoryNodesScope:      {operation: "AppendRawHistoryNodes"},
		PersistenceDeleteHistoryNodesScope:         {operation: "DeleteHistoryNodes"},
		PersistenceParseHistoryBranchInfoScope:     {operation: "ParseHistoryBranch"},
		PersistenceUpdateHistoryBranchInfoScope:    {operation: "UpdateHistoryBranch"},
		PersistenceNewHistoryBranchScope:           {operation: "NewHistoryBranch"},
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
		MatchingClientInvalidateTaskQueueMetadataScope: {operation: "MatchingClientInvalidateTaskQueueMetadata", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientGetTaskQueueMetadataScope:        {operation: "MatchingClientGetTaskQueueMetadata", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},

		FrontendClientDeprecateNamespaceScope:                 {operation: "FrontendClientDeprecateNamespace", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientDescribeBatchOperationScope:             {operation: "FrontendClientDescribeBatchOperation", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientDescribeNamespaceScope:                  {operation: "FrontendClientDescribeNamespace", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientDescribeTaskQueueScope:                  {operation: "FrontendClientDescribeTaskQueue", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientDescribeWorkflowExecutionScope:          {operation: "FrontendClientDescribeWorkflowExecution", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientGetWorkflowExecutionHistoryScope:        {operation: "FrontendClientGetWorkflowExecutionHistory", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientGetWorkflowExecutionHistoryReverseScope: {operation: "FrontendClientGetWorkflowExecutionHistoryReverse", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientGetWorkflowExecutionRawHistoryScope:     {operation: "FrontendClientGetWorkflowExecutionRawHistory", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientPollForWorkflowExecutionRawHistoryScope: {operation: "FrontendClientPollForWorkflowExecutionRawHistoryScope", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientListArchivedWorkflowExecutionsScope:     {operation: "FrontendClientListArchivedWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientListBatchOperationsScope:                {operation: "FrontendClientListBatchOperations", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
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
		FrontendClientStartBatchOperationScope:                {operation: "FrontendClientStartBatchOperation", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientStartWorkflowExecutionScope:             {operation: "FrontendClientStartWorkflowExecution", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientStopBatchOperationScope:                 {operation: "FrontendClientStopBatchOperation", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
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
		DCRedirectionDescribeBatchOperationScope:             {operation: "DCRedirectionDescribeBatchOperation", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionListBatchOperationsScope:                {operation: "DCRedirectionListBatchOperations", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionStartBatchOperationScope:                {operation: "DCRedirectionStartBatchOperation", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionStopBatchOperationScope:                 {operation: "DCRedirectionStopBatchOperation", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},

		MessagingClientPublishScope:      {operation: "MessagingClientPublish"},
		MessagingClientPublishBatchScope: {operation: "MessagingClientPublishBatch"},

		NamespaceCacheScope:           {operation: "NamespaceCache"},
		SequentialTaskProcessingScope: {operation: "SequentialTaskProcessing"},
		ParallelTaskProcessingScope:   {operation: "ParallelTaskProcessing"},
		TaskSchedulerScope:            {operation: "TaskScheduler"},

		VisibilityArchiverScope: {operation: "VisibilityArchiver"},

		BlobstoreClientUploadScope:          {operation: "BlobstoreClientUpload", tags: map[string]string{ServiceRoleTagName: BlobstoreRoleTagValue}},
		BlobstoreClientDownloadScope:        {operation: "BlobstoreClientDownload", tags: map[string]string{ServiceRoleTagName: BlobstoreRoleTagValue}},
		BlobstoreClientGetMetadataScope:     {operation: "BlobstoreClientGetMetadata", tags: map[string]string{ServiceRoleTagName: BlobstoreRoleTagValue}},
		BlobstoreClientExistsScope:          {operation: "BlobstoreClientExists", tags: map[string]string{ServiceRoleTagName: BlobstoreRoleTagValue}},
		BlobstoreClientDeleteScope:          {operation: "BlobstoreClientDelete", tags: map[string]string{ServiceRoleTagName: BlobstoreRoleTagValue}},
		BlobstoreClientDirectoryExistsScope: {operation: "BlobstoreClientDirectoryExists", tags: map[string]string{ServiceRoleTagName: BlobstoreRoleTagValue}},

		DynamicConfigScope: {operation: "DynamicConfig"},
	},
	// History Scope Names
	// Worker Scope Names

	UnitTestService: {
		TestScope1: {operation: "test_scope_1_operation"},
		TestScope2: {operation: "test_scope_2_operation"},
	},
}

// Common Metrics enum
const (
	ArchivalConfigFailures = iota

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

	NamespaceReplicationDLQAckLevelGauge

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
	NoopImplementationIsUsed

	NumCommonMetrics // Needs to be last on this list for iota numbering
)

// Matching metrics en

// UnitTestService metrics enum
const (
	TestCounterMetric1 = iota + NumCommonMetrics
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

		ArchivalConfigFailures: NewCounterDef("archivalconfig_failures"),

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
		NamespaceReplicationDLQAckLevelGauge:        NewGaugeDef("namespace_dlq_ack_level"),

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

		NoopImplementationIsUsed: NewCounterDef("noop_implementation_is_used"),
	},
	History: {},

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

// Empty returns true if the MetricName is an empty string
func (mn MetricName) Empty() bool {
	return mn == ""
}

// String returns string representation of this metric name
func (mn MetricName) String() string {
	return string(mn)
}

func NewTimerDef(name string) metricDefinition {
	return metricDefinition{MetricName: MetricName(name), MetricType: Timer, Unit: Milliseconds}
}

func NewRollupTimerDef(name string, rollupName string) metricDefinition {
	return metricDefinition{MetricName: MetricName(name), MetricRollupName: MetricName(rollupName), MetricType: Timer, Unit: Milliseconds}
}

func NewBytesHistogramDef(name string) metricDefinition {
	return metricDefinition{MetricName: MetricName(name), MetricType: Histogram, Unit: Bytes}
}

func NewDimensionlessHistogramDef(name string) metricDefinition {
	return metricDefinition{MetricName: MetricName(name), MetricType: Histogram, Unit: Dimensionless}
}

func NewCounterDef(name string) metricDefinition {
	return metricDefinition{MetricName: MetricName(name), MetricType: Counter}
}

// Rollup counter name is used to report aggregated metric excluding namespace tag.
func NewRollupCounterDef(name string, rollupName string) metricDefinition {
	return metricDefinition{MetricName: MetricName(name), MetricRollupName: MetricName(rollupName), MetricType: Counter}
}

func NewGaugeDef(name string) metricDefinition {
	return metricDefinition{MetricName: MetricName(name), MetricType: Gauge}
}
