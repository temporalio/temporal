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

import (
	"github.com/uber-go/tally"
)

// types used/defined by the package
type (
	// MetricName is the name of the metric
	MetricName string

	// MetricType is the type of the metric
	MetricType int

	// metricDefinition contains the definition for a metric
	metricDefinition struct {
		// nolint
		metricType       MetricType    // metric type
		metricName       MetricName    // metric name
		metricRollupName MetricName    // optional. if non-empty, this name must be used for rolled-up version of this metric
		buckets          tally.Buckets // buckets if we are emitting histograms
	}

	// scopeDefinition holds the tag definitions for a scope
	scopeDefinition struct {
		operation string            // 'operation' tag for scope
		tags      map[string]string // additional tags for scope
	}

	// ServiceIdx is an index that uniquely identifies the service
	ServiceIdx int
)

// MetricTypes which are supported
const (
	Counter MetricType = iota
	Timer
	Gauge
)

// Service names for all services that emit metrics.
const (
	Common = iota
	Frontend
	History
	Matching
	Worker
	NumServices
)

// Values used for metrics propagation
const (
	HistoryWorkflowExecutionCacheLatency = "history_workflow_execution_cache_latency"
)

// Common tags for all services
const (
	OperationTagName   = "operation"
	ServiceRoleTagName = "service_role"
	StatsTypeTagName   = "stats_type"
	CacheTypeTagName   = "cache_type"
	FailureTagName     = "failure"
)

// This package should hold all the metrics and tags for temporal
const (
	HistoryRoleTagValue       = "history"
	MatchingRoleTagValue      = "matching"
	FrontendRoleTagValue      = "frontend"
	AdminRoleTagValue         = "admin"
	DCRedirectionRoleTagValue = "dc_redirection"
	BlobstoreRoleTagValue     = "blobstore"

	SizeStatsTypeTagValue  = "size"
	CountStatsTypeTagValue = "count"

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

	// PersistenceCreateShardScope tracks CreateShard calls made by service to persistence layer
	PersistenceCreateShardScope
	// PersistenceGetShardScope tracks GetShard calls made by service to persistence layer
	PersistenceGetShardScope
	// PersistenceUpdateShardScope tracks UpdateShard calls made by service to persistence layer
	PersistenceUpdateShardScope
	// PersistenceCreateWorkflowExecutionScope tracks CreateWorkflowExecution calls made by service to persistence layer
	PersistenceCreateWorkflowExecutionScope
	// PersistenceGetWorkflowExecutionScope tracks GetWorkflowExecution calls made by service to persistence layer
	PersistenceGetWorkflowExecutionScope
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
	// PersistenceRangeCompleteTransferTaskScope tracks CompleteTransferTasks calls made by service to persistence layer
	PersistenceRangeCompleteTransferTaskScope

	// PersistenceGetVisibilityTaskScope tracks GetVisibilityTask calls made by service to persistence layer
	PersistenceGetVisibilityTaskScope
	// PersistenceGetVisibilityTasksScope tracks GetVisibilityTasks calls made by service to persistence layer
	PersistenceGetVisibilityTasksScope
	// PersistenceCompleteVisibilityTaskScope tracks CompleteVisibilityTasks calls made by service to persistence layer
	PersistenceCompleteVisibilityTaskScope
	// PersistenceRangeCompleteVisibilityTaskScope tracks CompleteVisibilityTasks calls made by service to persistence layer
	PersistenceRangeCompleteVisibilityTaskScope

	// PersistenceGetReplicationTaskScope tracks GetReplicationTask calls made by service to persistence layer
	PersistenceGetReplicationTaskScope
	// PersistenceGetReplicationTasksScope tracks GetReplicationTasks calls made by service to persistence layer
	PersistenceGetReplicationTasksScope
	// PersistenceCompleteReplicationTaskScope tracks CompleteReplicationTasks calls made by service to persistence layer
	PersistenceCompleteReplicationTaskScope
	// PersistenceRangeCompleteReplicationTaskScope tracks RangeCompleteReplicationTasks calls made by service to persistence layer
	PersistenceRangeCompleteReplicationTaskScope
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
	// PersistenceGetTimerIndexTasksScope tracks GetTimerIndexTasks calls made by service to persistence layer
	PersistenceGetTimerIndexTasksScope
	// PersistenceCompleteTimerTaskScope tracks CompleteTimerTasks calls made by service to persistence layer
	PersistenceCompleteTimerTaskScope
	// PersistenceRangeCompleteTimerTaskScope tracks CompleteTimerTasks calls made by service to persistence layer
	PersistenceRangeCompleteTimerTaskScope
	// PersistenceCreateTaskScope tracks CreateTask calls made by service to persistence layer
	PersistenceCreateTaskScope
	// PersistenceGetTasksScope tracks GetTasks calls made by service to persistence layer
	PersistenceGetTasksScope
	// PersistenceCompleteTaskScope tracks CompleteTask calls made by service to persistence layer
	PersistenceCompleteTaskScope
	// PersistenceCompleteTasksLessThanScope is the metric scope for persistence.TaskManager.PersistenceCompleteTasksLessThan API
	PersistenceCompleteTasksLessThanScope
	// PersistenceLeaseTaskQueueScope tracks LeaseTaskQueue calls made by service to persistence layer
	PersistenceLeaseTaskQueueScope
	// PersistenceUpdateTaskQueueScope tracks PersistenceUpdateTaskQueueScope calls made by service to persistence layer
	PersistenceUpdateTaskQueueScope
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
	// PersistenceDeleteNamespaceByNameScope tracks DeleteNamespaceByName calls made by service to persistence layer
	PersistenceDeleteNamespaceByNameScope
	// PersistenceListNamespaceScope tracks DeleteNamespaceByName calls made by service to persistence layer
	PersistenceListNamespaceScope
	// PersistenceGetMetadataScope tracks DeleteNamespaceByName calls made by service to persistence layer
	PersistenceGetMetadataScope
	// PersistenceRecordWorkflowExecutionStartedScope tracks RecordWorkflowExecutionStarted calls made by service to persistence layer
	PersistenceRecordWorkflowExecutionStartedScope
	// PersistenceRecordWorkflowExecutionClosedScope tracks RecordWorkflowExecutionClosed calls made by service to persistence layer
	PersistenceRecordWorkflowExecutionClosedScope
	// PersistenceUpsertWorkflowExecutionScope tracks UpsertWorkflowExecution calls made by service to persistence layer
	PersistenceUpsertWorkflowExecutionScope
	// PersistenceListOpenWorkflowExecutionsScope tracks ListOpenWorkflowExecutions calls made by service to persistence layer
	PersistenceListOpenWorkflowExecutionsScope
	// PersistenceListClosedWorkflowExecutionsScope tracks ListClosedWorkflowExecutions calls made by service to persistence layer
	PersistenceListClosedWorkflowExecutionsScope
	// PersistenceListOpenWorkflowExecutionsByTypeScope tracks ListOpenWorkflowExecutionsByType calls made by service to persistence layer
	PersistenceListOpenWorkflowExecutionsByTypeScope
	// PersistenceListClosedWorkflowExecutionsByTypeScope tracks ListClosedWorkflowExecutionsByType calls made by service to persistence layer
	PersistenceListClosedWorkflowExecutionsByTypeScope
	// PersistenceListOpenWorkflowExecutionsByWorkflowIDScope tracks ListOpenWorkflowExecutionsByWorkflowID calls made by service to persistence layer
	PersistenceListOpenWorkflowExecutionsByWorkflowIDScope
	// PersistenceListClosedWorkflowExecutionsByWorkflowIDScope tracks ListClosedWorkflowExecutionsByWorkflowID calls made by service to persistence layer
	PersistenceListClosedWorkflowExecutionsByWorkflowIDScope
	// PersistenceListClosedWorkflowExecutionsByStatusScope tracks ListClosedWorkflowExecutionsByStatus calls made by service to persistence layer
	PersistenceListClosedWorkflowExecutionsByStatusScope
	// PersistenceVisibilityDeleteWorkflowExecutionScope is the metrics scope for persistence.VisibilityManager.DeleteWorkflowExecution
	PersistenceVisibilityDeleteWorkflowExecutionScope
	// PersistenceListWorkflowExecutionsScope tracks ListWorkflowExecutions calls made by service to persistence layer
	PersistenceListWorkflowExecutionsScope
	// PersistenceScanWorkflowExecutionsScope tracks ScanWorkflowExecutions calls made by service to persistence layer
	PersistenceScanWorkflowExecutionsScope
	// PersistenceCountWorkflowExecutionsScope tracks CountWorkflowExecutions calls made by service to persistence layer
	PersistenceCountWorkflowExecutionsScope
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
	// PersistenceGetClusterMetadataScope tracks GetClusterMetadata calls made by service to persistence layer
	PersistenceGetClusterMetadataScope
	// PersistenceSaveClusterMetadataScope tracks SaveClusterMetadata calls made by service to persistence layer
	PersistenceSaveClusterMetadataScope
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
	// HistoryClientResetWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientResetWorkflowExecutionScope
	// HistoryClientScheduleWorkflowTaskScope tracks RPC calls to history service
	HistoryClientScheduleWorkflowTaskScope
	// HistoryClientRecordChildExecutionCompletedScope tracks RPC calls to history service
	HistoryClientRecordChildExecutionCompletedScope
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
	// FrontendClientGetReplicationTasksScope tracks RPC calls to frontend service
	FrontendClientGetReplicationTasksScope
	// FrontendClientGetNamespaceReplicationTasksScope tracks RPC calls to frontend service
	FrontendClientGetNamespaceReplicationTasksScope
	// FrontendClientGetDLQReplicationTasksScope tracks RPC calls to frontend service
	FrontendClientGetDLQReplicationTasksScope
	// FrontendClientReapplyEventsScope tracks RPC calls to frontend service
	FrontendClientReapplyEventsScope
	// FrontendClientGetClusterInfoScope tracks RPC calls to frontend
	FrontendClientGetClusterInfoScope
	// FrontendClientListTaskQueuePartitionsScope tracks RPC calls to frontend service
	FrontendClientListTaskQueuePartitionsScope
	// AdminClientAddSearchAttributesScope tracks RPC calls to admin service
	AdminClientAddSearchAttributesScope
	// AdminClientRemoveSearchAttributesScope tracks RPC calls to admin service
	AdminClientRemoveSearchAttributesScope
	// AdminClientGetSearchAttributesScope tracks RPC calls to admin service
	AdminClientGetSearchAttributesScope
	// AdminClientCloseShardScope tracks RPC calls to admin service
	AdminClientCloseShardScope
	// AdminClientDescribeHistoryHostScope tracks RPC calls to admin service
	AdminClientDescribeHistoryHostScope
	// AdminClientDescribeWorkflowMutableStateScope tracks RPC calls to admin service
	AdminClientDescribeWorkflowMutableStateScope
	// AdminClientGetWorkflowExecutionRawHistoryScope tracks RPC calls to admin service
	AdminClientGetWorkflowExecutionRawHistoryScope
	// AdminClientGetWorkflowExecutionRawHistoryV2Scope tracks RPC calls to admin service
	AdminClientGetWorkflowExecutionRawHistoryV2Scope
	// AdminClientDescribeClusterScope tracks RPC calls to admin service
	AdminClientDescribeClusterScope
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
	// PersistenceDeleteHistoryNodesScope tracks DeleteHistoryNodes calls made by service to persistence layer
	PersistenceDeleteHistoryNodesScope
	// PersistenceReadHistoryBranchScope tracks ReadHistoryBranch calls made by service to persistence layer
	PersistenceReadHistoryBranchScope
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

	// ElasticsearchRecordWorkflowExecutionStartedScope tracks RecordWorkflowExecutionStarted calls made by service to persistence layer
	ElasticsearchRecordWorkflowExecutionStartedScope
	// ElasticsearchRecordWorkflowExecutionClosedScope tracks RecordWorkflowExecutionClosed calls made by service to persistence layer
	ElasticsearchRecordWorkflowExecutionClosedScope
	// ElasticsearchUpsertWorkflowExecutionScope tracks UpsertWorkflowExecution calls made by service to persistence layer
	ElasticsearchUpsertWorkflowExecutionScope
	// ElasticsearchListOpenWorkflowExecutionsScope tracks ListOpenWorkflowExecutions calls made by service to persistence layer
	ElasticsearchListOpenWorkflowExecutionsScope
	// ElasticsearchListClosedWorkflowExecutionsScope tracks ListClosedWorkflowExecutions calls made by service to persistence layer
	ElasticsearchListClosedWorkflowExecutionsScope
	// ElasticsearchListOpenWorkflowExecutionsByTypeScope tracks ListOpenWorkflowExecutionsByType calls made by service to persistence layer
	ElasticsearchListOpenWorkflowExecutionsByTypeScope
	// ElasticsearchListClosedWorkflowExecutionsByTypeScope tracks ListClosedWorkflowExecutionsByType calls made by service to persistence layer
	ElasticsearchListClosedWorkflowExecutionsByTypeScope
	// ElasticsearchListOpenWorkflowExecutionsByWorkflowIDScope tracks ListOpenWorkflowExecutionsByWorkflowID calls made by service to persistence layer
	ElasticsearchListOpenWorkflowExecutionsByWorkflowIDScope
	// ElasticsearchListClosedWorkflowExecutionsByWorkflowIDScope tracks ListClosedWorkflowExecutionsByWorkflowID calls made by service to persistence layer
	ElasticsearchListClosedWorkflowExecutionsByWorkflowIDScope
	// ElasticsearchListClosedWorkflowExecutionsByStatusScope tracks ListClosedWorkflowExecutionsByStatus calls made by service to persistence layer
	ElasticsearchListClosedWorkflowExecutionsByStatusScope
	// ElasticsearchListWorkflowExecutionsScope tracks ListWorkflowExecutions calls made by service to persistence layer
	ElasticsearchListWorkflowExecutionsScope
	// ElasticsearchScanWorkflowExecutionsScope tracks ScanWorkflowExecutions calls made by service to persistence layer
	ElasticsearchScanWorkflowExecutionsScope
	// ElasticsearchCountWorkflowExecutionsScope tracks CountWorkflowExecutions calls made by service to persistence layer
	ElasticsearchCountWorkflowExecutionsScope
	// ElasticsearchDeleteWorkflowExecutionsScope tracks DeleteWorkflowExecution calls made by service to persistence layer
	ElasticsearchDeleteWorkflowExecutionsScope

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
	// AdminRemoveTaskScope is the metric scope for admin.AdminRemoveTaskScope
	AdminRemoveTaskScope
	// AdminCloseShardTaskScope is the metric scope for admin.AdminRemoveTaskScope
	AdminCloseShardTaskScope
	// AdminReadDLQMessagesScope is the metric scope for admin.AdminReadDLQMessagesScope
	AdminReadDLQMessagesScope
	// AdminPurgeDLQMessagesScope is the metric scope for admin.AdminPurgeDLQMessagesScope
	AdminPurgeDLQMessagesScope
	// AdminMergeDLQMessagesScope is the metric scope for admin.AdminMergeDLQMessagesScope
	AdminMergeDLQMessagesScope

	NumAdminScopes
)

// -- Operation scopes for Frontend service --
const (
	// FrontendStartWorkflowExecutionScope is the metric scope for frontend.StartWorkflowExecution
	FrontendStartWorkflowExecutionScope = iota + NumAdminScopes
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
	// HistoryRecordChildExecutionCompletedScope tracks CompleteChildExecution API calls received by service
	HistoryRecordChildExecutionCompletedScope
	// HistoryRequestCancelWorkflowExecutionScope tracks RequestCancelWorkflowExecution API calls received by service
	HistoryRequestCancelWorkflowExecutionScope
	// HistorySyncShardStatusScope tracks HistorySyncShardStatus API calls received by service
	HistorySyncShardStatusScope
	// HistorySyncActivityScope tracks HistoryActivity API calls received by service
	HistorySyncActivityScope
	// HistoryDescribeMutableStateScope tracks HistoryActivity API calls received by service
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
	// HistoryHistoryRemoveTaskScope is the scope used by remove task API
	HistoryHistoryRemoveTaskScope
	// HistoryCloseShard is the scope used by close shard API
	HistoryCloseShard
	// HistoryReplicateEventsV2 is the scope used by replicate events API
	HistoryReplicateEventsV2
	// HistoryResetStickyTaskQueue is the scope used by reset sticky task queue API
	HistoryResetStickyTaskQueue
	// HistoryReapplyEvents is the scope used by reapply events API
	HistoryReapplyEvents
	// HistoryDescribeHistoryHost is the scope used by describe history host API
	HistoryDescribeHistoryHost
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
	// ExecutionSizeStatsScope is the scope used for emiting workflow execution size related stats
	ExecutionSizeStatsScope
	// ExecutionCountStatsScope is the scope used for emiting workflow execution count related stats
	ExecutionCountStatsScope
	// SessionSizeStatsScope is the scope used for emiting session update size related stats
	SessionSizeStatsScope
	// SessionCountStatsScope is the scope used for emiting session update count related stats
	SessionCountStatsScope
	// HistoryResetWorkflowExecutionScope tracks ResetWorkflowExecution API calls received by service
	HistoryResetWorkflowExecutionScope
	// HistoryQueryWorkflowScope tracks QueryWorkflow API calls received by service
	HistoryQueryWorkflowScope
	// HistoryProcessDeleteHistoryEventScope tracks ProcessDeleteHistoryEvent processing calls
	HistoryProcessDeleteHistoryEventScope
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

	NumWorkerScopes
)

// ScopeDefs record the scopes for all services
var ScopeDefs = map[ServiceIdx]map[int]scopeDefinition{
	// common scope Names
	Common: {
		UnknownScope:                                             {operation: "Unknown"},
		PersistenceCreateShardScope:                              {operation: "CreateShard"},
		PersistenceGetShardScope:                                 {operation: "GetShard"},
		PersistenceUpdateShardScope:                              {operation: "UpdateShard"},
		PersistenceCreateWorkflowExecutionScope:                  {operation: "CreateWorkflowExecution"},
		PersistenceGetWorkflowExecutionScope:                     {operation: "GetWorkflowExecution"},
		PersistenceUpdateWorkflowExecutionScope:                  {operation: "UpdateWorkflowExecution"},
		PersistenceConflictResolveWorkflowExecutionScope:         {operation: "ConflictResolveWorkflowExecution"},
		PersistenceResetWorkflowExecutionScope:                   {operation: "ResetWorkflowExecution"},
		PersistenceDeleteWorkflowExecutionScope:                  {operation: "DeleteWorkflowExecution"},
		PersistenceDeleteCurrentWorkflowExecutionScope:           {operation: "DeleteCurrentWorkflowExecution"},
		PersistenceGetCurrentExecutionScope:                      {operation: "GetCurrentExecution"},
		PersistenceListConcreteExecutionsScope:                   {operation: "ListConcreteExecutions"},
		PersistenceAddTasksScope:                                 {operation: "AddTasks"},
		PersistenceGetTransferTaskScope:                          {operation: "GetTransferTask"},
		PersistenceGetTransferTasksScope:                         {operation: "GetTransferTasks"},
		PersistenceCompleteTransferTaskScope:                     {operation: "CompleteTransferTask"},
		PersistenceRangeCompleteTransferTaskScope:                {operation: "RangeCompleteTransferTask"},
		PersistenceGetVisibilityTaskScope:                        {operation: "GetVisibilityTask"},
		PersistenceGetVisibilityTasksScope:                       {operation: "GetVisibilityTasks"},
		PersistenceCompleteVisibilityTaskScope:                   {operation: "CompleteVisibilityTask"},
		PersistenceRangeCompleteVisibilityTaskScope:              {operation: "RangeCompleteVisibilityTask"},
		PersistenceGetReplicationTaskScope:                       {operation: "GetReplicationTask"},
		PersistenceGetReplicationTasksScope:                      {operation: "GetReplicationTasks"},
		PersistenceCompleteReplicationTaskScope:                  {operation: "CompleteReplicationTask"},
		PersistenceRangeCompleteReplicationTaskScope:             {operation: "RangeCompleteReplicationTask"},
		PersistencePutReplicationTaskToDLQScope:                  {operation: "PutReplicationTaskToDLQ"},
		PersistenceGetReplicationTasksFromDLQScope:               {operation: "GetReplicationTasksFromDLQ"},
		PersistenceDeleteReplicationTaskFromDLQScope:             {operation: "DeleteReplicationTaskFromDLQ"},
		PersistenceRangeDeleteReplicationTaskFromDLQScope:        {operation: "RangeDeleteReplicationTaskFromDLQ"},
		PersistenceGetTimerTaskScope:                             {operation: "GetTimerTask"},
		PersistenceGetTimerIndexTasksScope:                       {operation: "GetTimerIndexTasks"},
		PersistenceCompleteTimerTaskScope:                        {operation: "CompleteTimerTask"},
		PersistenceRangeCompleteTimerTaskScope:                   {operation: "RangeCompleteTimerTask"},
		PersistenceCreateTaskScope:                               {operation: "CreateTask"},
		PersistenceGetTasksScope:                                 {operation: "GetTasks"},
		PersistenceCompleteTaskScope:                             {operation: "CompleteTask"},
		PersistenceCompleteTasksLessThanScope:                    {operation: "CompleteTasksLessThan"},
		PersistenceLeaseTaskQueueScope:                           {operation: "LeaseTaskQueue"},
		PersistenceUpdateTaskQueueScope:                          {operation: "UpdateTaskQueue"},
		PersistenceListTaskQueueScope:                            {operation: "ListTaskQueue"},
		PersistenceDeleteTaskQueueScope:                          {operation: "DeleteTaskQueue"},
		PersistenceAppendHistoryEventsScope:                      {operation: "AppendHistoryEvents"},
		PersistenceGetWorkflowExecutionHistoryScope:              {operation: "GetWorkflowExecutionHistory"},
		PersistenceDeleteWorkflowExecutionHistoryScope:           {operation: "DeleteWorkflowExecutionHistory"},
		PersistenceInitializeSystemNamespaceScope:                {operation: "InitializeSystemNamespace"},
		PersistenceCreateNamespaceScope:                          {operation: "CreateNamespace"},
		PersistenceGetNamespaceScope:                             {operation: "GetNamespace"},
		PersistenceUpdateNamespaceScope:                          {operation: "UpdateNamespace"},
		PersistenceDeleteNamespaceScope:                          {operation: "DeleteNamespace"},
		PersistenceDeleteNamespaceByNameScope:                    {operation: "DeleteNamespaceByName"},
		PersistenceListNamespaceScope:                            {operation: "ListNamespace"},
		PersistenceGetMetadataScope:                              {operation: "GetMetadata"},
		PersistenceRecordWorkflowExecutionStartedScope:           {operation: "RecordWorkflowExecutionStarted"},
		PersistenceRecordWorkflowExecutionClosedScope:            {operation: "RecordWorkflowExecutionClosed"},
		PersistenceUpsertWorkflowExecutionScope:                  {operation: "UpsertWorkflowExecution"},
		PersistenceListOpenWorkflowExecutionsScope:               {operation: "ListOpenWorkflowExecutions"},
		PersistenceListClosedWorkflowExecutionsScope:             {operation: "ListClosedWorkflowExecutions"},
		PersistenceListOpenWorkflowExecutionsByTypeScope:         {operation: "ListOpenWorkflowExecutionsByType"},
		PersistenceListClosedWorkflowExecutionsByTypeScope:       {operation: "ListClosedWorkflowExecutionsByType"},
		PersistenceListOpenWorkflowExecutionsByWorkflowIDScope:   {operation: "ListOpenWorkflowExecutionsByWorkflowID"},
		PersistenceListClosedWorkflowExecutionsByWorkflowIDScope: {operation: "ListClosedWorkflowExecutionsByWorkflowID"},
		PersistenceListClosedWorkflowExecutionsByStatusScope:     {operation: "ListClosedWorkflowExecutionsByStatus"},
		PersistenceVisibilityDeleteWorkflowExecutionScope:        {operation: "VisibilityDeleteWorkflowExecution"},
		PersistenceListWorkflowExecutionsScope:                   {operation: "ListWorkflowExecutions"},
		PersistenceScanWorkflowExecutionsScope:                   {operation: "ScanWorkflowExecutions"},
		PersistenceCountWorkflowExecutionsScope:                  {operation: "CountWorkflowExecutions"},
		PersistenceAppendHistoryNodesScope:                       {operation: "AppendHistoryNodes"},
		PersistenceDeleteHistoryNodesScope:                       {operation: "DeleteHistoryNodes"},
		PersistenceReadHistoryBranchScope:                        {operation: "ReadHistoryBranch"},
		PersistenceForkHistoryBranchScope:                        {operation: "ForkHistoryBranch"},
		PersistenceDeleteHistoryBranchScope:                      {operation: "DeleteHistoryBranch"},
		PersistenceTrimHistoryBranchScope:                        {operation: "TrimHistoryBranch"},
		PersistenceCompleteForkBranchScope:                       {operation: "CompleteForkBranch"},
		PersistenceGetHistoryTreeScope:                           {operation: "GetHistoryTree"},
		PersistenceGetAllHistoryTreeBranchesScope:                {operation: "GetAllHistoryTreeBranches"},
		PersistenceEnqueueMessageScope:                           {operation: "EnqueueMessage"},
		PersistenceEnqueueMessageToDLQScope:                      {operation: "EnqueueMessageToDLQ"},
		PersistenceReadQueueMessagesScope:                        {operation: "ReadQueueMessages"},
		PersistenceReadQueueMessagesFromDLQScope:                 {operation: "ReadQueueMessagesFromDLQ"},
		PersistenceDeleteQueueMessagesScope:                      {operation: "DeleteQueueMessages"},
		PersistenceDeleteQueueMessageFromDLQScope:                {operation: "DeleteQueueMessageFromDLQ"},
		PersistenceRangeDeleteMessagesFromDLQScope:               {operation: "RangeDeleteMessagesFromDLQ"},
		PersistenceUpdateAckLevelScope:                           {operation: "UpdateAckLevel"},
		PersistenceGetAckLevelScope:                              {operation: "GetAckLevel"},
		PersistenceUpdateDLQAckLevelScope:                        {operation: "UpdateDLQAckLevel"},
		PersistenceGetDLQAckLevelScope:                           {operation: "GetDLQAckLevel"},
		PersistenceNamespaceReplicationQueueScope:                {operation: "NamespaceReplicationQueue"},
		PersistenceGetClusterMetadataScope:                       {operation: "GetClusterMetadata"},
		PersistenceSaveClusterMetadataScope:                      {operation: "SaveClusterMetadata"},
		PersistencePruneClusterMembershipScope:                   {operation: "PruneClusterMembership"},
		PersistenceGetClusterMembersScope:                        {operation: "GetClusterMembership"},
		PersistenceUpsertClusterMembershipScope:                  {operation: "UpsertClusterMembership"},

		ClusterMetadataArchivalConfigScope: {operation: "ArchivalConfig"},

		HistoryClientStartWorkflowExecutionScope:              {operation: "HistoryClientStartWorkflowExecution", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRecordActivityTaskHeartbeatScope:         {operation: "HistoryClientRecordActivityTaskHeartbeat", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRespondWorkflowTaskCompletedScope:        {operation: "HistoryClientRespondWorkflowTaskCompleted", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRespondWorkflowTaskFailedScope:           {operation: "HistoryClientRespondWorkflowTaskFailed", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRespondActivityTaskCompletedScope:        {operation: "HistoryClientRespondActivityTaskCompleted", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRespondActivityTaskFailedScope:           {operation: "HistoryClientRespondActivityTaskFailed", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRespondActivityTaskCanceledScope:         {operation: "HistoryClientRespondActivityTaskCanceled", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientGetMutableStateScope:                     {operation: "HistoryClientGetMutableState", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientPollMutableStateScope:                    {operation: "HistoryClientPollMutableState", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientResetStickyTaskQueueScope:                {operation: "HistoryClientResetStickyTaskQueueScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientDescribeWorkflowExecutionScope:           {operation: "HistoryClientDescribeWorkflowExecution", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRecordWorkflowTaskStartedScope:           {operation: "HistoryClientRecordWorkflowTaskStarted", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRecordActivityTaskStartedScope:           {operation: "HistoryClientRecordActivityTaskStarted", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRequestCancelWorkflowExecutionScope:      {operation: "HistoryClientRequestCancelWorkflowExecution", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientSignalWorkflowExecutionScope:             {operation: "HistoryClientSignalWorkflowExecution", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientSignalWithStartWorkflowExecutionScope:    {operation: "HistoryClientSignalWithStartWorkflowExecution", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRemoveSignalMutableStateScope:            {operation: "HistoryClientRemoveSignalMutableStateScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientTerminateWorkflowExecutionScope:          {operation: "HistoryClientTerminateWorkflowExecution", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientResetWorkflowExecutionScope:              {operation: "HistoryClientResetWorkflowExecution", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientScheduleWorkflowTaskScope:                {operation: "HistoryClientScheduleWorkflowTask", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRecordChildExecutionCompletedScope:       {operation: "HistoryClientRecordChildExecutionCompleted", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientReplicateEventsV2Scope:                   {operation: "HistoryClientReplicateEventsV2", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientSyncShardStatusScope:                     {operation: "HistoryClientSyncShardStatusScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientSyncActivityScope:                        {operation: "HistoryClientSyncActivityScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientGetReplicationTasksScope:                 {operation: "HistoryClientGetReplicationTasksScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientGetDLQReplicationTasksScope:              {operation: "HistoryClientGetDLQReplicationTasksScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientQueryWorkflowScope:                       {operation: "HistoryClientQueryWorkflowScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientReapplyEventsScope:                       {operation: "HistoryClientReapplyEventsScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientGetDLQMessagesScope:                      {operation: "HistoryClientGetDLQMessagesScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientPurgeDLQMessagesScope:                    {operation: "HistoryClientPurgeDLQMessagesScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientMergeDLQMessagesScope:                    {operation: "HistoryClientMergeDLQMessagesScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRefreshWorkflowTasksScope:                {operation: "HistoryClientRefreshWorkflowTasksScope", tags: map[string]string{ServiceRoleTagName: HistoryRoleTagValue}},
		MatchingClientPollWorkflowTaskQueueScope:              {operation: "MatchingClientPollWorkflowTaskQueue", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientPollActivityTaskQueueScope:              {operation: "MatchingClientPollActivityTaskQueue", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientAddActivityTaskScope:                    {operation: "MatchingClientAddActivityTask", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientAddWorkflowTaskScope:                    {operation: "MatchingClientAddWorkflowTask", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientQueryWorkflowScope:                      {operation: "MatchingClientQueryWorkflow", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientRespondQueryTaskCompletedScope:          {operation: "MatchingClientRespondQueryTaskCompleted", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientCancelOutstandingPollScope:              {operation: "MatchingClientCancelOutstandingPoll", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientDescribeTaskQueueScope:                  {operation: "MatchingClientDescribeTaskQueue", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		MatchingClientListTaskQueuePartitionsScope:            {operation: "MatchingClientListTaskQueuePartitions", tags: map[string]string{ServiceRoleTagName: MatchingRoleTagValue}},
		FrontendClientDeprecateNamespaceScope:                 {operation: "FrontendClientDeprecateNamespace", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientDescribeNamespaceScope:                  {operation: "FrontendClientDescribeNamespace", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientDescribeTaskQueueScope:                  {operation: "FrontendClientDescribeTaskQueue", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientDescribeWorkflowExecutionScope:          {operation: "FrontendClientDescribeWorkflowExecution", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientGetWorkflowExecutionHistoryScope:        {operation: "FrontendClientGetWorkflowExecutionHistory", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
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
		FrontendClientGetReplicationTasksScope:                {operation: "FrontendClientGetReplicationTasksScope", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientGetNamespaceReplicationTasksScope:       {operation: "FrontendClientGetNamespaceReplicationTasksScope", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientGetDLQReplicationTasksScope:             {operation: "FrontendClientGetDLQReplicationTasksScope", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientReapplyEventsScope:                      {operation: "FrontendClientReapplyEventsScope", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientGetClusterInfoScope:                     {operation: "FrontendClientGetClusterInfoScope", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		FrontendClientListTaskQueuePartitionsScope:            {operation: "FrontendClientListTaskQueuePartitions", tags: map[string]string{ServiceRoleTagName: FrontendRoleTagValue}},
		AdminClientAddSearchAttributesScope:                   {operation: "AdminClientAddSearchAttributes", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientRemoveSearchAttributesScope:                {operation: "AdminClientRemoveSearchAttributes", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientGetSearchAttributesScope:                   {operation: "AdminClientGetSearchAttributes", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientDescribeHistoryHostScope:                   {operation: "AdminClientDescribeHistoryHost", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientDescribeWorkflowMutableStateScope:          {operation: "AdminClientDescribeWorkflowMutableState", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientGetWorkflowExecutionRawHistoryScope:        {operation: "AdminClientGetWorkflowExecutionRawHistory", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientGetWorkflowExecutionRawHistoryV2Scope:      {operation: "AdminClientGetWorkflowExecutionRawHistoryV2", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientDescribeClusterScope:                       {operation: "AdminClientDescribeCluster", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientRefreshWorkflowTasksScope:                  {operation: "AdminClientRefreshWorkflowTasks", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientResendReplicationTasksScope:                {operation: "AdminClientResendReplicationTasks", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientCloseShardScope:                            {operation: "AdminClientCloseShard", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientGetDLQMessagesScope:                        {operation: "AdminClientGetDLQMessages", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientPurgeDLQMessagesScope:                      {operation: "AdminClientPurgeDLQMessages", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		AdminClientMergeDLQMessagesScope:                      {operation: "AdminClientMergeDLQMessages", tags: map[string]string{ServiceRoleTagName: AdminRoleTagValue}},
		DCRedirectionDeprecateNamespaceScope:                  {operation: "DCRedirectionDeprecateNamespace", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionDescribeNamespaceScope:                   {operation: "DCRedirectionDescribeNamespace", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionDescribeTaskQueueScope:                   {operation: "DCRedirectionDescribeTaskQueue", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionDescribeWorkflowExecutionScope:           {operation: "DCRedirectionDescribeWorkflowExecution", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionGetWorkflowExecutionHistoryScope:         {operation: "DCRedirectionGetWorkflowExecutionHistory", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionGetWorkflowExecutionRawHistoryScope:      {operation: "DCRedirectionGetWorkflowExecutionRawHistoryScope", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionPollForWorkflowExecutionRawHistoryScope:  {operation: "DCRedirectionPollForWorkflowExecutionRawHistoryScope", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionListArchivedWorkflowExecutionsScope:      {operation: "DCRedirectionListArchivedWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionListClosedWorkflowExecutionsScope:        {operation: "DCRedirectionListClosedWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionListNamespacesScope:                      {operation: "DCRedirectionListNamespaces", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionListOpenWorkflowExecutionsScope:          {operation: "DCRedirectionListOpenWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionListWorkflowExecutionsScope:              {operation: "DCRedirectionListWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionScanWorkflowExecutionsScope:              {operation: "DCRedirectionScanWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionCountWorkflowExecutionsScope:             {operation: "DCRedirectionCountWorkflowExecutions", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionGetSearchAttributesScope:                 {operation: "DCRedirectionGetSearchAttributes", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionPollActivityTaskQueueScope:               {operation: "DCRedirectionPollActivityTaskQueue", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionPollWorkflowTaskQueueScope:               {operation: "DCRedirectionPollWorkflowTaskQueue", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionQueryWorkflowScope:                       {operation: "DCRedirectionQueryWorkflow", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRecordActivityTaskHeartbeatScope:         {operation: "DCRedirectionRecordActivityTaskHeartbeat", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRecordActivityTaskHeartbeatByIdScope:     {operation: "DCRedirectionRecordActivityTaskHeartbeatById", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRegisterNamespaceScope:                   {operation: "DCRedirectionRegisterNamespace", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRequestCancelWorkflowExecutionScope:      {operation: "DCRedirectionRequestCancelWorkflowExecution", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionResetStickyTaskQueueScope:                {operation: "DCRedirectionResetStickyTaskQueue", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionResetWorkflowExecutionScope:              {operation: "DCRedirectionResetWorkflowExecution", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRespondActivityTaskCanceledScope:         {operation: "DCRedirectionRespondActivityTaskCanceled", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRespondActivityTaskCanceledByIdScope:     {operation: "DCRedirectionRespondActivityTaskCanceledById", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRespondActivityTaskCompletedScope:        {operation: "DCRedirectionRespondActivityTaskCompleted", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRespondActivityTaskCompletedByIdScope:    {operation: "DCRedirectionRespondActivityTaskCompletedById", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRespondActivityTaskFailedScope:           {operation: "DCRedirectionRespondActivityTaskFailed", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRespondActivityTaskFailedByIdScope:       {operation: "DCRedirectionRespondActivityTaskFailedById", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRespondWorkflowTaskCompletedScope:        {operation: "DCRedirectionRespondWorkflowTaskCompleted", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRespondWorkflowTaskFailedScope:           {operation: "DCRedirectionRespondWorkflowTaskFailed", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionRespondQueryTaskCompletedScope:           {operation: "DCRedirectionRespondQueryTaskCompleted", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionSignalWithStartWorkflowExecutionScope:    {operation: "DCRedirectionSignalWithStartWorkflowExecution", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionSignalWorkflowExecutionScope:             {operation: "DCRedirectionSignalWorkflowExecution", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionStartWorkflowExecutionScope:              {operation: "DCRedirectionStartWorkflowExecution", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionTerminateWorkflowExecutionScope:          {operation: "DCRedirectionTerminateWorkflowExecution", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionUpdateNamespaceScope:                     {operation: "DCRedirectionUpdateNamespace", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},
		DCRedirectionListTaskQueuePartitionsScope:             {operation: "DCRedirectionListTaskQueuePartitions", tags: map[string]string{ServiceRoleTagName: DCRedirectionRoleTagValue}},

		MessagingClientPublishScope:      {operation: "MessagingClientPublish"},
		MessagingClientPublishBatchScope: {operation: "MessagingClientPublishBatch"},

		NamespaceCacheScope:                                   {operation: "NamespaceCache"},
		HistoryRereplicationByTransferTaskScope:               {operation: "HistoryRereplicationByTransferTask"},
		HistoryRereplicationByTimerTaskScope:                  {operation: "HistoryRereplicationByTimerTask"},
		HistoryRereplicationByHistoryReplicationScope:         {operation: "HistoryRereplicationByHistoryReplication"},
		HistoryRereplicationByHistoryMetadataReplicationScope: {operation: "HistoryRereplicationByHistoryMetadataReplication"},
		HistoryRereplicationByActivityReplicationScope:        {operation: "HistoryRereplicationByActivityReplication"},

		ElasticsearchRecordWorkflowExecutionStartedScope:           {operation: "RecordWorkflowExecutionStarted"},
		ElasticsearchRecordWorkflowExecutionClosedScope:            {operation: "RecordWorkflowExecutionClosed"},
		ElasticsearchUpsertWorkflowExecutionScope:                  {operation: "UpsertWorkflowExecution"},
		ElasticsearchListOpenWorkflowExecutionsScope:               {operation: "ListOpenWorkflowExecutions"},
		ElasticsearchListClosedWorkflowExecutionsScope:             {operation: "ListClosedWorkflowExecutions"},
		ElasticsearchListOpenWorkflowExecutionsByTypeScope:         {operation: "ListOpenWorkflowExecutionsByType"},
		ElasticsearchListClosedWorkflowExecutionsByTypeScope:       {operation: "ListClosedWorkflowExecutionsByType"},
		ElasticsearchListOpenWorkflowExecutionsByWorkflowIDScope:   {operation: "ListOpenWorkflowExecutionsByWorkflowID"},
		ElasticsearchListClosedWorkflowExecutionsByWorkflowIDScope: {operation: "ListClosedWorkflowExecutionsByWorkflowID"},
		ElasticsearchListClosedWorkflowExecutionsByStatusScope:     {operation: "ListClosedWorkflowExecutionsByStatus"},
		ElasticsearchListWorkflowExecutionsScope:                   {operation: "ListWorkflowExecutions"},
		ElasticsearchScanWorkflowExecutionsScope:                   {operation: "ScanWorkflowExecutions"},
		ElasticsearchCountWorkflowExecutionsScope:                  {operation: "CountWorkflowExecutions"},
		ElasticsearchDeleteWorkflowExecutionsScope:                 {operation: "DeleteWorkflowExecution"},
		ElasticsearchBulkProcessor:                                 {operation: "ElasticsearchBulkProcessor"},
		ElasticsearchVisibility:                                    {operation: "ElasticsearchVisibility"},

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
	},
	// Frontend Scope Names
	Frontend: {
		// Admin API scope co-locates with with frontend
		AdminRemoveTaskScope:                       {operation: "AdminRemoveTask"},
		AdminCloseShardTaskScope:                   {operation: "AdminCloseShardTask"},
		AdminReadDLQMessagesScope:                  {operation: "AdminReadDLQMessages"},
		AdminPurgeDLQMessagesScope:                 {operation: "AdminPurgeDLQMessages"},
		AdminMergeDLQMessagesScope:                 {operation: "AdminMergeDLQMessages"},
		AdminDescribeHistoryHostScope:              {operation: "DescribeHistoryHost"},
		AdminAddSearchAttributesScope:              {operation: "AdminAddSearchAttributes"},
		AdminRemoveSearchAttributesScope:           {operation: "AdminRemoveSearchAttributes"},
		AdminGetSearchAttributesScope:              {operation: "AdminGetSearchAttributes"},
		AdminDescribeWorkflowExecutionScope:        {operation: "DescribeWorkflowExecution"},
		AdminGetWorkflowExecutionRawHistoryScope:   {operation: "GetWorkflowExecutionRawHistory"},
		AdminGetWorkflowExecutionRawHistoryV2Scope: {operation: "GetWorkflowExecutionRawHistoryV2"},
		AdminGetReplicationMessagesScope:           {operation: "GetReplicationMessages"},
		AdminGetNamespaceReplicationMessagesScope:  {operation: "GetNamespaceReplicationMessages"},
		AdminGetDLQReplicationMessagesScope:        {operation: "AdminGetDLQReplicationMessages"},
		AdminReapplyEventsScope:                    {operation: "ReapplyEvents"},
		AdminRefreshWorkflowTasksScope:             {operation: "RefreshWorkflowTasks"},
		AdminResendReplicationTasksScope:           {operation: "ResendReplicationTasks"},

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
		VersionCheckScope:                               {operation: "VersionCheck"},
		AuthorizationScope:                              {operation: "Authorization"},
	},
	// History Scope Names
	History: {
		HistoryStartWorkflowExecutionScope:           {operation: "StartWorkflowExecution"},
		HistoryRecordActivityTaskHeartbeatScope:      {operation: "RecordActivityTaskHeartbeat"},
		HistoryRespondWorkflowTaskCompletedScope:     {operation: "RespondWorkflowTaskCompleted"},
		HistoryRespondWorkflowTaskFailedScope:        {operation: "RespondWorkflowTaskFailed"},
		HistoryRespondActivityTaskCompletedScope:     {operation: "RespondActivityTaskCompleted"},
		HistoryRespondActivityTaskFailedScope:        {operation: "RespondActivityTaskFailed"},
		HistoryRespondActivityTaskCanceledScope:      {operation: "RespondActivityTaskCanceled"},
		HistoryGetMutableStateScope:                  {operation: "GetMutableState"},
		HistoryPollMutableStateScope:                 {operation: "PollMutableState"},
		HistoryResetStickyTaskQueueScope:             {operation: "ResetStickyTaskQueueScope"},
		HistoryDescribeWorkflowExecutionScope:        {operation: "DescribeWorkflowExecution"},
		HistoryRecordWorkflowTaskStartedScope:        {operation: "RecordWorkflowTaskStarted"},
		HistoryRecordActivityTaskStartedScope:        {operation: "RecordActivityTaskStarted"},
		HistorySignalWorkflowExecutionScope:          {operation: "SignalWorkflowExecution"},
		HistorySignalWithStartWorkflowExecutionScope: {operation: "SignalWithStartWorkflowExecution"},
		HistoryRemoveSignalMutableStateScope:         {operation: "RemoveSignalMutableState"},
		HistoryTerminateWorkflowExecutionScope:       {operation: "TerminateWorkflowExecution"},
		HistoryResetWorkflowExecutionScope:           {operation: "ResetWorkflowExecution"},
		HistoryQueryWorkflowScope:                    {operation: "QueryWorkflow"},
		HistoryProcessDeleteHistoryEventScope:        {operation: "ProcessDeleteHistoryEvent"},
		HistoryScheduleWorkflowTaskScope:             {operation: "ScheduleWorkflowTask"},
		HistoryRecordChildExecutionCompletedScope:    {operation: "RecordChildExecutionCompleted"},
		HistoryRequestCancelWorkflowExecutionScope:   {operation: "RequestCancelWorkflowExecution"},
		HistorySyncShardStatusScope:                  {operation: "SyncShardStatus"},
		HistorySyncActivityScope:                     {operation: "SyncActivity"},
		HistoryDescribeMutableStateScope:             {operation: "DescribeMutableState"},
		HistoryGetReplicationMessagesScope:           {operation: "GetReplicationMessages"},
		HistoryGetDLQReplicationMessagesScope:        {operation: "GetDLQReplicationMessages"},
		HistoryReadDLQMessagesScope:                  {operation: "GetDLQMessages"},
		HistoryPurgeDLQMessagesScope:                 {operation: "PurgeDLQMessages"},
		HistoryMergeDLQMessagesScope:                 {operation: "MergeDLQMessages"},
		HistoryShardControllerScope:                  {operation: "ShardController"},
		HistoryReapplyEventsScope:                    {operation: "EventReapplication"},
		HistoryRefreshWorkflowTasksScope:             {operation: "RefreshWorkflowTasks"},
		HistoryHistoryRemoveTaskScope:                {operation: "RemoveTask"},
		HistoryCloseShard:                            {operation: "CloseShard"},
		HistoryReplicateEventsV2:                     {operation: "ReplicateEventsV2"},
		HistoryResetStickyTaskQueue:                  {operation: "ResetStickyTaskQueue"},
		HistoryReapplyEvents:                         {operation: "ReapplyEvents"},
		HistoryDescribeHistoryHost:                   {operation: "DescribeHistoryHost"},
		TaskPriorityAssignerScope:                    {operation: "TaskPriorityAssigner"},
		TransferQueueProcessorScope:                  {operation: "TransferQueueProcessor"},
		TransferActiveQueueProcessorScope:            {operation: "TransferActiveQueueProcessor"},
		TransferStandbyQueueProcessorScope:           {operation: "TransferStandbyQueueProcessor"},
		TransferActiveTaskActivityScope:              {operation: "TransferActiveTaskActivity"},
		TransferActiveTaskWorkflowTaskScope:          {operation: "TransferActiveTaskWorkflowTask"},
		TransferActiveTaskCloseExecutionScope:        {operation: "TransferActiveTaskCloseExecution"},
		TransferActiveTaskCancelExecutionScope:       {operation: "TransferActiveTaskCancelExecution"},
		TransferActiveTaskSignalExecutionScope:       {operation: "TransferActiveTaskSignalExecution"},
		TransferActiveTaskStartChildExecutionScope:   {operation: "TransferActiveTaskStartChildExecution"},
		TransferActiveTaskResetWorkflowScope:         {operation: "TransferActiveTaskResetWorkflow"},
		TransferStandbyTaskActivityScope:             {operation: "TransferStandbyTaskActivity"},
		TransferStandbyTaskWorkflowTaskScope:         {operation: "TransferStandbyTaskWorkflowTask"},
		TransferStandbyTaskCloseExecutionScope:       {operation: "TransferStandbyTaskCloseExecution"},
		TransferStandbyTaskCancelExecutionScope:      {operation: "TransferStandbyTaskCancelExecution"},
		TransferStandbyTaskSignalExecutionScope:      {operation: "TransferStandbyTaskSignalExecution"},
		TransferStandbyTaskStartChildExecutionScope:  {operation: "TransferStandbyTaskStartChildExecution"},
		TransferStandbyTaskResetWorkflowScope:        {operation: "TransferStandbyTaskResetWorkflow"},

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
		ExecutionSizeStatsScope:                   {operation: "ExecutionStats", tags: map[string]string{StatsTypeTagName: SizeStatsTypeTagValue}},
		ExecutionCountStatsScope:                  {operation: "ExecutionStats", tags: map[string]string{StatsTypeTagName: CountStatsTypeTagValue}},
		SessionSizeStatsScope:                     {operation: "SessionStats", tags: map[string]string{StatsTypeTagName: SizeStatsTypeTagValue}},
		SessionCountStatsScope:                    {operation: "SessionStats", tags: map[string]string{StatsTypeTagName: CountStatsTypeTagValue}},
		WorkflowCompletionStatsScope:              {operation: "CompletionStats", tags: map[string]string{StatsTypeTagName: CountStatsTypeTagValue}},
		ArchiverClientScope:                       {operation: "ArchiverClient"},
		ReplicationTaskFetcherScope:               {operation: "ReplicationTaskFetcher"},
		ReplicationTaskCleanupScope:               {operation: "ReplicationTaskCleanup"},
		ReplicationDLQStatsScope:                  {operation: "ReplicationDLQStats"},
		SyncShardTaskScope:                        {operation: "SyncShardTask"},
		SyncActivityTaskScope:                     {operation: "SyncActivityTask"},
		HistoryMetadataReplicationTaskScope:       {operation: "HistoryMetadataReplicationTask"},
		HistoryReplicationTaskScope:               {operation: "HistoryReplicationTask"},
		ReplicatorScope:                           {operation: "Replicator"},
	},
	// Matching Scope Names
	Matching: {
		MatchingPollWorkflowTaskQueueScope:     {operation: "PollWorkflowTaskQueue"},
		MatchingPollActivityTaskQueueScope:     {operation: "PollActivityTaskQueue"},
		MatchingAddActivityTaskScope:           {operation: "AddActivityTask"},
		MatchingAddWorkflowTaskScope:           {operation: "AddWorkflowTask"},
		MatchingTaskQueueMgrScope:              {operation: "TaskQueueMgr"},
		MatchingQueryWorkflowScope:             {operation: "QueryWorkflow"},
		MatchingRespondQueryTaskCompletedScope: {operation: "RespondQueryTaskCompleted"},
		MatchingCancelOutstandingPollScope:     {operation: "CancelOutstandingPoll"},
		MatchingDescribeTaskQueueScope:         {operation: "DescribeTaskQueue"},
		MatchingListTaskQueuePartitionsScope:   {operation: "ListTaskQueuePartitions"},
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
	},
}

// Common Metrics enum
const (
	ServiceRequests = iota
	ServiceFailures
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

	PersistenceRequests
	PersistenceFailures
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
	PersistenceSampledCounter

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

	ElasticsearchRequests
	ElasticsearchFailures
	ElasticsearchLatency
	ElasticsearchErrBadRequestCounter
	ElasticsearchErrBusyCounter

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

	// The following metrics are only used by internal history archiver implemention.
	// TODO: move them to internal repo once temporal plugin model is in place.

	HistoryArchiverBlobExistsCount
	HistoryArchiverBlobSize
	HistoryArchiverRunningDeterministicConstructionCheckCount
	HistoryArchiverDeterministicConstructionCheckFailedCount
	HistoryArchiverRunningBlobIntegrityCheckCount
	HistoryArchiverBlobIntegrityCheckFailedCount
	HistoryArchiverDuplicateArchivalsCount

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

	ElasticsearchInvalidSearchAttributeCount

	NumCommonMetrics // Needs to be last on this list for iota numbering
)

// History Metrics enum
const (
	TaskRequests = iota + NumCommonMetrics
	TaskLatency
	TaskFailures
	TaskDiscarded
	TaskAttemptTimer
	TaskStandbyRetryCounter
	TaskNotActiveCounter
	TaskLimitExceededCounter
	TaskBatchCompleteCounter
	TaskProcessingLatency
	TaskNoUserProcessingLatency
	TaskQueueLatency
	TaskUserLatency
	TaskNoUserLatency
	TaskNoUserQueueLatency
	TaskRedispatchQueuePendingTasksTimer

	TransferTaskMissingEventCounter

	TransferTaskThrottledCounter
	TimerTaskThrottledCounter

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
	EmptyCompletionCommandsCounter
	MultipleCompletionCommandsCounter
	FailedWorkflowTasksCounter
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
	NewTimerCounter
	NewTimerNotifyCounter
	AcquireShardsCounter
	AcquireShardsLatency
	ShardClosedCounter
	ShardItemCreatedCounter
	ShardItemRemovedCounter
	ShardItemAcquisitionLatency
	ShardInfoReplicationPendingTasksTimer
	ShardInfoTransferActivePendingTasksTimer
	ShardInfoTransferStandbyPendingTasksTimer
	ShardInfoTimerActivePendingTasksTimer
	ShardInfoTimerStandbyPendingTasksTimer
	ShardInfoReplicationLagTimer
	ShardInfoTransferLagTimer
	ShardInfoTimerLagTimer
	ShardInfoTransferDiffTimer
	ShardInfoTimerDiffTimer
	ShardInfoTransferFailoverInProgressTimer
	ShardInfoTimerFailoverInProgressTimer
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
	ActivityInfoSize
	TimerInfoSize
	ChildInfoSize
	SignalInfoSize
	BufferedEventsSize
	ActivityInfoCount
	TimerInfoCount
	ChildInfoCount
	SignalInfoCount
	RequestCancelInfoCount
	BufferedEventsCount
	DeleteActivityInfoCount
	DeleteTimerInfoCount
	DeleteChildInfoCount
	DeleteSignalInfoCount
	DeleteRequestCancelInfoCount
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
	ElasticsearchBulkProcessorRetries
	ElasticsearchBulkProcessorFailures
	ElasticsearchBulkProcessorCorruptedData
	ElasticsearchBulkProcessorRequestLatency
	ElasticsearchBulkProcessorCommitLatency
	ElasticsearchBulkProcessorWaitLatency
	ElasticsearchBulkProcessorBulkSize
	ElasticsearchBulkProcessorDeadlock

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
	ForwardTaskCallsPerTaskQueue
	ForwardTaskErrorsPerTaskQueue
	ForwardTaskLatencyPerTaskQueue
	ForwardQueryCallsPerTaskQueue
	ForwardQueryErrorsPerTaskQueue
	ForwardQueryLatencyPerTaskQueue
	ForwardPollCallsPerTaskQueue
	ForwardPollErrorsPerTaskQueue
	ForwardPollLatencyPerTaskQueue
	LocalToLocalMatchPerTaskQueueCounter
	LocalToRemoteMatchPerTaskQueueCounter
	RemoteToLocalMatchPerTaskQueueCounter
	RemoteToRemoteMatchPerTaskQueueCounter

	NumMatchingMetrics
)

// Worker metrics enum
const (
	ReplicatorMessages = iota + NumMatchingMetrics
	ReplicatorFailures
	ReplicatorMessagesDropped
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

	NumWorkerMetrics
)

// MetricDefs record the metrics for all services
var MetricDefs = map[ServiceIdx]map[int]metricDefinition{
	Common: {
		ServiceRequests:                                     {metricName: "service_requests", metricType: Counter},
		ServiceFailures:                                     {metricName: "service_errors", metricType: Counter},
		ServiceCriticalFailures:                             {metricName: "service_errors_critical", metricType: Counter},
		ServiceLatency:                                      {metricName: "service_latency", metricType: Timer},
		ServiceLatencyNoUserLatency:                         {metricName: "service_latency_nouserlatency", metricType: Timer},
		ServiceLatencyUserLatency:                           {metricName: "service_latency_userlatency", metricType: Timer},
		ServiceErrInvalidArgumentCounter:                    {metricName: "service_errors_invalid_argument", metricType: Counter},
		ServiceErrNamespaceNotActiveCounter:                 {metricName: "service_errors_namespace_not_active", metricType: Counter},
		ServiceErrResourceExhaustedCounter:                  {metricName: "service_errors_resource_exhausted", metricType: Counter},
		ServiceErrNotFoundCounter:                           {metricName: "service_errors_entity_not_found", metricType: Counter},
		ServiceErrExecutionAlreadyStartedCounter:            {metricName: "service_errors_execution_already_started", metricType: Counter},
		ServiceErrNamespaceAlreadyExistsCounter:             {metricName: "service_errors_namespace_already_exists", metricType: Counter},
		ServiceErrCancellationAlreadyRequestedCounter:       {metricName: "service_errors_cancellation_already_requested", metricType: Counter},
		ServiceErrQueryFailedCounter:                        {metricName: "service_errors_query_failed", metricType: Counter},
		ServiceErrContextCancelledCounter:                   {metricName: "service_errors_context_cancelled", metricType: Counter},
		ServiceErrContextTimeoutCounter:                     {metricName: "service_errors_context_timeout", metricType: Counter},
		ServiceErrRetryTaskCounter:                          {metricName: "service_errors_retry_task", metricType: Counter},
		ServiceErrBadBinaryCounter:                          {metricName: "service_errors_bad_binary", metricType: Counter},
		ServiceErrClientVersionNotSupportedCounter:          {metricName: "service_errors_client_version_not_supported", metricType: Counter},
		ServiceErrIncompleteHistoryCounter:                  {metricName: "service_errors_incomplete_history", metricType: Counter},
		ServiceErrNonDeterministicCounter:                   {metricName: "service_errors_nondeterministic", metricType: Counter},
		ServiceErrUnauthorizedCounter:                       {metricName: "service_errors_unauthorized", metricType: Counter},
		ServiceErrAuthorizeFailedCounter:                    {metricName: "service_errors_authorize_failed", metricType: Counter},
		PersistenceRequests:                                 {metricName: "persistence_requests", metricType: Counter},
		PersistenceFailures:                                 {metricName: "persistence_errors", metricType: Counter},
		PersistenceLatency:                                  {metricName: "persistence_latency", metricType: Timer},
		PersistenceErrShardExistsCounter:                    {metricName: "persistence_errors_shard_exists", metricType: Counter},
		PersistenceErrShardOwnershipLostCounter:             {metricName: "persistence_errors_shard_ownership_lost", metricType: Counter},
		PersistenceErrConditionFailedCounter:                {metricName: "persistence_errors_condition_failed", metricType: Counter},
		PersistenceErrCurrentWorkflowConditionFailedCounter: {metricName: "persistence_errors_current_workflow_condition_failed", metricType: Counter},
		PersistenceErrWorkflowConditionFailedCounter:        {metricName: "persistence_errors_workflow_condition_failed", metricType: Counter},
		PersistenceErrTimeoutCounter:                        {metricName: "persistence_errors_timeout", metricType: Counter},
		PersistenceErrBusyCounter:                           {metricName: "persistence_errors_busy", metricType: Counter},
		PersistenceErrEntityNotExistsCounter:                {metricName: "persistence_errors_entity_not_exists", metricType: Counter},
		PersistenceErrNamespaceAlreadyExistsCounter:         {metricName: "persistence_errors_namespace_already_exists", metricType: Counter},
		PersistenceErrBadRequestCounter:                     {metricName: "persistence_errors_bad_request", metricType: Counter},
		PersistenceSampledCounter:                           {metricName: "persistence_sampled", metricType: Counter},
		ClientRequests:                                      {metricName: "client_requests", metricType: Counter},
		ClientFailures:                                      {metricName: "client_errors", metricType: Counter},
		ClientLatency:                                       {metricName: "client_latency", metricType: Timer},
		ClientRedirectionRequests:                           {metricName: "client_redirection_requests", metricType: Counter},
		ClientRedirectionFailures:                           {metricName: "client_redirection_errors", metricType: Counter},
		ClientRedirectionLatency:                            {metricName: "client_redirection_latency", metricType: Timer},
		ServiceAuthorizationLatency:                         {metricName: "service_authorization_latency", metricType: Timer},
		NamespaceCachePrepareCallbacksLatency:               {metricName: "namespace_cache_prepare_callbacks_latency", metricType: Timer},
		NamespaceCacheCallbacksLatency:                      {metricName: "namespace_cache_callbacks_latency", metricType: Timer},
		StateTransitionCount:                                {metricName: "state_transition_count", metricType: Timer},
		HistorySize:                                         {metricName: "history_size", metricType: Timer},
		HistoryCount:                                        {metricName: "history_count", metricType: Timer},
		EventBlobSize:                                       {metricName: "event_blob_size", metricType: Timer},
		SearchAttributesSize:                                {metricName: "search_attributes_size", metricType: Timer},
		LockRequests:                                        {metricName: "lock_requests", metricType: Counter},
		LockFailures:                                        {metricName: "lock_failures", metricType: Counter},
		LockLatency:                                         {metricName: "lock_latency", metricType: Timer},
		ArchivalConfigFailures:                              {metricName: "archivalconfig_failures", metricType: Counter},
		ElasticsearchRequests:                               {metricName: "elasticsearch_requests", metricType: Counter},
		ElasticsearchFailures:                               {metricName: "elasticsearch_errors", metricType: Counter},
		ElasticsearchLatency:                                {metricName: "elasticsearch_latency", metricType: Timer},
		ElasticsearchErrBadRequestCounter:                   {metricName: "elasticsearch_errors_bad_request", metricType: Counter},
		ElasticsearchErrBusyCounter:                         {metricName: "elasticsearch_errors_busy", metricType: Counter},
		SequentialTaskSubmitRequest:                         {metricName: "sequentialtask_submit_request", metricType: Counter},
		SequentialTaskSubmitRequestTaskQueueExist:           {metricName: "sequentialtask_submit_request_taskqueue_exist", metricType: Counter},
		SequentialTaskSubmitRequestTaskQueueMissing:         {metricName: "sequentialtask_submit_request_taskqueue_missing", metricType: Counter},
		SequentialTaskSubmitLatency:                         {metricName: "sequentialtask_submit_latency", metricType: Timer},
		SequentialTaskQueueSize:                             {metricName: "sequentialtask_queue_size", metricType: Timer},
		SequentialTaskQueueProcessingLatency:                {metricName: "sequentialtask_queue_processing_latency", metricType: Timer},
		SequentialTaskTaskProcessingLatency:                 {metricName: "sequentialtask_task_processing_latency", metricType: Timer},
		ParallelTaskSubmitRequest:                           {metricName: "paralleltask_submit_request", metricType: Counter},
		ParallelTaskSubmitLatency:                           {metricName: "paralleltask_submit_latency", metricType: Timer},
		ParallelTaskTaskProcessingLatency:                   {metricName: "paralleltask_task_processing_latency", metricType: Timer},
		PriorityTaskSubmitRequest:                           {metricName: "prioritytask_submit_request", metricType: Counter},
		PriorityTaskSubmitLatency:                           {metricName: "prioritytask_submit_latency", metricType: Timer},

		HistoryArchiverArchiveNonRetryableErrorCount:              {metricName: "history_archiver_archive_non_retryable_error", metricType: Counter},
		HistoryArchiverArchiveTransientErrorCount:                 {metricName: "history_archiver_archive_transient_error", metricType: Counter},
		HistoryArchiverArchiveSuccessCount:                        {metricName: "history_archiver_archive_success", metricType: Counter},
		HistoryArchiverHistoryMutatedCount:                        {metricName: "history_archiver_history_mutated", metricType: Counter},
		HistoryArchiverTotalUploadSize:                            {metricName: "history_archiver_total_upload_size", metricType: Timer},
		HistoryArchiverHistorySize:                                {metricName: "history_archiver_history_size", metricType: Timer},
		HistoryArchiverBlobExistsCount:                            {metricName: "history_archiver_blob_exists", metricType: Counter},
		HistoryArchiverBlobSize:                                   {metricName: "history_archiver_blob_size", metricType: Timer},
		HistoryArchiverRunningDeterministicConstructionCheckCount: {metricName: "history_archiver_running_deterministic_construction_check", metricType: Counter},
		HistoryArchiverDeterministicConstructionCheckFailedCount:  {metricName: "history_archiver_deterministic_construction_check_failed", metricType: Counter},
		HistoryArchiverRunningBlobIntegrityCheckCount:             {metricName: "history_archiver_running_blob_integrity_check", metricType: Counter},
		HistoryArchiverBlobIntegrityCheckFailedCount:              {metricName: "history_archiver_blob_integrity_check_failed", metricType: Counter},
		HistoryArchiverDuplicateArchivalsCount:                    {metricName: "history_archiver_duplicate_archivals", metricType: Counter},
		VisibilityArchiverArchiveNonRetryableErrorCount:           {metricName: "visibility_archiver_archive_non_retryable_error", metricType: Counter},
		VisibilityArchiverArchiveTransientErrorCount:              {metricName: "visibility_archiver_archive_transient_error", metricType: Counter},
		VisibilityArchiveSuccessCount:                             {metricName: "visibility_archiver_archive_success", metricType: Counter},
		VersionCheckSuccessCount:                                  {metricName: "version_check_success", metricType: Counter},
		VersionCheckFailedCount:                                   {metricName: "version_check_failed", metricType: Counter},
		VersionCheckRequestFailedCount:                            {metricName: "version_check_request_failed", metricType: Counter},
		VersionCheckLatency:                                       {metricName: "version_check_latency", metricType: Timer},

		ParentClosePolicyProcessorSuccess:  {metricName: "parent_close_policy_processor_requests", metricType: Counter},
		ParentClosePolicyProcessorFailures: {metricName: "parent_close_policy_processor_errors", metricType: Counter},

		AddSearchAttributesWorkflowSuccessCount:  {metricName: "add_search_attributes_workflow_success", metricType: Counter},
		AddSearchAttributesWorkflowFailuresCount: {metricName: "add_search_attributes_workflow_failure", metricType: Counter},

		MatchingClientForwardedCounter:     {metricName: "forwarded", metricType: Counter},
		MatchingClientInvalidTaskQueueName: {metricName: "invalid_task_queue_name", metricType: Counter},

		NamespaceReplicationTaskAckLevelGauge: {metricName: "namespace_replication_task_ack_level", metricType: Gauge},
		NamespaceReplicationDLQAckLevelGauge:  {metricName: "namespace_dlq_ack_level", metricType: Gauge},
		NamespaceReplicationDLQMaxLevelGauge:  {metricName: "namespace_dlq_max_level", metricType: Gauge},

		// per task queue common metrics

		ServiceRequestsPerTaskQueue: {
			metricName: "service_requests_per_tl", metricRollupName: "service_requests", metricType: Counter,
		},
		ServiceFailuresPerTaskQueue: {
			metricName: "service_errors_per_tl", metricRollupName: "service_errors", metricType: Counter,
		},
		ServiceLatencyPerTaskQueue: {
			metricName: "service_latency_per_tl", metricRollupName: "service_latency", metricType: Timer,
		},
		ServiceErrInvalidArgumentPerTaskQueueCounter: {
			metricName: "service_errors_invalid_argument_per_tl", metricRollupName: "service_errors_invalid_argument", metricType: Counter,
		},
		ServiceErrNamespaceNotActivePerTaskQueueCounter: {
			metricName: "service_errors_namespace_not_active_per_tl", metricRollupName: "service_errors_namespace_not_active", metricType: Counter,
		},
		ServiceErrResourceExhaustedPerTaskQueueCounter: {
			metricName: "service_errors_resource_exhausted_per_tl", metricRollupName: "service_errors_resource_exhausted", metricType: Counter,
		},
		ServiceErrNotFoundPerTaskQueueCounter: {
			metricName: "service_errors_entity_not_found_per_tl", metricRollupName: "service_errors_entity_not_found", metricType: Counter,
		},
		ServiceErrExecutionAlreadyStartedPerTaskQueueCounter: {
			metricName: "service_errors_execution_already_started_per_tl", metricRollupName: "service_errors_execution_already_started", metricType: Counter,
		},
		ServiceErrNamespaceAlreadyExistsPerTaskQueueCounter: {
			metricName: "service_errors_namespace_already_exists_per_tl", metricRollupName: "service_errors_namespace_already_exists", metricType: Counter,
		},
		ServiceErrCancellationAlreadyRequestedPerTaskQueueCounter: {
			metricName: "service_errors_cancellation_already_requested_per_tl", metricRollupName: "service_errors_cancellation_already_requested", metricType: Counter,
		},
		ServiceErrQueryFailedPerTaskQueueCounter: {
			metricName: "service_errors_query_failed_per_tl", metricRollupName: "service_errors_query_failed", metricType: Counter,
		},
		ServiceErrContextTimeoutPerTaskQueueCounter: {
			metricName: "service_errors_context_timeout_per_tl", metricRollupName: "service_errors_context_timeout", metricType: Counter,
		},
		ServiceErrRetryTaskPerTaskQueueCounter: {
			metricName: "service_errors_retry_task_per_tl", metricRollupName: "service_errors_retry_task", metricType: Counter,
		},
		ServiceErrBadBinaryPerTaskQueueCounter: {
			metricName: "service_errors_bad_binary_per_tl", metricRollupName: "service_errors_bad_binary", metricType: Counter,
		},
		ServiceErrClientVersionNotSupportedPerTaskQueueCounter: {
			metricName: "service_errors_client_version_not_supported_per_tl", metricRollupName: "service_errors_client_version_not_supported", metricType: Counter,
		},
		ServiceErrIncompleteHistoryPerTaskQueueCounter: {
			metricName: "service_errors_incomplete_history_per_tl", metricRollupName: "service_errors_incomplete_history", metricType: Counter,
		},
		ServiceErrNonDeterministicPerTaskQueueCounter: {
			metricName: "service_errors_nondeterministic_per_tl", metricRollupName: "service_errors_nondeterministic", metricType: Counter,
		},
		ServiceErrUnauthorizedPerTaskQueueCounter: {
			metricName: "service_errors_unauthorized_per_tl", metricRollupName: "service_errors_unauthorized", metricType: Counter,
		},
		ServiceErrAuthorizeFailedPerTaskQueueCounter: {
			metricName: "service_errors_authorize_failed_per_tl", metricRollupName: "service_errors_authorize_failed", metricType: Counter,
		},
		ElasticsearchInvalidSearchAttributeCount: {metricName: "elasticsearch_invalid_search_attribute_counter", metricType: Counter},
	},
	History: {
		TaskRequests: {metricName: "task_requests", metricType: Counter},

		TaskLatency:       {metricName: "task_latency", metricType: Timer},               // overall/all attempts within single worker
		TaskUserLatency:   {metricName: "task_latency_userlatency", metricType: Timer},   // from task generated to task complete
		TaskNoUserLatency: {metricName: "task_latency_nouserlatency", metricType: Timer}, // from task generated to task complete

		TaskAttemptTimer:         {metricName: "task_attempt", metricType: Timer},
		TaskFailures:             {metricName: "task_errors", metricType: Counter},
		TaskDiscarded:            {metricName: "task_errors_discarded", metricType: Counter},
		TaskStandbyRetryCounter:  {metricName: "task_errors_standby_retry_counter", metricType: Counter},
		TaskNotActiveCounter:     {metricName: "task_errors_not_active_counter", metricType: Counter},
		TaskLimitExceededCounter: {metricName: "task_errors_limit_exceeded_counter", metricType: Counter},

		TaskProcessingLatency:       {metricName: "task_latency_processing", metricType: Timer},               // per-attempt
		TaskNoUserProcessingLatency: {metricName: "task_latency_processing_nouserlatency", metricType: Timer}, // per-attempt

		TaskQueueLatency:       {metricName: "task_latency_queue", metricType: Timer},               // from task generated to task complete
		TaskNoUserQueueLatency: {metricName: "task_latency_queue_nouserlatency", metricType: Timer}, // from task generated to task complete

		TransferTaskMissingEventCounter:                   {metricName: "transfer_task_missing_event_counter", metricType: Counter},
		TaskBatchCompleteCounter:                          {metricName: "task_batch_complete_counter", metricType: Counter},
		TaskRedispatchQueuePendingTasksTimer:              {metricName: "task_redispatch_queue_pending_tasks", metricType: Timer},
		TransferTaskThrottledCounter:                      {metricName: "transfer_task_throttled_counter", metricType: Counter},
		TimerTaskThrottledCounter:                         {metricName: "timer_task_throttled_counter", metricType: Counter},
		ActivityE2ELatency:                                {metricName: "activity_end_to_end_latency", metricType: Timer},
		AckLevelUpdateCounter:                             {metricName: "ack_level_update", metricType: Counter},
		AckLevelUpdateFailedCounter:                       {metricName: "ack_level_update_failed", metricType: Counter},
		CommandTypeScheduleActivityCounter:                {metricName: "schedule_activity_command", metricType: Counter},
		CommandTypeCompleteWorkflowCounter:                {metricName: "complete_workflow_command", metricType: Counter},
		CommandTypeFailWorkflowCounter:                    {metricName: "fail_workflow_command", metricType: Counter},
		CommandTypeCancelWorkflowCounter:                  {metricName: "cancel_workflow_command", metricType: Counter},
		CommandTypeStartTimerCounter:                      {metricName: "start_timer_command", metricType: Counter},
		CommandTypeCancelActivityCounter:                  {metricName: "cancel_activity_command", metricType: Counter},
		CommandTypeCancelTimerCounter:                     {metricName: "cancel_timer_command", metricType: Counter},
		CommandTypeRecordMarkerCounter:                    {metricName: "record_marker_command", metricType: Counter},
		CommandTypeCancelExternalWorkflowCounter:          {metricName: "cancel_external_workflow_command", metricType: Counter},
		CommandTypeContinueAsNewCounter:                   {metricName: "continue_as_new_command", metricType: Counter},
		CommandTypeSignalExternalWorkflowCounter:          {metricName: "signal_external_workflow_command", metricType: Counter},
		CommandTypeUpsertWorkflowSearchAttributesCounter:  {metricName: "upsert_workflow_search_attributes_command", metricType: Counter},
		CommandTypeChildWorkflowCounter:                   {metricName: "child_workflow_command", metricType: Counter},
		EmptyCompletionCommandsCounter:                    {metricName: "empty_completion_commands", metricType: Counter},
		MultipleCompletionCommandsCounter:                 {metricName: "multiple_completion_commands", metricType: Counter},
		FailedWorkflowTasksCounter:                        {metricName: "failed_workflow_tasks", metricType: Counter},
		StaleMutableStateCounter:                          {metricName: "stale_mutable_state", metricType: Counter},
		AutoResetPointsLimitExceededCounter:               {metricName: "auto_reset_points_exceed_limit", metricType: Counter},
		AutoResetPointCorruptionCounter:                   {metricName: "auto_reset_point_corruption", metricType: Counter},
		ConcurrencyUpdateFailureCounter:                   {metricName: "concurrency_update_failure", metricType: Counter},
		ServiceErrShardOwnershipLostCounter:               {metricName: "service_errors_shard_ownership_lost", metricType: Counter},
		ServiceErrTaskAlreadyStartedCounter:               {metricName: "service_errors_task_already_started", metricType: Counter},
		HeartbeatTimeoutCounter:                           {metricName: "heartbeat_timeout", metricType: Counter},
		ScheduleToStartTimeoutCounter:                     {metricName: "schedule_to_start_timeout", metricType: Counter},
		StartToCloseTimeoutCounter:                        {metricName: "start_to_close_timeout", metricType: Counter},
		ScheduleToCloseTimeoutCounter:                     {metricName: "schedule_to_close_timeout", metricType: Counter},
		NewTimerCounter:                                   {metricName: "new_timer", metricType: Counter},
		NewTimerNotifyCounter:                             {metricName: "new_timer_notifications", metricType: Counter},
		AcquireShardsCounter:                              {metricName: "acquire_shards_count", metricType: Counter},
		AcquireShardsLatency:                              {metricName: "acquire_shards_latency", metricType: Timer},
		ShardClosedCounter:                                {metricName: "shard_closed_count", metricType: Counter},
		ShardItemCreatedCounter:                           {metricName: "sharditem_created_count", metricType: Counter},
		ShardItemRemovedCounter:                           {metricName: "sharditem_removed_count", metricType: Counter},
		ShardItemAcquisitionLatency:                       {metricName: "sharditem_acquisition_latency", metricType: Timer},
		ShardInfoReplicationPendingTasksTimer:             {metricName: "shardinfo_replication_pending_task", metricType: Timer},
		ShardInfoTransferActivePendingTasksTimer:          {metricName: "shardinfo_transfer_active_pending_task", metricType: Timer},
		ShardInfoTransferStandbyPendingTasksTimer:         {metricName: "shardinfo_transfer_standby_pending_task", metricType: Timer},
		ShardInfoTimerActivePendingTasksTimer:             {metricName: "shardinfo_timer_active_pending_task", metricType: Timer},
		ShardInfoTimerStandbyPendingTasksTimer:            {metricName: "shardinfo_timer_standby_pending_task", metricType: Timer},
		ShardInfoReplicationLagTimer:                      {metricName: "shardinfo_replication_lag", metricType: Timer},
		ShardInfoTransferLagTimer:                         {metricName: "shardinfo_transfer_lag", metricType: Timer},
		ShardInfoTimerLagTimer:                            {metricName: "shardinfo_timer_lag", metricType: Timer},
		ShardInfoTransferDiffTimer:                        {metricName: "shardinfo_transfer_diff", metricType: Timer},
		ShardInfoTimerDiffTimer:                           {metricName: "shardinfo_timer_diff", metricType: Timer},
		ShardInfoTransferFailoverInProgressTimer:          {metricName: "shardinfo_transfer_failover_in_progress", metricType: Timer},
		ShardInfoTimerFailoverInProgressTimer:             {metricName: "shardinfo_timer_failover_in_progress", metricType: Timer},
		ShardInfoTransferFailoverLatencyTimer:             {metricName: "shardinfo_transfer_failover_latency", metricType: Timer},
		ShardInfoTimerFailoverLatencyTimer:                {metricName: "shardinfo_timer_failover_latency", metricType: Timer},
		SyncShardFromRemoteCounter:                        {metricName: "syncshard_remote_count", metricType: Counter},
		SyncShardFromRemoteFailure:                        {metricName: "syncshard_remote_failed", metricType: Counter},
		MembershipChangedCounter:                          {metricName: "membership_changed_count", metricType: Counter},
		NumShardsGauge:                                    {metricName: "numshards_gauge", metricType: Gauge},
		GetEngineForShardErrorCounter:                     {metricName: "get_engine_for_shard_errors", metricType: Counter},
		GetEngineForShardLatency:                          {metricName: "get_engine_for_shard_latency", metricType: Timer},
		RemoveEngineForShardLatency:                       {metricName: "remove_engine_for_shard_latency", metricType: Timer},
		CompleteWorkflowTaskWithStickyEnabledCounter:      {metricName: "complete_workflow_task_sticky_enabled_count", metricType: Counter},
		CompleteWorkflowTaskWithStickyDisabledCounter:     {metricName: "complete_workflow_task_sticky_disabled_count", metricType: Counter},
		WorkflowTaskHeartbeatTimeoutCounter:               {metricName: "workflow_task_heartbeat_timeout_count", metricType: Counter},
		HistoryEventNotificationQueueingLatency:           {metricName: "history_event_notification_queueing_latency", metricType: Timer},
		HistoryEventNotificationFanoutLatency:             {metricName: "history_event_notification_fanout_latency", metricType: Timer},
		HistoryEventNotificationInFlightMessageGauge:      {metricName: "history_event_notification_inflight_message_gauge", metricType: Gauge},
		HistoryEventNotificationFailDeliveryCount:         {metricName: "history_event_notification_fail_delivery_count", metricType: Counter},
		EmptyReplicationEventsCounter:                     {metricName: "empty_replication_events", metricType: Counter},
		DuplicateReplicationEventsCounter:                 {metricName: "duplicate_replication_events", metricType: Counter},
		StaleReplicationEventsCounter:                     {metricName: "stale_replication_events", metricType: Counter},
		ReplicationEventsSizeTimer:                        {metricName: "replication_events_size", metricType: Timer},
		BufferReplicationTaskTimer:                        {metricName: "buffer_replication_tasks", metricType: Timer},
		UnbufferReplicationTaskTimer:                      {metricName: "unbuffer_replication_tasks", metricType: Timer},
		HistoryConflictsCounter:                           {metricName: "history_conflicts", metricType: Counter},
		CompleteTaskFailedCounter:                         {metricName: "complete_task_fail_count", metricType: Counter},
		CacheRequests:                                     {metricName: "cache_requests", metricType: Counter},
		CacheFailures:                                     {metricName: "cache_errors", metricType: Counter},
		CacheLatency:                                      {metricName: "cache_latency", metricType: Timer},
		CacheMissCounter:                                  {metricName: "cache_miss", metricType: Counter},
		AcquireLockFailedCounter:                          {metricName: "acquire_lock_failed", metricType: Counter},
		WorkflowContextCleared:                            {metricName: "workflow_context_cleared", metricType: Counter},
		MutableStateSize:                                  {metricName: "mutable_state_size", metricType: Timer},
		ExecutionInfoSize:                                 {metricName: "execution_info_size", metricType: Timer},
		ActivityInfoSize:                                  {metricName: "activity_info_size", metricType: Timer},
		TimerInfoSize:                                     {metricName: "timer_info_size", metricType: Timer},
		ChildInfoSize:                                     {metricName: "child_info_size", metricType: Timer},
		SignalInfoSize:                                    {metricName: "signal_info_size", metricType: Timer},
		BufferedEventsSize:                                {metricName: "buffered_events_size", metricType: Timer},
		ActivityInfoCount:                                 {metricName: "activity_info_count", metricType: Timer},
		TimerInfoCount:                                    {metricName: "timer_info_count", metricType: Timer},
		ChildInfoCount:                                    {metricName: "child_info_count", metricType: Timer},
		SignalInfoCount:                                   {metricName: "signal_info_count", metricType: Timer},
		RequestCancelInfoCount:                            {metricName: "request_cancel_info_count", metricType: Timer},
		BufferedEventsCount:                               {metricName: "buffered_events_count", metricType: Timer},
		DeleteActivityInfoCount:                           {metricName: "delete_activity_info", metricType: Timer},
		DeleteTimerInfoCount:                              {metricName: "delete_timer_info", metricType: Timer},
		DeleteChildInfoCount:                              {metricName: "delete_child_info", metricType: Timer},
		DeleteSignalInfoCount:                             {metricName: "delete_signal_info", metricType: Timer},
		DeleteRequestCancelInfoCount:                      {metricName: "delete_request_cancel_info", metricType: Timer},
		WorkflowRetryBackoffTimerCount:                    {metricName: "workflow_retry_backoff_timer", metricType: Counter},
		WorkflowCronBackoffTimerCount:                     {metricName: "workflow_cron_backoff_timer", metricType: Counter},
		WorkflowCleanupDeleteCount:                        {metricName: "workflow_cleanup_delete", metricType: Counter},
		WorkflowCleanupArchiveCount:                       {metricName: "workflow_cleanup_archive", metricType: Counter},
		WorkflowCleanupNopCount:                           {metricName: "workflow_cleanup_nop", metricType: Counter},
		WorkflowCleanupDeleteHistoryInlineCount:           {metricName: "workflow_cleanup_delete_history_inline", metricType: Counter},
		WorkflowSuccessCount:                              {metricName: "workflow_success", metricType: Counter},
		WorkflowCancelCount:                               {metricName: "workflow_cancel", metricType: Counter},
		WorkflowFailedCount:                               {metricName: "workflow_failed", metricType: Counter},
		WorkflowTimeoutCount:                              {metricName: "workflow_timeout", metricType: Counter},
		WorkflowTerminateCount:                            {metricName: "workflow_terminate", metricType: Counter},
		ArchiverClientSendSignalCount:                     {metricName: "archiver_client_sent_signal", metricType: Counter},
		ArchiverClientSendSignalFailureCount:              {metricName: "archiver_client_send_signal_error", metricType: Counter},
		ArchiverClientHistoryRequestCount:                 {metricName: "archiver_client_history_request", metricType: Counter},
		ArchiverClientHistoryInlineArchiveAttemptCount:    {metricName: "archiver_client_history_inline_archive_attempt", metricType: Counter},
		ArchiverClientHistoryInlineArchiveFailureCount:    {metricName: "archiver_client_history_inline_archive_failure", metricType: Counter},
		ArchiverClientVisibilityRequestCount:              {metricName: "archiver_client_visibility_request", metricType: Counter},
		ArchiverClientVisibilityInlineArchiveAttemptCount: {metricName: "archiver_client_visibility_inline_archive_attempt", metricType: Counter},
		ArchiverClientVisibilityInlineArchiveFailureCount: {metricName: "archiver_client_visibility_inline_archive_failure", metricType: Counter},
		LastRetrievedMessageID:                            {metricName: "last_retrieved_message_id", metricType: Gauge},
		LastProcessedMessageID:                            {metricName: "last_processed_message_id", metricType: Gauge},
		ReplicationTasksApplied:                           {metricName: "replication_tasks_applied", metricType: Counter},
		ReplicationTasksFailed:                            {metricName: "replication_tasks_failed", metricType: Counter},
		ReplicationTasksLag:                               {metricName: "replication_tasks_lag", metricType: Timer},
		ReplicationTasksFetched:                           {metricName: "replication_tasks_fetched", metricType: Timer},
		ReplicationTasksReturned:                          {metricName: "replication_tasks_returned", metricType: Timer},
		ReplicationTasksAppliedLatency:                    {metricName: "replication_tasks_applied_latency", metricType: Timer},
		ReplicationDLQFailed:                              {metricName: "replication_dlq_enqueue_failed", metricType: Counter},
		ReplicationDLQMaxLevelGauge:                       {metricName: "replication_dlq_max_level", metricType: Gauge},
		ReplicationDLQAckLevelGauge:                       {metricName: "replication_dlq_ack_level", metricType: Gauge},
		GetReplicationMessagesForShardLatency:             {metricName: "get_replication_messages_for_shard", metricType: Timer},
		GetDLQReplicationMessagesLatency:                  {metricName: "get_dlq_replication_messages", metricType: Timer},
		EventReapplySkippedCount:                          {metricName: "event_reapply_skipped_count", metricType: Counter},
		DirectQueryDispatchLatency:                        {metricName: "direct_query_dispatch_latency", metricType: Timer},
		DirectQueryDispatchStickyLatency:                  {metricName: "direct_query_dispatch_sticky_latency", metricType: Timer},
		DirectQueryDispatchNonStickyLatency:               {metricName: "direct_query_dispatch_non_sticky_latency", metricType: Timer},
		DirectQueryDispatchStickySuccessCount:             {metricName: "direct_query_dispatch_sticky_success", metricType: Counter},
		DirectQueryDispatchNonStickySuccessCount:          {metricName: "direct_query_dispatch_non_sticky_success", metricType: Counter},
		DirectQueryDispatchClearStickinessLatency:         {metricName: "direct_query_dispatch_clear_stickiness_latency", metricType: Timer},
		DirectQueryDispatchClearStickinessSuccessCount:    {metricName: "direct_query_dispatch_clear_stickiness_success", metricType: Counter},
		DirectQueryDispatchTimeoutBeforeNonStickyCount:    {metricName: "direct_query_dispatch_timeout_before_non_sticky", metricType: Counter},
		WorkflowTaskQueryLatency:                          {metricName: "workflow_task_query_latency", metricType: Timer},
		ConsistentQueryTimeoutCount:                       {metricName: "consistent_query_timeout", metricType: Counter},
		QueryBeforeFirstWorkflowTaskCount:                 {metricName: "query_before_first_workflow_task", metricType: Counter},
		QueryBufferExceededCount:                          {metricName: "query_buffer_exceeded", metricType: Counter},
		QueryRegistryInvalidStateCount:                    {metricName: "query_registry_invalid_state", metricType: Counter},
		WorkerNotSupportsConsistentQueryCount:             {metricName: "worker_not_supports_consistent_query", metricType: Counter},
		WorkflowTaskTimeoutOverrideCount:                  {metricName: "workflow_task_timeout_overrides", metricType: Counter},
		WorkflowRunTimeoutOverrideCount:                   {metricName: "workflow_run_timeout_overrides", metricType: Counter},
		ReplicationTaskCleanupCount:                       {metricName: "replication_task_cleanup_count", metricType: Counter},
		ReplicationTaskCleanupFailure:                     {metricName: "replication_task_cleanup_failed", metricType: Counter},
		MutableStateChecksumMismatch:                      {metricName: "mutable_state_checksum_mismatch", metricType: Counter},
		MutableStateChecksumInvalidated:                   {metricName: "mutable_state_checksum_invalidated", metricType: Counter},

		ElasticsearchBulkProcessorRequests:       {metricName: "elasticsearch_bulk_processor_requests"},
		ElasticsearchBulkProcessorRetries:        {metricName: "elasticsearch_bulk_processor_retries"},
		ElasticsearchBulkProcessorFailures:       {metricName: "elasticsearch_bulk_processor_errors"},
		ElasticsearchBulkProcessorCorruptedData:  {metricName: "elasticsearch_bulk_processor_corrupted_data"},
		ElasticsearchBulkProcessorRequestLatency: {metricName: "elasticsearch_bulk_processor_request_latency", metricType: Timer},
		ElasticsearchBulkProcessorCommitLatency:  {metricName: "elasticsearch_bulk_processor_commit_latency", metricType: Timer},
		ElasticsearchBulkProcessorWaitLatency:    {metricName: "elasticsearch_bulk_processor_wait_latency", metricType: Timer},
		ElasticsearchBulkProcessorBulkSize:       {metricName: "elasticsearch_bulk_processor_bulk_size", metricType: Timer},
		ElasticsearchBulkProcessorDeadlock:       {metricName: "elasticsearch_bulk_processor_deadlock"},
	},
	Matching: {
		PollSuccessPerTaskQueueCounter:            {metricName: "poll_success_per_tl", metricRollupName: "poll_success"},
		PollTimeoutPerTaskQueueCounter:            {metricName: "poll_timeouts_per_tl", metricRollupName: "poll_timeouts"},
		PollSuccessWithSyncPerTaskQueueCounter:    {metricName: "poll_success_sync_per_tl", metricRollupName: "poll_success_sync"},
		LeaseRequestPerTaskQueueCounter:           {metricName: "lease_requests_per_tl", metricRollupName: "lease_requests"},
		LeaseFailurePerTaskQueueCounter:           {metricName: "lease_failures_per_tl", metricRollupName: "lease_failures"},
		ConditionFailedErrorPerTaskQueueCounter:   {metricName: "condition_failed_errors_per_tl", metricRollupName: "condition_failed_errors"},
		RespondQueryTaskFailedPerTaskQueueCounter: {metricName: "respond_query_failed_per_tl", metricRollupName: "respond_query_failed"},
		SyncThrottlePerTaskQueueCounter:           {metricName: "sync_throttle_count_per_tl", metricRollupName: "sync_throttle_count"},
		BufferThrottlePerTaskQueueCounter:         {metricName: "buffer_throttle_count_per_tl", metricRollupName: "buffer_throttle_count"},
		ExpiredTasksPerTaskQueueCounter:           {metricName: "tasks_expired_per_tl", metricRollupName: "tasks_expired"},
		ForwardedPerTaskQueueCounter:              {metricName: "forwarded_per_tl"},
		ForwardTaskCallsPerTaskQueue:              {metricName: "forward_task_calls_per_tl", metricRollupName: "forward_task_calls"},
		ForwardTaskErrorsPerTaskQueue:             {metricName: "forward_task_errors_per_tl", metricRollupName: "forward_task_errors"},
		ForwardQueryCallsPerTaskQueue:             {metricName: "forward_query_calls_per_tl", metricRollupName: "forward_query_calls"},
		ForwardQueryErrorsPerTaskQueue:            {metricName: "forward_query_errors_per_tl", metricRollupName: "forward_query_errors"},
		ForwardPollCallsPerTaskQueue:              {metricName: "forward_poll_calls_per_tl", metricRollupName: "forward_poll_calls"},
		ForwardPollErrorsPerTaskQueue:             {metricName: "forward_poll_errors_per_tl", metricRollupName: "forward_poll_errors"},
		SyncMatchLatencyPerTaskQueue:              {metricName: "syncmatch_latency_per_tl", metricRollupName: "syncmatch_latency", metricType: Timer},
		AsyncMatchLatencyPerTaskQueue:             {metricName: "asyncmatch_latency_per_tl", metricRollupName: "asyncmatch_latency", metricType: Timer},
		ForwardTaskLatencyPerTaskQueue:            {metricName: "forward_task_latency_per_tl", metricRollupName: "forward_task_latency"},
		ForwardQueryLatencyPerTaskQueue:           {metricName: "forward_query_latency_per_tl", metricRollupName: "forward_query_latency"},
		ForwardPollLatencyPerTaskQueue:            {metricName: "forward_poll_latency_per_tl", metricRollupName: "forward_poll_latency"},
		LocalToLocalMatchPerTaskQueueCounter:      {metricName: "local_to_local_matches_per_tl", metricRollupName: "local_to_local_matches"},
		LocalToRemoteMatchPerTaskQueueCounter:     {metricName: "local_to_remote_matches_per_tl", metricRollupName: "local_to_remote_matches"},
		RemoteToLocalMatchPerTaskQueueCounter:     {metricName: "remote_to_local_matches_per_tl", metricRollupName: "remote_to_local_matches"},
		RemoteToRemoteMatchPerTaskQueueCounter:    {metricName: "remote_to_remote_matches_per_tl", metricRollupName: "remote_to_remote_matches"},
	},
	Worker: {
		ReplicatorMessages:                            {metricName: "replicator_messages"},
		ReplicatorFailures:                            {metricName: "replicator_errors"},
		ReplicatorMessagesDropped:                     {metricName: "replicator_messages_dropped"},
		ReplicatorLatency:                             {metricName: "replicator_latency"},
		ReplicatorDLQFailures:                         {metricName: "replicator_dlq_enqueue_fails", metricType: Counter},
		ArchiverNonRetryableErrorCount:                {metricName: "archiver_non_retryable_error"},
		ArchiverStartedCount:                          {metricName: "archiver_started"},
		ArchiverStoppedCount:                          {metricName: "archiver_stopped"},
		ArchiverCoroutineStartedCount:                 {metricName: "archiver_coroutine_started"},
		ArchiverCoroutineStoppedCount:                 {metricName: "archiver_coroutine_stopped"},
		ArchiverHandleHistoryRequestLatency:           {metricName: "archiver_handle_history_request_latency"},
		ArchiverHandleVisibilityRequestLatency:        {metricName: "archiver_handle_visibility_request_latency"},
		ArchiverUploadWithRetriesLatency:              {metricName: "archiver_upload_with_retries_latency"},
		ArchiverDeleteWithRetriesLatency:              {metricName: "archiver_delete_with_retries_latency"},
		ArchiverUploadFailedAllRetriesCount:           {metricName: "archiver_upload_failed_all_retries"},
		ArchiverUploadSuccessCount:                    {metricName: "archiver_upload_success"},
		ArchiverDeleteFailedAllRetriesCount:           {metricName: "archiver_delete_failed_all_retries"},
		ArchiverDeleteSuccessCount:                    {metricName: "archiver_delete_success"},
		ArchiverHandleVisibilityFailedAllRetiresCount: {metricName: "archiver_handle_visibility_failed_all_retries"},
		ArchiverHandleVisibilitySuccessCount:          {metricName: "archiver_handle_visibility_success"},
		ArchiverBacklogSizeGauge:                      {metricName: "archiver_backlog_size"},
		ArchiverPumpTimeoutCount:                      {metricName: "archiver_pump_timeout"},
		ArchiverPumpSignalThresholdCount:              {metricName: "archiver_pump_signal_threshold"},
		ArchiverPumpTimeoutWithoutSignalsCount:        {metricName: "archiver_pump_timeout_without_signals"},
		ArchiverPumpSignalChannelClosedCount:          {metricName: "archiver_pump_signal_channel_closed"},
		ArchiverWorkflowStartedCount:                  {metricName: "archiver_workflow_started"},
		ArchiverNumPumpedRequestsCount:                {metricName: "archiver_num_pumped_requests"},
		ArchiverNumHandledRequestsCount:               {metricName: "archiver_num_handled_requests"},
		ArchiverPumpedNotEqualHandledCount:            {metricName: "archiver_pumped_not_equal_handled"},
		ArchiverHandleAllRequestsLatency:              {metricName: "archiver_handle_all_requests_latency"},
		ArchiverWorkflowStoppingCount:                 {metricName: "archiver_workflow_stopping"},
		TaskProcessedCount:                            {metricName: "task_processed", metricType: Gauge},
		TaskDeletedCount:                              {metricName: "task_deleted", metricType: Gauge},
		TaskQueueProcessedCount:                       {metricName: "taskqueue_processed", metricType: Gauge},
		TaskQueueDeletedCount:                         {metricName: "taskqueue_deleted", metricType: Gauge},
		TaskQueueOutstandingCount:                     {metricName: "taskqueue_outstanding", metricType: Gauge},
		ExecutionsOutstandingCount:                    {metricName: "executions_outstanding", metricType: Gauge},
		StartedCount:                                  {metricName: "started", metricType: Counter},
		StoppedCount:                                  {metricName: "stopped", metricType: Counter},
		ScanDuration:                                  {metricName: "scan_duration", metricType: Timer},
		ExecutorTasksDoneCount:                        {metricName: "executor_done", metricType: Counter},
		ExecutorTasksErrCount:                         {metricName: "executor_err", metricType: Counter},
		ExecutorTasksDeferredCount:                    {metricName: "executor_deferred", metricType: Counter},
		ExecutorTasksDroppedCount:                     {metricName: "executor_dropped", metricType: Counter},
		BatcherProcessorSuccess:                       {metricName: "batcher_processor_requests", metricType: Counter},
		BatcherProcessorFailures:                      {metricName: "batcher_processor_errors", metricType: Counter},
		HistoryScavengerSuccessCount:                  {metricName: "scavenger_success", metricType: Counter},
		HistoryScavengerErrorCount:                    {metricName: "scavenger_errors", metricType: Counter},
		HistoryScavengerSkipCount:                     {metricName: "scavenger_skips", metricType: Counter},
		NamespaceReplicationEnqueueDLQCount:           {metricName: "namespace_replication_dlq_enqueue_requests", metricType: Counter},
		ScavengerValidationRequestsCount:              {metricName: "scavenger_validation_requests", metricType: Counter},
		ScavengerValidationFailuresCount:              {metricName: "scavenger_validation_failures", metricType: Counter},
		AddSearchAttributesFailuresCount:              {metricName: "add_search_attributes_failures", metricType: Counter},
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
