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

package metrics

// types used/defined by the package
type (
	// MetricName is the name of the metric
	MetricName string

	// MetricType is the type of the metric
	MetricType int

	// metricDefinition contains the definition for a metric
	metricDefinition struct {
		metricType MetricType // metric type
		metricName MetricName // metric name
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

// Common tags for all services
const (
	HostnameTagName  = "hostname"
	OperationTagName = "operation"
	// ShardTagName is temporary until we can get all metric data removed for the service
	ShardTagName       = "shard"
	CadenceRoleTagName = "cadence-role"
	StatsTypeTagName   = "stats-type"
)

// This package should hold all the metrics and tags for cadence
const (
	UnknownDirectoryTagValue = "Unknown"
	AllShardsTagValue        = "ALL"
	NoneShardsTagValue       = "NONE"

	HistoryRoleTagValue  = "history"
	MatchingRoleTagValue = "matching"
	FrontendRoleTagValue = "frontend"

	SizeStatsTypeTagValue  = "size"
	CountStatsTypeTagValue = "count"
)

// Common service base metrics
const (
	RestartCount         = "restarts"
	NumGoRoutinesGauge   = "num-goroutines"
	GoMaxProcsGauge      = "gomaxprocs"
	MemoryAllocatedGauge = "memory.allocated"
	MemoryHeapGauge      = "memory.heap"
	MemoryHeapIdleGauge  = "memory.heapidle"
	MemoryHeapInuseGauge = "memory.heapinuse"
	MemoryStackGauge     = "memory.stack"
	NumGCCounter         = "memory.num-gc"
	GcPauseMsTimer       = "memory.gc-pause-ms"
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
	// -- Common Operation scopes --

	// PersistenceCreateShardScope tracks CreateShard calls made by service to persistence layer
	PersistenceCreateShardScope = iota
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
	// PersistenceResetMutableStateScope tracks ResetMutableState calls made by service to persistence layer
	PersistenceResetMutableStateScope
	// PersistenceDeleteWorkflowExecutionScope tracks DeleteWorkflowExecution calls made by service to persistence layer
	PersistenceDeleteWorkflowExecutionScope
	// PersistenceGetCurrentExecutionScope tracks GetCurrentExecution calls made by service to persistence layer
	PersistenceGetCurrentExecutionScope
	// PersistenceGetTransferTasksScope tracks GetTransferTasks calls made by service to persistence layer
	PersistenceGetTransferTasksScope
	// PersistenceGetReplicationTasksScope tracks GetReplicationTasks calls made by service to persistence layer
	PersistenceGetReplicationTasksScope
	// PersistenceCompleteTransferTaskScope tracks CompleteTransferTasks calls made by service to persistence layer
	PersistenceCompleteTransferTaskScope
	// PersistenceRangeCompleteTransferTaskScope tracks CompleteTransferTasks calls made by service to persistence layer
	PersistenceRangeCompleteTransferTaskScope
	// PersistenceCompleteReplicationTaskScope tracks CompleteReplicationTasks calls made by service to persistence layer
	PersistenceCompleteReplicationTaskScope
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
	// PersistenceLeaseTaskListScope tracks LeaseTaskList calls made by service to persistence layer
	PersistenceLeaseTaskListScope
	// PersistenceUpdateTaskListScope tracks PersistenceUpdateTaskListScope calls made by service to persistence layer
	PersistenceUpdateTaskListScope
	// PersistenceAppendHistoryEventsScope tracks AppendHistoryEvents calls made by service to persistence layer
	PersistenceAppendHistoryEventsScope
	// PersistenceGetWorkflowExecutionHistoryScope tracks GetWorkflowExecutionHistory calls made by service to persistence layer
	PersistenceGetWorkflowExecutionHistoryScope
	// PersistenceDeleteWorkflowExecutionHistoryScope tracks DeleteWorkflowExecutionHistory calls made by service to persistence layer
	PersistenceDeleteWorkflowExecutionHistoryScope
	// PersistenceCreateDomainScope tracks CreateDomain calls made by service to persistence layer
	PersistenceCreateDomainScope
	// PersistenceGetDomainScope tracks GetDomain calls made by service to persistence layer
	PersistenceGetDomainScope
	// PersistenceUpdateDomainScope tracks UpdateDomain calls made by service to persistence layer
	PersistenceUpdateDomainScope
	// PersistenceDeleteDomainScope tracks DeleteDomain calls made by service to persistence layer
	PersistenceDeleteDomainScope
	// PersistenceDeleteDomainByNameScope tracks DeleteDomainByName calls made by service to persistence layer
	PersistenceDeleteDomainByNameScope
	// PersistenceListDomainScope tracks DeleteDomainByName calls made by service to persistence layer
	PersistenceListDomainScope
	// PersistenceGetMetadataScope tracks DeleteDomainByName calls made by service to persistence layer
	PersistenceGetMetadataScope
	// PersistenceRecordWorkflowExecutionStartedScope tracks RecordWorkflowExecutionStarted calls made by service to persistence layer
	PersistenceRecordWorkflowExecutionStartedScope
	// PersistenceRecordWorkflowExecutionClosedScope tracks RecordWorkflowExecutionClosed calls made by service to persistence layer
	PersistenceRecordWorkflowExecutionClosedScope
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
	// PersistenceGetClosedWorkflowExecutionScope tracks GetClosedWorkflowExecution calls made by service to persistence layer
	PersistenceGetClosedWorkflowExecutionScope
	// HistoryClientStartWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientStartWorkflowExecutionScope
	// HistoryClientRecordActivityTaskHeartbeatScope tracks RPC calls to history service
	HistoryClientRecordActivityTaskHeartbeatScope
	// HistoryClientRespondDecisionTaskCompletedScope tracks RPC calls to history service
	HistoryClientRespondDecisionTaskCompletedScope
	// HistoryClientRespondDecisionTaskFailedScope tracks RPC calls to history service
	HistoryClientRespondDecisionTaskFailedScope
	// HistoryClientRespondActivityTaskCompletedScope tracks RPC calls to history service
	HistoryClientRespondActivityTaskCompletedScope
	// HistoryClientRespondActivityTaskFailedScope tracks RPC calls to history service
	HistoryClientRespondActivityTaskFailedScope
	// HistoryClientRespondActivityTaskCanceledScope tracks RPC calls to history service
	HistoryClientRespondActivityTaskCanceledScope
	// HistoryClientGetMutableStateScope tracks RPC calls to history service
	HistoryClientGetMutableStateScope
	// HistoryClientResetStickyTaskListScope tracks RPC calls to history service
	HistoryClientResetStickyTaskListScope
	// HistoryClientDescribeWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientDescribeWorkflowExecutionScope
	// HistoryClientRecordDecisionTaskStartedScope tracks RPC calls to history service
	HistoryClientRecordDecisionTaskStartedScope
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
	// HistoryClientScheduleDecisionTaskScope tracks RPC calls to history service
	HistoryClientScheduleDecisionTaskScope
	// HistoryClientRecordChildExecutionCompletedScope tracks RPC calls to history service
	HistoryClientRecordChildExecutionCompletedScope
	// HistoryClientReplicateEventsScope tracks RPC calls to history service
	HistoryClientReplicateEventsScope
	// HistoryClientSyncShardStatusScope tracks RPC calls to history service
	HistoryClientSyncShardStatusScope
	// HistoryClientSyncActivityScope tracks RPC calls to history service
	HistoryClientSyncActivityScope
	// MatchingClientPollForDecisionTaskScope tracks RPC calls to matching service
	MatchingClientPollForDecisionTaskScope
	// MatchingClientPollForActivityTaskScope tracks RPC calls to matching service
	MatchingClientPollForActivityTaskScope
	// MatchingClientAddActivityTaskScope tracks RPC calls to matching service
	MatchingClientAddActivityTaskScope
	// MatchingClientAddDecisionTaskScope tracks RPC calls to matching service
	MatchingClientAddDecisionTaskScope
	// MatchingClientQueryWorkflowScope tracks RPC calls to matching service
	MatchingClientQueryWorkflowScope
	// MatchingClientRespondQueryTaskCompletedScope tracks RPC calls to matching service
	MatchingClientRespondQueryTaskCompletedScope
	// MatchingClientCancelOutstandingPollScope tracks RPC calls to matching service
	MatchingClientCancelOutstandingPollScope
	// MatchingClientDescribeTaskListScope tracks RPC calls to matching service
	MatchingClientDescribeTaskListScope
	// FrontendClientDeprecateDomainScope tracks RPC calls to frontend service
	FrontendClientDeprecateDomainScope
	// FrontendClientDescribeDomainScope tracks RPC calls to frontend service
	FrontendClientDescribeDomainScope
	// FrontendClientDescribeTaskListScope tracks RPC calls to frontend service
	FrontendClientDescribeTaskListScope
	// FrontendClientDescribeWorkflowExecutionScope tracks RPC calls to frontend service
	FrontendClientDescribeWorkflowExecutionScope
	// FrontendClientGetWorkflowExecutionHistoryScope tracks RPC calls to frontend service
	FrontendClientGetWorkflowExecutionHistoryScope
	// FrontendClientListClosedWorkflowExecutionsScope tracks RPC calls to frontend service
	FrontendClientListClosedWorkflowExecutionsScope
	// FrontendClientListDomainsScope tracks RPC calls to frontend service
	FrontendClientListDomainsScope
	// FrontendClientListOpenWorkflowExecutionsScope tracks RPC calls to frontend service
	FrontendClientListOpenWorkflowExecutionsScope
	// FrontendClientPollForActivityTaskScope tracks RPC calls to frontend service
	FrontendClientPollForActivityTaskScope
	// FrontendClientPollForDecisionTaskScope tracks RPC calls to frontend service
	FrontendClientPollForDecisionTaskScope
	// FrontendClientQueryWorkflowScope tracks RPC calls to frontend service
	FrontendClientQueryWorkflowScope
	// FrontendClientRecordActivityTaskHeartbeatScope tracks RPC calls to frontend service
	FrontendClientRecordActivityTaskHeartbeatScope
	// FrontendClientRecordActivityTaskHeartbeatByIDScope tracks RPC calls to frontend service
	FrontendClientRecordActivityTaskHeartbeatByIDScope
	// FrontendClientRegisterDomainScope tracks RPC calls to frontend service
	FrontendClientRegisterDomainScope
	// FrontendClientRequestCancelWorkflowExecutionScope tracks RPC calls to frontend service
	FrontendClientRequestCancelWorkflowExecutionScope
	// FrontendClientResetStickyTaskListScope tracks RPC calls to frontend service
	FrontendClientResetStickyTaskListScope
	// FrontendClientRespondActivityTaskCanceledScope tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCanceledScope
	// FrontendClientRespondActivityTaskCanceledByIDScope tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCanceledByIDScope
	// FrontendClientRespondActivityTaskCompletedScope tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCompletedScope
	// FrontendClientRespondActivityTaskCompletedByIDScope tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskCompletedByIDScope
	// FrontendClientRespondActivityTaskFailedScope tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskFailedScope
	// FrontendClientRespondActivityTaskFailedByIDScope tracks RPC calls to frontend service
	FrontendClientRespondActivityTaskFailedByIDScope
	// FrontendClientRespondDecisionTaskCompletedScope tracks RPC calls to frontend service
	FrontendClientRespondDecisionTaskCompletedScope
	// FrontendClientRespondDecisionTaskFailedScope tracks RPC calls to frontend service
	FrontendClientRespondDecisionTaskFailedScope
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
	// FrontendClientUpdateDomainScope tracks RPC calls to frontend service
	FrontendClientUpdateDomainScope

	// MessagingPublishScope tracks Publish calls made by service to messaging layer
	MessagingClientPublishScope
	// MessagingPublishBatchScope tracks Publish calls made by service to messaging layer
	MessagingClientPublishBatchScope

	// DomainCacheScope tracks domain cache callbacks
	DomainCacheScope
	// PersistenceAppendHistoryNodesScope tracks AppendHistoryNodes calls made by service to persistence layer
	PersistenceAppendHistoryNodesScope
	// PersistenceReadHistoryBranchScope tracks ReadHistoryBranch calls made by service to persistence layer
	PersistenceReadHistoryBranchScope
	// PersistenceForkHistoryBranchScope tracks ForkHistoryBranch calls made by service to persistence layer
	PersistenceForkHistoryBranchScope
	// PersistenceDeleteHistoryBranchScope tracks DeleteHistoryBranch calls made by service to persistence layer
	PersistenceDeleteHistoryBranchScope
	// PersistenceGetHistoryTreeScope tracks GetHistoryTree calls made by service to persistence layer
	PersistenceGetHistoryTreeScope

	NumCommonScopes
)

// -- Operation scopes for Frontend service --
const (
	// FrontendStartWorkflowExecutionScope is the metric scope for frontend.StartWorkflowExecution
	FrontendStartWorkflowExecutionScope = iota + NumCommonScopes
	// PollForDecisionTaskScope is the metric scope for frontend.PollForDecisionTask
	FrontendPollForDecisionTaskScope
	// FrontendPollForActivityTaskScope is the metric scope for frontend.PollForActivityTask
	FrontendPollForActivityTaskScope
	// FrontendRecordActivityTaskHeartbeatScope is the metric scope for frontend.RecordActivityTaskHeartbeat
	FrontendRecordActivityTaskHeartbeatScope
	// FrontendRecordActivityTaskHeartbeatByIDScope is the metric scope for frontend.RespondDecisionTaskCompleted
	FrontendRecordActivityTaskHeartbeatByIDScope
	// FrontendRespondDecisionTaskCompletedScope is the metric scope for frontend.RespondDecisionTaskCompleted
	FrontendRespondDecisionTaskCompletedScope
	// FrontendRespondDecisionTaskFailedScope is the metric scope for frontend.RespondDecisionTaskFailed
	FrontendRespondDecisionTaskFailedScope
	// FrontendRespondQueryTaskCompletedScope is the metric scope for frontend.RespondQueryTaskCompleted
	FrontendRespondQueryTaskCompletedScope
	// FrontendRespondActivityTaskCompletedScope is the metric scope for frontend.RespondActivityTaskCompleted
	FrontendRespondActivityTaskCompletedScope
	// FrontendRespondActivityTaskFailedScope is the metric scope for frontend.RespondActivityTaskFailed
	FrontendRespondActivityTaskFailedScope
	// FrontendRespondActivityTaskCanceledScope is the metric scope for frontend.RespondActivityTaskCanceled
	FrontendRespondActivityTaskCanceledScope
	// FrontendRespondActivityTaskCompletedScope is the metric scope for frontend.RespondActivityTaskCompletedByID
	FrontendRespondActivityTaskCompletedByIDScope
	// FrontendRespondActivityTaskFailedScope is the metric scope for frontend.RespondActivityTaskFailedByID
	FrontendRespondActivityTaskFailedByIDScope
	// FrontendRespondActivityTaskCanceledScope is the metric scope for frontend.RespondActivityTaskCanceledByID
	FrontendRespondActivityTaskCanceledByIDScope
	// FrontendGetWorkflowExecutionHistoryScope is the metric scope for frontend.GetWorkflowExecutionHistory
	FrontendGetWorkflowExecutionHistoryScope
	// FrontendSignalWorkflowExecutionScope is the metric scope for frontend.SignalWorkflowExecution
	FrontendSignalWorkflowExecutionScope
	// FrontendSignalWithStartWorkflowExecutionScope is the metric scope for frontend.SignalWithStartWorkflowExecution
	FrontendSignalWithStartWorkflowExecutionScope
	// FrontendTerminateWorkflowExecutionScope is the metric scope for frontend.TerminateWorkflowExecution
	FrontendTerminateWorkflowExecutionScope
	// FrontendRequestCancelWorkflowExecutionScope is the metric scope for frontend.RequestCancelWorkflowExecution
	FrontendRequestCancelWorkflowExecutionScope
	// FrontendListOpenWorkflowExecutionsScope is the metric scope for frontend.ListOpenWorkflowExecutions
	FrontendListOpenWorkflowExecutionsScope
	// FrontendListClosedWorkflowExecutionsScope is the metric scope for frontend.ListClosedWorkflowExecutions
	FrontendListClosedWorkflowExecutionsScope
	// FrontendRegisterDomainScope is the metric scope for frontend.RegisterDomain
	FrontendRegisterDomainScope
	// FrontendDescribeDomainScope is the metric scope for frontend.DescribeDomain
	FrontendDescribeDomainScope
	// FrontendUpdateDomainScope is the metric scope for frontend.DescribeDomain
	FrontendUpdateDomainScope
	// FrontendDeprecateDomainScope is the metric scope for frontend.DeprecateDomain
	FrontendDeprecateDomainScope
	// FrontendQueryWorkflowScope is the metric scope for frontend.QueryWorkflow
	FrontendQueryWorkflowScope
	// FrontendDescribeWorkflowExecutionScope is the metric scope for frontend.DescribeWorkflowExecution
	FrontendDescribeWorkflowExecutionScope
	// FrontendDescribeTaskListScope is the metric scope for frontend.DescribeTaskList
	FrontendDescribeTaskListScope
	// FrontendResetStickyTaskListScope is the metric scope for frontend.ResetStickyTaskList
	FrontendResetStickyTaskListScope
	// FrontendListDomainsScope is the metric scope for frontend.ListDomain
	FrontendListDomainsScope

	NumFrontendScopes
)

// -- Operation scopes for History service --
const (
	// HistoryStartWorkflowExecutionScope tracks StartWorkflowExecution API calls received by service
	HistoryStartWorkflowExecutionScope = iota + NumCommonScopes
	// HistoryRecordActivityTaskHeartbeatScope tracks RecordActivityTaskHeartbeat API calls received by service
	HistoryRecordActivityTaskHeartbeatScope
	// HistoryRespondDecisionTaskCompletedScope tracks RespondDecisionTaskCompleted API calls received by service
	HistoryRespondDecisionTaskCompletedScope
	// HistoryRespondDecisionTaskFailedScope tracks RespondDecisionTaskFailed API calls received by service
	HistoryRespondDecisionTaskFailedScope
	// HistoryRespondActivityTaskCompletedScope tracks RespondActivityTaskCompleted API calls received by service
	HistoryRespondActivityTaskCompletedScope
	// HistoryRespondActivityTaskFailedScope tracks RespondActivityTaskFailed API calls received by service
	HistoryRespondActivityTaskFailedScope
	// HistoryRespondActivityTaskCanceledScope tracks RespondActivityTaskCanceled API calls received by service
	HistoryRespondActivityTaskCanceledScope
	// HistoryGetMutableStateScope tracks GetMutableStateScope API calls received by service
	HistoryGetMutableStateScope
	// HistoryResetStickyTaskListScope tracks ResetStickyTaskListScope API calls received by service
	HistoryResetStickyTaskListScope
	// HistoryDescribeWorkflowExecutionScope tracks DescribeWorkflowExecution API calls received by service
	HistoryDescribeWorkflowExecutionScope
	// HistoryRecordDecisionTaskStartedScope tracks RecordDecisionTaskStarted API calls received by service
	HistoryRecordDecisionTaskStartedScope
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
	// HistoryScheduleDecisionTaskScope tracks ScheduleDecisionTask API calls received by service
	HistoryScheduleDecisionTaskScope
	// HistoryRecordChildExecutionCompletedScope tracks CompleteChildExecution API calls received by service
	HistoryRecordChildExecutionCompletedScope
	// HistoryRequestCancelWorkflowExecutionScope tracks RequestCancelWorkflowExecution API calls received by service
	HistoryRequestCancelWorkflowExecutionScope
	// HistoryReplicateEventsScope tracks ReplicateEvents API calls received by service
	HistoryReplicateEventsScope
	// HistorySyncShardStatusScope tracks HistorySyncShardStatus API calls received by service
	HistorySyncShardStatusScope
	// HistorySyncActivityScope tracks HistoryActivity API calls received by service
	HistorySyncActivityScope
	// HistoryDescribeMutableStateScope tracks HistoryActivity API calls received by service
	HistoryDescribeMutableStateScope
	// HistoryShardControllerScope is the scope used by shard controller
	HistoryShardControllerScope
	// TransferQueueProcessorScope is the scope used by all metric emitted by transfer queue processor
	TransferQueueProcessorScope
	// TransferActiveQueueProcessorScope is the scope used by all metric emitted by transfer queue processor
	TransferActiveQueueProcessorScope
	// TransferStandbyQueueProcessorScope is the scope used by all metric emitted by transfer queue processor
	TransferStandbyQueueProcessorScope
	// TransferActiveTaskActivityScope is the scope used for activity task processing by transfer queue processor
	TransferActiveTaskActivityScope
	// TransferActiveTaskDecisionScope is the scope used for decision task processing by transfer queue processor
	TransferActiveTaskDecisionScope
	// TransferActiveTaskCloseExecutionScope is the scope used for close execution task processing by transfer queue processor
	TransferActiveTaskCloseExecutionScope
	// TransferActiveTaskCancelExecutionScope is the scope used for cancel execution task processing by transfer queue processor
	TransferActiveTaskCancelExecutionScope
	// TransferActiveTaskSignalExecutionScope is the scope used for signal execution task processing by transfer queue processor
	TransferActiveTaskSignalExecutionScope
	// TransferActiveTaskStartChildExecutionScope is the scope used for start child execution task processing by transfer queue processor
	TransferActiveTaskStartChildExecutionScope
	// TransferStandbyTaskActivityScope is the scope used for activity task processing by transfer queue processor
	TransferStandbyTaskActivityScope
	// TransferStandbyTaskDecisionScope is the scope used for decision task processing by transfer queue processor
	TransferStandbyTaskDecisionScope
	// TransferStandbyTaskCloseExecutionScope is the scope used for close execution task processing by transfer queue processor
	TransferStandbyTaskCloseExecutionScope
	// TransferStandbyTaskCancelExecutionScope is the scope used for cancel execution task processing by transfer queue processor
	TransferStandbyTaskCancelExecutionScope
	// TransferStandbyTaskSignalExecutionScope is the scope used for signal execution task processing by transfer queue processor
	TransferStandbyTaskSignalExecutionScope
	// TransferStandbyTaskStartChildExecutionScope is the scope used for start child execution task processing by transfer queue processor
	TransferStandbyTaskStartChildExecutionScope
	// TimerQueueProcessorScope is the scope used by all metric emitted by timer queue processor
	TimerQueueProcessorScope
	// TimerActiveQueueProcessorScope is the scope used by all metric emitted by timer queue processor
	TimerActiveQueueProcessorScope
	// TimerQueueProcessorScope is the scope used by all metric emitted by timer queue processor
	TimerStandbyQueueProcessorScope
	// TimerActiveTaskActivityTimeoutScope is the scope used by metric emitted by timer queue processor for processing activity timeouts
	TimerActiveTaskActivityTimeoutScope
	// TimerActiveTaskDecisionTimeoutScope is the scope used by metric emitted by timer queue processor for processing decision timeouts
	TimerActiveTaskDecisionTimeoutScope
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
	// TimerStandbyTaskDecisionTimeoutScope is the scope used by metric emitted by timer queue processor for processing decision timeouts
	TimerStandbyTaskDecisionTimeoutScope
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
	// HistoryCacheGetAndCreateScope is the scope used by history cache
	HistoryCacheGetAndCreateScope
	// HistoryCacheGetOrCreateScope is the scope used by history cache
	HistoryCacheGetOrCreateScope
	// HistoryCacheGetCurrentExecutionScope is the scope used by history cache for getting current execution
	HistoryCacheGetCurrentExecutionScope
	// ExecutionSizeStatsScope is the scope used for emiting workflow execution size related stats
	ExecutionSizeStatsScope
	// ExecutionCountStatsScope is the scope used for emiting workflow execution count related stats
	ExecutionCountStatsScope
	// SessionSizeStatsScope is the scope used for emiting session update size related stats
	SessionSizeStatsScope
	// SessionCountStatsScope is the scope used for emiting session update count related stats
	SessionCountStatsScope

	NumHistoryScopes
)

// -- Operation scopes for Matching service --
const (
	// PollForDecisionTaskScope tracks PollForDecisionTask API calls received by service
	MatchingPollForDecisionTaskScope = iota + NumCommonScopes
	// PollForActivityTaskScope tracks PollForActivityTask API calls received by service
	MatchingPollForActivityTaskScope
	// MatchingAddActivityTaskScope tracks AddActivityTask API calls received by service
	MatchingAddActivityTaskScope
	// MatchingAddDecisionTaskScope tracks AddDecisionTask API calls received by service
	MatchingAddDecisionTaskScope
	// MatchingTaskListMgrScope is the metrics scope for matching.TaskListManager component
	MatchingTaskListMgrScope
	// MatchingQueryWorkflowScope tracks AddDecisionTask API calls received by service
	MatchingQueryWorkflowScope
	// MatchingRespondQueryTaskCompletedScope tracks AddDecisionTask API calls received by service
	MatchingRespondQueryTaskCompletedScope
	// MatchingCancelOutstandingPollScope tracks CancelOutstandingPoll API calls received by service
	MatchingCancelOutstandingPollScope
	// MatchingDescribeTaskListScope tracks DescribeTaskList API calls received by service
	MatchingDescribeTaskListScope

	NumMatchingScopes
)

// -- Operation scopes for Worker service --
const (
	// ReplicationScope is the scope used by all metric emitted by replicator
	ReplicatorScope = iota + NumCommonScopes
	// DomainReplicationTaskScope is the scope used by domain task replication processing
	DomainReplicationTaskScope
	// HistoryReplicationTaskScope is the scope used by history task replication processing
	HistoryReplicationTaskScope
	// SyncShardTaskScope is the scope used by sync shrad information processing
	SyncShardTaskScope
	// SyncActivityTaskScope is the scope used by sync activity information processing
	SyncActivityTaskScope

	NumWorkerScopes
)

// ScopeDefs record the scopes for all services
var ScopeDefs = map[ServiceIdx]map[int]scopeDefinition{
	// common scope Names
	Common: {
		PersistenceCreateShardScope:                              {operation: "CreateShard", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceGetShardScope:                                 {operation: "GetShard", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceUpdateShardScope:                              {operation: "UpdateShard", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceCreateWorkflowExecutionScope:                  {operation: "CreateWorkflowExecution"},
		PersistenceGetWorkflowExecutionScope:                     {operation: "GetWorkflowExecution"},
		PersistenceUpdateWorkflowExecutionScope:                  {operation: "UpdateWorkflowExecution"},
		PersistenceResetMutableStateScope:                        {operation: "ResetMutableState"},
		PersistenceDeleteWorkflowExecutionScope:                  {operation: "DeleteWorkflowExecution"},
		PersistenceGetCurrentExecutionScope:                      {operation: "GetCurrentExecution"},
		PersistenceGetTransferTasksScope:                         {operation: "GetTransferTasks"},
		PersistenceGetReplicationTasksScope:                      {operation: "GetReplicationTasks"},
		PersistenceCompleteTransferTaskScope:                     {operation: "CompleteTransferTask"},
		PersistenceRangeCompleteTransferTaskScope:                {operation: "RangeCompleteTransferTask"},
		PersistenceCompleteReplicationTaskScope:                  {operation: "CompleteReplicationTask"},
		PersistenceGetTimerIndexTasksScope:                       {operation: "GetTimerIndexTasks"},
		PersistenceCompleteTimerTaskScope:                        {operation: "CompleteTimerTask"},
		PersistenceRangeCompleteTimerTaskScope:                   {operation: "RangeCompleteTimerTask"},
		PersistenceCreateTaskScope:                               {operation: "CreateTask", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceGetTasksScope:                                 {operation: "GetTasks", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceCompleteTaskScope:                             {operation: "CompleteTask", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceLeaseTaskListScope:                            {operation: "LeaseTaskList", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceUpdateTaskListScope:                           {operation: "UpdateTaskList", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceAppendHistoryEventsScope:                      {operation: "AppendHistoryEvents", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceGetWorkflowExecutionHistoryScope:              {operation: "GetWorkflowExecutionHistory", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceDeleteWorkflowExecutionHistoryScope:           {operation: "DeleteWorkflowExecutionHistory", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceCreateDomainScope:                             {operation: "CreateDomain", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceGetDomainScope:                                {operation: "GetDomain", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceUpdateDomainScope:                             {operation: "UpdateDomain", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceDeleteDomainScope:                             {operation: "DeleteDomain", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceDeleteDomainByNameScope:                       {operation: "DeleteDomainByName", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceListDomainScope:                               {operation: "ListDomain", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceGetMetadataScope:                              {operation: "GetMetadata", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceRecordWorkflowExecutionStartedScope:           {operation: "RecordWorkflowExecutionStarted"},
		PersistenceRecordWorkflowExecutionClosedScope:            {operation: "RecordWorkflowExecutionClosed"},
		PersistenceListOpenWorkflowExecutionsScope:               {operation: "ListOpenWorkflowExecutions"},
		PersistenceListClosedWorkflowExecutionsScope:             {operation: "ListClosedWorkflowExecutions"},
		PersistenceListOpenWorkflowExecutionsByTypeScope:         {operation: "ListOpenWorkflowExecutionsByType"},
		PersistenceListClosedWorkflowExecutionsByTypeScope:       {operation: "ListClosedWorkflowExecutionsByType"},
		PersistenceListOpenWorkflowExecutionsByWorkflowIDScope:   {operation: "ListOpenWorkflowExecutionsByWorkflowID"},
		PersistenceListClosedWorkflowExecutionsByWorkflowIDScope: {operation: "ListClosedWorkflowExecutionsByWorkflowID"},
		PersistenceListClosedWorkflowExecutionsByStatusScope:     {operation: "ListClosedWorkflowExecutionsByStatus"},
		PersistenceGetClosedWorkflowExecutionScope:               {operation: "GetClosedWorkflowExecution"},
		PersistenceAppendHistoryNodesScope:                       {operation: "AppendHistoryNodes", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceReadHistoryBranchScope:                        {operation: "ReadHistoryBranch", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceForkHistoryBranchScope:                        {operation: "ForkHistoryBranch", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceDeleteHistoryBranchScope:                      {operation: "DeleteHistoryBranch", tags: map[string]string{ShardTagName: NoneShardsTagValue}},
		PersistenceGetHistoryTreeScope:                           {operation: "GetHistoryTree", tags: map[string]string{ShardTagName: NoneShardsTagValue}},

		HistoryClientStartWorkflowExecutionScope:            {operation: "HistoryClientStartWorkflowExecution", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRecordActivityTaskHeartbeatScope:       {operation: "HistoryClientRecordActivityTaskHeartbeat", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRespondDecisionTaskCompletedScope:      {operation: "HistoryClientRespondDecisionTaskCompleted", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRespondDecisionTaskFailedScope:         {operation: "HistoryClientRespondDecisionTaskFailed", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRespondActivityTaskCompletedScope:      {operation: "HistoryClientRespondActivityTaskCompleted", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRespondActivityTaskFailedScope:         {operation: "HistoryClientRespondActivityTaskFailed", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRespondActivityTaskCanceledScope:       {operation: "HistoryClientRespondActivityTaskCanceled", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientGetMutableStateScope:                   {operation: "HistoryClientGetMutableState", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientResetStickyTaskListScope:               {operation: "HistoryClientResetStickyTaskListScope", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientDescribeWorkflowExecutionScope:         {operation: "HistoryClientDescribeWorkflowExecution", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRecordDecisionTaskStartedScope:         {operation: "HistoryClientRecordDecisionTaskStarted", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRecordActivityTaskStartedScope:         {operation: "HistoryClientRecordActivityTaskStarted", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRequestCancelWorkflowExecutionScope:    {operation: "HistoryClientRequestCancelWorkflowExecution", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientSignalWorkflowExecutionScope:           {operation: "HistoryClientSignalWorkflowExecution", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientSignalWithStartWorkflowExecutionScope:  {operation: "HistoryClientSignalWithStartWorkflowExecution", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRemoveSignalMutableStateScope:          {operation: "HistoryClientRemoveSignalMutableStateScope", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientTerminateWorkflowExecutionScope:        {operation: "HistoryClientTerminateWorkflowExecution", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientScheduleDecisionTaskScope:              {operation: "HistoryClientScheduleDecisionTask", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientRecordChildExecutionCompletedScope:     {operation: "HistoryClientRecordChildExecutionCompleted", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientReplicateEventsScope:                   {operation: "HistoryClientReplicateEvents", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientSyncShardStatusScope:                   {operation: "HistoryClientSyncShardStatusScope", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		HistoryClientSyncActivityScope:                      {operation: "HistoryClientSyncActivityScope", tags: map[string]string{CadenceRoleTagName: HistoryRoleTagValue}},
		MatchingClientPollForDecisionTaskScope:              {operation: "MatchingClientPollForDecisionTask", tags: map[string]string{CadenceRoleTagName: MatchingRoleTagValue}},
		MatchingClientPollForActivityTaskScope:              {operation: "MatchingClientPollForActivityTask", tags: map[string]string{CadenceRoleTagName: MatchingRoleTagValue}},
		MatchingClientAddActivityTaskScope:                  {operation: "MatchingClientAddActivityTask", tags: map[string]string{CadenceRoleTagName: MatchingRoleTagValue}},
		MatchingClientAddDecisionTaskScope:                  {operation: "MatchingClientAddDecisionTask", tags: map[string]string{CadenceRoleTagName: MatchingRoleTagValue}},
		MatchingClientQueryWorkflowScope:                    {operation: "MatchingClientQueryWorkflow", tags: map[string]string{CadenceRoleTagName: MatchingRoleTagValue}},
		MatchingClientRespondQueryTaskCompletedScope:        {operation: "MatchingClientRespondQueryTaskCompleted", tags: map[string]string{CadenceRoleTagName: MatchingRoleTagValue}},
		MatchingClientCancelOutstandingPollScope:            {operation: "MatchingClientCancelOutstandingPoll", tags: map[string]string{CadenceRoleTagName: MatchingRoleTagValue}},
		MatchingClientDescribeTaskListScope:                 {operation: "MatchingClientDescribeTaskList", tags: map[string]string{CadenceRoleTagName: MatchingRoleTagValue}},
		FrontendClientDeprecateDomainScope:                  {operation: "FrontendClientDeprecateDomain", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientDescribeDomainScope:                   {operation: "FrontendClientDescribeDomain", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientDescribeTaskListScope:                 {operation: "FrontendClientDescribeTaskList", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientDescribeWorkflowExecutionScope:        {operation: "FrontendClientDescribeWorkflowExecution", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientGetWorkflowExecutionHistoryScope:      {operation: "FrontendClientGetWorkflowExecutionHistory", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientListClosedWorkflowExecutionsScope:     {operation: "FrontendClientListClosedWorkflowExecutions", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientListDomainsScope:                      {operation: "FrontendClientListDomains", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientListOpenWorkflowExecutionsScope:       {operation: "FrontendClientListOpenWorkflowExecutions", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientPollForActivityTaskScope:              {operation: "FrontendClientPollForActivityTask", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientPollForDecisionTaskScope:              {operation: "FrontendClientPollForDecisionTask", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientQueryWorkflowScope:                    {operation: "FrontendClientQueryWorkflow", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRecordActivityTaskHeartbeatScope:      {operation: "FrontendClientRecordActivityTaskHeartbeat", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRecordActivityTaskHeartbeatByIDScope:  {operation: "FrontendClientRecordActivityTaskHeartbeatByID", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRegisterDomainScope:                   {operation: "FrontendClientRegisterDomain", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRequestCancelWorkflowExecutionScope:   {operation: "FrontendClientRequestCancelWorkflowExecution", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientResetStickyTaskListScope:              {operation: "FrontendClientResetStickyTaskList", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRespondActivityTaskCanceledScope:      {operation: "FrontendClientRespondActivityTaskCanceled", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRespondActivityTaskCanceledByIDScope:  {operation: "FrontendClientRespondActivityTaskCanceledByID", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRespondActivityTaskCompletedScope:     {operation: "FrontendClientRespondActivityTaskCompleted", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRespondActivityTaskCompletedByIDScope: {operation: "FrontendClientRespondActivityTaskCompletedByID", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRespondActivityTaskFailedScope:        {operation: "FrontendClientRespondActivityTaskFailed", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRespondActivityTaskFailedByIDScope:    {operation: "FrontendClientRespondActivityTaskFailedByID", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRespondDecisionTaskCompletedScope:     {operation: "FrontendClientRespondDecisionTaskCompleted", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRespondDecisionTaskFailedScope:        {operation: "FrontendClientRespondDecisionTaskFailed", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientRespondQueryTaskCompletedScope:        {operation: "FrontendClientRespondQueryTaskCompleted", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientSignalWithStartWorkflowExecutionScope: {operation: "FrontendClientSignalWithStartWorkflowExecution", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientSignalWorkflowExecutionScope:          {operation: "FrontendClientSignalWorkflowExecution", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientStartWorkflowExecutionScope:           {operation: "FrontendClientStartWorkflowExecution", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientTerminateWorkflowExecutionScope:       {operation: "FrontendClientTerminateWorkflowExecution", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},
		FrontendClientUpdateDomainScope:                     {operation: "FrontendClientUpdateDomain", tags: map[string]string{CadenceRoleTagName: FrontendRoleTagValue}},

		MessagingClientPublishScope:      {operation: "MessagingClientPublish"},
		MessagingClientPublishBatchScope: {operation: "MessagingClientPublishBatch"},

		DomainCacheScope: {operation: "DomainCache"},
	},
	// Frontend Scope Names
	Frontend: {
		FrontendStartWorkflowExecutionScope:           {operation: "StartWorkflowExecution"},
		FrontendPollForDecisionTaskScope:              {operation: "PollForDecisionTask"},
		FrontendPollForActivityTaskScope:              {operation: "PollForActivityTask"},
		FrontendRecordActivityTaskHeartbeatScope:      {operation: "RecordActivityTaskHeartbeat"},
		FrontendRecordActivityTaskHeartbeatByIDScope:  {operation: "RecordActivityTaskHeartbeatByID"},
		FrontendRespondDecisionTaskCompletedScope:     {operation: "RespondDecisionTaskCompleted"},
		FrontendRespondDecisionTaskFailedScope:        {operation: "RespondDecisionTaskFailed"},
		FrontendRespondQueryTaskCompletedScope:        {operation: "RespondQueryTaskCompleted"},
		FrontendRespondActivityTaskCompletedScope:     {operation: "RespondActivityTaskCompleted"},
		FrontendRespondActivityTaskFailedScope:        {operation: "RespondActivityTaskFailed"},
		FrontendRespondActivityTaskCanceledScope:      {operation: "RespondActivityTaskCanceled"},
		FrontendRespondActivityTaskCompletedByIDScope: {operation: "RespondActivityTaskCompletedByID"},
		FrontendRespondActivityTaskFailedByIDScope:    {operation: "RespondActivityTaskFailedByID"},
		FrontendRespondActivityTaskCanceledByIDScope:  {operation: "RespondActivityTaskCanceledByID"},
		FrontendGetWorkflowExecutionHistoryScope:      {operation: "GetWorkflowExecutionHistory"},
		FrontendSignalWorkflowExecutionScope:          {operation: "SignalWorkflowExecution"},
		FrontendSignalWithStartWorkflowExecutionScope: {operation: "SignalWithStartWorkflowExecution"},
		FrontendTerminateWorkflowExecutionScope:       {operation: "TerminateWorkflowExecution"},
		FrontendRequestCancelWorkflowExecutionScope:   {operation: "RequestCancelWorkflowExecution"},
		FrontendListOpenWorkflowExecutionsScope:       {operation: "ListOpenWorkflowExecutions"},
		FrontendListClosedWorkflowExecutionsScope:     {operation: "ListClosedWorkflowExecutions"},
		FrontendRegisterDomainScope:                   {operation: "RegisterDomain"},
		FrontendDescribeDomainScope:                   {operation: "DescribeDomain"},
		FrontendListDomainsScope:                      {operation: "ListDomain"},
		FrontendUpdateDomainScope:                     {operation: "UpdateDomain"},
		FrontendDeprecateDomainScope:                  {operation: "DeprecateDomain"},
		FrontendQueryWorkflowScope:                    {operation: "QueryWorkflow"},
		FrontendDescribeWorkflowExecutionScope:        {operation: "DescribeWorkflowExecution"},
		FrontendDescribeTaskListScope:                 {operation: "DescribeTaskList"},
		FrontendResetStickyTaskListScope:              {operation: "ResetStickyTaskList"},
	},
	// History Scope Names
	History: {
		HistoryStartWorkflowExecutionScope:           {operation: "StartWorkflowExecution"},
		HistoryRecordActivityTaskHeartbeatScope:      {operation: "RecordActivityTaskHeartbeat"},
		HistoryRespondDecisionTaskCompletedScope:     {operation: "RespondDecisionTaskCompleted"},
		HistoryRespondDecisionTaskFailedScope:        {operation: "RespondDecisionTaskFailed"},
		HistoryRespondActivityTaskCompletedScope:     {operation: "RespondActivityTaskCompleted"},
		HistoryRespondActivityTaskFailedScope:        {operation: "RespondActivityTaskFailed"},
		HistoryRespondActivityTaskCanceledScope:      {operation: "RespondActivityTaskCanceled"},
		HistoryGetMutableStateScope:                  {operation: "GetMutableState"},
		HistoryResetStickyTaskListScope:              {operation: "ResetStickyTaskListScope"},
		HistoryDescribeWorkflowExecutionScope:        {operation: "DescribeWorkflowExecution"},
		HistoryRecordDecisionTaskStartedScope:        {operation: "RecordDecisionTaskStarted"},
		HistoryRecordActivityTaskStartedScope:        {operation: "RecordActivityTaskStarted"},
		HistorySignalWorkflowExecutionScope:          {operation: "SignalWorkflowExecution"},
		HistorySignalWithStartWorkflowExecutionScope: {operation: "SignalWithStartWorkflowExecution"},
		HistoryRemoveSignalMutableStateScope:         {operation: "RemoveSignalMutableState"},
		HistoryTerminateWorkflowExecutionScope:       {operation: "TerminateWorkflowExecution"},
		HistoryScheduleDecisionTaskScope:             {operation: "ScheduleDecisionTask"},
		HistoryRecordChildExecutionCompletedScope:    {operation: "RecordChildExecutionCompleted"},
		HistoryRequestCancelWorkflowExecutionScope:   {operation: "RequestCancelWorkflowExecution"},
		HistoryReplicateEventsScope:                  {operation: "ReplicateEvents"},
		HistorySyncShardStatusScope:                  {operation: "SyncShardStatus"},
		HistorySyncActivityScope:                     {operation: "SyncActivity"},
		HistoryDescribeMutableStateScope:             {operation: "DescribeMutableState"},
		HistoryShardControllerScope:                  {operation: "ShardController"},
		TransferQueueProcessorScope:                  {operation: "TransferQueueProcessor"},
		TransferActiveQueueProcessorScope:            {operation: "TransferActiveQueueProcessor"},
		TransferStandbyQueueProcessorScope:           {operation: "TransferStandbyQueueProcessor"},
		TransferActiveTaskActivityScope:              {operation: "TransferActiveTaskActivity"},
		TransferActiveTaskDecisionScope:              {operation: "TransferActiveTaskDecision"},
		TransferActiveTaskCloseExecutionScope:        {operation: "TransferActiveTaskCloseExecution"},
		TransferActiveTaskCancelExecutionScope:       {operation: "TransferActiveTaskCancelExecution"},
		TransferActiveTaskSignalExecutionScope:       {operation: "TransferActiveTaskSignalExecution"},
		TransferActiveTaskStartChildExecutionScope:   {operation: "TransferActiveTaskStartChildExecution"},
		TransferStandbyTaskActivityScope:             {operation: "TransferStandbyTaskActivity"},
		TransferStandbyTaskDecisionScope:             {operation: "TransferStandbyTaskDecision"},
		TransferStandbyTaskCloseExecutionScope:       {operation: "TransferStandbyTaskCloseExecution"},
		TransferStandbyTaskCancelExecutionScope:      {operation: "TransferStandbyTaskCancelExecution"},
		TransferStandbyTaskSignalExecutionScope:      {operation: "TransferStandbyTaskSignalExecution"},
		TransferStandbyTaskStartChildExecutionScope:  {operation: "TransferStandbyTaskStartChildExecution"},
		TimerQueueProcessorScope:                     {operation: "TimerQueueProcessor"},
		TimerActiveQueueProcessorScope:               {operation: "TimerActiveQueueProcessor"},
		TimerStandbyQueueProcessorScope:              {operation: "TimerStandbyQueueProcessor"},
		TimerActiveTaskActivityTimeoutScope:          {operation: "TimerActiveTaskActivityTimeout"},
		TimerActiveTaskDecisionTimeoutScope:          {operation: "TimerActiveTaskDecisionTimeout"},
		TimerActiveTaskUserTimerScope:                {operation: "TimerActiveTaskUserTimer"},
		TimerActiveTaskWorkflowTimeoutScope:          {operation: "TimerActiveTaskWorkflowTimeout"},
		TimerActiveTaskActivityRetryTimerScope:       {operation: "TimerActiveTaskActivityRetryTimer"},
		TimerActiveTaskWorkflowBackoffTimerScope:     {operation: "TimerActiveTaskWorkflowBackoffTimer"},
		TimerActiveTaskDeleteHistoryEventScope:       {operation: "TimerActiveTaskDeleteHistoryEvent"},
		TimerStandbyTaskActivityTimeoutScope:         {operation: "TimerStandbyTaskActivityTimeout"},
		TimerStandbyTaskDecisionTimeoutScope:         {operation: "TimerStandbyTaskDecisionTimeout"},
		TimerStandbyTaskUserTimerScope:               {operation: "TimerStandbyTaskUserTimer"},
		TimerStandbyTaskWorkflowTimeoutScope:         {operation: "TimerStandbyTaskWorkflowTimeout"},
		TimerStandbyTaskActivityRetryTimerScope:      {operation: "TimerStandbyTaskActivityRetryTimer"},
		TimerStandbyTaskWorkflowBackoffTimerScope:    {operation: "TimerStandbyTaskWorkflowBackoffTimer"},
		TimerStandbyTaskDeleteHistoryEventScope:      {operation: "TimerStandbyTaskDeleteHistoryEvent"},
		HistoryEventNotificationScope:                {operation: "HistoryEventNotification"},
		ReplicatorQueueProcessorScope:                {operation: "ReplicatorQueueProcessor"},
		ReplicatorTaskHistoryScope:                   {operation: "ReplicatorTaskHistory"},
		ReplicatorTaskSyncActivityScope:              {operation: "ReplicatorTaskSyncActivity"},
		ReplicateHistoryEventsScope:                  {operation: "ReplicateHistoryEvents"},
		ShardInfoScope:                               {operation: "ShardInfo"},
		WorkflowContextScope:                         {operation: "WorkflowContext"},
		HistoryCacheGetAndCreateScope:                {operation: "HistoryCacheGetAndCreate"},
		HistoryCacheGetOrCreateScope:                 {operation: "HistoryCacheGetOrCreate"},
		HistoryCacheGetCurrentExecutionScope:         {operation: "HistoryCacheGetCurrentExecution"},
		ExecutionSizeStatsScope:                      {operation: "ExecutionStats", tags: map[string]string{StatsTypeTagName: SizeStatsTypeTagValue}},
		ExecutionCountStatsScope:                     {operation: "ExecutionStats", tags: map[string]string{StatsTypeTagName: CountStatsTypeTagValue}},
		SessionSizeStatsScope:                        {operation: "SessionStats", tags: map[string]string{StatsTypeTagName: SizeStatsTypeTagValue}},
		SessionCountStatsScope:                       {operation: "SessionStats", tags: map[string]string{StatsTypeTagName: CountStatsTypeTagValue}},
	},
	// Matching Scope Names
	Matching: {
		MatchingPollForDecisionTaskScope:       {operation: "PollForDecisionTask"},
		MatchingPollForActivityTaskScope:       {operation: "PollForActivityTask"},
		MatchingAddActivityTaskScope:           {operation: "AddActivityTask"},
		MatchingAddDecisionTaskScope:           {operation: "AddDecisionTask"},
		MatchingTaskListMgrScope:               {operation: "TaskListMgr"},
		MatchingQueryWorkflowScope:             {operation: "QueryWorkflow"},
		MatchingRespondQueryTaskCompletedScope: {operation: "RespondQueryTaskCompleted"},
		MatchingCancelOutstandingPollScope:     {operation: "CancelOutstandingPoll"},
		MatchingDescribeTaskListScope:          {operation: "DescribeTaskList"},
	},
	// Worker Scope Names
	Worker: {
		ReplicatorScope:             {operation: "Replicator"},
		DomainReplicationTaskScope:  {operation: "DomainReplicationTask"},
		HistoryReplicationTaskScope: {operation: "HistoryReplicationTask"},
		SyncShardTaskScope:          {operation: "SyncShardTask"},
		SyncActivityTaskScope:       {operation: "SyncActivityTask"},
	},
}

// Common Metrics enum
const (
	CadenceRequests = iota
	CadenceFailures
	CadenceCriticalFailures
	CadenceLatency
	CadenceErrBadRequestCounter
	CadenceErrDomainNotActiveCounter
	CadenceErrServiceBusyCounter
	CadenceErrEntityNotExistsCounter
	CadenceErrExecutionAlreadyStartedCounter
	CadenceErrDomainAlreadyExistsCounter
	CadenceErrCancellationAlreadyRequestedCounter
	CadenceErrQueryFailedCounter
	CadenceErrLimitExceededCounter
	CadenceErrContextTimeoutCounter
	CadenceErrRetryTaskCounter
	PersistenceRequests
	PersistenceFailures
	PersistenceLatency
	PersistenceErrShardExistsCounter
	PersistenceErrShardOwnershipLostCounter
	PersistenceErrConditionFailedCounter
	PersistenceErrCurrentWorkflowConditionFailedCounter
	PersistenceErrTimeoutCounter
	PersistenceErrBusyCounter
	PersistenceErrEntityNotExistsCounter
	PersistenceErrExecutionAlreadyStartedCounter
	PersistenceErrDomainAlreadyExistsCounter
	PersistenceErrBadRequestCounter
	PersistenceSampledCounter

	CadenceClientRequests
	CadenceClientFailures
	CadenceClientLatency

	DomainCachePrepareCallbacksLatency
	DomainCacheCallbacksLatency

	HistorySize
	HistoryCount
	EventBlobSize

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
	TaskQueueLatency

	AckLevelUpdateCounter
	AckLevelUpdateFailedCounter
	DecisionTypeScheduleActivityCounter
	DecisionTypeCompleteWorkflowCounter
	DecisionTypeFailWorkflowCounter
	DecisionTypeCancelWorkflowCounter
	DecisionTypeStartTimerCounter
	DecisionTypeCancelActivityCounter
	DecisionTypeCancelTimerCounter
	DecisionTypeRecordMarkerCounter
	DecisionTypeCancelExternalWorkflowCounter
	DecisionTypeChildWorkflowCounter
	DecisionTypeContinueAsNewCounter
	DecisionTypeSignalExternalWorkflowCounter
	MultipleCompletionDecisionsCounter
	FailedDecisionsCounter
	StaleMutableStateCounter
	ConcurrencyUpdateFailureCounter
	CadenceErrEventAlreadyStartedCounter
	CadenceErrShardOwnershipLostCounter
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
	MembershipChangedCounter
	NumShardsGauge
	GetEngineForShardErrorCounter
	GetEngineForShardLatency
	RemoveEngineForShardLatency
	CompleteDecisionWithStickyEnabledCounter
	CompleteDecisionWithStickyDisabledCounter
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
	HistoryCacheRequests
	HistoryCacheFailures
	HistoryCacheLatency
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
	BufferedReplicationTasksSize
	ActivityInfoCount
	TimerInfoCount
	ChildInfoCount
	SignalInfoCount
	RequestCancelInfoCount
	BufferedEventsCount
	BufferedReplicationTasksCount
	DeleteActivityInfoCount
	DeleteTimerInfoCount
	DeleteChildInfoCount
	DeleteSignalInfoCount
	DeleteRequestCancelInfoCount
	WorkflowRetryBackoffTimerCount
	WorkflowCronBackoffTimerCount

	NumHistoryMetrics
)

// Matching metrics enum
const (
	PollSuccessCounter = iota + NumCommonMetrics
	PollTimeoutCounter
	PollSuccessWithSyncCounter
	LeaseRequestCounter
	LeaseFailureCounter
	ConditionFailedErrorCounter
	RespondQueryTaskFailedCounter
	SyncThrottleCounter
	BufferThrottleCounter
	SyncMatchLatency

	NumMatchingMetrics
)

// Worker metrics enum
const (
	ReplicatorMessages = iota + NumCommonMetrics
	ReplicatorFailures
	ReplicatorLatency

	NumWorkerMetrics
)

// MetricDefs record the metrics for all services
var MetricDefs = map[ServiceIdx]map[int]metricDefinition{
	Common: {
		CadenceRequests:                                     {metricName: "cadence.requests", metricType: Counter},
		CadenceFailures:                                     {metricName: "cadence.errors", metricType: Counter},
		CadenceCriticalFailures:                             {metricName: "cadence.errors.critical", metricType: Counter},
		CadenceLatency:                                      {metricName: "cadence.latency", metricType: Timer},
		CadenceErrBadRequestCounter:                         {metricName: "cadence.errors.bad-request", metricType: Counter},
		CadenceErrDomainNotActiveCounter:                    {metricName: "cadence.errors.domain-not-active", metricType: Counter},
		CadenceErrServiceBusyCounter:                        {metricName: "cadence.errors.service-busy", metricType: Counter},
		CadenceErrEntityNotExistsCounter:                    {metricName: "cadence.errors.entity-not-exists", metricType: Counter},
		CadenceErrExecutionAlreadyStartedCounter:            {metricName: "cadence.errors.execution-already-started", metricType: Counter},
		CadenceErrDomainAlreadyExistsCounter:                {metricName: "cadence.errors.domain-already-exists", metricType: Counter},
		CadenceErrCancellationAlreadyRequestedCounter:       {metricName: "cadence.errors.cancellation-already-requested", metricType: Counter},
		CadenceErrQueryFailedCounter:                        {metricName: "cadence.errors.query-failed", metricType: Counter},
		CadenceErrLimitExceededCounter:                      {metricName: "cadence.errors.limit-exceeded", metricType: Counter},
		CadenceErrContextTimeoutCounter:                     {metricName: "cadence.errors.context-timeout", metricType: Counter},
		CadenceErrRetryTaskCounter:                          {metricName: "cadence.errors.retry-task", metricType: Counter},
		PersistenceRequests:                                 {metricName: "persistence.requests", metricType: Counter},
		PersistenceFailures:                                 {metricName: "persistence.errors", metricType: Counter},
		PersistenceLatency:                                  {metricName: "persistence.latency", metricType: Timer},
		PersistenceErrShardExistsCounter:                    {metricName: "persistence.errors.shard-exists", metricType: Counter},
		PersistenceErrShardOwnershipLostCounter:             {metricName: "persistence.errors.shard-ownership-lost", metricType: Counter},
		PersistenceErrConditionFailedCounter:                {metricName: "persistence.errors.condition-failed", metricType: Counter},
		PersistenceErrCurrentWorkflowConditionFailedCounter: {metricName: "persistence.errors.current-workflow-condition-failed", metricType: Counter},
		PersistenceErrTimeoutCounter:                        {metricName: "persistence.errors.timeout", metricType: Counter},
		PersistenceErrBusyCounter:                           {metricName: "persistence.errors.busy", metricType: Counter},
		PersistenceErrEntityNotExistsCounter:                {metricName: "persistence.errors.entity-not-exists", metricType: Counter},
		PersistenceErrExecutionAlreadyStartedCounter:        {metricName: "persistence.errors.execution-already-started", metricType: Counter},
		PersistenceErrDomainAlreadyExistsCounter:            {metricName: "persistence.errors.domain-already-exists", metricType: Counter},
		PersistenceErrBadRequestCounter:                     {metricName: "persistence.errors.bad-request", metricType: Counter},
		PersistenceSampledCounter:                           {metricName: "persistence.sampled", metricType: Counter},
		CadenceClientRequests:                               {metricName: "cadence.client.requests", metricType: Counter},
		CadenceClientFailures:                               {metricName: "cadence.client.errors", metricType: Counter},
		CadenceClientLatency:                                {metricName: "cadence.client.latency", metricType: Timer},
		DomainCachePrepareCallbacksLatency:                  {metricName: "domain-cache.prepare-callbacks.latency", metricType: Timer},
		DomainCacheCallbacksLatency:                         {metricName: "domain-cache.callbacks.latency", metricType: Timer},
		HistorySize:                                         {metricName: "history-size", metricType: Timer},
		HistoryCount:                                        {metricName: "history-count", metricType: Timer},
		EventBlobSize:                                       {metricName: "event-blob-size", metricType: Timer},
	},
	Frontend: {},
	History: {
		TaskRequests:                                 {metricName: "task.requests", metricType: Counter},
		TaskLatency:                                  {metricName: "task.latency", metricType: Timer},
		TaskAttemptTimer:                             {metricName: "task.attempt", metricType: Timer},
		TaskFailures:                                 {metricName: "task.errors", metricType: Counter},
		TaskDiscarded:                                {metricName: "task.errors.discarded", metricType: Counter},
		TaskStandbyRetryCounter:                      {metricName: "task.errors.standby-retry-counter", metricType: Counter},
		TaskNotActiveCounter:                         {metricName: "task.errors.not-active-counter", metricType: Counter},
		TaskLimitExceededCounter:                     {metricName: "task.errors.limit-exceeded-counter", metricType: Counter},
		TaskProcessingLatency:                        {metricName: "task.latency.processing", metricType: Timer},
		TaskQueueLatency:                             {metricName: "task.latency.queue", metricType: Timer},
		TaskBatchCompleteCounter:                     {metricName: "task.batch-complete-counter", metricType: Counter},
		AckLevelUpdateCounter:                        {metricName: "ack-level-update", metricType: Counter},
		AckLevelUpdateFailedCounter:                  {metricName: "ack-level-update-failed", metricType: Counter},
		DecisionTypeScheduleActivityCounter:          {metricName: "schedule-activity-decision", metricType: Counter},
		DecisionTypeCompleteWorkflowCounter:          {metricName: "complete-workflow-decision", metricType: Counter},
		DecisionTypeFailWorkflowCounter:              {metricName: "fail-workflow-decision", metricType: Counter},
		DecisionTypeCancelWorkflowCounter:            {metricName: "cancel-workflow-decision", metricType: Counter},
		DecisionTypeStartTimerCounter:                {metricName: "start-timer-decision", metricType: Counter},
		DecisionTypeCancelActivityCounter:            {metricName: "cancel-activity-decision", metricType: Counter},
		DecisionTypeCancelTimerCounter:               {metricName: "cancel-timer-decision", metricType: Counter},
		DecisionTypeRecordMarkerCounter:              {metricName: "record-marker-decision", metricType: Counter},
		DecisionTypeCancelExternalWorkflowCounter:    {metricName: "cancel-external-workflow-decision", metricType: Counter},
		DecisionTypeContinueAsNewCounter:             {metricName: "continue-as-new-decision", metricType: Counter},
		DecisionTypeSignalExternalWorkflowCounter:    {metricName: "signal-external-workflow-decision", metricType: Counter},
		DecisionTypeChildWorkflowCounter:             {metricName: "child-workflow-decision", metricType: Counter},
		MultipleCompletionDecisionsCounter:           {metricName: "multiple-completion-decisions", metricType: Counter},
		FailedDecisionsCounter:                       {metricName: "failed-decisions", metricType: Counter},
		StaleMutableStateCounter:                     {metricName: "stale-mutable-state", metricType: Counter},
		ConcurrencyUpdateFailureCounter:              {metricName: "concurrency-update-failure", metricType: Counter},
		CadenceErrShardOwnershipLostCounter:          {metricName: "cadence.errors.shard-ownership-lost", metricType: Counter},
		CadenceErrEventAlreadyStartedCounter:         {metricName: "cadence.errors.event-already-started", metricType: Counter},
		HeartbeatTimeoutCounter:                      {metricName: "heartbeat-timeout", metricType: Counter},
		ScheduleToStartTimeoutCounter:                {metricName: "schedule-to-start-timeout", metricType: Counter},
		StartToCloseTimeoutCounter:                   {metricName: "start-to-close-timeout", metricType: Counter},
		ScheduleToCloseTimeoutCounter:                {metricName: "schedule-to-close-timeout", metricType: Counter},
		NewTimerCounter:                              {metricName: "new-timer", metricType: Counter},
		NewTimerNotifyCounter:                        {metricName: "new-timer-notifications", metricType: Counter},
		AcquireShardsCounter:                         {metricName: "acquire-shards-count", metricType: Counter},
		AcquireShardsLatency:                         {metricName: "acquire-shards-latency", metricType: Timer},
		ShardClosedCounter:                           {metricName: "shard-closed-count", metricType: Counter},
		ShardItemCreatedCounter:                      {metricName: "sharditem-created-count", metricType: Counter},
		ShardItemRemovedCounter:                      {metricName: "sharditem-removed-count", metricType: Counter},
		ShardInfoReplicationPendingTasksTimer:        {metricName: "shardinfo-replication-pending-task", metricType: Timer},
		ShardInfoTransferActivePendingTasksTimer:     {metricName: "shardinfo-transfer-active-pending-task", metricType: Timer},
		ShardInfoTransferStandbyPendingTasksTimer:    {metricName: "shardinfo-transfer-standby-pending-task", metricType: Timer},
		ShardInfoTimerActivePendingTasksTimer:        {metricName: "shardinfo-timer-active-pending-task", metricType: Timer},
		ShardInfoTimerStandbyPendingTasksTimer:       {metricName: "shardinfo-timer-standby-pending-task", metricType: Timer},
		ShardInfoReplicationLagTimer:                 {metricName: "shardinfo-replication-lag", metricType: Timer},
		ShardInfoTransferLagTimer:                    {metricName: "shardinfo-transfer-lag", metricType: Timer},
		ShardInfoTimerLagTimer:                       {metricName: "shardinfo-timer-lag", metricType: Timer},
		ShardInfoTransferDiffTimer:                   {metricName: "shardinfo-transfer-diff", metricType: Timer},
		ShardInfoTimerDiffTimer:                      {metricName: "shardinfo-timer-diff", metricType: Timer},
		ShardInfoTransferFailoverInProgressTimer:     {metricName: "shardinfo-transfer-failover-in-progress", metricType: Timer},
		ShardInfoTimerFailoverInProgressTimer:        {metricName: "shardinfo-timer-failover-in-progress", metricType: Timer},
		ShardInfoTransferFailoverLatencyTimer:        {metricName: "shardinfo-transfer-failover-latency", metricType: Timer},
		ShardInfoTimerFailoverLatencyTimer:           {metricName: "shardinfo-timer-failover-latency", metricType: Timer},
		MembershipChangedCounter:                     {metricName: "membership-changed-count", metricType: Counter},
		NumShardsGauge:                               {metricName: "numshards-gauge", metricType: Gauge},
		GetEngineForShardErrorCounter:                {metricName: "get-engine-for-shard-errors", metricType: Counter},
		GetEngineForShardLatency:                     {metricName: "get-engine-for-shard-latency", metricType: Timer},
		RemoveEngineForShardLatency:                  {metricName: "remove-engine-for-shard-latency", metricType: Timer},
		CompleteDecisionWithStickyEnabledCounter:     {metricName: "complete-decision-sticky-enabled-count", metricType: Counter},
		CompleteDecisionWithStickyDisabledCounter:    {metricName: "complete-decision-sticky-disabled-count", metricType: Counter},
		HistoryEventNotificationQueueingLatency:      {metricName: "history-event-notification-queueing-latency", metricType: Timer},
		HistoryEventNotificationFanoutLatency:        {metricName: "history-event-notification-fanout-latency", metricType: Timer},
		HistoryEventNotificationInFlightMessageGauge: {metricName: "history-event-notification-inflight-message-gauge", metricType: Gauge},
		HistoryEventNotificationFailDeliveryCount:    {metricName: "history-event-notification-fail-delivery-count", metricType: Counter},
		EmptyReplicationEventsCounter:                {metricName: "empty-replication-events", metricType: Counter},
		DuplicateReplicationEventsCounter:            {metricName: "duplicate-replication-events", metricType: Counter},
		StaleReplicationEventsCounter:                {metricName: "stale-replication-events", metricType: Counter},
		ReplicationEventsSizeTimer:                   {metricName: "replication-events-size", metricType: Timer},
		BufferReplicationTaskTimer:                   {metricName: "buffer-replication-tasks", metricType: Timer},
		UnbufferReplicationTaskTimer:                 {metricName: "unbuffer-replication-tasks", metricType: Timer},
		HistoryConflictsCounter:                      {metricName: "history-conflicts", metricType: Counter},
		CompleteTaskFailedCounter:                    {metricName: "complete-task-fail-count", metricType: Counter},
		HistoryCacheRequests:                         {metricName: "history-cache.requests", metricType: Counter},
		HistoryCacheFailures:                         {metricName: "history-cache.errors", metricType: Counter},
		HistoryCacheLatency:                          {metricName: "history-cache.latency", metricType: Timer},
		CacheMissCounter:                             {metricName: "cache-miss", metricType: Counter},
		AcquireLockFailedCounter:                     {metricName: "acquire-lock-failed", metricType: Counter},
		WorkflowContextCleared:                       {metricName: "workflow-context-cleared", metricType: Counter},
		MutableStateSize:                             {metricName: "mutable-state-size", metricType: Timer},
		ExecutionInfoSize:                            {metricName: "execution-info-size", metricType: Timer},
		ActivityInfoSize:                             {metricName: "activity-info-size", metricType: Timer},
		TimerInfoSize:                                {metricName: "timer-info-size", metricType: Timer},
		ChildInfoSize:                                {metricName: "child-info-size", metricType: Timer},
		SignalInfoSize:                               {metricName: "signal-info", metricType: Timer},
		BufferedEventsSize:                           {metricName: "buffered-events-size", metricType: Timer},
		BufferedReplicationTasksSize:                 {metricName: "buffered-replication-tasks-size", metricType: Timer},
		ActivityInfoCount:                            {metricName: "activity-info-count", metricType: Timer},
		TimerInfoCount:                               {metricName: "timer-info-count", metricType: Timer},
		ChildInfoCount:                               {metricName: "child-info-count", metricType: Timer},
		SignalInfoCount:                              {metricName: "signal-info-count", metricType: Timer},
		RequestCancelInfoCount:                       {metricName: "request-cancel-info-count", metricType: Timer},
		BufferedEventsCount:                          {metricName: "buffered-events-count", metricType: Timer},
		BufferedReplicationTasksCount:                {metricName: "buffered-replication-tasks-count", metricType: Timer},
		DeleteActivityInfoCount:                      {metricName: "delete-activity-info", metricType: Timer},
		DeleteTimerInfoCount:                         {metricName: "delete-timer-info", metricType: Timer},
		DeleteChildInfoCount:                         {metricName: "delete-child-info", metricType: Timer},
		DeleteSignalInfoCount:                        {metricName: "delete-signal-info", metricType: Timer},
		DeleteRequestCancelInfoCount:                 {metricName: "delete-request-cancel-info", metricType: Timer},
		WorkflowRetryBackoffTimerCount:               {metricName: "workflow-retry-backoff-timer", metricType: Counter},
		WorkflowCronBackoffTimerCount:                {metricName: "workflow-cron-backoff-timer", metricType: Counter},
	},
	Matching: {
		PollSuccessCounter:            {metricName: "poll.success"},
		PollTimeoutCounter:            {metricName: "poll.timeouts"},
		PollSuccessWithSyncCounter:    {metricName: "poll.success.sync"},
		LeaseRequestCounter:           {metricName: "lease.requests"},
		LeaseFailureCounter:           {metricName: "lease.failures"},
		ConditionFailedErrorCounter:   {metricName: "condition-failed-errors"},
		RespondQueryTaskFailedCounter: {metricName: "respond-query-failed"},
		SyncThrottleCounter:           {metricName: "sync.throttle.count"},
		BufferThrottleCounter:         {metricName: "buffer.throttle.count"},
		SyncMatchLatency:              {metricName: "syncmatch.latency", metricType: Timer},
	},
	Worker: {
		ReplicatorMessages: {metricName: "replicator.messages"},
		ReplicatorFailures: {metricName: "replicator.errors"},
		ReplicatorLatency:  {metricName: "replicator.latency"},
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
