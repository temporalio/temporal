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
	ShardTagName = "shard"
)

// This package should hold all the metrics and tags for cadence
const (
	UnknownDirectoryTagValue = "Unknown"
	AllShardsTagValue        = "ALL"
	NoneShardsTagValue       = "NONE"
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
	// DomainCacheScope tracks domain cache callbacks
	DomainCacheScope

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
	// HistorySyncShardStatusScope tracks ReplicateEvents API calls received by service
	HistorySyncShardStatusScope
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
	// TimerActiveTaskWorkflowRetryTimerScope is the scope used by metric emitted by timer queue processor for processing retry task.
	TimerActiveTaskWorkflowRetryTimerScope
	// TimerActiveTaskDeleteHistoryEvent is the scope used by metric emitted by timer queue processor for processing history event cleanup
	TimerActiveTaskDeleteHistoryEvent
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
	// TimerStandbyTaskDeleteHistoryEvent is the scope used by metric emitted by timer queue processor for processing history event cleanup
	TimerStandbyTaskDeleteHistoryEvent
	// TimerStandbyTaskWorkflowRetryTimerScope is the scope used by metric emitted by timer queue processor for processing retry task.
	TimerStandbyTaskWorkflowRetryTimerScope
	// HistoryEventNotificationScope is the scope used by shard history event nitification
	HistoryEventNotificationScope
	// ReplicatorQueueProcessorScope is the scope used by all metric emitted by replicator queue processor
	ReplicatorQueueProcessorScope
	// ReplicatorTaskHistoryScope is the scope used for history task processing by replicator queue processor
	ReplicatorTaskHistoryScope
	// ReplicateHistoryEventsScope is the scope used by historyReplicator API for applying events
	ReplicateHistoryEventsScope
	// ShardInfoScope is the scope used when updating shard info
	ShardInfoScope

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

		HistoryClientStartWorkflowExecutionScope:           {operation: "HistoryClientStartWorkflowExecution"},
		HistoryClientRecordActivityTaskHeartbeatScope:      {operation: "HistoryClientRecordActivityTaskHeartbeat"},
		HistoryClientRespondDecisionTaskCompletedScope:     {operation: "HistoryClientRespondDecisionTaskCompleted"},
		HistoryClientRespondDecisionTaskFailedScope:        {operation: "HistoryClientRespondDecisionTaskFailed"},
		HistoryClientRespondActivityTaskCompletedScope:     {operation: "HistoryClientRespondActivityTaskCompleted"},
		HistoryClientRespondActivityTaskFailedScope:        {operation: "HistoryClientRespondActivityTaskFailed"},
		HistoryClientRespondActivityTaskCanceledScope:      {operation: "HistoryClientRespondActivityTaskCanceled"},
		HistoryClientGetMutableStateScope:                  {operation: "HistoryClientGetMutableState"},
		HistoryClientResetStickyTaskListScope:              {operation: "HistoryClientResetStickyTaskListScope"},
		HistoryClientDescribeWorkflowExecutionScope:        {operation: "HistoryClientDescribeWorkflowExecution"},
		HistoryClientRecordDecisionTaskStartedScope:        {operation: "HistoryClientRecordDecisionTaskStarted"},
		HistoryClientRecordActivityTaskStartedScope:        {operation: "HistoryClientRecordActivityTaskStarted"},
		HistoryClientRequestCancelWorkflowExecutionScope:   {operation: "HistoryClientRequestCancelWorkflowExecution"},
		HistoryClientSignalWorkflowExecutionScope:          {operation: "HistoryClientSignalWorkflowExecution"},
		HistoryClientSignalWithStartWorkflowExecutionScope: {operation: "HistoryClientSignalWithStartWorkflowExecution"},
		HistoryClientRemoveSignalMutableStateScope:         {operation: "HistoryClientRemoveSignalMutableStateScope"},
		HistoryClientTerminateWorkflowExecutionScope:       {operation: "HistoryClientTerminateWorkflowExecution"},
		HistoryClientScheduleDecisionTaskScope:             {operation: "HistoryClientScheduleDecisionTask"},
		HistoryClientRecordChildExecutionCompletedScope:    {operation: "HistoryClientRecordChildExecutionCompleted"},
		HistoryClientReplicateEventsScope:                  {operation: "HistoryClientReplicateEvents"},
		HistoryClientSyncShardStatusScope:                  {operation: "HistoryClientSyncShardStatusScope"},
		MatchingClientPollForDecisionTaskScope:             {operation: "MatchingClientPollForDecisionTask"},
		MatchingClientPollForActivityTaskScope:             {operation: "MatchingClientPollForActivityTask"},
		MatchingClientAddActivityTaskScope:                 {operation: "MatchingClientAddActivityTask"},
		MatchingClientAddDecisionTaskScope:                 {operation: "MatchingClientAddDecisionTask"},
		MatchingClientQueryWorkflowScope:                   {operation: "MatchingClientQueryWorkflow"},
		MatchingClientRespondQueryTaskCompletedScope:       {operation: "MatchingClientRespondQueryTaskCompleted"},
		MatchingClientCancelOutstandingPollScope:           {operation: "MatchingClientCancelOutstandingPoll"},
		MatchingClientDescribeTaskListScope:                {operation: "MatchingClientDescribeTaskList"},
		DomainCacheScope:                                   {operation: "DomainCache"},
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
		TimerActiveTaskWorkflowRetryTimerScope:       {operation: "TimerActiveTaskWorkflowRetryTimer"},
		TimerActiveTaskDeleteHistoryEvent:            {operation: "TimerActiveTaskDeleteHistoryEvent"},
		TimerStandbyTaskActivityTimeoutScope:         {operation: "TimerStandbyTaskActivityTimeout"},
		TimerStandbyTaskDecisionTimeoutScope:         {operation: "TimerStandbyTaskDecisionTimeout"},
		TimerStandbyTaskUserTimerScope:               {operation: "TimerStandbyTaskUserTimer"},
		TimerStandbyTaskWorkflowTimeoutScope:         {operation: "TimerStandbyTaskWorkflowTimeout"},
		TimerStandbyTaskActivityRetryTimerScope:      {operation: "TimerStandbyTaskActivityRetryTimer"},
		TimerStandbyTaskWorkflowRetryTimerScope:      {operation: "TimerStandbyTaskWorkflowRetryTimer"},
		TimerStandbyTaskDeleteHistoryEvent:           {operation: "TimerStandbyTaskDeleteHistoryEvent"},
		HistoryEventNotificationScope:                {operation: "HistoryEventNotification"},
		ReplicatorQueueProcessorScope:                {operation: "ReplicatorQueueProcessor"},
		ReplicatorTaskHistoryScope:                   {operation: "ReplicatorTaskHistory"},
		ReplicateHistoryEventsScope:                  {operation: "ReplicateHistoryEvents"},
		ShardInfoScope:                               {operation: "ShardInfo"},
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
	PersistenceErrTimeoutCounter
	PersistenceErrBusyCounter
	PersistenceSampledCounter

	HistoryClientFailures
	MatchingClientFailures

	DomainCacheTotalCallbacksLatency
	DomainCacheBeforeCallbackLatency
	DomainCacheAfterCallbackLatency

	NumCommonMetrics // Needs to be last on this list for iota numbering
)

// History Metrics enum
const (
	TaskRequests = iota + NumCommonMetrics
	TaskFailures
	TaskLatency
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
	ShardInfoTransferDiffTimer
	ShardInfoTimerDiffTimer
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
	SignalSizeTimer
	EmptyReplicationEventsCounter
	DuplicateReplicationEventsCounter
	StaleReplicationEventsCounter
	ReplicationEventsSizeTimer
	BufferReplicationTaskTimer
	UnbufferReplicationTaskTimer
	HistoryConflictsCounter
	HistoryTaskStandbyRetryCounter
	HistoryTaskNotActiveCounter
	HistoryTaskBatchCompleteCounter
	ActiveTransferTaskQueueLatency
	ActiveTimerTaskQueueLatency
	StandbyTransferTaskQueueLatency
	StandbyTimerTaskQueueLatency
	CompleteTaskFailedCounter

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
		CadenceRequests:                               {metricName: "cadence.requests", metricType: Counter},
		CadenceFailures:                               {metricName: "cadence.errors", metricType: Counter},
		CadenceCriticalFailures:                       {metricName: "cadence.errors.critical", metricType: Counter},
		CadenceLatency:                                {metricName: "cadence.latency", metricType: Timer},
		CadenceErrBadRequestCounter:                   {metricName: "cadence.errors.bad-request", metricType: Counter},
		CadenceErrDomainNotActiveCounter:              {metricName: "cadence.errors.domain-not-active", metricType: Counter},
		CadenceErrServiceBusyCounter:                  {metricName: "cadence.errors.service-busy", metricType: Counter},
		CadenceErrEntityNotExistsCounter:              {metricName: "cadence.errors.entity-not-exists", metricType: Counter},
		CadenceErrExecutionAlreadyStartedCounter:      {metricName: "cadence.errors.execution-already-started", metricType: Counter},
		CadenceErrDomainAlreadyExistsCounter:          {metricName: "cadence.errors.domain-already-exists", metricType: Counter},
		CadenceErrCancellationAlreadyRequestedCounter: {metricName: "cadence.errors.cancellation-already-requested", metricType: Counter},
		CadenceErrQueryFailedCounter:                  {metricName: "cadence.errors.query-failed", metricType: Counter},
		CadenceErrLimitExceededCounter:                {metricName: "cadence.errors.limit-exceeded", metricType: Counter},
		CadenceErrContextTimeoutCounter:               {metricName: "cadence.errors.context-timeout", metricType: Counter},
		CadenceErrRetryTaskCounter:                    {metricName: "cadence.errors.retry-task", metricType: Counter},
		PersistenceRequests:                           {metricName: "persistence.requests", metricType: Counter},
		PersistenceFailures:                           {metricName: "persistence.errors", metricType: Counter},
		PersistenceLatency:                            {metricName: "persistence.latency", metricType: Timer},
		PersistenceErrShardExistsCounter:              {metricName: "persistence.errors.shard-exists", metricType: Counter},
		PersistenceErrShardOwnershipLostCounter:       {metricName: "persistence.errors.shard-ownership-lost", metricType: Counter},
		PersistenceErrConditionFailedCounter:          {metricName: "persistence.errors.condition-failed", metricType: Counter},
		PersistenceErrTimeoutCounter:                  {metricName: "persistence.errors.timeout", metricType: Counter},
		PersistenceErrBusyCounter:                     {metricName: "persistence.errors.busy", metricType: Counter},
		PersistenceSampledCounter:                     {metricName: "persistence.sampled", metricType: Counter},
		HistoryClientFailures:                         {metricName: "client.history.errors", metricType: Counter},
		MatchingClientFailures:                        {metricName: "client.matching.errors", metricType: Counter},
		DomainCacheTotalCallbacksLatency:              {metricName: "domain-cache.total-callbacks.latency", metricType: Timer},
		DomainCacheBeforeCallbackLatency:              {metricName: "domain-cache.before-callbacks.latency", metricType: Timer},
		DomainCacheAfterCallbackLatency:               {metricName: "domain-cache.after-callbacks.latency", metricType: Timer},
	},
	Frontend: {},
	History: {
		TaskRequests:                                 {metricName: "task.requests", metricType: Counter},
		TaskFailures:                                 {metricName: "task.errors", metricType: Counter},
		TaskLatency:                                  {metricName: "task.latency", metricType: Timer},
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
		ShardInfoTransferDiffTimer:                   {metricName: "shardinfo-transfer-diff", metricType: Timer},
		ShardInfoTimerDiffTimer:                      {metricName: "shardinfo-timer-diff", metricType: Timer},
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
		SignalSizeTimer:                              {metricName: "signal-size", metricType: Timer},
		EmptyReplicationEventsCounter:                {metricName: "empty-replication-events", metricType: Counter},
		DuplicateReplicationEventsCounter:            {metricName: "duplicate-replication-events", metricType: Counter},
		StaleReplicationEventsCounter:                {metricName: "stale-replication-events", metricType: Counter},
		ReplicationEventsSizeTimer:                   {metricName: "replication-events-size", metricType: Timer},
		BufferReplicationTaskTimer:                   {metricName: "buffer-replication-tasks", metricType: Timer},
		UnbufferReplicationTaskTimer:                 {metricName: "unbuffer-replication-tasks", metricType: Timer},
		HistoryConflictsCounter:                      {metricName: "history-conflicts", metricType: Counter},
		HistoryTaskStandbyRetryCounter:               {metricName: "history-task-standby-retry-counter", metricType: Counter},
		HistoryTaskNotActiveCounter:                  {metricName: "history-task-not-active-counter", metricType: Counter},
		HistoryTaskBatchCompleteCounter:              {metricName: "history-task-batch-complete-counter", metricType: Counter},
		ActiveTransferTaskQueueLatency:               {metricName: "active.transfertask.queue.latency", metricType: Timer},
		ActiveTimerTaskQueueLatency:                  {metricName: "active.timertask.queue.latency", metricType: Timer},
		StandbyTransferTaskQueueLatency:              {metricName: "standby.transfertask.queue.latency", metricType: Timer},
		StandbyTimerTaskQueueLatency:                 {metricName: "standby.timertask.queue.latency", metricType: Timer},
		CompleteTaskFailedCounter:                    {metricName: "complete-task-fail-count", metricType: Counter},
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
