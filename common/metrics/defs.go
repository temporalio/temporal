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
	// PersistenceDeleteWorkflowExecutionScope tracks DeleteWorkflowExecution calls made by service to persistence layer
	PersistenceDeleteWorkflowExecutionScope
	// PersistenceGetCurrentExecutionScope tracks GetCurrentExecution calls made by service to persistence layer
	PersistenceGetCurrentExecutionScope
	// PersistenceGetTransferTasksScope tracks GetTransferTasks calls made by service to persistence layer
	PersistenceGetTransferTasksScope
	// PersistenceCompleteTransferTaskScope tracks CompleteTransferTasks calls made by service to persistence layer
	PersistenceCompleteTransferTaskScope
	// PersistenceGetTimerIndexTasksScope tracks GetTimerIndexTasks calls made by service to persistence layer
	PersistenceGetTimerIndexTasksScope
	// PersistenceCompleteTimerTaskScope tracks CompleteTimerTasks calls made by service to persistence layer
	PersistenceCompleteTimerTaskScope
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
	// HistoryClientRemoveSignalMutableStateScope tracks RPC calls to history service
	HistoryClientRemoveSignalMutableStateScope
	// HistoryClientTerminateWorkflowExecutionScope tracks RPC calls to history service
	HistoryClientTerminateWorkflowExecutionScope
	// HistoryClientScheduleDecisionTaskScope tracks RPC calls to history service
	HistoryClientScheduleDecisionTaskScope
	// HistoryClientRecordChildExecutionCompletedScope tracks RPC calls to history service
	HistoryClientRecordChildExecutionCompletedScope
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
	// HistoryShardControllerScope is the scope used by shard controller
	HistoryShardControllerScope
	// TransferQueueProcessorScope is the scope used by all metric emitted by transfer queue processor
	TransferQueueProcessorScope
	// TransferTaskActivityScope is the scope used for activity task processing by transfer queue processor
	TransferTaskActivityScope
	// TransferTaskDecisionScope is the scope used for decision task processing by transfer queue processor
	TransferTaskDecisionScope
	// TransferTaskCloseExecutionScope is the scope used for close execution task processing by transfer queue processor
	TransferTaskCloseExecutionScope
	// TransferTaskCancelExecutionScope is the scope used for cancel execution task processing by transfer queue processor
	TransferTaskCancelExecutionScope
	// TransferTaskSignalExecutionScope is the scope used for signal execution task processing by transfer queue processor
	TransferTaskSignalExecutionScope
	// TransferTaskStartChildExecutionScope is the scope used for start child execution task processing by transfer queue processor
	TransferTaskStartChildExecutionScope
	// TimerQueueProcessorScope is the scope used by all metric emitted by timer queue processor
	TimerQueueProcessorScope
	// TimerTaskActivityTimeoutScope is the scope used by metric emitted by timer queue processor for processing activity timeouts
	TimerTaskActivityTimeoutScope
	// TimerTaskDecisionTimeoutScope is the scope used by metric emitted by timer queue processor for processing decision timeouts
	TimerTaskDecisionTimeoutScope
	// TimerTaskUserTimerScope is the scope used by metric emitted by timer queue processor for processing user timers
	TimerTaskUserTimerScope
	// TimerTaskWorkflowTimeoutScope is the scope used by metric emitted by timer queue processor for processing workflow timeouts.
	TimerTaskWorkflowTimeoutScope
	// TimerTaskDeleteHistoryEvent is the scope used by metric emitted by timer queue processor for processing history event cleanup
	TimerTaskDeleteHistoryEvent
	// HistoryEventNotificationScope is the scope used by shard history event nitification
	HistoryEventNotificationScope

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
		PersistenceDeleteWorkflowExecutionScope:                  {operation: "DeleteWorkflowExecution"},
		PersistenceGetCurrentExecutionScope:                      {operation: "GetCurrentExecution"},
		PersistenceGetTransferTasksScope:                         {operation: "GetTransferTasks"},
		PersistenceCompleteTransferTaskScope:                     {operation: "CompleteTransferTask"},
		PersistenceGetTimerIndexTasksScope:                       {operation: "GetTimerIndexTasks"},
		PersistenceCompleteTimerTaskScope:                        {operation: "CompleteTimerTask"},
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

		HistoryClientStartWorkflowExecutionScope:         {operation: "HistoryClientStartWorkflowExecution"},
		HistoryClientRecordActivityTaskHeartbeatScope:    {operation: "HistoryClientRecordActivityTaskHeartbeat"},
		HistoryClientRespondDecisionTaskCompletedScope:   {operation: "HistoryClientRespondDecisionTaskCompleted"},
		HistoryClientRespondDecisionTaskFailedScope:      {operation: "HistoryClientRespondDecisionTaskFailed"},
		HistoryClientRespondActivityTaskCompletedScope:   {operation: "HistoryClientRespondActivityTaskCompleted"},
		HistoryClientRespondActivityTaskFailedScope:      {operation: "HistoryClientRespondActivityTaskFailed"},
		HistoryClientRespondActivityTaskCanceledScope:    {operation: "HistoryClientRespondActivityTaskCanceled"},
		HistoryClientGetMutableStateScope:                {operation: "HistoryClientGetMutableState"},
		HistoryClientResetStickyTaskListScope:            {operation: "HistoryClientResetStickyTaskListScope"},
		HistoryClientDescribeWorkflowExecutionScope:      {operation: "HistoryClientDescribeWorkflowExecution"},
		HistoryClientRecordDecisionTaskStartedScope:      {operation: "HistoryClientRecordDecisionTaskStarted"},
		HistoryClientRecordActivityTaskStartedScope:      {operation: "HistoryClientRecordActivityTaskStarted"},
		HistoryClientRequestCancelWorkflowExecutionScope: {operation: "HistoryClientRequestCancelWorkflowExecution"},
		HistoryClientSignalWorkflowExecutionScope:        {operation: "HistoryClientSignalWorkflowExecution"},
		HistoryClientRemoveSignalMutableStateScope:       {operation: "HistoryClientRemoveSignalMutableStateScope"},
		HistoryClientTerminateWorkflowExecutionScope:     {operation: "HistoryClientTerminateWorkflowExecution"},
		HistoryClientScheduleDecisionTaskScope:           {operation: "HistoryClientScheduleDecisionTask"},
		HistoryClientRecordChildExecutionCompletedScope:  {operation: "HistoryClientRecordChildExecutionCompleted"},
		MatchingClientPollForDecisionTaskScope:           {operation: "MatchingClientPollForDecisionTask"},
		MatchingClientPollForActivityTaskScope:           {operation: "MatchingClientPollForActivityTask"},
		MatchingClientAddActivityTaskScope:               {operation: "MatchingClientAddActivityTask"},
		MatchingClientAddDecisionTaskScope:               {operation: "MatchingClientAddDecisionTask"},
		MatchingClientQueryWorkflowScope:                 {operation: "MatchingClientQueryWorkflow"},
		MatchingClientRespondQueryTaskCompletedScope:     {operation: "MatchingClientRespondQueryTaskCompleted"},
		MatchingClientCancelOutstandingPollScope:         {operation: "MatchingClientCancelOutstandingPoll"},
		MatchingClientDescribeTaskListScope:              {operation: "MatchingClientDescribeTaskList"},
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
		FrontendTerminateWorkflowExecutionScope:       {operation: "TerminateWorkflowExecution"},
		FrontendRequestCancelWorkflowExecutionScope:   {operation: "RequestCancelWorkflowExecution"},
		FrontendListOpenWorkflowExecutionsScope:       {operation: "ListOpenWorkflowExecutions"},
		FrontendListClosedWorkflowExecutionsScope:     {operation: "ListClosedWorkflowExecutions"},
		FrontendRegisterDomainScope:                   {operation: "RegisterDomain"},
		FrontendDescribeDomainScope:                   {operation: "DescribeDomain"},
		FrontendUpdateDomainScope:                     {operation: "UpdateDomain"},
		FrontendDeprecateDomainScope:                  {operation: "DeprecateDomain"},
		FrontendQueryWorkflowScope:                    {operation: "QueryWorkflow"},
		FrontendDescribeWorkflowExecutionScope:        {operation: "DescribeWorkflowExecution"},
		FrontendDescribeTaskListScope:                 {operation: "DescribeTaskList"},
	},
	// History Scope Names
	History: {
		HistoryStartWorkflowExecutionScope:         {operation: "StartWorkflowExecution"},
		HistoryRecordActivityTaskHeartbeatScope:    {operation: "RecordActivityTaskHeartbeat"},
		HistoryRespondDecisionTaskCompletedScope:   {operation: "RespondDecisionTaskCompleted"},
		HistoryRespondDecisionTaskFailedScope:      {operation: "RespondDecisionTaskFailed"},
		HistoryRespondActivityTaskCompletedScope:   {operation: "RespondActivityTaskCompleted"},
		HistoryRespondActivityTaskFailedScope:      {operation: "RespondActivityTaskFailed"},
		HistoryRespondActivityTaskCanceledScope:    {operation: "RespondActivityTaskCanceled"},
		HistoryGetMutableStateScope:                {operation: "GetMutableState"},
		HistoryResetStickyTaskListScope:            {operation: "ResetStickyTaskListScope"},
		HistoryDescribeWorkflowExecutionScope:      {operation: "DescribeWorkflowExecution"},
		HistoryRecordDecisionTaskStartedScope:      {operation: "RecordDecisionTaskStarted"},
		HistoryRecordActivityTaskStartedScope:      {operation: "RecordActivityTaskStarted"},
		HistorySignalWorkflowExecutionScope:        {operation: "SignalWorkflowExecution"},
		HistoryRemoveSignalMutableStateScope:       {operation: "RemoveSignalMutableState"},
		HistoryTerminateWorkflowExecutionScope:     {operation: "TerminateWorkflowExecution"},
		HistoryScheduleDecisionTaskScope:           {operation: "ScheduleDecisionTask"},
		HistoryRecordChildExecutionCompletedScope:  {operation: "RecordChildExecutionCompleted"},
		HistoryRequestCancelWorkflowExecutionScope: {operation: "RequestCancelWorkflowExecution"},
		HistoryShardControllerScope:                {operation: "ShardController"},
		TransferQueueProcessorScope:                {operation: "TransferQueueProcessor"},
		TransferTaskActivityScope:                  {operation: "TransferTaskActivity"},
		TransferTaskDecisionScope:                  {operation: "TransferTaskDecision"},
		TransferTaskCloseExecutionScope:            {operation: "TransferTaskCloseExecution"},
		TransferTaskCancelExecutionScope:           {operation: "TransferTaskCancelExecution"},
		TransferTaskSignalExecutionScope:           {operation: "TransferTaskSignalExecution"},
		TransferTaskStartChildExecutionScope:       {operation: "TransferTaskStartChildExecution"},
		TimerQueueProcessorScope:                   {operation: "TimerQueueProcessor"},
		TimerTaskActivityTimeoutScope:              {operation: "TimerTaskActivityTimeout"},
		TimerTaskDecisionTimeoutScope:              {operation: "TimerTaskDecisionTimeout"},
		TimerTaskUserTimerScope:                    {operation: "TimerTaskUserTimer"},
		TimerTaskWorkflowTimeoutScope:              {operation: "TimerTaskWorkflowTimeout"},
		TimerTaskDeleteHistoryEvent:                {operation: "TimerTaskDeleteHistoryEvent"},
		HistoryEventNotificationScope:              {operation: "HistoryEventNotification"},
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
		ReplicatorScope: {operation: "Replicator"},
	},
}

// Common Metrics enum
const (
	CadenceRequests = iota
	CadenceFailures
	CadenceLatency
	CadenceErrBadRequestCounter
	CadenceErrServiceBusyCounter
	CadenceErrEntityNotExistsCounter
	CadenceErrExecutionAlreadyStartedCounter
	CadenceErrDomainAlreadyExistsCounter
	CadenceErrCancellationAlreadyRequestedCounter
	CadenceErrQueryFailedCounter
	PersistenceRequests
	PersistenceFailures
	PersistenceLatency
	PersistenceErrShardExistsCounter
	PersistenceErrShardOwnershipLostCounter
	PersistenceErrConditionFailedCounter
	PersistenceErrTimeoutCounter
	PersistenceErrBusyCounter

	HistoryClientFailures
	MatchingClientFailures

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
)

// Worker metrics enum
const (
	ReplicatorMessages = iota + NumCommonMetrics
	ReplicatorFailures
	ReplicatorLatency
)

// MetricDefs record the metrics for all services
var MetricDefs = map[ServiceIdx]map[int]metricDefinition{
	Common: {
		CadenceRequests:                               {metricName: "cadence.requests", metricType: Counter},
		CadenceFailures:                               {metricName: "cadence.errors", metricType: Counter},
		CadenceLatency:                                {metricName: "cadence.latency", metricType: Timer},
		CadenceErrBadRequestCounter:                   {metricName: "cadence.errors.bad-request", metricType: Counter},
		CadenceErrServiceBusyCounter:                  {metricName: "cadence.errors.service-busy", metricType: Counter},
		CadenceErrEntityNotExistsCounter:              {metricName: "cadence.errors.entity-not-exists", metricType: Counter},
		CadenceErrExecutionAlreadyStartedCounter:      {metricName: "cadence.errors.execution-already-started", metricType: Counter},
		CadenceErrDomainAlreadyExistsCounter:          {metricName: "cadence.errors.domain-already-exists", metricType: Counter},
		CadenceErrCancellationAlreadyRequestedCounter: {metricName: "cadence.errors.cancellation-already-requested", metricType: Counter},
		CadenceErrQueryFailedCounter:                  {metricName: "cadence.errors.query-failed", metricType: Counter},
		PersistenceRequests:                           {metricName: "persistence.requests", metricType: Counter},
		PersistenceFailures:                           {metricName: "persistence.errors", metricType: Counter},
		PersistenceLatency:                            {metricName: "persistence.latency", metricType: Timer},
		PersistenceErrShardExistsCounter:              {metricName: "persistence.errors.shard-exists", metricType: Counter},
		PersistenceErrShardOwnershipLostCounter:       {metricName: "persistence.errors.shard-ownership-lost", metricType: Counter},
		PersistenceErrConditionFailedCounter:          {metricName: "persistence.errors.condition-failed", metricType: Counter},
		PersistenceErrTimeoutCounter:                  {metricName: "persistence.errors.timeout", metricType: Counter},
		PersistenceErrBusyCounter:                     {metricName: "persistence.errors.busy", metricType: Counter},
		HistoryClientFailures:                         {metricName: "client.history.errors", metricType: Counter},
		MatchingClientFailures:                        {metricName: "client.matching.errors", metricType: Counter},
	},
	Frontend: {},
	History: {
		TaskRequests:                                 {metricName: "task.requests", metricType: Counter},
		TaskFailures:                                 {metricName: "task.errors", metricType: Counter},
		TaskLatency:                                  {metricName: "task.latency", metricType: Counter},
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
		DecisionTypeChildWorkflowCounter:             {metricName: "child-workflow-decision", metricType: Counter},
		MultipleCompletionDecisionsCounter:           {metricName: "multiple-completion-decisions", metricType: Counter},
		FailedDecisionsCounter:                       {metricName: "failed-decisions", metricType: Counter},
		StaleMutableStateCounter:                     {metricName: "stale-mutable-state", metricType: Counter},
		ConcurrencyUpdateFailureCounter:              {metricName: "concurrency-update-failure", metricType: Counter},
		CadenceErrShardOwnershipLostCounter:          {metricName: "cadence.errors.shard-ownership-lost", metricType: Counter},
		CadenceErrEventAlreadyStartedCounter:         {metricName: "cadence.errors.event-already-started", metricType: Counter},
		HeartbeatTimeoutCounter:                      {metricName: "heartbeat-tiemout", metricType: Counter},
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
