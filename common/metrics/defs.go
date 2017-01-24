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
//  Please keep them in sync with the {Counter,Timer,Gauge}Names below. Order matters
const (
	Common = iota
	Frontend
	NumServices
)

// Common tags for all services
const (
	HostnameTagName  = "hostname"
	OperationTagName = "operation"
)

// This package should hold all the metrics and tags for cadence
const (
	UnknownDirectoryTagValue = "Unknown"
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
	// TODO: divide into common and per service operation scopes
	CreateShardScope = iota
	GetShardScope
	UpdateShardScope
	CreateWorkflowExecutionScope
	GetWorkflowExecutionScope
	UpdateWorkflowExecutionScope
	DeleteWorkflowExecutionScope
	GetTransferTasksScope
	CompleteTransferTaskScope
	CreateTaskScope
	GetTasksScope
	CompleteTaskScope
	LeaseTaskListScope
	StartWorkflowExecutionScope
	PollForDecisionTaskScope
	PollForActivityTaskScope
	RespondDecisionTaskCompletedScope
	RespondActivityTaskCompletedScope
	RespondActivityTaskFailedScope
	GetWorkflowExecutionHistoryScope
	GetTimerIndexTasksScope
	GetWorkflowMutableStateScope
)

// ScopeDefs record the scopes for all services
var ScopeDefs = map[ServiceIdx]map[int]scopeDefinition{
	// common scope Names
	Common: {},
	// frontend Scope Names
	Frontend: {
		CreateShardScope:                  {operation: "CreateShard"},
		GetShardScope:                     {operation: "GetShard"},
		UpdateShardScope:                  {operation: "UpdateShard"},
		CreateWorkflowExecutionScope:      {operation: "CreateWorkflowExecution"},
		GetWorkflowExecutionScope:         {operation: "GetWorkflowExecution"},
		UpdateWorkflowExecutionScope:      {operation: "UpdateWorkflowExecution"},
		DeleteWorkflowExecutionScope:      {operation: "DeleteWorkflowExecution"},
		GetTransferTasksScope:             {operation: "GetTransferTasks"},
		CompleteTransferTaskScope:         {operation: "CompleteTransferTask"},
		CreateTaskScope:                   {operation: "CreateTask"},
		GetTasksScope:                     {operation: "GetTasks"},
		CompleteTaskScope:                 {operation: "CompleteTask"},
		LeaseTaskListScope:                {operation: "LeaseTaskList"},
		StartWorkflowExecutionScope:       {operation: "StartWorkflowExecution"},
		PollForDecisionTaskScope:          {operation: "PollForDecisionTask"},
		PollForActivityTaskScope:          {operation: "PollForActivityTask"},
		RespondDecisionTaskCompletedScope: {operation: "RespondDecisionTaskCompleted"},
		RespondActivityTaskCompletedScope: {operation: "RespondActivityTaskCompleted"},
		RespondActivityTaskFailedScope:    {operation: "RespondActivityTaskFailed"},
		GetWorkflowExecutionHistoryScope:  {operation: "GetWorkflowExecutionHistory"},
		GetTimerIndexTasksScope:           {operation: "GetTimerIndexTasks"},
		GetWorkflowMutableStateScope:      {operation: "GetWorkflowMutableState"},
	},
}

// Metrics enum
const (
	// TODO: divide into common and per service metrics
	WorkflowRequests = iota
	WorkflowFailures
	WorkflowLatency
)

// MetricDefs record the metrics for all services
var MetricDefs = map[ServiceIdx]map[int]metricDefinition{
	Common: {
		WorkflowRequests: {metricName: "cadence.requests", metricType: Counter},
		WorkflowFailures: {metricName: "cadence.errors", metricType: Counter},
		WorkflowLatency:  {metricName: "cadence.latency", metricType: Timer},
	},
	Frontend: {},
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
