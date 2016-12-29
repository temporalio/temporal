package metrics

// MetricName is the name of the metric
type MetricName string

// MetricType is the type of the metric, which can be one of the 3 below
type MetricType int

// MetricTypes which are supported
const (
	Counter MetricType = iota
	Timer
	Gauge
)

// Common tags for all services
const (
	HostnameTagName  = "hostname"
	OperationTagName = "operation"
)

// This package should hold all the metrics and tags for cherami
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

// Service names for all service who emit m3 Please keep them in sync with the {Counter,Timer,Gauge}Names below.  Order matters
const (
	Frontend = iota
	NumServices
)

// operation scopes for frontend
const (
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
	StartWorkflowExecutionScope
	PollForDecisionTaskScope
	PollForActivityTaskScope
	RespondDecisionTaskCompletedScope
	RespondActivityTaskCompletedScope
	RespondActivityTaskFailedScope
	GetWorkflowExecutionHistoryScope
)

// ScopeToTags record the scope name for all services
var ScopeToTags = [NumServices][]map[string]string{
	// frontend Scope Names
	{
		{OperationTagName: CreateShardOperationTagValue},
		{OperationTagName: GetShardOperationTagValue},
		{OperationTagName: UpdateShardOperationTagValue},
		{OperationTagName: CreateWorkflowExecutionOperationTagValue},
		{OperationTagName: GetWorkflowExecutionOperationTagValue},
		{OperationTagName: UpdateWorkflowExecutionOperationTagValue},
		{OperationTagName: DeleteWorkflowExecutionOperationTagValue},
		{OperationTagName: GetTransferTasksOperationTagValue},
		{OperationTagName: CompleteTransferTaskOperationTagValue},
		{OperationTagName: CreateTaskOperationTagValue},
		{OperationTagName: GetTasksOperationTagValue},
		{OperationTagName: CompleteTaskOperationTagValue},
		{OperationTagName: StartWorkflowExecutionOperationTagValue},
		{OperationTagName: PollForDecisionTaskOperationTagValue},
		{OperationTagName: PollForActivityTaskOperationTagValue},
		{OperationTagName: RespondDecisionTaskCompletedOperationTagValue},
		{OperationTagName: RespondActivityTaskCompletedOperationTagValue},
		{OperationTagName: RespondActivityTaskFailedOperationTagValue},
		{OperationTagName: GetWorkflowExecutionHistoryOperationTagValue},
	},
}

// Counter enums for frontend.  Please keep them in sync with the Frontend Service counter names below.
// Order between the two also matters.
const (
	WorkflowRequests = iota
	WorkflowFailures
)

// Timer enums for frontend.  Please keep them in sync with the Workflow Service timers below.  Order between the
// two also matters.
const (
	WorkflowLatencyTimer = iota
)

// CounterNames is counter names for metrics
var CounterNames = [NumServices]map[int]string{
	// Frontend Counter Names
	{
		WorkflowRequests: "workflow.requests",
		WorkflowFailures: "workflow.errors",
	},
}

// TimerNames is timer names for metrics
var TimerNames = [NumServices]map[int]string{
	// Frontend Timer Names
	{
		WorkflowLatencyTimer: "workflow.latency",
	},
}

// GaugeNames is gauge names for metrics
var GaugeNames = [NumServices]map[int]string{}

// Frontend operation tag values as seen by the M3 backend
const (
	CreateShardOperationTagValue                  = "CreateShard"
	GetShardOperationTagValue                     = "GetShard"
	UpdateShardOperationTagValue                  = "UpdateShard"
	CreateWorkflowExecutionOperationTagValue      = "CreateWorkflowExecution"
	GetWorkflowExecutionOperationTagValue         = "GetWorkflowExecution"
	UpdateWorkflowExecutionOperationTagValue      = "UpdateWorkflowExecution"
	DeleteWorkflowExecutionOperationTagValue      = "DeleteWorkflowExecution"
	GetTransferTasksOperationTagValue             = "GetTransferTasks"
	CompleteTransferTaskOperationTagValue         = "CompleteTransferTask"
	CreateTaskOperationTagValue                   = "CreateTask"
	GetTasksOperationTagValue                     = "GetTasks"
	CompleteTaskOperationTagValue                 = "CompleteTask"
	StartWorkflowExecutionOperationTagValue       = "StartWorkflowExecution"
	PollForDecisionTaskOperationTagValue          = "PollForDecisionTask"
	PollForActivityTaskOperationTagValue          = "PollForActivityTask"
	RespondDecisionTaskCompletedOperationTagValue = "RespondDecisionTaskCompleted"
	RespondActivityTaskCompletedOperationTagValue = "RespondActivityTaskCompleted"
	RespondActivityTaskFailedOperationTagValue    = "RespondActivityTaskFailed"
	GetWorkflowExecutionHistoryOperationTagValue  = "GetWorkflowExecutionHistory"
)

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
