package metrics

import (
	"code.uber.internal/devexp/minions/common"
)

// MetricName is the name of the metric
//type MetricName string

// MetricType is the type of the metric, which can be one of the 3 below
//type MetricType int

// MetricTypes which are supported
const (
	Counter common.MetricType = iota
	Timer
	Gauge
)

// Common tags for all services
const (
	HostnameTagName  = "hostname"
	OperationTagName = "operation"
)

// Service names for all service who emit m3 Please keep them in sync with the {Counter,Timer,Gauge}Names below.  Order matters
const (
	Workflow = iota
	NumServices
)

// operation scopes  for workflow service
const (
	CreateWorkflowExecutionScope = iota
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
	// Worfklow Scope Names
	{
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

// Counter enums for workflow.  Please keep them in sync with the Workflow Service counter names below.
// Order between the two also matters.
const (
	WorkflowRequests = iota
	WorkflowFailures
)

// Timer enums for workflow.  Please keep them in sync with the Workflow Service timers below.  Order between the
// two also matters.
const (
	WorkflowLatencyTimer = iota
)

// CounterNames is counter names for metrics
var CounterNames = [NumServices]map[int]string{
	// Workflow Counter Names
	{
		WorkflowRequests: "workflow.requests",
		WorkflowFailures: "workflow.errors",
	},
}

// TimerNames is timer names for metrics
var TimerNames = [NumServices]map[int]string{
	// Workflow Timer Names
	{
		WorkflowLatencyTimer: "workflow.latency",
	},
}

// GaugeNames is gauge names for metrics
var GaugeNames = [NumServices]map[int]string{}

// Workflow operation tag values as seen by the M3 backend
const (
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
