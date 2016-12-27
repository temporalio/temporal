package flow

import (
	m "code.uber.internal/devexp/minions/.gen/go/minions"
	gen "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common/metrics"
)

type (
	// Error to return from Workflow and Activity implementations.
	Error interface {
		error
		Reason() string
		Details() []byte
	}

	// ResultHandler that returns result
	ResultHandler func(result []byte, err Error)

	// WorkflowContext Represents the context for workflow/decider.
	// Should only be used within the scope of workflow definition
	// TODO: Should model around GO context (When adding Cancel feature)
	WorkflowContext interface {
		AsyncActivityClient
		WorkflowInfo() *WorkflowInfo
		Complete(result []byte, err Error)
	}

	// ActivityExecutionContext is context object passed to an activity implementation.
	// TODO: Should model around GO context (When adding Cancel feature)
	ActivityExecutionContext interface {
		TaskToken() []byte
		RecordActivityHeartbeat(details []byte) error
	}

	// WorkflowDefinition wraps the code that can execute a workflow.
	WorkflowDefinition interface {
		Execute(context WorkflowContext, input []byte)
	}

	// ActivityImplementation wraps the code to execute an activity
	ActivityImplementation interface {
		Execute(context ActivityExecutionContext, input []byte) ([]byte, Error)
	}

	// WorkflowDefinitionFactory that returns a workflow definition for a specific
	// workflow type.
	WorkflowDefinitionFactory func(workflowType gen.WorkflowType) (WorkflowDefinition, Error)

	// ActivityImplementationFactory that returns a activity implementation for a specific
	// activity type.
	ActivityImplementationFactory func(activityType gen.ActivityType) (ActivityImplementation, Error)

	// ExecuteActivityParameters configuration parameters for scheduling an activity
	ExecuteActivityParameters struct {
		ActivityID                    *string // Users can choose IDs but our framework makes it optional to decrease the crust.
		ActivityType                  gen.ActivityType
		TaskListName                  string
		Input                         []byte
		ScheduleToCloseTimeoutSeconds int32
		ScheduleToStartTimeoutSeconds int32
		StartToCloseTimeoutSeconds    int32
		HeartbeatTimeoutSeconds       int32
	}

	// AsyncActivityClient for requesting activity execution
	AsyncActivityClient interface {
		ExecuteActivity(parameters ExecuteActivityParameters, callback ResultHandler)
	}

	// StartWorkflowOptions configuration parameters for starting a workflow
	StartWorkflowOptions struct {
		WorkflowID                             string
		WorkflowType                           gen.WorkflowType
		TaskListName                           string
		WorkflowInput                          []byte
		ExecutionStartToCloseTimeoutSeconds    int32
		DecisionTaskStartToCloseTimeoutSeconds int32
		Identity                               string
	}

	// WorkflowClient is the client facing for starting a workflow.
	WorkflowClient struct {
		options           StartWorkflowOptions
		workflowExecution gen.WorkflowExecution
		workflowService   m.TChanWorkflowService
		Identity          string
		reporter          metrics.Reporter
	}

	// WorkflowInfo is the information that the decider has access to during workflow execution.
	WorkflowInfo struct {
		workflowExecution gen.WorkflowExecution
		workflowType      gen.WorkflowType
		taskListName      string
	}
)
