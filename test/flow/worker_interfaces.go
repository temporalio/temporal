package flow

import (
	m "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
)

type (
	// ResultHandler that returns result
	ResultHandler func(err error, result []byte)

	// WorkflowContext Represents the context for workflow/decider.
	// Should only be used within the scope of workflow definition
	// TODO: Should model around GO context (When adding Cancel feature)
	WorkflowContext interface {
		ActivityClient
		WorkflowInfo() *WorkflowInfo
		Complete(result []byte)
		Fail(reason string, details []byte)
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
		Execute(context ActivityExecutionContext, input []byte) ([]byte, error)
	}

	// WorkflowDefinitionFactory that returns a workflow definition for a specific
	// workflow type.
	WorkflowDefinitionFactory func(workflowType m.WorkflowType) (WorkflowDefinition, error)

	// ActivityImplementationFactory that returns a activity implementation for a specific
	// activity type.
	ActivityImplementationFactory func(activityType m.ActivityType) (ActivityImplementation, error)

	// ExecuteActivityParameters configuration parameters for scheduling an activity
	ExecuteActivityParameters struct {
		ActivityID                    *string // Users can choose IDs but our framework makes it optional to decrease the crust.
		ActivityType                  m.ActivityType
		TaskListName                  string
		Input                         []byte
		ScheduleToCloseTimeoutSeconds int32
		ScheduleToStartTimeoutSeconds int32
		StartToCloseTimeoutSeconds    int32
		HeartbeatTimeoutSeconds       int32
	}

	// ActivityClient for dynamically schedule an activity for execution
	ActivityClient interface {
		ScheduleActivityTask(parameters ExecuteActivityParameters, callback ResultHandler)
	}

	// StartWorkflowOptions configuration parameters for starting a workflow
	StartWorkflowOptions struct {
		WorkflowID                             string
		WorkflowType                           m.WorkflowType
		TaskListName                           string
		WorkflowInput                          []byte
		ExecutionStartToCloseTimeoutSeconds    int32
		DecisionTaskStartToCloseTimeoutSeconds int32
		Identity                               string
	}

	// WorkflowClient is the client facing for starting a workflow.
	WorkflowClient struct {
		options           StartWorkflowOptions
		workflowExecution m.WorkflowExecution
		workflowService   m.TChanWorkflowService
		Identity          string
		reporter          common.Reporter
		// struct methods.
		// WorkflowExecution() m.WorkflowExecution
		// WorkflowType() m.WorkflowType
		// StartWorkflowExecution() (m.WorkflowExecution, error)
	}

	// WorkflowInfo is the information that the decider has access to during workflow execution.
	WorkflowInfo struct {
		workflowExecution m.WorkflowExecution
		workflowType      m.WorkflowType
		taskListName      string
	}
)
