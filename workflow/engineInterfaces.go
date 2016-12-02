package workflow

import (
	workflow "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
)

var nilWorkflowExecution = workflow.WorkflowExecution{}

type (
	// Engine represents an interface for workflow engine
	Engine interface {
		common.Daemon
		// TODO: Convert workflow.WorkflowExecution to pointer all over the place
		// TODO: This is only needed temporarily because it is used by thrift handler.
		// The handler will be changed to rely on the HistoryEngine and MatchingEngine interfaces directly, so this can be removed.
		StartWorkflowExecution(request *workflow.StartWorkflowExecutionRequest) (workflow.WorkflowExecution, error)
		PollForDecisionTask(request *workflow.PollForDecisionTaskRequest) (*workflow.PollForDecisionTaskResponse, error)
		PollForActivityTask(request *workflow.PollForActivityTaskRequest) (*workflow.PollForActivityTaskResponse, error)
		RespondDecisionTaskCompleted(request *workflow.RespondDecisionTaskCompletedRequest) error
		RespondActivityTaskCompleted(request *workflow.RespondActivityTaskCompletedRequest) error
		RespondActivityTaskFailed(request *workflow.RespondActivityTaskFailedRequest) error
	}
)
