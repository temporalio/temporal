package workflow

import (
	gen "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common"
)

var nilWorkflowExecution = gen.WorkflowExecution{}

type (
	// Engine represents an interface for workflow engine
	Engine interface {
		common.Daemon
		// TODO: This is only needed temporarily because it is used by thrift handler.
		// The handler will be changed to rely on the HistoryEngine and MatchingEngine interfaces directly, so this can be removed.
		StartWorkflowExecution(request *gen.StartWorkflowExecutionRequest) (*gen.StartWorkflowExecutionResponse, error)
		GetWorkflowExecutionHistory(
			request *gen.GetWorkflowExecutionHistoryRequest) (*gen.GetWorkflowExecutionHistoryResponse, error)
		PollForDecisionTask(request *gen.PollForDecisionTaskRequest) (*gen.PollForDecisionTaskResponse, error)
		PollForActivityTask(request *gen.PollForActivityTaskRequest) (*gen.PollForActivityTaskResponse, error)
		RespondDecisionTaskCompleted(request *gen.RespondDecisionTaskCompletedRequest) error
		RespondActivityTaskCompleted(request *gen.RespondActivityTaskCompletedRequest) error
		RespondActivityTaskFailed(request *gen.RespondActivityTaskFailedRequest) error
	}
)
