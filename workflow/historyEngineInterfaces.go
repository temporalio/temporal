package workflow

import (
	workflow "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/persistence"
)

type (
	// HistoryEngine represents an interface for managing workflow execution history.
	HistoryEngine interface {
		common.Daemon
		// TODO: Convert workflow.WorkflowExecution to pointer all over the place
		StartWorkflowExecution(request *workflow.StartWorkflowExecutionRequest) (workflow.WorkflowExecution, error)
		GetWorkflowExecution(
			request *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error)
		UpdateWorkflowExecution(request *persistence.UpdateWorkflowExecutionRequest) error
		RespondDecisionTaskCompleted(request *workflow.RespondDecisionTaskCompletedRequest) error
		RespondActivityTaskCompleted(request *workflow.RespondActivityTaskCompletedRequest) error
		RespondActivityTaskFailed(request *workflow.RespondActivityTaskFailedRequest) error
	}

	historySerializer interface {
		Serialize(history []*workflow.HistoryEvent) ([]byte, error)
		Deserialize(data []byte) ([]*workflow.HistoryEvent, error)
	}

	transferQueueProcessor interface {
		common.Daemon
	}
)
