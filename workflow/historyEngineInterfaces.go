package workflow

import (
	h "code.uber.internal/devexp/minions/.gen/go/history"
	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common"
)

type (
	// HistoryEngine represents an interface for managing workflow execution history.
	HistoryEngine interface {
		common.Daemon
		// TODO: Convert workflow.WorkflowExecution to pointer all over the place
		StartWorkflowExecution(request *workflow.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error)
		GetWorkflowExecutionHistory(
			request *workflow.GetWorkflowExecutionHistoryRequest) (*workflow.GetWorkflowExecutionHistoryResponse, error)
		RecordDecisionTaskStarted(request *h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error)
		RecordActivityTaskStarted(request *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error)
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

	timerQueueProcessor interface {
		common.Daemon
		NotifyNewTimer()
	}
)
