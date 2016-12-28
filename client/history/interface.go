package history

import (
	h "code.uber.internal/devexp/minions/.gen/go/history"
	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
)

// Client is the interface exposed by history service client
type Client interface {
	StartWorkflowExecution(request *workflow.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error)
	GetWorkflowExecutionHistory(
		request *workflow.GetWorkflowExecutionHistoryRequest) (*workflow.GetWorkflowExecutionHistoryResponse, error)
	RecordDecisionTaskStarted(request *h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error)
	RecordActivityTaskStarted(request *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error)
	RespondDecisionTaskCompleted(request *workflow.RespondDecisionTaskCompletedRequest) error
	RespondActivityTaskCompleted(request *workflow.RespondActivityTaskCompletedRequest) error
	RespondActivityTaskFailed(request *workflow.RespondActivityTaskFailedRequest) error
}
