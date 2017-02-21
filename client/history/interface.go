package history

import (
	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
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
	RespondActivityTaskCanceled(request *workflow.RespondActivityTaskCanceledRequest) error
	RecordActivityTaskHeartbeat(request *workflow.RecordActivityTaskHeartbeatRequest) (*workflow.RecordActivityTaskHeartbeatResponse, error)
}
