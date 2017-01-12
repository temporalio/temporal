package frontend

import (
	"code.uber.internal/devexp/minions/.gen/go/shared"
)

// Client is the interface exposed by frontend service client
type Client interface {
	GetWorkflowExecutionHistory(getRequest *shared.GetWorkflowExecutionHistoryRequest) (*shared.GetWorkflowExecutionHistoryResponse, error)
	PollForActivityTask(pollRequest *shared.PollForActivityTaskRequest) (*shared.PollForActivityTaskResponse, error)
	PollForDecisionTask(pollRequest *shared.PollForDecisionTaskRequest) (*shared.PollForDecisionTaskResponse, error)
	RecordActivityTaskHeartbeat(heartbeatRequest *shared.RecordActivityTaskHeartbeatRequest) (*shared.RecordActivityTaskHeartbeatResponse, error)
	RespondActivityTaskCompleted(completeRequest *shared.RespondActivityTaskCompletedRequest) error
	RespondActivityTaskFailed(failRequest *shared.RespondActivityTaskFailedRequest) error
	RespondDecisionTaskCompleted(completeRequest *shared.RespondDecisionTaskCompletedRequest) error
	StartWorkflowExecution(startRequest *shared.StartWorkflowExecutionRequest) (*shared.StartWorkflowExecutionResponse, error)
}
