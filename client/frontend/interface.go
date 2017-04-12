package frontend

import (
	"github.com/uber/cadence/.gen/go/shared"
)

// Client is the interface exposed by frontend service client
type Client interface {
	RegisterDomain(registerRequest *shared.RegisterDomainRequest) error
	DescribeDomain(describeRequest *shared.DescribeDomainRequest) (*shared.DescribeDomainResponse, error)
	UpdateDomain(updateRequest *shared.UpdateDomainRequest) (*shared.UpdateDomainResponse, error)
	DeprecateDomain(deprecateRequest *shared.DeprecateDomainRequest) error
	GetWorkflowExecutionHistory(getRequest *shared.GetWorkflowExecutionHistoryRequest) (*shared.GetWorkflowExecutionHistoryResponse, error)
	PollForActivityTask(pollRequest *shared.PollForActivityTaskRequest) (*shared.PollForActivityTaskResponse, error)
	PollForDecisionTask(pollRequest *shared.PollForDecisionTaskRequest) (*shared.PollForDecisionTaskResponse, error)
	RecordActivityTaskHeartbeat(heartbeatRequest *shared.RecordActivityTaskHeartbeatRequest) (*shared.RecordActivityTaskHeartbeatResponse, error)
	RespondActivityTaskCompleted(completeRequest *shared.RespondActivityTaskCompletedRequest) error
	RespondActivityTaskFailed(failRequest *shared.RespondActivityTaskFailedRequest) error
	RespondActivityTaskCanceled(cancelRequest *shared.RespondActivityTaskCanceledRequest) error
	RespondDecisionTaskCompleted(completeRequest *shared.RespondDecisionTaskCompletedRequest) error
	StartWorkflowExecution(startRequest *shared.StartWorkflowExecutionRequest) (*shared.StartWorkflowExecutionResponse, error)
	SignalWorkflowExecution(request *shared.SignalWorkflowExecutionRequest) error
	TerminateWorkflowExecution(terminateRequest *shared.TerminateWorkflowExecutionRequest) error
	ListOpenWorkflowExecutions(listRequest *shared.ListOpenWorkflowExecutionsRequest) (*shared.ListOpenWorkflowExecutionsResponse, error)
	ListClosedWorkflowExecutions(listRequest *shared.ListClosedWorkflowExecutionsRequest) (*shared.ListClosedWorkflowExecutionsResponse, error)
}
