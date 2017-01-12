package frontend

import (
	"log"

	"code.uber.internal/devexp/minions/.gen/go/minions"
	gen "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/client/history"
	"code.uber.internal/devexp/minions/client/matching"
	"code.uber.internal/devexp/minions/common"
	"github.com/uber/tchannel-go/thrift"
)

var _ minions.TChanWorkflowService = (*WorkflowHandler)(nil)

// WorkflowHandler - Thrift handler inteface for workflow service
type WorkflowHandler struct {
	history  history.Client
	matching matching.Client
	common.Service
}

// NewWorkflowHandler creates a thrift handler for the minions service
func NewWorkflowHandler(sVice common.Service) (*WorkflowHandler, []thrift.TChanServer) {
	handler := &WorkflowHandler{
		Service: sVice,
	}
	return handler, []thrift.TChanServer{minions.NewTChanWorkflowServiceServer(handler)}
}

// Start starts the handler
func (wh *WorkflowHandler) Start(thriftService []thrift.TChanServer) error {
	wh.Service.Start(thriftService)
	var err error
	wh.history, err = wh.Service.GetClientFactory().NewHistoryClient()
	if err != nil {
		return err
	}
	wh.matching, err = wh.Service.GetClientFactory().NewMatchingClient()
	if err != nil {
		return err
	}
	return nil
}

// Stop stops the handler
func (wh *WorkflowHandler) Stop() {
	wh.Service.Stop()
}

// IsHealthy - Health endpoint.
func (wh *WorkflowHandler) IsHealthy(ctx thrift.Context) (bool, error) {
	log.Println("Workflow Health endpoint reached.")
	return true, nil
}

// PollForActivityTask - Poll for an activity task.
func (wh *WorkflowHandler) PollForActivityTask(
	ctx thrift.Context,
	pollRequest *gen.PollForActivityTaskRequest) (*gen.PollForActivityTaskResponse, error) {
	wh.Service.GetLogger().Debug("Received PollForActivityTask")
	return wh.matching.PollForActivityTask(pollRequest)
}

// PollForDecisionTask - Poll for a decision task.
func (wh *WorkflowHandler) PollForDecisionTask(
	ctx thrift.Context,
	pollRequest *gen.PollForDecisionTaskRequest) (*gen.PollForDecisionTaskResponse, error) {
	wh.Service.GetLogger().Debug("Received PollForDecisionTask")
	return wh.matching.PollForDecisionTask(pollRequest)
}

// RecordActivityTaskHeartbeat - Record Activity Task Heart beat.
func (wh *WorkflowHandler) RecordActivityTaskHeartbeat(
	ctx thrift.Context,
	heartbeatRequest *gen.RecordActivityTaskHeartbeatRequest) (*gen.RecordActivityTaskHeartbeatResponse, error) {
	panic("Not Implemented")
}

// RespondActivityTaskCompleted - Record Activity Task Heart beat
func (wh *WorkflowHandler) RespondActivityTaskCompleted(
	ctx thrift.Context,
	completeRequest *gen.RespondActivityTaskCompletedRequest) error {
	return wh.history.RespondActivityTaskCompleted(completeRequest)
}

// RespondActivityTaskFailed - Record Activity Task Heart beat
func (wh *WorkflowHandler) RespondActivityTaskFailed(
	ctx thrift.Context,
	failRequest *gen.RespondActivityTaskFailedRequest) error {
	return wh.history.RespondActivityTaskFailed(failRequest)

}

// RespondDecisionTaskCompleted - Record Activity Task Heart beat
func (wh *WorkflowHandler) RespondDecisionTaskCompleted(
	ctx thrift.Context,
	completeRequest *gen.RespondDecisionTaskCompletedRequest) error {
	return wh.history.RespondDecisionTaskCompleted(completeRequest)
}

// StartWorkflowExecution - Record Activity Task Heart beat
func (wh *WorkflowHandler) StartWorkflowExecution(
	ctx thrift.Context,
	startRequest *gen.StartWorkflowExecutionRequest) (*gen.StartWorkflowExecutionResponse, error) {
	wh.Service.GetLogger().Debug(" Received StartWorkflowExecution")
	return wh.history.StartWorkflowExecution(startRequest)
}

// GetWorkflowExecutionHistory - retrieves the hisotry of workflow execution
func (wh *WorkflowHandler) GetWorkflowExecutionHistory(
	ctx thrift.Context,
	getRequest *gen.GetWorkflowExecutionHistoryRequest) (*gen.GetWorkflowExecutionHistoryResponse, error) {
	return wh.history.GetWorkflowExecutionHistory(getRequest)
}
