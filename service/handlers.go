package service

import (
	"log"

	"code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/workflow"
	"github.com/uber/tchannel-go/thrift"
)

// WorkflowHandler - Thrift handler inteface for workflow service
type WorkflowHandler struct {
	engine workflow.Engine
}

// NewWorkflowHandler creates a thrift handler for the minions service
func NewWorkflowHandler(engine workflow.Engine) *WorkflowHandler {
	return &WorkflowHandler{
		engine: engine,
	}
}

// IsHealthy - Health endpoint.
func (wh *WorkflowHandler) IsHealthy(ctx thrift.Context) (bool, error) {
	log.Println("Workflow Health endpoint reached.")
	return true, nil
}

// PollForActivityTask - Poll for an activity task.
func (wh *WorkflowHandler) PollForActivityTask(
	ctx thrift.Context,
	pollRequest *minions.PollForActivityTaskRequest) (*minions.PollForActivityTaskResponse, error) {
	return wh.engine.PollForActivityTask(pollRequest)
}

// PollForDecisionTask - Poll for a decision task.
func (wh *WorkflowHandler) PollForDecisionTask(
	ctx thrift.Context,
	pollRequest *minions.PollForDecisionTaskRequest) (*minions.PollForDecisionTaskResponse, error) {
	return wh.engine.PollForDecisionTask(pollRequest)
}

// RecordActivityTaskHeartbeat - Record Activity Task Heart beat.
func (wh *WorkflowHandler) RecordActivityTaskHeartbeat(
	ctx thrift.Context,
	heartbeatRequest *minions.RecordActivityTaskHeartbeatRequest) (*minions.RecordActivityTaskHeartbeatResponse, error) {
	panic("Not Implemented")
}

// RespondActivityTaskCompleted - Record Activity Task Heart beat
func (wh *WorkflowHandler) RespondActivityTaskCompleted(
	ctx thrift.Context,
	completeRequest *minions.RespondActivityTaskCompletedRequest) error {
	return wh.engine.RespondActivityTaskCompleted(completeRequest)
}

// RespondActivityTaskFailed - Record Activity Task Heart beat
func (wh *WorkflowHandler) RespondActivityTaskFailed(
	ctx thrift.Context,
	failRequest *minions.RespondActivityTaskFailedRequest) error {
	return wh.engine.RespondActivityTaskFailed(failRequest)

}

// RespondDecisionTaskCompleted - Record Activity Task Heart beat
func (wh *WorkflowHandler) RespondDecisionTaskCompleted(
	ctx thrift.Context,
	completeRequest *minions.RespondDecisionTaskCompletedRequest) error {
	return wh.engine.RespondDecisionTaskCompleted(completeRequest)
}

// StartWorkflowExecution - Record Activity Task Heart beat
func (wh *WorkflowHandler) StartWorkflowExecution(
	ctx thrift.Context,
	startRequest *minions.StartWorkflowExecutionRequest) (*minions.StartWorkflowExecutionResponse, error) {
	wf, err := wh.engine.StartWorkflowExecution(startRequest)
	if err != nil {
		return nil, err
	}
	return &minions.StartWorkflowExecutionResponse{
		RunId: wf.RunId,
	}, err
}
