package service

import (
	"log"

	"code.uber.internal/devexp/minions/.gen/go/minions"
	"github.com/uber/tchannel-go/thrift"
)

// WorkflowHandler - Thrift handler inteface for workflow service
type WorkflowHandler struct {
}

// IsHealthy - Health endpoint.
func (wh WorkflowHandler) IsHealthy(ctx thrift.Context) (bool, error) {
	log.Println("Workflow Health endpoint reached.")
	return true, nil
}

// PollForActivityTask - Poll for an activity task.
func (wh WorkflowHandler) PollForActivityTask(
	ctx thrift.Context,
	pollRequest *minions.PollForActivityTaskRequest) (*minions.PollForActivityTaskResponse, error) {
	panic("Not Implemented")
}

// PollForDecisionTask - Poll for a decision task.
func (wh WorkflowHandler) PollForDecisionTask(
	ctx thrift.Context,
	pollRequest *minions.PollForDecisionTaskRequest) (*minions.PollForDecisionTaskResponse, error) {
	panic("Not Implemented")
}

// RecordActivityTaskHeartbeat - Record Activity Task Heart beat.
func (wh WorkflowHandler) RecordActivityTaskHeartbeat(
	ctx thrift.Context,
	heartbeatRequest *minions.RecordActivityTaskHeartbeatRequest) (*minions.RecordActivityTaskHeartbeatResponse, error) {
	panic("Not Implemented")
}

// RespondActivityTaskCompleted - Record Activity Task Heart beat
func (wh WorkflowHandler) RespondActivityTaskCompleted(
	ctx thrift.Context,
	completeRequest *minions.RespondActivityTaskCompletedRequest) error {
	panic("Not Implemented")
}

// RespondActivityTaskFailed - Record Activity Task Heart beat
func (wh WorkflowHandler) RespondActivityTaskFailed(
	ctx thrift.Context,
	failRequest *minions.RespondActivityTaskFailedRequest) error {
	panic("Not Implemented")

}

// RespondDecisionTaskCompleted - Record Activity Task Heart beat
func (wh WorkflowHandler) RespondDecisionTaskCompleted(
	ctx thrift.Context,
	completeRequest *minions.RespondDecisionTaskCompletedRequest) error {
	panic("Not Implemented")
}

// StartWorkflowExecution - Record Activity Task Heart beat
func (wh WorkflowHandler) StartWorkflowExecution(
	ctx thrift.Context,
	startRequest *minions.StartWorkflowExecutionRequest) (*minions.StartWorkflowExecutionResponse, error) {
	panic("Not Implemented")
}
