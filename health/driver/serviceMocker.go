package driver

import (
	m "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/workflow"
	log "github.com/Sirupsen/logrus"
	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"
)

type (
	// ServiceMockEngine implements TChanWorkflowService to talk to engine directly
	ServiceMockEngine struct {
		engine workflow.Engine
		logger bark.Logger
	}
)

// NewServiceMockEngine creats an isntance of mocker service layer for the engine
func NewServiceMockEngine(engine workflow.Engine) *ServiceMockEngine {
	mockEngine := &ServiceMockEngine{}
	mockEngine.logger = bark.NewLoggerFromLogrus(log.New())
	mockEngine.engine = engine
	return mockEngine
}

// PollForActivityTask polls for activity task.
func (se *ServiceMockEngine) PollForActivityTask(ctx thrift.Context, pollRequest *m.PollForActivityTaskRequest) (*m.PollForActivityTaskResponse, error) {
	return se.engine.PollForActivityTask(pollRequest)
}

// PollForDecisionTask polls for decision task.
func (se *ServiceMockEngine) PollForDecisionTask(ctx thrift.Context, pollRequest *m.PollForDecisionTaskRequest) (*m.PollForDecisionTaskResponse, error) {
	return se.engine.PollForDecisionTask(pollRequest)
}

// RecordActivityTaskHeartbeat records activity task heart beat.
func (se *ServiceMockEngine) RecordActivityTaskHeartbeat(ctx thrift.Context, heartbeatRequest *m.RecordActivityTaskHeartbeatRequest) (*m.RecordActivityTaskHeartbeatResponse, error) {
	// TODO:
	return nil, nil
}

// RespondActivityTaskCompleted responds to an activity completion.
func (se *ServiceMockEngine) RespondActivityTaskCompleted(ctx thrift.Context, completeRequest *m.RespondActivityTaskCompletedRequest) error {
	return se.engine.RespondActivityTaskCompleted(completeRequest)
}

// RespondActivityTaskFailed responds to an activity failure.
func (se *ServiceMockEngine) RespondActivityTaskFailed(ctx thrift.Context, failRequest *m.RespondActivityTaskFailedRequest) error {
	return se.engine.RespondActivityTaskFailed(failRequest)
}

// RespondDecisionTaskCompleted responds to an decision completion.
func (se *ServiceMockEngine) RespondDecisionTaskCompleted(ctx thrift.Context, completeRequest *m.RespondDecisionTaskCompletedRequest) error {
	return se.engine.RespondDecisionTaskCompleted(completeRequest)
}

// StartWorkflowExecution starts a workflow.
func (se *ServiceMockEngine) StartWorkflowExecution(ctx thrift.Context, startRequest *m.StartWorkflowExecutionRequest) (*m.StartWorkflowExecutionResponse, error) {
	workflowExecution, err := se.engine.StartWorkflowExecution(startRequest)
	return &m.StartWorkflowExecutionResponse{RunId: workflowExecution.RunId}, err
}

// Start the workflow engine in a different go routine.
func (se *ServiceMockEngine) Start() {
	go se.engine.Start()
}

// GetWorkflowExecutionHistory retrieves the history for given workflow execution
func (se *ServiceMockEngine) GetWorkflowExecutionHistory(ctx thrift.Context,
	request *m.GetWorkflowExecutionHistoryRequest) (*m.GetWorkflowExecutionHistoryResponse, error) {
	return se.engine.GetWorkflowExecutionHistory(request)
}
