package frontend

import (
	"log"

	"code.uber.internal/devexp/minions/.gen/go/minions"
	gen "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/client/history"
	"code.uber.internal/devexp/minions/client/matching"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/service"
	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"
)

var _ minions.TChanWorkflowService = (*WorkflowHandler)(nil)

// WorkflowHandler - Thrift handler inteface for workflow service
type WorkflowHandler struct {
	history         history.Client
	matching        matching.Client
	tokenSerializer common.TaskTokenSerializer
	service.Service
}

// NewWorkflowHandler creates a thrift handler for the minions service
func NewWorkflowHandler(sVice service.Service) (*WorkflowHandler, []thrift.TChanServer) {
	handler := &WorkflowHandler{
		Service:         sVice,
		tokenSerializer: common.NewJSONTaskTokenSerializer(),
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
	resp, err := wh.matching.PollForActivityTask(pollRequest)
	if err != nil {
		wh.Service.GetLogger().Errorf(
			"PollForActivityTask failed. TaskList: %v, Error: %v", pollRequest.GetTaskList().GetName(), err)
	}
	return resp, err
}

// PollForDecisionTask - Poll for a decision task.
func (wh *WorkflowHandler) PollForDecisionTask(
	ctx thrift.Context,
	pollRequest *gen.PollForDecisionTaskRequest) (*gen.PollForDecisionTaskResponse, error) {
	wh.Service.GetLogger().Debug("Received PollForDecisionTask")
	err, resp := wh.matching.PollForDecisionTask(pollRequest)
	if err != nil {
		wh.Service.GetLogger().Errorf(
			"PollForDecisionTask failed. TaskList: %v, Error: %v", pollRequest.GetTaskList().GetName(), err)
	}
	return err, resp
}

// RecordActivityTaskHeartbeat - Record Activity Task Heart beat.
func (wh *WorkflowHandler) RecordActivityTaskHeartbeat(
	ctx thrift.Context,
	heartbeatRequest *gen.RecordActivityTaskHeartbeatRequest) (*gen.RecordActivityTaskHeartbeatResponse, error) {
	wh.Service.GetLogger().Debug("Received RecordActivityTaskHeartbeat")
	return wh.history.RecordActivityTaskHeartbeat(heartbeatRequest)
}

// RespondActivityTaskCompleted - Record Activity Task Heart beat
func (wh *WorkflowHandler) RespondActivityTaskCompleted(
	ctx thrift.Context,
	completeRequest *gen.RespondActivityTaskCompletedRequest) error {
	err := wh.history.RespondActivityTaskCompleted(completeRequest)
	if err != nil {
		logger := wh.getLoggerForTask(completeRequest.GetTaskToken())
		logger.Errorf("RespondActivityTaskCompleted. Error: %v", err)
	}
	return err
}

// RespondActivityTaskFailed - Record Activity Task Heart beat
func (wh *WorkflowHandler) RespondActivityTaskFailed(
	ctx thrift.Context,
	failRequest *gen.RespondActivityTaskFailedRequest) error {
	err := wh.history.RespondActivityTaskFailed(failRequest)
	if err != nil {
		logger := wh.getLoggerForTask(failRequest.GetTaskToken())
		logger.Errorf("RespondActivityTaskFailed. Error: %v", err)
	}
	return err

}

// RespondDecisionTaskCompleted - Record Activity Task Heart beat
func (wh *WorkflowHandler) RespondDecisionTaskCompleted(
	ctx thrift.Context,
	completeRequest *gen.RespondDecisionTaskCompletedRequest) error {
	err := wh.history.RespondDecisionTaskCompleted(completeRequest)
	if err != nil {
		logger := wh.getLoggerForTask(completeRequest.GetTaskToken())
		logger.Errorf("RespondActivityTaskFailed. Error: %v", err)
	}
	return err
}

// StartWorkflowExecution - Record Activity Task Heart beat
func (wh *WorkflowHandler) StartWorkflowExecution(
	ctx thrift.Context,
	startRequest *gen.StartWorkflowExecutionRequest) (*gen.StartWorkflowExecutionResponse, error) {
	wh.Service.GetLogger().Debugf("Received StartWorkflowExecution. WorkflowID: %v", startRequest.GetWorkflowId())
	resp, err := wh.history.StartWorkflowExecution(startRequest)
	if err != nil {
		wh.Service.GetLogger().Errorf("StartWorkflowExecution failed. WorkflowID: %v. Error: %v", startRequest.GetWorkflowId(), err)
	}
	return resp, err
}

// GetWorkflowExecutionHistory - retrieves the hisotry of workflow execution
func (wh *WorkflowHandler) GetWorkflowExecutionHistory(
	ctx thrift.Context,
	getRequest *gen.GetWorkflowExecutionHistoryRequest) (*gen.GetWorkflowExecutionHistoryResponse, error) {
	return wh.history.GetWorkflowExecutionHistory(getRequest)
}

func (wh *WorkflowHandler) getLoggerForTask(taskToken []byte) bark.Logger {
	logger := wh.Service.GetLogger()
	task, err := wh.tokenSerializer.Deserialize(taskToken)
	if err == nil {
		logger = logger.WithFields(bark.Fields{
			"WorkflowID": task.WorkflowID,
			"RunID":      task.RunID,
			"ScheduleID": task.ScheduleID,
		})
	}
	return logger
}
