package frontend

import (
	"log"

	"github.com/uber-common/bark"
	"github.com/uber/cadence/.gen/go/cadence"
	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/service"
	"github.com/uber/tchannel-go/thrift"
)

var _ cadence.TChanWorkflowService = (*WorkflowHandler)(nil)

// WorkflowHandler - Thrift handler inteface for workflow service
type WorkflowHandler struct {
	history         history.Client
	matching        matching.Client
	tokenSerializer common.TaskTokenSerializer
	service.Service
}

// NewWorkflowHandler creates a thrift handler for the cadence service
func NewWorkflowHandler(sVice service.Service) (*WorkflowHandler, []thrift.TChanServer) {
	handler := &WorkflowHandler{
		Service:         sVice,
		tokenSerializer: common.NewJSONTaskTokenSerializer(),
	}
	return handler, []thrift.TChanServer{cadence.NewTChanWorkflowServiceServer(handler)}
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
	resp, err := wh.matching.PollForActivityTask(ctx, pollRequest)
	if err != nil {
		wh.Service.GetLogger().Errorf(
			"PollForActivityTask failed. TaskList: %v, Error: %v", pollRequest.GetTaskList().GetName(), err)
	}
	return resp, wrapError(err)
}

// PollForDecisionTask - Poll for a decision task.
func (wh *WorkflowHandler) PollForDecisionTask(
	ctx thrift.Context,
	pollRequest *gen.PollForDecisionTaskRequest) (*gen.PollForDecisionTaskResponse, error) {
	wh.Service.GetLogger().Debug("Received PollForDecisionTask")
	resp, err := wh.matching.PollForDecisionTask(ctx, pollRequest)
	if err != nil {
		wh.Service.GetLogger().Errorf(
			"PollForDecisionTask failed. TaskList: %v, Error: %v", pollRequest.GetTaskList().GetName(), err)
	}
	return resp, wrapError(err)
}

// RecordActivityTaskHeartbeat - Record Activity Task Heart beat.
func (wh *WorkflowHandler) RecordActivityTaskHeartbeat(
	ctx thrift.Context,
	heartbeatRequest *gen.RecordActivityTaskHeartbeatRequest) (*gen.RecordActivityTaskHeartbeatResponse, error) {
	wh.Service.GetLogger().Debug("Received RecordActivityTaskHeartbeat")
	resp, err := wh.history.RecordActivityTaskHeartbeat(ctx, heartbeatRequest)
	return resp, wrapError(err)
}

// RespondActivityTaskCompleted - Record Activity Task Heart beat
func (wh *WorkflowHandler) RespondActivityTaskCompleted(
	ctx thrift.Context,
	completeRequest *gen.RespondActivityTaskCompletedRequest) error {
	err := wh.history.RespondActivityTaskCompleted(ctx, completeRequest)
	if err != nil {
		logger := wh.getLoggerForTask(completeRequest.GetTaskToken())
		logger.Errorf("RespondActivityTaskCompleted. Error: %v", err)
	}
	return wrapError(err)
}

// RespondActivityTaskFailed - Record Activity Task Heart beat
func (wh *WorkflowHandler) RespondActivityTaskFailed(
	ctx thrift.Context,
	failRequest *gen.RespondActivityTaskFailedRequest) error {
	err := wh.history.RespondActivityTaskFailed(ctx, failRequest)
	if err != nil {
		logger := wh.getLoggerForTask(failRequest.GetTaskToken())
		logger.Errorf("RespondActivityTaskFailed. Error: %v", err)
	}
	return wrapError(err)

}

// RespondActivityTaskCanceled - Record Activity Task Heart beat
func (wh *WorkflowHandler) RespondActivityTaskCanceled(
	ctx thrift.Context,
	cancelRequest *gen.RespondActivityTaskCanceledRequest) error {
	err := wh.history.RespondActivityTaskCanceled(ctx, cancelRequest)
	if err != nil {
		logger := wh.getLoggerForTask(cancelRequest.GetTaskToken())
		logger.Errorf("RespondActivityTaskCanceled. Error: %v", err)
	}
	return wrapError(err)

}

// RespondDecisionTaskCompleted - Record Activity Task Heart beat
func (wh *WorkflowHandler) RespondDecisionTaskCompleted(
	ctx thrift.Context,
	completeRequest *gen.RespondDecisionTaskCompletedRequest) error {
	err := wh.history.RespondDecisionTaskCompleted(ctx, completeRequest)
	if err != nil {
		logger := wh.getLoggerForTask(completeRequest.GetTaskToken())
		logger.Errorf("RespondDecisionTaskCompleted. Error: %v", err)
	}
	return wrapError(err)
}

// StartWorkflowExecution - Record Activity Task Heart beat
func (wh *WorkflowHandler) StartWorkflowExecution(
	ctx thrift.Context,
	startRequest *gen.StartWorkflowExecutionRequest) (*gen.StartWorkflowExecutionResponse, error) {
	wh.Service.GetLogger().Debugf("Received StartWorkflowExecution. WorkflowID: %v", startRequest.GetWorkflowId())
	resp, err := wh.history.StartWorkflowExecution(ctx, startRequest)
	if err != nil {
		wh.Service.GetLogger().Errorf("StartWorkflowExecution failed. WorkflowID: %v. Error: %v", startRequest.GetWorkflowId(), err)
	}
	return resp, wrapError(err)
}

// GetWorkflowExecutionHistory - retrieves the hisotry of workflow execution
func (wh *WorkflowHandler) GetWorkflowExecutionHistory(
	ctx thrift.Context,
	getRequest *gen.GetWorkflowExecutionHistoryRequest) (*gen.GetWorkflowExecutionHistoryResponse, error) {
	return wh.history.GetWorkflowExecutionHistory(ctx, getRequest)
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

func wrapError(err error) error {
	if err != nil && shouldWrapInInternalServiceError(err) {
		return &gen.InternalServiceError{Message: err.Error()}
	}
	return err
}

func shouldWrapInInternalServiceError(err error) bool {
	switch err.(type) {
	case *gen.InternalServiceError:
		return false
	case *gen.BadRequestError:
		return false
	case *gen.EntityNotExistsError:
		return false
	case *gen.WorkflowExecutionAlreadyStartedError:
		return false
	}

	return true
}
