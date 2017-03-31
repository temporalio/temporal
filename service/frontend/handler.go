package frontend

import (
	"log"
	"sync"

	"github.com/uber-common/bark"
	"github.com/uber/cadence/.gen/go/cadence"
	h "github.com/uber/cadence/.gen/go/history"
	m "github.com/uber/cadence/.gen/go/matching"
	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/service"
	"github.com/uber/tchannel-go/thrift"
	"errors"
)

var _ cadence.TChanWorkflowService = (*WorkflowHandler)(nil)

// WorkflowHandler - Thrift handler inteface for workflow service
type WorkflowHandler struct {
	history         history.Client
	matching        matching.Client
	tokenSerializer common.TaskTokenSerializer
	startWG         sync.WaitGroup
	service.Service
}

// NewWorkflowHandler creates a thrift handler for the cadence service
func NewWorkflowHandler(sVice service.Service) (*WorkflowHandler, []thrift.TChanServer) {
	handler := &WorkflowHandler{
		Service:         sVice,
		tokenSerializer: common.NewJSONTaskTokenSerializer(),
	}
	// prevent us from trying to serve requests before handler's Start() is complete
	handler.startWG.Add(1)
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
	wh.startWG.Done()
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

func (wh *WorkflowHandler) RegisterDomain(ctx thrift.Context, registerRequest *gen.RegisterDomainRequest) error {
	return errors.New("Not Implemented.")
}

func (wh *WorkflowHandler) DescribeDomain(ctx thrift.Context,
	describeRequest *gen.DescribeDomainRequest) (*gen.DescribeDomainResponse, error) {
	return nil, errors.New("Not Implemented.")
}

func (wh *WorkflowHandler) UpdateDomain(ctx thrift.Context,
	updateRequest *gen.UpdateDomainRequest) (*gen.UpdateDomainResponse, error) {
	return nil, errors.New("Not Implemented.")
}

func (wh *WorkflowHandler) DeprecateDomain(ctx thrift.Context, deprecateRequest *gen.DeprecateDomainRequest) error {
	return errors.New("Not Implemented.")
}

// PollForActivityTask - Poll for an activity task.
func (wh *WorkflowHandler) PollForActivityTask(
	ctx thrift.Context,
	pollRequest *gen.PollForActivityTaskRequest) (*gen.PollForActivityTaskResponse, error) {
	wh.startWG.Wait()

	wh.Service.GetLogger().Debug("Received PollForActivityTask")
	resp, err := wh.matching.PollForActivityTask(ctx, &m.PollForActivityTaskRequest{
		PollRequest: pollRequest,
	})
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
	wh.startWG.Wait()

	wh.Service.GetLogger().Debug("Received PollForDecisionTask")
	resp, err := wh.matching.PollForDecisionTask(ctx, &m.PollForDecisionTaskRequest{
		PollRequest: pollRequest,
	})
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
	wh.startWG.Wait()

	wh.Service.GetLogger().Debug("Received RecordActivityTaskHeartbeat")
	resp, err := wh.history.RecordActivityTaskHeartbeat(ctx, &h.RecordActivityTaskHeartbeatRequest{
		HeartbeatRequest: heartbeatRequest,
	})
	return resp, wrapError(err)
}

// RespondActivityTaskCompleted - response to an activity task
func (wh *WorkflowHandler) RespondActivityTaskCompleted(
	ctx thrift.Context,
	completeRequest *gen.RespondActivityTaskCompletedRequest) error {
	wh.startWG.Wait()

	err := wh.history.RespondActivityTaskCompleted(ctx, &h.RespondActivityTaskCompletedRequest{
		CompleteRequest: completeRequest,
	})
	if err != nil {
		logger := wh.getLoggerForTask(completeRequest.GetTaskToken())
		logger.Errorf("RespondActivityTaskCompleted. Error: %v", err)
	}
	return wrapError(err)
}

// RespondActivityTaskFailed - response to an activity task failure
func (wh *WorkflowHandler) RespondActivityTaskFailed(
	ctx thrift.Context,
	failedRequest *gen.RespondActivityTaskFailedRequest) error {
	wh.startWG.Wait()

	err := wh.history.RespondActivityTaskFailed(ctx, &h.RespondActivityTaskFailedRequest{
		FailedRequest: failedRequest,
	})
	if err != nil {
		logger := wh.getLoggerForTask(failedRequest.GetTaskToken())
		logger.Errorf("RespondActivityTaskFailed. Error: %v", err)
	}
	return wrapError(err)

}

// RespondActivityTaskCanceled - called to cancel an activity task
func (wh *WorkflowHandler) RespondActivityTaskCanceled(
	ctx thrift.Context,
	cancelRequest *gen.RespondActivityTaskCanceledRequest) error {
	wh.startWG.Wait()

	err := wh.history.RespondActivityTaskCanceled(ctx, &h.RespondActivityTaskCanceledRequest{
		CancelRequest: cancelRequest,
	})
	if err != nil {
		logger := wh.getLoggerForTask(cancelRequest.GetTaskToken())
		logger.Errorf("RespondActivityTaskCanceled. Error: %v", err)
	}
	return wrapError(err)

}

// RespondDecisionTaskCompleted - response to a decision task
func (wh *WorkflowHandler) RespondDecisionTaskCompleted(
	ctx thrift.Context,
	completeRequest *gen.RespondDecisionTaskCompletedRequest) error {
	wh.startWG.Wait()

	err := wh.history.RespondDecisionTaskCompleted(ctx, &h.RespondDecisionTaskCompletedRequest{
		CompleteRequest: completeRequest,
	})
	if err != nil {
		logger := wh.getLoggerForTask(completeRequest.GetTaskToken())
		logger.Errorf("RespondDecisionTaskCompleted. Error: %v", err)
	}
	return wrapError(err)
}

// StartWorkflowExecution - Creates a new workflow execution
func (wh *WorkflowHandler) StartWorkflowExecution(
	ctx thrift.Context,
	startRequest *gen.StartWorkflowExecutionRequest) (*gen.StartWorkflowExecutionResponse, error) {
	wh.startWG.Wait()

	wh.Service.GetLogger().Debugf("Received StartWorkflowExecution. WorkflowID: %v", startRequest.GetWorkflowId())
	resp, err := wh.history.StartWorkflowExecution(ctx, &h.StartWorkflowExecutionRequest{
		StartRequest: startRequest,
	})
	if err != nil {
		wh.Service.GetLogger().Errorf("StartWorkflowExecution failed. WorkflowID: %v. Error: %v", startRequest.GetWorkflowId(), err)
	}
	return resp, wrapError(err)
}

// GetWorkflowExecutionHistory - retrieves the hisotry of workflow execution
func (wh *WorkflowHandler) GetWorkflowExecutionHistory(
	ctx thrift.Context,
	getRequest *gen.GetWorkflowExecutionHistoryRequest) (*gen.GetWorkflowExecutionHistoryResponse, error) {
	wh.startWG.Wait()

	return wh.history.GetWorkflowExecutionHistory(ctx, &h.GetWorkflowExecutionHistoryRequest{
		GetRequest: getRequest,
	})
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
