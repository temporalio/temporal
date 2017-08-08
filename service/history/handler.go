// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package history

import (
	"fmt"
	"sync"

	"github.com/uber/cadence/.gen/go/health"
	hist "github.com/uber/cadence/.gen/go/history"
	gen "github.com/uber/cadence/.gen/go/shared"
	hc "github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/tchannel-go/thrift"
)

// Handler - Thrift handler inteface for history service
type Handler struct {
	numberOfShards        int
	shardManager          persistence.ShardManager
	metadataMgr           persistence.MetadataManager
	visibilityMgr         persistence.VisibilityManager
	historyMgr            persistence.HistoryManager
	executionMgrFactory   persistence.ExecutionManagerFactory
	historyServiceClient  hc.Client
	matchingServiceClient matching.Client
	hServiceResolver      membership.ServiceResolver
	controller            *shardController
	tokenSerializer       common.TaskTokenSerializer
	startWG               sync.WaitGroup
	metricsClient         metrics.Client
	config                *Config
	service.Service
}

var _ hist.TChanHistoryService = (*Handler)(nil)
var _ EngineFactory = (*Handler)(nil)

var (
	errDomainNotSet            = &gen.BadRequestError{Message: "Domain not set on request."}
	errWorkflowExecutionNotSet = &gen.BadRequestError{Message: "WorkflowExecution not set on request."}
)

// NewHandler creates a thrift handler for the history service
func NewHandler(sVice service.Service, config *Config, shardManager persistence.ShardManager,
	metadataMgr persistence.MetadataManager, visibilityMgr persistence.VisibilityManager,
	historyMgr persistence.HistoryManager, executionMgrFactory persistence.ExecutionManagerFactory) (*Handler, []thrift.TChanServer) {
	handler := &Handler{
		Service:             sVice,
		config:              config,
		shardManager:        shardManager,
		metadataMgr:         metadataMgr,
		historyMgr:          historyMgr,
		visibilityMgr:       visibilityMgr,
		executionMgrFactory: executionMgrFactory,
		tokenSerializer:     common.NewJSONTaskTokenSerializer(),
	}
	// prevent us from trying to serve requests before shard controller is started and ready
	handler.startWG.Add(1)
	return handler, []thrift.TChanServer{hist.NewTChanHistoryServiceServer(handler), health.NewTChanMetaServer(handler)}
}

// Start starts the handler
func (h *Handler) Start(thriftService []thrift.TChanServer) error {
	h.Service.Start(thriftService)
	matchingServiceClient, err0 := h.Service.GetClientFactory().NewMatchingClient()
	if err0 != nil {
		return err0
	}
	h.matchingServiceClient = matchingServiceClient

	historyServiceClient, err0 := h.Service.GetClientFactory().NewHistoryClient()
	if err0 != nil {
		return err0
	}
	h.historyServiceClient = historyServiceClient

	hServiceResolver, err1 := h.GetMembershipMonitor().GetResolver(common.HistoryServiceName)
	if err1 != nil {
		h.Service.GetLogger().Fatalf("Unable to get history service resolver.")
	}
	h.hServiceResolver = hServiceResolver
	h.controller = newShardController(h.GetHostInfo(), hServiceResolver, h.shardManager, h.historyMgr,
		h.executionMgrFactory, h, h.config, h.GetLogger(), h.GetMetricsClient())
	h.controller.Start()
	h.metricsClient = h.GetMetricsClient()
	h.startWG.Done()
	return nil
}

// Stop stops the handler
func (h *Handler) Stop() {
	h.controller.Stop()
	h.shardManager.Close()
	h.historyMgr.Close()
	h.metadataMgr.Close()
	h.visibilityMgr.Close()
	h.Service.Stop()
}

// CreateEngine is implementation for HistoryEngineFactory used for creating the engine instance for shard
func (h *Handler) CreateEngine(context ShardContext) Engine {
	return NewEngineWithShardContext(context, h.metadataMgr, h.visibilityMgr, h.matchingServiceClient, h.historyServiceClient)
}

// Health is for health check
func (h *Handler) Health(ctx thrift.Context) (*health.HealthStatus, error) {
	h.startWG.Wait()
	h.GetLogger().Debug("History health check endpoint reached.")
	hs := &health.HealthStatus{Ok: true, Msg: common.StringPtr("history good")}
	return hs, nil
}

// RecordActivityTaskHeartbeat - Record Activity Task Heart beat.
func (h *Handler) RecordActivityTaskHeartbeat(ctx thrift.Context,
	wrappedRequest *hist.RecordActivityTaskHeartbeatRequest) (*gen.RecordActivityTaskHeartbeatResponse, error) {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryRecordActivityTaskHeartbeatScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRecordActivityTaskHeartbeatScope, metrics.CadenceLatency)
	defer sw.Stop()

	if !wrappedRequest.IsSetDomainUUID() {
		return nil, errDomainNotSet
	}

	heartbeatRequest := wrappedRequest.GetHeartbeatRequest()
	token, err0 := h.tokenSerializer.Deserialize(heartbeatRequest.GetTaskToken())
	if err0 != nil {
		err0 = &gen.BadRequestError{Message: fmt.Sprintf("Error deserializing task token. Error: %v", err0)}
		h.updateErrorMetric(metrics.HistoryRecordActivityTaskHeartbeatScope, err0)
		return nil, err0
	}

	engine, err1 := h.controller.GetEngine(token.WorkflowID)
	if err1 != nil {
		h.updateErrorMetric(metrics.HistoryRecordActivityTaskHeartbeatScope, err1)
		return nil, err1
	}

	response, err2 := engine.RecordActivityTaskHeartbeat(wrappedRequest)
	if err2 != nil {
		h.updateErrorMetric(metrics.HistoryRecordActivityTaskHeartbeatScope, h.convertError(err2))
		return nil, h.convertError(err2)
	}

	return response, nil
}

// RecordActivityTaskStarted - Record Activity Task started.
func (h *Handler) RecordActivityTaskStarted(ctx thrift.Context,
	recordRequest *hist.RecordActivityTaskStartedRequest) (*hist.RecordActivityTaskStartedResponse, error) {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryRecordActivityTaskStartedScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRecordActivityTaskStartedScope, metrics.CadenceLatency)
	defer sw.Stop()

	if !recordRequest.IsSetDomainUUID() {
		return nil, errDomainNotSet
	}

	workflowExecution := recordRequest.GetWorkflowExecution()
	engine, err1 := h.controller.GetEngine(workflowExecution.GetWorkflowId())
	if err1 != nil {
		h.updateErrorMetric(metrics.HistoryRecordActivityTaskStartedScope, err1)
		return nil, err1
	}

	response, err2 := engine.RecordActivityTaskStarted(recordRequest)
	if err2 != nil {
		h.updateErrorMetric(metrics.HistoryRecordActivityTaskStartedScope, h.convertError(err2))
		return nil, h.convertError(err2)
	}

	return response, nil
}

// RecordDecisionTaskStarted - Record Decision Task started.
func (h *Handler) RecordDecisionTaskStarted(ctx thrift.Context,
	recordRequest *hist.RecordDecisionTaskStartedRequest) (*hist.RecordDecisionTaskStartedResponse, error) {
	h.startWG.Wait()
	h.Service.GetLogger().Debugf("RecordDecisionTaskStarted. DomainID: %v, WorkflowID: %v, RunID: %v, ScheduleID: %v",
		recordRequest.GetDomainUUID(), recordRequest.GetWorkflowExecution().GetWorkflowId(),
		recordRequest.GetWorkflowExecution().GetRunId(), recordRequest.GetScheduleId())

	h.metricsClient.IncCounter(metrics.HistoryRecordDecisionTaskStartedScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRecordDecisionTaskStartedScope, metrics.CadenceLatency)
	defer sw.Stop()

	if !recordRequest.IsSetDomainUUID() {
		return nil, errDomainNotSet
	}

	workflowExecution := recordRequest.GetWorkflowExecution()
	engine, err1 := h.controller.GetEngine(workflowExecution.GetWorkflowId())
	if err1 != nil {
		h.Service.GetLogger().Errorf("RecordDecisionTaskStarted failed. Error: %v. WorkflowID: %v, RunID: %v, ScheduleID: %v",
			err1,
			recordRequest.GetWorkflowExecution().GetWorkflowId(),
			recordRequest.GetWorkflowExecution().GetRunId(),
			recordRequest.GetScheduleId())
		h.updateErrorMetric(metrics.HistoryRecordDecisionTaskStartedScope, err1)
		return nil, err1
	}

	response, err2 := engine.RecordDecisionTaskStarted(recordRequest)
	if err2 != nil {
		h.updateErrorMetric(metrics.HistoryRecordDecisionTaskStartedScope, h.convertError(err2))
		return nil, h.convertError(err2)
	}

	return response, nil
}

// RespondActivityTaskCompleted - records completion of an activity task
func (h *Handler) RespondActivityTaskCompleted(ctx thrift.Context,
	wrappedRequest *hist.RespondActivityTaskCompletedRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryRespondActivityTaskCompletedScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRespondActivityTaskCompletedScope, metrics.CadenceLatency)
	defer sw.Stop()

	if !wrappedRequest.IsSetDomainUUID() {
		return errDomainNotSet
	}

	completeRequest := wrappedRequest.GetCompleteRequest()
	token, err0 := h.tokenSerializer.Deserialize(completeRequest.GetTaskToken())
	if err0 != nil {
		err0 = &gen.BadRequestError{Message: fmt.Sprintf("Error deserializing task token. Error: %v", err0)}
		h.updateErrorMetric(metrics.HistoryRespondActivityTaskCompletedScope, err0)
		return err0
	}

	engine, err1 := h.controller.GetEngine(token.WorkflowID)
	if err1 != nil {
		h.updateErrorMetric(metrics.HistoryRespondActivityTaskCompletedScope, err1)
		return err1
	}

	err2 := engine.RespondActivityTaskCompleted(wrappedRequest)
	if err2 != nil {
		h.updateErrorMetric(metrics.HistoryRespondActivityTaskCompletedScope, h.convertError(err2))
		return h.convertError(err2)
	}

	return nil
}

// RespondActivityTaskFailed - records failure of an activity task
func (h *Handler) RespondActivityTaskFailed(ctx thrift.Context,
	wrappedRequest *hist.RespondActivityTaskFailedRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryRespondActivityTaskFailedScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRespondActivityTaskFailedScope, metrics.CadenceLatency)
	defer sw.Stop()

	if !wrappedRequest.IsSetDomainUUID() {
		return errDomainNotSet
	}

	failRequest := wrappedRequest.GetFailedRequest()
	token, err0 := h.tokenSerializer.Deserialize(failRequest.GetTaskToken())
	if err0 != nil {
		err0 = &gen.BadRequestError{Message: fmt.Sprintf("Error deserializing task token. Error: %v", err0)}
		h.updateErrorMetric(metrics.HistoryRespondActivityTaskFailedScope, err0)
		return err0
	}

	engine, err1 := h.controller.GetEngine(token.WorkflowID)
	if err1 != nil {
		h.updateErrorMetric(metrics.HistoryRespondActivityTaskFailedScope, err1)
		return err1
	}

	err2 := engine.RespondActivityTaskFailed(wrappedRequest)
	if err2 != nil {
		h.updateErrorMetric(metrics.HistoryRespondActivityTaskFailedScope, h.convertError(err2))
		return h.convertError(err2)
	}

	return nil
}

// RespondActivityTaskCanceled - records failure of an activity task
func (h *Handler) RespondActivityTaskCanceled(ctx thrift.Context,
	wrappedRequest *hist.RespondActivityTaskCanceledRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryRespondActivityTaskCanceledScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRespondActivityTaskCanceledScope, metrics.CadenceLatency)
	defer sw.Stop()

	if !wrappedRequest.IsSetDomainUUID() {
		return errDomainNotSet
	}

	cancelRequest := wrappedRequest.GetCancelRequest()
	token, err0 := h.tokenSerializer.Deserialize(cancelRequest.GetTaskToken())
	if err0 != nil {
		err0 = &gen.BadRequestError{Message: fmt.Sprintf("Error deserializing task token. Error: %v", err0)}
		h.updateErrorMetric(metrics.HistoryRespondActivityTaskCanceledScope, err0)
		return err0
	}

	engine, err1 := h.controller.GetEngine(token.WorkflowID)
	if err1 != nil {
		h.updateErrorMetric(metrics.HistoryRespondActivityTaskCanceledScope, err1)
		return err1
	}

	err2 := engine.RespondActivityTaskCanceled(wrappedRequest)
	if err2 != nil {
		h.updateErrorMetric(metrics.HistoryRespondActivityTaskCanceledScope, h.convertError(err2))
		return h.convertError(err2)
	}

	return nil
}

// RespondDecisionTaskCompleted - records completion of a decision task
func (h *Handler) RespondDecisionTaskCompleted(ctx thrift.Context,
	wrappedRequest *hist.RespondDecisionTaskCompletedRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.CadenceLatency)
	defer sw.Stop()

	if !wrappedRequest.IsSetDomainUUID() {
		return errDomainNotSet
	}

	completeRequest := wrappedRequest.GetCompleteRequest()
	token, err0 := h.tokenSerializer.Deserialize(completeRequest.GetTaskToken())
	if err0 != nil {
		err0 = &gen.BadRequestError{Message: fmt.Sprintf("Error deserializing task token. Error: %v", err0)}
		h.updateErrorMetric(metrics.HistoryRespondDecisionTaskCompletedScope, err0)
		return err0
	}

	h.Service.GetLogger().Debugf("RespondDecisionTaskCompleted. DomainID: %v, WorkflowID: %v, RunID: %v, ScheduleID: %v",
		token.DomainID,
		token.WorkflowID,
		token.RunID,
		token.ScheduleID)

	engine, err1 := h.controller.GetEngine(token.WorkflowID)
	if err1 != nil {
		h.updateErrorMetric(metrics.HistoryRespondDecisionTaskCompletedScope, err1)
		return err1
	}

	err2 := engine.RespondDecisionTaskCompleted(wrappedRequest)
	if err2 != nil {
		h.updateErrorMetric(metrics.HistoryRespondDecisionTaskCompletedScope, h.convertError(err2))
		return h.convertError(err2)
	}

	return nil
}

// StartWorkflowExecution - creates a new workflow execution
func (h *Handler) StartWorkflowExecution(ctx thrift.Context,
	wrappedRequest *hist.StartWorkflowExecutionRequest) (*gen.StartWorkflowExecutionResponse, error) {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryStartWorkflowExecutionScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryStartWorkflowExecutionScope, metrics.CadenceLatency)
	defer sw.Stop()

	if !wrappedRequest.IsSetDomainUUID() {
		return nil, errDomainNotSet
	}

	startRequest := wrappedRequest.GetStartRequest()
	engine, err1 := h.controller.GetEngine(startRequest.GetWorkflowId())
	if err1 != nil {
		h.updateErrorMetric(metrics.HistoryStartWorkflowExecutionScope, err1)
		return nil, err1
	}

	response, err2 := engine.StartWorkflowExecution(wrappedRequest)
	if err2 != nil {
		h.updateErrorMetric(metrics.HistoryStartWorkflowExecutionScope, h.convertError(err2))
		return nil, h.convertError(err2)
	}

	return response, nil
}

// GetWorkflowExecutionNextEventID - returns the id of the next event in the execution's history
func (h *Handler) GetWorkflowExecutionNextEventID(ctx thrift.Context,
	getRequest *hist.GetWorkflowExecutionNextEventIDRequest) (*hist.GetWorkflowExecutionNextEventIDResponse, error) {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryGetWorkflowExecutionNextEventIDScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryGetWorkflowExecutionNextEventIDScope, metrics.CadenceLatency)
	defer sw.Stop()

	if !getRequest.IsSetDomainUUID() {
		return nil, errDomainNotSet
	}

	workflowExecution := getRequest.GetExecution()
	engine, err1 := h.controller.GetEngine(workflowExecution.GetWorkflowId())
	if err1 != nil {
		h.updateErrorMetric(metrics.HistoryGetWorkflowExecutionNextEventIDScope, err1)
		return nil, err1
	}

	resp, err2 := engine.GetWorkflowExecutionNextEventID(getRequest)
	if err2 != nil {
		h.updateErrorMetric(metrics.HistoryGetWorkflowExecutionNextEventIDScope, h.convertError(err2))
		return nil, h.convertError(err2)
	}
	return resp, nil
}

// RequestCancelWorkflowExecution - requests cancellation of a workflow
func (h *Handler) RequestCancelWorkflowExecution(ctx thrift.Context,
	request *hist.RequestCancelWorkflowExecutionRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryRequestCancelWorkflowExecutionScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRequestCancelWorkflowExecutionScope, metrics.CadenceLatency)
	defer sw.Stop()

	cancelRequest := request.GetCancelRequest()
	h.Service.GetLogger().Debugf("RequestCancelWorkflowExecution. DomainID: %v/%v, WorkflowID: %v, RunID: %v.",
		cancelRequest.GetDomain(),
		request.GetDomainUUID(),
		cancelRequest.GetWorkflowExecution().GetWorkflowId(),
		cancelRequest.GetWorkflowExecution().GetRunId())

	engine, err1 := h.controller.GetEngine(cancelRequest.GetWorkflowExecution().GetWorkflowId())
	if err1 != nil {
		h.updateErrorMetric(metrics.HistoryRequestCancelWorkflowExecutionScope, err1)
		return err1
	}

	err2 := engine.RequestCancelWorkflowExecution(request)
	if err2 != nil {
		h.updateErrorMetric(metrics.HistoryRequestCancelWorkflowExecutionScope, h.convertError(err2))
		return h.convertError(err2)
	}

	return nil
}

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a decision task being created for the execution.
func (h *Handler) SignalWorkflowExecution(ctx thrift.Context,
	wrappedRequest *hist.SignalWorkflowExecutionRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistorySignalWorkflowExecutionScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistorySignalWorkflowExecutionScope, metrics.CadenceLatency)
	defer sw.Stop()

	if !wrappedRequest.IsSetDomainUUID() {
		return errDomainNotSet
	}

	signalRequest := wrappedRequest.GetSignalRequest()
	workflowExecution := signalRequest.GetWorkflowExecution()
	engine, err1 := h.controller.GetEngine(workflowExecution.GetWorkflowId())
	if err1 != nil {
		h.updateErrorMetric(metrics.HistorySignalWorkflowExecutionScope, err1)
		return err1
	}

	err2 := engine.SignalWorkflowExecution(wrappedRequest)
	if err2 != nil {
		h.updateErrorMetric(metrics.HistorySignalWorkflowExecutionScope, h.convertError(err2))
		return h.convertError(err2)
	}

	return nil
}

// TerminateWorkflowExecution terminates an existing workflow execution by recording WorkflowExecutionTerminated event
// in the history and immediately terminating the execution instance.
func (h *Handler) TerminateWorkflowExecution(ctx thrift.Context,
	wrappedRequest *hist.TerminateWorkflowExecutionRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryTerminateWorkflowExecutionScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryTerminateWorkflowExecutionScope, metrics.CadenceLatency)
	defer sw.Stop()

	if !wrappedRequest.IsSetDomainUUID() {
		return errDomainNotSet
	}

	terminateRequest := wrappedRequest.GetTerminateRequest()
	workflowExecution := terminateRequest.GetWorkflowExecution()
	engine, err1 := h.controller.GetEngine(workflowExecution.GetWorkflowId())
	if err1 != nil {
		h.updateErrorMetric(metrics.HistoryTerminateWorkflowExecutionScope, err1)
		return err1
	}

	err2 := engine.TerminateWorkflowExecution(wrappedRequest)
	if err2 != nil {
		h.updateErrorMetric(metrics.HistoryTerminateWorkflowExecutionScope, h.convertError(err2))
		return h.convertError(err2)
	}

	return nil
}

// ScheduleDecisionTask is used for creating a decision task for already started workflow execution.  This is mainly
// used by transfer queue processor during the processing of StartChildWorkflowExecution task, where it first starts
// child execution without creating the decision task and then calls this API after updating the mutable state of
// parent execution.
func (h *Handler) ScheduleDecisionTask(ctx thrift.Context, request *hist.ScheduleDecisionTaskRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryScheduleDecisionTaskScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryScheduleDecisionTaskScope, metrics.CadenceLatency)
	defer sw.Stop()

	if !request.IsSetDomainUUID() {
		return errDomainNotSet
	}

	if !request.IsSetWorkflowExecution() {
		return errWorkflowExecutionNotSet
	}

	workflowExecution := request.GetWorkflowExecution()
	engine, err1 := h.controller.GetEngine(workflowExecution.GetWorkflowId())
	if err1 != nil {
		h.updateErrorMetric(metrics.HistoryScheduleDecisionTaskScope, err1)
		return err1
	}

	err2 := engine.ScheduleDecisionTask(request)
	if err2 != nil {
		h.updateErrorMetric(metrics.HistoryScheduleDecisionTaskScope, h.convertError(err2))
		return h.convertError(err2)
	}

	return nil
}

// RecordChildExecutionCompleted is used for reporting the completion of child workflow execution to parent.
// This is mainly called by transfer queue processor during the processing of DeleteExecution task.
func (h *Handler) RecordChildExecutionCompleted(ctx thrift.Context, request *hist.RecordChildExecutionCompletedRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryRecordChildExecutionCompletedScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRecordChildExecutionCompletedScope, metrics.CadenceLatency)
	defer sw.Stop()

	if !request.IsSetDomainUUID() {
		return errDomainNotSet
	}

	if !request.IsSetWorkflowExecution() {
		return errWorkflowExecutionNotSet
	}

	workflowExecution := request.GetWorkflowExecution()
	engine, err1 := h.controller.GetEngine(workflowExecution.GetWorkflowId())
	if err1 != nil {
		h.updateErrorMetric(metrics.HistoryRecordChildExecutionCompletedScope, err1)
		return err1
	}

	err2 := engine.RecordChildExecutionCompleted(request)
	if err2 != nil {
		h.updateErrorMetric(metrics.HistoryRecordChildExecutionCompletedScope, h.convertError(err2))
		return h.convertError(err2)
	}

	return nil
}

// convertError is a helper method to convert ShardOwnershipLostError from persistence layer returned by various
// HistoryEngine API calls to ShardOwnershipLost error return by HistoryService for client to be redirected to the
// correct shard.
func (h *Handler) convertError(err error) error {
	switch err.(type) {
	case *persistence.ShardOwnershipLostError:
		shardID := err.(*persistence.ShardOwnershipLostError).ShardID
		info, err := h.hServiceResolver.Lookup(string(shardID))
		if err != nil {
			return createShardOwnershipLostError(h.GetHostInfo().GetAddress(), info.GetAddress())
		}
		return createShardOwnershipLostError(h.GetHostInfo().GetAddress(), "")
	}

	return err
}

func (h *Handler) updateErrorMetric(scope int, err error) {
	switch err.(type) {
	case *hist.ShardOwnershipLostError:
		h.metricsClient.IncCounter(scope, metrics.CadenceErrShardOwnershipLostCounter)
	case *hist.EventAlreadyStartedError:
		h.metricsClient.IncCounter(scope, metrics.CadenceErrEventAlreadyStartedCounter)
	case *gen.BadRequestError:
		h.metricsClient.IncCounter(scope, metrics.CadenceErrBadRequestCounter)
	case *gen.EntityNotExistsError:
		h.metricsClient.IncCounter(scope, metrics.CadenceErrEntityNotExistsCounter)
	case *gen.CancellationAlreadyRequestedError:
		h.metricsClient.IncCounter(scope, metrics.CadenceErrCancellationAlreadyRequestedCounter)
	default:
		h.metricsClient.IncCounter(scope, metrics.CadenceFailures)
	}
}

func createShardOwnershipLostError(currentHost, ownerHost string) *hist.ShardOwnershipLostError {
	shardLostErr := hist.NewShardOwnershipLostError()
	shardLostErr.Message = common.StringPtr(fmt.Sprintf("Shard is not owned by host: %v", currentHost))
	shardLostErr.Owner = common.StringPtr(ownerHost)

	return shardLostErr
}
