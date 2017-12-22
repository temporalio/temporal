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
	"context"
	"fmt"
	"sync"

	"github.com/uber/cadence/.gen/go/health"
	"github.com/uber/cadence/.gen/go/health/metaserver"
	hist "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/history/historyserviceserver"
	gen "github.com/uber/cadence/.gen/go/shared"
	hc "github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
)

// Handler - Thrift handler inteface for history service
type (
	Handler struct {
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
		historyEventNotifier  historyEventNotifier
		service.Service
	}
)

var _ historyserviceserver.Interface = (*Handler)(nil)
var _ EngineFactory = (*Handler)(nil)

var (
	errDomainNotSet            = &gen.BadRequestError{Message: "Domain not set on request."}
	errWorkflowExecutionNotSet = &gen.BadRequestError{Message: "WorkflowExecution not set on request."}
	errTaskListNotSet          = &gen.BadRequestError{Message: "Tasklist not set."}
)

// NewHandler creates a thrift handler for the history service
func NewHandler(sVice service.Service, config *Config, shardManager persistence.ShardManager,
	metadataMgr persistence.MetadataManager, visibilityMgr persistence.VisibilityManager,
	historyMgr persistence.HistoryManager, executionMgrFactory persistence.ExecutionManagerFactory) *Handler {
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
	return handler
}

// Start starts the handler
func (h *Handler) Start() error {
	h.Service.GetDispatcher().Register(historyserviceserver.New(h))
	h.Service.GetDispatcher().Register(metaserver.New(h))
	h.Service.Start()
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
		h.metadataMgr, h.executionMgrFactory, h, h.config, h.GetLogger(), h.GetMetricsClient())
	h.metricsClient = h.GetMetricsClient()
	h.historyEventNotifier = newHistoryEventNotifier(h.GetMetricsClient(), h.config.GetShardID)
	// events notifier must starts before controller
	h.historyEventNotifier.Start()
	h.controller.Start()
	h.startWG.Done()
	return nil
}

// Stop stops the handler
func (h *Handler) Stop() {
	h.controller.Stop()
	h.shardManager.Close()
	h.historyMgr.Close()
	h.executionMgrFactory.Close()
	h.metadataMgr.Close()
	h.visibilityMgr.Close()
	h.Service.Stop()
	h.historyEventNotifier.Stop()
}

// CreateEngine is implementation for HistoryEngineFactory used for creating the engine instance for shard
func (h *Handler) CreateEngine(context ShardContext) Engine {
	return NewEngineWithShardContext(context, context.GetDomainCache(), h.visibilityMgr,
		h.matchingServiceClient, h.historyServiceClient, h.historyEventNotifier)
}

// Health is for health check
func (h *Handler) Health(ctx context.Context) (*health.HealthStatus, error) {
	h.startWG.Wait()
	h.GetLogger().Debug("History health check endpoint reached.")
	hs := &health.HealthStatus{Ok: true, Msg: common.StringPtr("history good")}
	return hs, nil
}

// RecordActivityTaskHeartbeat - Record Activity Task Heart beat.
func (h *Handler) RecordActivityTaskHeartbeat(ctx context.Context,
	wrappedRequest *hist.RecordActivityTaskHeartbeatRequest) (*gen.RecordActivityTaskHeartbeatResponse, error) {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryRecordActivityTaskHeartbeatScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRecordActivityTaskHeartbeatScope, metrics.CadenceLatency)
	defer sw.Stop()

	if wrappedRequest.DomainUUID == nil {
		return nil, errDomainNotSet
	}

	heartbeatRequest := wrappedRequest.HeartbeatRequest
	token, err0 := h.tokenSerializer.Deserialize(heartbeatRequest.TaskToken)
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
func (h *Handler) RecordActivityTaskStarted(ctx context.Context,
	recordRequest *hist.RecordActivityTaskStartedRequest) (*hist.RecordActivityTaskStartedResponse, error) {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryRecordActivityTaskStartedScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRecordActivityTaskStartedScope, metrics.CadenceLatency)
	defer sw.Stop()

	if recordRequest.DomainUUID == nil {
		return nil, errDomainNotSet
	}

	workflowExecution := recordRequest.WorkflowExecution
	engine, err1 := h.controller.GetEngine(*workflowExecution.WorkflowId)
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
func (h *Handler) RecordDecisionTaskStarted(ctx context.Context,
	recordRequest *hist.RecordDecisionTaskStartedRequest) (*hist.RecordDecisionTaskStartedResponse, error) {
	h.startWG.Wait()
	h.Service.GetLogger().Debugf("RecordDecisionTaskStarted. DomainID: %v, WorkflowID: %v, RunID: %v, ScheduleID: %v",
		*recordRequest.DomainUUID, *recordRequest.WorkflowExecution.WorkflowId,
		common.StringDefault(recordRequest.WorkflowExecution.RunId), *recordRequest.ScheduleId)

	h.metricsClient.IncCounter(metrics.HistoryRecordDecisionTaskStartedScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRecordDecisionTaskStartedScope, metrics.CadenceLatency)
	defer sw.Stop()

	if recordRequest.DomainUUID == nil {
		return nil, errDomainNotSet
	}

	if recordRequest.PollRequest == nil || recordRequest.PollRequest.TaskList == nil ||
		recordRequest.PollRequest.TaskList.GetName() == "" {
		return nil, errTaskListNotSet
	}

	workflowExecution := recordRequest.WorkflowExecution
	engine, err1 := h.controller.GetEngine(*workflowExecution.WorkflowId)
	if err1 != nil {
		h.Service.GetLogger().Errorf("RecordDecisionTaskStarted failed. Error: %v. WorkflowID: %v, RunID: %v, ScheduleID: %v",
			err1,
			*recordRequest.WorkflowExecution.WorkflowId,
			common.StringDefault(recordRequest.WorkflowExecution.RunId),
			*recordRequest.ScheduleId)
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
func (h *Handler) RespondActivityTaskCompleted(ctx context.Context,
	wrappedRequest *hist.RespondActivityTaskCompletedRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryRespondActivityTaskCompletedScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRespondActivityTaskCompletedScope, metrics.CadenceLatency)
	defer sw.Stop()

	if wrappedRequest.DomainUUID == nil {
		return errDomainNotSet
	}

	completeRequest := wrappedRequest.CompleteRequest
	token, err0 := h.tokenSerializer.Deserialize(completeRequest.TaskToken)
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
func (h *Handler) RespondActivityTaskFailed(ctx context.Context,
	wrappedRequest *hist.RespondActivityTaskFailedRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryRespondActivityTaskFailedScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRespondActivityTaskFailedScope, metrics.CadenceLatency)
	defer sw.Stop()

	if wrappedRequest.DomainUUID == nil {
		return errDomainNotSet
	}

	failRequest := wrappedRequest.FailedRequest
	token, err0 := h.tokenSerializer.Deserialize(failRequest.TaskToken)
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
func (h *Handler) RespondActivityTaskCanceled(ctx context.Context,
	wrappedRequest *hist.RespondActivityTaskCanceledRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryRespondActivityTaskCanceledScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRespondActivityTaskCanceledScope, metrics.CadenceLatency)
	defer sw.Stop()

	if wrappedRequest.DomainUUID == nil {
		return errDomainNotSet
	}

	cancelRequest := wrappedRequest.CancelRequest
	token, err0 := h.tokenSerializer.Deserialize(cancelRequest.TaskToken)
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
func (h *Handler) RespondDecisionTaskCompleted(ctx context.Context,
	wrappedRequest *hist.RespondDecisionTaskCompletedRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.CadenceLatency)
	defer sw.Stop()

	if wrappedRequest.DomainUUID == nil {
		return errDomainNotSet
	}

	completeRequest := wrappedRequest.CompleteRequest
	token, err0 := h.tokenSerializer.Deserialize(completeRequest.TaskToken)
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

	err2 := engine.RespondDecisionTaskCompleted(ctx, wrappedRequest)
	if err2 != nil {
		h.updateErrorMetric(metrics.HistoryRespondDecisionTaskCompletedScope, h.convertError(err2))
		return h.convertError(err2)
	}

	return nil
}

// RespondDecisionTaskFailed - failed response to decision task
func (h *Handler) RespondDecisionTaskFailed(ctx context.Context,
	wrappedRequest *hist.RespondDecisionTaskFailedRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskFailedScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRespondDecisionTaskFailedScope, metrics.CadenceLatency)
	defer sw.Stop()

	if wrappedRequest.DomainUUID == nil {
		return errDomainNotSet
	}

	failedRequest := wrappedRequest.FailedRequest
	token, err0 := h.tokenSerializer.Deserialize(failedRequest.TaskToken)
	if err0 != nil {
		err0 = &gen.BadRequestError{Message: fmt.Sprintf("Error deserializing task token. Error: %v", err0)}
		h.updateErrorMetric(metrics.HistoryRespondDecisionTaskFailedScope, err0)
		return err0
	}

	h.Service.GetLogger().Debugf("RespondDecisionTaskFailed. DomainID: %v, WorkflowID: %v, RunID: %v, ScheduleID: %v",
		token.DomainID,
		token.WorkflowID,
		token.RunID,
		token.ScheduleID)

	engine, err1 := h.controller.GetEngine(token.WorkflowID)
	if err1 != nil {
		h.updateErrorMetric(metrics.HistoryRespondDecisionTaskFailedScope, err1)
		return err1
	}

	err2 := engine.RespondDecisionTaskFailed(wrappedRequest)
	if err2 != nil {
		h.updateErrorMetric(metrics.HistoryRespondDecisionTaskFailedScope, h.convertError(err2))
		return h.convertError(err2)
	}

	return nil
}

// StartWorkflowExecution - creates a new workflow execution
func (h *Handler) StartWorkflowExecution(ctx context.Context,
	wrappedRequest *hist.StartWorkflowExecutionRequest) (*gen.StartWorkflowExecutionResponse, error) {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryStartWorkflowExecutionScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryStartWorkflowExecutionScope, metrics.CadenceLatency)
	defer sw.Stop()

	if wrappedRequest.DomainUUID == nil {
		return nil, errDomainNotSet
	}

	startRequest := wrappedRequest.StartRequest
	engine, err1 := h.controller.GetEngine(*startRequest.WorkflowId)
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

// GetMutableState - returns the id of the next event in the execution's history
func (h *Handler) GetMutableState(ctx context.Context,
	getRequest *hist.GetMutableStateRequest) (*hist.GetMutableStateResponse, error) {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryGetMutableStateScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryGetMutableStateScope, metrics.CadenceLatency)
	defer sw.Stop()

	if getRequest.DomainUUID == nil {
		return nil, errDomainNotSet
	}

	workflowExecution := getRequest.Execution
	engine, err1 := h.controller.GetEngine(*workflowExecution.WorkflowId)
	if err1 != nil {
		h.updateErrorMetric(metrics.HistoryGetMutableStateScope, err1)
		return nil, err1
	}

	resp, err2 := engine.GetMutableState(ctx, getRequest)
	if err2 != nil {
		h.updateErrorMetric(metrics.HistoryGetMutableStateScope, h.convertError(err2))
		return nil, h.convertError(err2)
	}
	return resp, nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (h *Handler) DescribeWorkflowExecution(ctx context.Context, request *hist.DescribeWorkflowExecutionRequest) (*gen.DescribeWorkflowExecutionResponse, error) {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryDescribeWorkflowExecutionScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryDescribeWorkflowExecutionScope, metrics.CadenceLatency)
	defer sw.Stop()

	if request.DomainUUID == nil {
		return nil, errDomainNotSet
	}

	workflowExecution := request.Request.Execution
	engine, err1 := h.controller.GetEngine(*workflowExecution.WorkflowId)
	if err1 != nil {
		h.updateErrorMetric(metrics.HistoryDescribeWorkflowExecutionScope, err1)
		return nil, err1
	}

	resp, err2 := engine.DescribeWorkflowExecution(request)
	if err2 != nil {
		h.updateErrorMetric(metrics.HistoryDescribeWorkflowExecutionScope, h.convertError(err2))
		return nil, h.convertError(err2)
	}
	return resp, nil
}

// RequestCancelWorkflowExecution - requests cancellation of a workflow
func (h *Handler) RequestCancelWorkflowExecution(ctx context.Context,
	request *hist.RequestCancelWorkflowExecutionRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryRequestCancelWorkflowExecutionScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRequestCancelWorkflowExecutionScope, metrics.CadenceLatency)
	defer sw.Stop()

	if request.DomainUUID == nil || request.CancelRequest.Domain == nil {
		return errDomainNotSet
	}

	cancelRequest := request.CancelRequest
	h.Service.GetLogger().Debugf("RequestCancelWorkflowExecution. DomainID: %v/%v, WorkflowID: %v, RunID: %v.",
		*cancelRequest.Domain,
		*request.DomainUUID,
		*cancelRequest.WorkflowExecution.WorkflowId,
		common.StringDefault(cancelRequest.WorkflowExecution.RunId))

	engine, err1 := h.controller.GetEngine(*cancelRequest.WorkflowExecution.WorkflowId)
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
func (h *Handler) SignalWorkflowExecution(ctx context.Context,
	wrappedRequest *hist.SignalWorkflowExecutionRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistorySignalWorkflowExecutionScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistorySignalWorkflowExecutionScope, metrics.CadenceLatency)
	defer sw.Stop()

	if wrappedRequest.DomainUUID == nil {
		return errDomainNotSet
	}

	signalRequest := wrappedRequest.SignalRequest
	workflowExecution := signalRequest.WorkflowExecution
	engine, err1 := h.controller.GetEngine(*workflowExecution.WorkflowId)
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
func (h *Handler) TerminateWorkflowExecution(ctx context.Context,
	wrappedRequest *hist.TerminateWorkflowExecutionRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryTerminateWorkflowExecutionScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryTerminateWorkflowExecutionScope, metrics.CadenceLatency)
	defer sw.Stop()

	if wrappedRequest.DomainUUID == nil {
		return errDomainNotSet
	}

	terminateRequest := wrappedRequest.TerminateRequest
	workflowExecution := terminateRequest.WorkflowExecution
	engine, err1 := h.controller.GetEngine(*workflowExecution.WorkflowId)
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
func (h *Handler) ScheduleDecisionTask(ctx context.Context, request *hist.ScheduleDecisionTaskRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryScheduleDecisionTaskScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryScheduleDecisionTaskScope, metrics.CadenceLatency)
	defer sw.Stop()

	if request.DomainUUID == nil {
		return errDomainNotSet
	}

	if request.WorkflowExecution == nil {
		return errWorkflowExecutionNotSet
	}

	workflowExecution := request.WorkflowExecution
	engine, err1 := h.controller.GetEngine(*workflowExecution.WorkflowId)
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
func (h *Handler) RecordChildExecutionCompleted(ctx context.Context, request *hist.RecordChildExecutionCompletedRequest) error {
	h.startWG.Wait()

	h.metricsClient.IncCounter(metrics.HistoryRecordChildExecutionCompletedScope, metrics.CadenceRequests)
	sw := h.metricsClient.StartTimer(metrics.HistoryRecordChildExecutionCompletedScope, metrics.CadenceLatency)
	defer sw.Stop()

	if request.DomainUUID == nil {
		return errDomainNotSet
	}

	if request.WorkflowExecution == nil {
		return errWorkflowExecutionNotSet
	}

	workflowExecution := request.WorkflowExecution
	engine, err1 := h.controller.GetEngine(*workflowExecution.WorkflowId)
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
		if err == nil {
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
	shardLostErr := &hist.ShardOwnershipLostError{}
	shardLostErr.Message = common.StringPtr(fmt.Sprintf("Shard is not owned by host: %v", currentHost))
	shardLostErr.Owner = common.StringPtr(ownerHost)

	return shardLostErr
}
