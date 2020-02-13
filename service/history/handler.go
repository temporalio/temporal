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
	"sync/atomic"

	"github.com/pborman/uuid"
	"go.uber.org/yarpc/yarpcerrors"

	"github.com/uber/cadence/.gen/go/health"
	"github.com/uber/cadence/.gen/go/health/metaserver"
	hist "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/history/historyserviceserver"
	r "github.com/uber/cadence/.gen/go/replicator"
	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/quotas"
	"github.com/uber/cadence/common/resource"
)

// Handler - Thrift handler interface for history service
type (
	Handler struct {
		resource.Resource

		controller              *shardController
		tokenSerializer         common.TaskTokenSerializer
		startWG                 sync.WaitGroup
		config                  *Config
		historyEventNotifier    historyEventNotifier
		publisher               messaging.Producer
		rateLimiter             quotas.Limiter
		replicationTaskFetchers ReplicationTaskFetchers
	}
)

var _ historyserviceserver.Interface = (*Handler)(nil)
var _ EngineFactory = (*Handler)(nil)

var (
	errDomainNotSet            = &gen.BadRequestError{Message: "Domain not set on request."}
	errWorkflowExecutionNotSet = &gen.BadRequestError{Message: "WorkflowExecution not set on request."}
	errTaskListNotSet          = &gen.BadRequestError{Message: "Tasklist not set."}
	errWorkflowIDNotSet        = &gen.BadRequestError{Message: "WorkflowId is not set on request."}
	errRunIDNotValid           = &gen.BadRequestError{Message: "RunID is not valid UUID."}
	errSourceClusterNotSet     = &gen.BadRequestError{Message: "Source Cluster not set on request."}
	errShardIDNotSet           = &gen.BadRequestError{Message: "Shard ID not set on request."}
	errTimestampNotSet         = &gen.BadRequestError{Message: "Timestamp not set on request."}
	errHistoryHostThrottle     = &gen.ServiceBusyError{Message: "History host rps exceeded"}
)

// NewHandler creates a thrift handler for the history service
func NewHandler(
	resource resource.Resource,
	config *Config,
) *Handler {
	handler := &Handler{
		Resource:        resource,
		config:          config,
		tokenSerializer: common.NewJSONTaskTokenSerializer(),
		rateLimiter: quotas.NewDynamicRateLimiter(
			func() float64 {
				return float64(config.RPS())
			},
		),
	}

	// prevent us from trying to serve requests before shard controller is started and ready
	handler.startWG.Add(1)
	return handler
}

// RegisterHandler register this handler, must be called before Start()
func (h *Handler) RegisterHandler() {
	h.GetDispatcher().Register(historyserviceserver.New(h))
	h.GetDispatcher().Register(metaserver.New(h))
}

// Start starts the handler
func (h *Handler) Start() {
	if h.GetClusterMetadata().IsGlobalDomainEnabled() {
		var err error
		h.publisher, err = h.GetMessagingClient().NewProducerWithClusterName(h.GetClusterMetadata().GetCurrentClusterName())
		if err != nil {
			h.GetLogger().Fatal("Creating kafka producer failed", tag.Error(err))
		}
	}

	h.replicationTaskFetchers = NewReplicationTaskFetchers(
		h.GetLogger(),
		h.config,
		h.GetClusterMetadata().GetReplicationConsumerConfig(),
		h.GetClusterMetadata(),
		h.GetClientBean())

	h.replicationTaskFetchers.Start()

	h.controller = newShardController(
		h.Resource,
		h,
		h.config,
	)
	h.historyEventNotifier = newHistoryEventNotifier(h.GetTimeSource(), h.GetMetricsClient(), h.config.GetShardID)
	// events notifier must starts before controller
	h.historyEventNotifier.Start()
	h.controller.Start()

	h.startWG.Done()
}

// Stop stops the handler
func (h *Handler) Stop() {
	h.replicationTaskFetchers.Stop()
	h.controller.Stop()
	h.historyEventNotifier.Stop()
}

// CreateEngine is implementation for HistoryEngineFactory used for creating the engine instance for shard
func (h *Handler) CreateEngine(
	shardContext ShardContext,
) Engine {
	return NewEngineWithShardContext(
		shardContext,
		h.GetVisibilityManager(),
		h.GetMatchingClient(),
		h.GetHistoryClient(),
		h.GetSDKClient(),
		h.historyEventNotifier,
		h.publisher,
		h.config,
		h.replicationTaskFetchers,
		h.GetMatchingRawClient(),
	)
}

// Health is for health check
func (h *Handler) Health(ctx context.Context) (*health.HealthStatus, error) {
	h.startWG.Wait()
	h.GetLogger().Debug("History health check endpoint reached.")
	hs := &health.HealthStatus{Ok: true, Msg: common.StringPtr("history good")}
	return hs, nil
}

// RecordActivityTaskHeartbeat - Record Activity Task Heart beat.
func (h *Handler) RecordActivityTaskHeartbeat(
	ctx context.Context,
	wrappedRequest *hist.RecordActivityTaskHeartbeatRequest,
) (resp *gen.RecordActivityTaskHeartbeatResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRecordActivityTaskHeartbeatScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := wrappedRequest.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	heartbeatRequest := wrappedRequest.HeartbeatRequest
	token, err0 := h.tokenSerializer.Deserialize(heartbeatRequest.TaskToken)
	if err0 != nil {
		err0 = &gen.BadRequestError{Message: fmt.Sprintf("Error deserializing task token. Error: %v", err0)}
		return nil, h.error(err0, scope, domainID, "")
	}

	err0 = validateTaskToken(token)
	if err0 != nil {
		return nil, h.error(err0, scope, domainID, "")
	}
	workflowID := token.WorkflowID

	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	response, err2 := engine.RecordActivityTaskHeartbeat(ctx, wrappedRequest)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return response, nil
}

// RecordActivityTaskStarted - Record Activity Task started.
func (h *Handler) RecordActivityTaskStarted(
	ctx context.Context,
	recordRequest *hist.RecordActivityTaskStartedRequest,
) (resp *hist.RecordActivityTaskStartedResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRecordActivityTaskStartedScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := recordRequest.GetDomainUUID()
	workflowExecution := recordRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	if recordRequest.GetDomainUUID() == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, workflowID)
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, workflowID)
	}

	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	response, err2 := engine.RecordActivityTaskStarted(ctx, recordRequest)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return response, nil
}

// RecordDecisionTaskStarted - Record Decision Task started.
func (h *Handler) RecordDecisionTaskStarted(
	ctx context.Context,
	recordRequest *hist.RecordDecisionTaskStartedRequest,
) (resp *hist.RecordDecisionTaskStartedResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()
	h.GetLogger().Debug(fmt.Sprintf("RecordDecisionTaskStarted. DomainID: %v, WorkflowID: %v, RunID: %v, ScheduleID: %v",
		recordRequest.GetDomainUUID(),
		recordRequest.WorkflowExecution.GetWorkflowId(),
		common.StringDefault(recordRequest.WorkflowExecution.RunId),
		recordRequest.GetScheduleId()))

	scope := metrics.HistoryRecordDecisionTaskStartedScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := recordRequest.GetDomainUUID()
	workflowExecution := recordRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, workflowID)
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, workflowID)
	}

	if recordRequest.PollRequest == nil || recordRequest.PollRequest.TaskList.GetName() == "" {
		return nil, h.error(errTaskListNotSet, scope, domainID, workflowID)
	}

	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		h.GetLogger().Error("RecordDecisionTaskStarted failed.",
			tag.Error(err1),
			tag.WorkflowID(recordRequest.WorkflowExecution.GetWorkflowId()),
			tag.WorkflowRunID(recordRequest.WorkflowExecution.GetRunId()),
			tag.WorkflowScheduleID(recordRequest.GetScheduleId()),
		)
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	response, err2 := engine.RecordDecisionTaskStarted(ctx, recordRequest)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return response, nil
}

// RespondActivityTaskCompleted - records completion of an activity task
func (h *Handler) RespondActivityTaskCompleted(
	ctx context.Context,
	wrappedRequest *hist.RespondActivityTaskCompletedRequest,
) (retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRespondActivityTaskCompletedScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := wrappedRequest.GetDomainUUID()
	if domainID == "" {
		return h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	completeRequest := wrappedRequest.CompleteRequest
	token, err0 := h.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err0 != nil {
		err0 = &gen.BadRequestError{Message: fmt.Sprintf("Error deserializing task token. Error: %v", err0)}
		return h.error(err0, scope, domainID, "")
	}

	err0 = validateTaskToken(token)
	if err0 != nil {
		return h.error(err0, scope, domainID, "")
	}
	workflowID := token.WorkflowID

	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.RespondActivityTaskCompleted(ctx, wrappedRequest)
	if err2 != nil {
		return h.error(err2, scope, domainID, workflowID)
	}

	return nil
}

// RespondActivityTaskFailed - records failure of an activity task
func (h *Handler) RespondActivityTaskFailed(
	ctx context.Context,
	wrappedRequest *hist.RespondActivityTaskFailedRequest,
) (retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRespondActivityTaskFailedScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := wrappedRequest.GetDomainUUID()
	if domainID == "" {
		return h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	failRequest := wrappedRequest.FailedRequest
	token, err0 := h.tokenSerializer.Deserialize(failRequest.TaskToken)
	if err0 != nil {
		err0 = &gen.BadRequestError{Message: fmt.Sprintf("Error deserializing task token. Error: %v", err0)}
		return h.error(err0, scope, domainID, "")
	}

	err0 = validateTaskToken(token)
	if err0 != nil {
		return h.error(err0, scope, domainID, "")
	}
	workflowID := token.WorkflowID

	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.RespondActivityTaskFailed(ctx, wrappedRequest)
	if err2 != nil {
		return h.error(err2, scope, domainID, workflowID)
	}

	return nil
}

// RespondActivityTaskCanceled - records failure of an activity task
func (h *Handler) RespondActivityTaskCanceled(
	ctx context.Context,
	wrappedRequest *hist.RespondActivityTaskCanceledRequest,
) (retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRespondActivityTaskCanceledScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := wrappedRequest.GetDomainUUID()
	if domainID == "" {
		return h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	cancelRequest := wrappedRequest.CancelRequest
	token, err0 := h.tokenSerializer.Deserialize(cancelRequest.TaskToken)
	if err0 != nil {
		err0 = &gen.BadRequestError{Message: fmt.Sprintf("Error deserializing task token. Error: %v", err0)}
		return h.error(err0, scope, domainID, "")
	}

	err0 = validateTaskToken(token)
	if err0 != nil {
		return h.error(err0, scope, domainID, "")
	}
	workflowID := token.WorkflowID

	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.RespondActivityTaskCanceled(ctx, wrappedRequest)
	if err2 != nil {
		return h.error(err2, scope, domainID, workflowID)
	}

	return nil
}

// RespondDecisionTaskCompleted - records completion of a decision task
func (h *Handler) RespondDecisionTaskCompleted(
	ctx context.Context,
	wrappedRequest *hist.RespondDecisionTaskCompletedRequest,
) (resp *hist.RespondDecisionTaskCompletedResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRespondDecisionTaskCompletedScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := wrappedRequest.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	completeRequest := wrappedRequest.CompleteRequest
	if len(completeRequest.Decisions) == 0 {
		h.GetMetricsClient().IncCounter(scope, metrics.EmptyCompletionDecisionsCounter)
	}
	token, err0 := h.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err0 != nil {
		err0 = &gen.BadRequestError{Message: fmt.Sprintf("Error deserializing task token. Error: %v", err0)}
		return nil, h.error(err0, scope, domainID, "")
	}

	h.GetLogger().Debug(fmt.Sprintf("RespondDecisionTaskCompleted. DomainID: %v, WorkflowID: %v, RunID: %v, ScheduleID: %v",
		token.DomainID,
		token.WorkflowID,
		token.RunID,
		token.ScheduleID))

	err0 = validateTaskToken(token)
	if err0 != nil {
		return nil, h.error(err0, scope, domainID, "")
	}
	workflowID := token.WorkflowID

	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	response, err2 := engine.RespondDecisionTaskCompleted(ctx, wrappedRequest)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return response, nil
}

// RespondDecisionTaskFailed - failed response to decision task
func (h *Handler) RespondDecisionTaskFailed(
	ctx context.Context,
	wrappedRequest *hist.RespondDecisionTaskFailedRequest,
) (retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRespondDecisionTaskFailedScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := wrappedRequest.GetDomainUUID()
	if domainID == "" {
		return h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	failedRequest := wrappedRequest.FailedRequest
	token, err0 := h.tokenSerializer.Deserialize(failedRequest.TaskToken)
	if err0 != nil {
		err0 = &gen.BadRequestError{Message: fmt.Sprintf("Error deserializing task token. Error: %v", err0)}
		return h.error(err0, scope, domainID, "")
	}

	h.GetLogger().Debug(fmt.Sprintf("RespondDecisionTaskFailed. DomainID: %v, WorkflowID: %v, RunID: %v, ScheduleID: %v",
		token.DomainID,
		token.WorkflowID,
		token.RunID,
		token.ScheduleID))

	if failedRequest != nil && failedRequest.GetCause() == gen.DecisionTaskFailedCauseUnhandledDecision {
		h.GetLogger().Info("Non-Deterministic Error", tag.WorkflowDomainID(token.DomainID), tag.WorkflowID(token.WorkflowID), tag.WorkflowRunID(token.RunID))
		domainName, err := h.GetDomainCache().GetDomainName(token.DomainID)
		var domainTag metrics.Tag

		if err == nil {
			domainTag = metrics.DomainTag(domainName)
		} else {
			domainTag = metrics.DomainUnknownTag()
		}

		h.GetMetricsClient().Scope(scope, domainTag).IncCounter(metrics.CadenceErrNonDeterministicCounter)
	}
	err0 = validateTaskToken(token)
	if err0 != nil {
		return h.error(err0, scope, domainID, "")
	}
	workflowID := token.WorkflowID

	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.RespondDecisionTaskFailed(ctx, wrappedRequest)
	if err2 != nil {
		return h.error(err2, scope, domainID, workflowID)
	}

	return nil
}

// StartWorkflowExecution - creates a new workflow execution
func (h *Handler) StartWorkflowExecution(
	ctx context.Context,
	wrappedRequest *hist.StartWorkflowExecutionRequest,
) (resp *gen.StartWorkflowExecutionResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryStartWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := wrappedRequest.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	startRequest := wrappedRequest.StartRequest
	workflowID := startRequest.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	response, err2 := engine.StartWorkflowExecution(ctx, wrappedRequest)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return response, nil
}

// DescribeHistoryHost returns information about the internal states of a history host
func (h *Handler) DescribeHistoryHost(
	ctx context.Context,
	request *gen.DescribeHistoryHostRequest,
) (resp *gen.DescribeHistoryHostResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	numOfItemsInCacheByID, numOfItemsInCacheByName := h.GetDomainCache().GetCacheSize()
	status := ""
	switch atomic.LoadInt32(&h.controller.status) {
	case common.DaemonStatusInitialized:
		status = "initialized"
	case common.DaemonStatusStarted:
		status = "started"
	case common.DaemonStatusStopped:
		status = "stopped"
	}

	resp = &gen.DescribeHistoryHostResponse{
		NumberOfShards: common.Int32Ptr(int32(h.controller.numShards())),
		ShardIDs:       h.controller.shardIDs(),
		DomainCache: &gen.DomainCacheInfo{
			NumOfItemsInCacheByID:   &numOfItemsInCacheByID,
			NumOfItemsInCacheByName: &numOfItemsInCacheByName,
		},
		ShardControllerStatus: &status,
		Address:               common.StringPtr(h.GetHostInfo().GetAddress()),
	}
	return resp, nil
}

// RemoveTask returns information about the internal states of a history host
func (h *Handler) RemoveTask(
	ctx context.Context,
	request *gen.RemoveTaskRequest,
) (retError error) {
	executionMgr, err := h.GetExecutionManager(int(request.GetShardID()))
	if err != nil {
		return err
	}
	deleteTaskRequest := &persistence.DeleteTaskRequest{
		TaskID:  request.GetTaskID(),
		Type:    int(request.GetType()),
		ShardID: int(request.GetShardID()),
	}
	err = executionMgr.DeleteTask(deleteTaskRequest)
	return err
}

// CloseShard returns information about the internal states of a history host
func (h *Handler) CloseShard(
	ctx context.Context,
	request *gen.CloseShardRequest,
) (retError error) {
	h.controller.removeEngineForShard(int(request.GetShardID()))
	return nil
}

// DescribeMutableState - returns the internal analysis of workflow execution state
func (h *Handler) DescribeMutableState(
	ctx context.Context,
	request *hist.DescribeMutableStateRequest,
) (resp *hist.DescribeMutableStateResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRecordActivityTaskHeartbeatScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	workflowExecution := request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	resp, err2 := engine.DescribeMutableState(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}
	return resp, nil
}

// GetMutableState - returns the id of the next event in the execution's history
func (h *Handler) GetMutableState(
	ctx context.Context,
	getRequest *hist.GetMutableStateRequest,
) (resp *hist.GetMutableStateResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryGetMutableStateScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := getRequest.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := getRequest.Execution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	resp, err2 := engine.GetMutableState(ctx, getRequest)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}
	return resp, nil
}

// PollMutableState - returns the id of the next event in the execution's history
func (h *Handler) PollMutableState(
	ctx context.Context,
	getRequest *hist.PollMutableStateRequest,
) (resp *hist.PollMutableStateResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryPollMutableStateScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := getRequest.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := getRequest.Execution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	resp, err2 := engine.PollMutableState(ctx, getRequest)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}
	return resp, nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (h *Handler) DescribeWorkflowExecution(
	ctx context.Context,
	request *hist.DescribeWorkflowExecutionRequest,
) (resp *gen.DescribeWorkflowExecutionResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryDescribeWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := request.Request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	resp, err2 := engine.DescribeWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}
	return resp, nil
}

// RequestCancelWorkflowExecution - requests cancellation of a workflow
func (h *Handler) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *hist.RequestCancelWorkflowExecutionRequest,
) (retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRequestCancelWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" || request.CancelRequest.GetDomain() == "" {
		return h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	cancelRequest := request.CancelRequest
	h.GetLogger().Debug(fmt.Sprintf("RequestCancelWorkflowExecution. DomainID: %v/%v, WorkflowID: %v, RunID: %v.",
		cancelRequest.GetDomain(),
		request.GetDomainUUID(),
		cancelRequest.WorkflowExecution.GetWorkflowId(),
		cancelRequest.WorkflowExecution.GetRunId()))

	workflowID := cancelRequest.WorkflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.RequestCancelWorkflowExecution(ctx, request)
	if err2 != nil {
		return h.error(err2, scope, domainID, workflowID)
	}

	return nil
}

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a decision task being created for the execution.
func (h *Handler) SignalWorkflowExecution(
	ctx context.Context,
	wrappedRequest *hist.SignalWorkflowExecutionRequest,
) (retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistorySignalWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := wrappedRequest.GetDomainUUID()
	if domainID == "" {
		return h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := wrappedRequest.SignalRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.SignalWorkflowExecution(ctx, wrappedRequest)
	if err2 != nil {
		return h.error(err2, scope, domainID, workflowID)
	}

	return nil
}

// SignalWithStartWorkflowExecution is used to ensure sending a signal event to a workflow execution.
// If workflow is running, this results in WorkflowExecutionSignaled event recorded in the history
// and a decision task being created for the execution.
// If workflow is not running or not found, this results in WorkflowExecutionStarted and WorkflowExecutionSignaled
// event recorded in history, and a decision task being created for the execution
func (h *Handler) SignalWithStartWorkflowExecution(
	ctx context.Context,
	wrappedRequest *hist.SignalWithStartWorkflowExecutionRequest,
) (resp *gen.StartWorkflowExecutionResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistorySignalWithStartWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := wrappedRequest.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	signalWithStartRequest := wrappedRequest.SignalWithStartRequest
	workflowID := signalWithStartRequest.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	resp, err2 := engine.SignalWithStartWorkflowExecution(ctx, wrappedRequest)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return resp, nil
}

// RemoveSignalMutableState is used to remove a signal request ID that was previously recorded.  This is currently
// used to clean execution info when signal decision finished.
func (h *Handler) RemoveSignalMutableState(
	ctx context.Context,
	wrappedRequest *hist.RemoveSignalMutableStateRequest,
) (retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRemoveSignalMutableStateScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := wrappedRequest.GetDomainUUID()
	if domainID == "" {
		return h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := wrappedRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.RemoveSignalMutableState(ctx, wrappedRequest)
	if err2 != nil {
		return h.error(err2, scope, domainID, workflowID)
	}

	return nil
}

// TerminateWorkflowExecution terminates an existing workflow execution by recording WorkflowExecutionTerminated event
// in the history and immediately terminating the execution instance.
func (h *Handler) TerminateWorkflowExecution(
	ctx context.Context,
	wrappedRequest *hist.TerminateWorkflowExecutionRequest,
) (retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryTerminateWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := wrappedRequest.GetDomainUUID()
	if domainID == "" {
		return h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := wrappedRequest.TerminateRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.TerminateWorkflowExecution(ctx, wrappedRequest)
	if err2 != nil {
		return h.error(err2, scope, domainID, workflowID)
	}

	return nil
}

// ResetWorkflowExecution reset an existing workflow execution
// in the history and immediately terminating the execution instance.
func (h *Handler) ResetWorkflowExecution(
	ctx context.Context,
	wrappedRequest *hist.ResetWorkflowExecutionRequest,
) (resp *gen.ResetWorkflowExecutionResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryResetWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := wrappedRequest.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := wrappedRequest.ResetRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	resp, err2 := engine.ResetWorkflowExecution(ctx, wrappedRequest)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return resp, nil
}

// QueryWorkflow queries a workflow.
func (h *Handler) QueryWorkflow(
	ctx context.Context,
	request *hist.QueryWorkflowRequest,
) (resp *hist.QueryWorkflowResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryQueryWorkflowScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowID := request.GetRequest().GetExecution().GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	resp, err2 := engine.QueryWorkflow(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return resp, nil
}

// ScheduleDecisionTask is used for creating a decision task for already started workflow execution.  This is mainly
// used by transfer queue processor during the processing of StartChildWorkflowExecution task, where it first starts
// child execution without creating the decision task and then calls this API after updating the mutable state of
// parent execution.
func (h *Handler) ScheduleDecisionTask(
	ctx context.Context,
	request *hist.ScheduleDecisionTaskRequest,
) (retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryScheduleDecisionTaskScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	if request.WorkflowExecution == nil {
		return h.error(errWorkflowExecutionNotSet, scope, domainID, "")
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.ScheduleDecisionTask(ctx, request)
	if err2 != nil {
		return h.error(err2, scope, domainID, workflowID)
	}

	return nil
}

// RecordChildExecutionCompleted is used for reporting the completion of child workflow execution to parent.
// This is mainly called by transfer queue processor during the processing of DeleteExecution task.
func (h *Handler) RecordChildExecutionCompleted(
	ctx context.Context,
	request *hist.RecordChildExecutionCompletedRequest,
) (retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRecordChildExecutionCompletedScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	if request.WorkflowExecution == nil {
		return h.error(errWorkflowExecutionNotSet, scope, domainID, "")
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.RecordChildExecutionCompleted(ctx, request)
	if err2 != nil {
		return h.error(err2, scope, domainID, workflowID)
	}

	return nil
}

// ResetStickyTaskList reset the volatile information in mutable state of a given workflow.
// Volatile information are the information related to client, such as:
// 1. StickyTaskList
// 2. StickyScheduleToStartTimeout
// 3. ClientLibraryVersion
// 4. ClientFeatureVersion
// 5. ClientImpl
func (h *Handler) ResetStickyTaskList(
	ctx context.Context,
	resetRequest *hist.ResetStickyTaskListRequest,
) (resp *hist.ResetStickyTaskListResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryResetStickyTaskListScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := resetRequest.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowID := resetRequest.Execution.GetWorkflowId()
	engine, err := h.controller.GetEngine(workflowID)
	if err != nil {
		return nil, h.error(err, scope, domainID, workflowID)
	}

	resp, err = engine.ResetStickyTaskList(ctx, resetRequest)
	if err != nil {
		return nil, h.error(err, scope, domainID, workflowID)
	}

	return resp, nil
}

// ReplicateEvents is called by processor to replicate history events for passive domains
func (h *Handler) ReplicateEvents(
	ctx context.Context,
	replicateRequest *hist.ReplicateEventsRequest,
) (retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryReplicateEventsScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := replicateRequest.GetDomainUUID()
	if domainID == "" {
		return h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := replicateRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.ReplicateEvents(ctx, replicateRequest)
	if err2 != nil {
		return h.error(err2, scope, domainID, workflowID)
	}

	return nil
}

// ReplicateRawEvents is called by processor to replicate history raw events for passive domains
func (h *Handler) ReplicateRawEvents(
	ctx context.Context,
	replicateRequest *hist.ReplicateRawEventsRequest,
) (retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryReplicateRawEventsScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := replicateRequest.GetDomainUUID()
	if domainID == "" {
		return h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := replicateRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.ReplicateRawEvents(ctx, replicateRequest)
	if err2 != nil {
		return h.error(err2, scope, domainID, workflowID)
	}

	return nil
}

// ReplicateEventsV2 is called by processor to replicate history events for passive domains
func (h *Handler) ReplicateEventsV2(
	ctx context.Context,
	replicateRequest *hist.ReplicateEventsV2Request,
) (retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryReplicateEventsV2Scope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := replicateRequest.GetDomainUUID()
	if domainID == "" {
		return h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := replicateRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.ReplicateEventsV2(ctx, replicateRequest)
	if err2 != nil {
		return h.error(err2, scope, domainID, workflowID)
	}

	return nil
}

// SyncShardStatus is called by processor to sync history shard information from another cluster
func (h *Handler) SyncShardStatus(
	ctx context.Context,
	syncShardStatusRequest *hist.SyncShardStatusRequest,
) (retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistorySyncShardStatusScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	if ok := h.rateLimiter.Allow(); !ok {
		return h.error(errHistoryHostThrottle, scope, "", "")
	}

	if syncShardStatusRequest.SourceCluster == nil {
		return h.error(errSourceClusterNotSet, scope, "", "")
	}

	if syncShardStatusRequest.ShardId == nil {
		return h.error(errShardIDNotSet, scope, "", "")
	}

	if syncShardStatusRequest.Timestamp == nil {
		return h.error(errTimestampNotSet, scope, "", "")
	}

	// shard ID is already provided in the request
	engine, err := h.controller.getEngineForShard(int(syncShardStatusRequest.GetShardId()))
	if err != nil {
		return h.error(err, scope, "", "")
	}

	err = engine.SyncShardStatus(ctx, syncShardStatusRequest)
	if err != nil {
		return h.error(err, scope, "", "")
	}

	return nil
}

// SyncActivity is called by processor to sync activity
func (h *Handler) SyncActivity(
	ctx context.Context,
	syncActivityRequest *hist.SyncActivityRequest,
) (retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistorySyncActivityScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := syncActivityRequest.GetDomainId()
	if syncActivityRequest.DomainId == nil || uuid.Parse(syncActivityRequest.GetDomainId()) == nil {
		return h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	if syncActivityRequest.WorkflowId == nil {
		return h.error(errWorkflowIDNotSet, scope, domainID, "")
	}

	if syncActivityRequest.RunId == nil || uuid.Parse(syncActivityRequest.GetRunId()) == nil {
		return h.error(errRunIDNotValid, scope, domainID, "")
	}

	workflowID := syncActivityRequest.GetWorkflowId()
	engine, err := h.controller.GetEngine(workflowID)
	if err != nil {
		return h.error(err, scope, domainID, workflowID)
	}

	err = engine.SyncActivity(ctx, syncActivityRequest)
	if err != nil {
		return h.error(err, scope, domainID, workflowID)
	}

	return nil
}

// GetReplicationMessages is called by remote peers to get replicated messages for cross DC replication
func (h *Handler) GetReplicationMessages(
	ctx context.Context,
	request *r.GetReplicationMessagesRequest,
) (resp *r.GetReplicationMessagesResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	h.GetLogger().Debug("Received GetReplicationMessages call.")

	scope := metrics.HistoryGetReplicationMessagesScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	var wg sync.WaitGroup
	wg.Add(len(request.Tokens))
	result := new(sync.Map)

	for _, token := range request.Tokens {
		go func(token *r.ReplicationToken) {
			defer wg.Done()

			engine, err := h.controller.getEngineForShard(int(token.GetShardID()))
			if err != nil {
				h.GetLogger().Warn("History engine not found for shard", tag.Error(err))
				return
			}
			tasks, err := engine.GetReplicationMessages(
				ctx,
				request.GetClusterName(),
				token.GetLastRetrievedMessageId(),
			)
			if err != nil {
				h.GetLogger().Warn("Failed to get replication tasks for shard", tag.Error(err))
				return
			}

			result.Store(token.GetShardID(), tasks)
		}(token)
	}

	wg.Wait()

	messagesByShard := make(map[int32]*r.ReplicationMessages)
	result.Range(func(key, value interface{}) bool {
		shardID := key.(int32)
		tasks := value.(*r.ReplicationMessages)
		messagesByShard[shardID] = tasks
		return true
	})

	h.GetLogger().Debug("GetReplicationMessages succeeded.")

	return &r.GetReplicationMessagesResponse{MessagesByShard: messagesByShard}, nil
}

// GetDLQReplicationMessages is called by remote peers to get replicated messages for DLQ merging
func (h *Handler) GetDLQReplicationMessages(
	ctx context.Context,
	request *r.GetDLQReplicationMessagesRequest,
) (resp *r.GetDLQReplicationMessagesResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryGetDLQReplicationMessagesScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	taskInfoPerExecution := map[definition.WorkflowIdentifier][]*r.ReplicationTaskInfo{}
	// do batch based on workflow ID and run ID
	for _, taskInfo := range request.GetTaskInfos() {
		identity := definition.NewWorkflowIdentifier(
			taskInfo.GetDomainID(),
			taskInfo.GetWorkflowID(),
			taskInfo.GetRunID(),
		)
		if _, ok := taskInfoPerExecution[identity]; !ok {
			taskInfoPerExecution[identity] = []*r.ReplicationTaskInfo{}
		}
		taskInfoPerExecution[identity] = append(taskInfoPerExecution[identity], taskInfo)
	}

	var wg sync.WaitGroup
	wg.Add(len(taskInfoPerExecution))
	tasksChan := make(chan *r.ReplicationTask, len(request.GetTaskInfos()))
	handleTaskInfoPerExecution := func(taskInfos []*r.ReplicationTaskInfo) {
		defer wg.Done()
		if len(taskInfos) == 0 {
			return
		}

		engine, err := h.controller.GetEngine(
			taskInfos[0].GetWorkflowID(),
		)
		if err != nil {
			h.GetLogger().Warn("History engine not found for workflow ID.", tag.Error(err))
			return
		}

		tasks, err := engine.GetDLQReplicationMessages(
			ctx,
			taskInfos,
		)
		if err != nil {
			h.GetLogger().Error("Failed to get dlq replication tasks.", tag.Error(err))
			return
		}

		for _, task := range tasks {
			tasksChan <- task
		}
	}

	for _, replicationTaskInfos := range taskInfoPerExecution {
		go handleTaskInfoPerExecution(replicationTaskInfos)
	}
	wg.Wait()
	close(tasksChan)

	replicationTasks := make([]*r.ReplicationTask, len(tasksChan))
	for task := range tasksChan {
		replicationTasks = append(replicationTasks, task)
	}
	return &r.GetDLQReplicationMessagesResponse{
		ReplicationTasks: replicationTasks,
	}, nil
}

// ReapplyEvents applies stale events to the current workflow and the current run
func (h *Handler) ReapplyEvents(
	ctx context.Context,
	request *hist.ReapplyEventsRequest,
) (retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryReapplyEventsScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	workflowID := request.GetRequest().GetWorkflowExecution().GetWorkflowId()
	engine, err := h.controller.GetEngine(workflowID)
	if err != nil {
		return h.error(err, scope, domainID, workflowID)
	}
	// deserialize history event object
	historyEvents, err := h.GetPayloadSerializer().DeserializeBatchEvents(&persistence.DataBlob{
		Encoding: common.EncodingTypeThriftRW,
		Data:     request.GetRequest().GetEvents().GetData(),
	})
	if err != nil {
		return h.error(err, scope, domainID, workflowID)
	}

	execution := request.GetRequest().GetWorkflowExecution()
	if err := engine.ReapplyEvents(
		ctx,
		request.GetDomainUUID(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
		historyEvents,
	); err != nil {
		return h.error(err, scope, domainID, workflowID)
	}
	return nil
}

// ReadDLQMessages reads replication DLQ messages
func (h *Handler) ReadDLQMessages(
	ctx context.Context,
	request *r.ReadDLQMessagesRequest,
) (resp *r.ReadDLQMessagesResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryReadDLQMessagesScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	engine, err := h.controller.getEngineForShard(int(request.GetShardID()))
	if err != nil {
		return nil, h.error(err, scope, "", "")
	}

	return engine.ReadDLQMessages(ctx, request)
}

// PurgeDLQMessages deletes replication DLQ messages
func (h *Handler) PurgeDLQMessages(
	ctx context.Context,
	request *r.PurgeDLQMessagesRequest,
) (retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryPurgeDLQMessagesScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	engine, err := h.controller.getEngineForShard(int(request.GetShardID()))
	if err != nil {
		return h.error(err, scope, "", "")
	}

	return engine.PurgeDLQMessages(ctx, request)
}

// MergeDLQMessages reads and applies replication DLQ messages
func (h *Handler) MergeDLQMessages(
	ctx context.Context,
	request *r.MergeDLQMessagesRequest,
) (resp *r.MergeDLQMessagesResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryMergeDLQMessagesScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()

	engine, err := h.controller.getEngineForShard(int(request.GetShardID()))
	if err != nil {
		return nil, h.error(err, scope, "", "")
	}

	return engine.MergeDLQMessages(ctx, request)
}

// RefreshWorkflowTasks refreshes all the tasks of a workflow
func (h *Handler) RefreshWorkflowTasks(
	ctx context.Context,
	request *hist.RefreshWorkflowTasksRequest) (retError error) {

	scope := metrics.HistoryRefreshWorkflowTasksScope
	h.GetMetricsClient().IncCounter(scope, metrics.CadenceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.CadenceLatency)
	defer sw.Stop()
	domainID := request.GetDomainUIID()
	execution := request.GetRequest().GetExecution()
	workflowID := execution.GetWorkflowId()
	engine, err := h.controller.GetEngine(workflowID)
	if err != nil {
		return h.error(err, scope, domainID, workflowID)
	}

	err = engine.RefreshWorkflowTasks(
		ctx,
		domainID,
		gen.WorkflowExecution{
			WorkflowId: execution.WorkflowId,
			RunId:      execution.RunId,
		},
	)

	if err != nil {
		return h.error(err, scope, domainID, workflowID)
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
		info, err := h.GetHistoryServiceResolver().Lookup(string(shardID))
		if err == nil {
			return createShardOwnershipLostError(h.GetHostInfo().GetAddress(), info.GetAddress())
		}
		return createShardOwnershipLostError(h.GetHostInfo().GetAddress(), "")
	case *persistence.WorkflowExecutionAlreadyStartedError:
		err := err.(*persistence.WorkflowExecutionAlreadyStartedError)
		return &gen.InternalServiceError{Message: err.Msg}
	case *persistence.CurrentWorkflowConditionFailedError:
		err := err.(*persistence.CurrentWorkflowConditionFailedError)
		return &gen.InternalServiceError{Message: err.Msg}
	case *persistence.TransactionSizeLimitError:
		err := err.(*persistence.TransactionSizeLimitError)
		return &gen.BadRequestError{Message: err.Msg}
	}

	return err
}

func (h *Handler) updateErrorMetric(
	scope int,
	domainID string,
	workflowID string,
	err error,
) {

	if err == context.DeadlineExceeded || err == context.Canceled {
		h.GetMetricsClient().IncCounter(scope, metrics.CadenceErrContextTimeoutCounter)
		return
	}

	switch err := err.(type) {
	case *hist.ShardOwnershipLostError:
		h.GetMetricsClient().IncCounter(scope, metrics.CadenceErrShardOwnershipLostCounter)
	case *hist.EventAlreadyStartedError:
		h.GetMetricsClient().IncCounter(scope, metrics.CadenceErrEventAlreadyStartedCounter)
	case *gen.BadRequestError:
		h.GetMetricsClient().IncCounter(scope, metrics.CadenceErrBadRequestCounter)
	case *gen.DomainNotActiveError:
		h.GetMetricsClient().IncCounter(scope, metrics.CadenceErrBadRequestCounter)
	case *gen.WorkflowExecutionAlreadyStartedError:
		h.GetMetricsClient().IncCounter(scope, metrics.CadenceErrExecutionAlreadyStartedCounter)
	case *gen.EntityNotExistsError:
		h.GetMetricsClient().IncCounter(scope, metrics.CadenceErrEntityNotExistsCounter)
	case *gen.CancellationAlreadyRequestedError:
		h.GetMetricsClient().IncCounter(scope, metrics.CadenceErrCancellationAlreadyRequestedCounter)
	case *gen.LimitExceededError:
		h.GetMetricsClient().IncCounter(scope, metrics.CadenceErrLimitExceededCounter)
	case *gen.RetryTaskError:
		h.GetMetricsClient().IncCounter(scope, metrics.CadenceErrRetryTaskCounter)
	case *gen.RetryTaskV2Error:
		h.GetMetricsClient().IncCounter(scope, metrics.CadenceErrRetryTaskCounter)
	case *gen.ServiceBusyError:
		h.GetMetricsClient().IncCounter(scope, metrics.CadenceErrServiceBusyCounter)
	case *yarpcerrors.Status:
		if err.Code() == yarpcerrors.CodeDeadlineExceeded {
			h.GetMetricsClient().IncCounter(scope, metrics.CadenceErrContextTimeoutCounter)
		}
		h.GetMetricsClient().IncCounter(scope, metrics.CadenceFailures)
	case *gen.InternalServiceError:
		h.GetMetricsClient().IncCounter(scope, metrics.CadenceFailures)
		h.GetLogger().Error("Internal service error",
			tag.Error(err),
			tag.WorkflowID(workflowID),
			tag.WorkflowDomainID(domainID))
	default:
		h.GetMetricsClient().IncCounter(scope, metrics.CadenceFailures)
		h.getLoggerWithTags(domainID, workflowID).Error("Uncategorized error", tag.Error(err))
	}
}

func (h *Handler) error(
	err error,
	scope int,
	domainID string,
	workflowID string,
) error {

	err = h.convertError(err)
	h.updateErrorMetric(scope, domainID, workflowID, err)

	return err
}

func (h *Handler) getLoggerWithTags(
	domainID string,
	workflowID string,
) log.Logger {

	logger := h.GetLogger()
	if domainID != "" {
		logger = logger.WithTags(tag.WorkflowDomainID(domainID))
	}

	if workflowID != "" {
		logger = logger.WithTags(tag.WorkflowID(workflowID))
	}

	return logger
}

func createShardOwnershipLostError(
	currentHost string,
	ownerHost string,
) *hist.ShardOwnershipLostError {

	shardLostErr := &hist.ShardOwnershipLostError{}
	shardLostErr.Message = common.StringPtr(fmt.Sprintf("Shard is not owned by host: %v", currentHost))
	shardLostErr.Owner = common.StringPtr(ownerHost)

	return shardLostErr
}

func validateTaskToken(token *common.TaskToken) error {
	if token.WorkflowID == "" {
		return errWorkflowIDNotSet
	}
	if token.RunID != "" && uuid.Parse(token.RunID) == nil {
		return errRunIDNotValid
	}
	return nil
}
