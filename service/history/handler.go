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
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/healthservice"
	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/.gen/proto/token"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/persistence/serialization"
	"github.com/temporalio/temporal/common/primitives"
	"github.com/temporalio/temporal/common/quotas"
	"github.com/temporalio/temporal/common/resource"
)

type (

	// Handler - gRPC handler interface for historyservice
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

var (
	_ EngineFactory                       = (*Handler)(nil)
	_ historyservice.HistoryServiceServer = (*Handler)(nil)

	errDomainNotSet            = serviceerror.NewInvalidArgument("Domain not set on request.")
	errWorkflowExecutionNotSet = serviceerror.NewInvalidArgument("WorkflowExecution not set on request.")
	errTaskListNotSet          = serviceerror.NewInvalidArgument("Task list not set.")
	errWorkflowIDNotSet        = serviceerror.NewInvalidArgument("WorkflowId is not set on request.")
	errRunIDNotValid           = serviceerror.NewInvalidArgument("RunID is not valid UUID.")
	errSourceClusterNotSet     = serviceerror.NewInvalidArgument("Source Cluster not set on request.")
	errShardIDNotSet           = serviceerror.NewInvalidArgument("Shard ID not set on request.")
	errTimestampNotSet         = serviceerror.NewInvalidArgument("Timestamp not set on request.")
	errDeserializeTaskToken    = serviceerror.NewInvalidArgument("Error to deserialize task token. Error: %v.")

	errHistoryHostThrottle = serviceerror.NewResourceExhausted("History host RPS exceeded.")
)

// NewHandler creates a thrift handler for the history service
func NewHandler(
	resource resource.Resource,
	config *Config,
) *Handler {
	handler := &Handler{
		Resource:        resource,
		config:          config,
		tokenSerializer: common.NewProtoTaskTokenSerializer(),
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
func (h *Handler) Health(context.Context, *healthservice.HealthRequest) (_ *healthservice.HealthStatus, retError error) {
	h.startWG.Wait()
	h.GetLogger().Debug("History service health check endpoint (gRPC) reached.")
	hs := &healthservice.HealthStatus{Ok: true, Msg: "History service is healthy."}
	return hs, nil
}

// RecordActivityTaskHeartbeat - Record Activity Task Heart beat.
func (h *Handler) RecordActivityTaskHeartbeat(ctx context.Context, request *historyservice.RecordActivityTaskHeartbeatRequest) (_ *historyservice.RecordActivityTaskHeartbeatResponse, retError error) {

	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRecordActivityTaskHeartbeatScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	heartbeatRequest := request.HeartbeatRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(heartbeatRequest.TaskToken)
	if err0 != nil {
		return nil, h.error(errDeserializeTaskToken.MessageArgs(err0), scope, domainID, "")
	}

	err0 = validateTaskToken(taskToken)
	if err0 != nil {
		return nil, h.error(err0, scope, domainID, "")
	}
	workflowID := taskToken.GetWorkflowId()

	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	response, err2 := engine.RecordActivityTaskHeartbeat(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return response, nil
}

// RecordActivityTaskStarted - Record Activity Task started.
func (h *Handler) RecordActivityTaskStarted(ctx context.Context, request *historyservice.RecordActivityTaskStartedRequest) (_ *historyservice.RecordActivityTaskStartedResponse, retError error) {

	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRecordActivityTaskStartedScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	if request.GetDomainUUID() == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, workflowID)
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, workflowID)
	}

	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	response, err2 := engine.RecordActivityTaskStarted(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return response, nil
}

// RecordDecisionTaskStarted - Record Decision Task started.
func (h *Handler) RecordDecisionTaskStarted(ctx context.Context, request *historyservice.RecordDecisionTaskStartedRequest) (_ *historyservice.RecordDecisionTaskStartedResponse, retError error) {

	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()
	h.GetLogger().Debug("RecordDecisionTaskStarted",
		tag.WorkflowDomainID(request.GetDomainUUID()),
		tag.WorkflowID(request.WorkflowExecution.GetWorkflowId()),
		tag.WorkflowRunID(request.WorkflowExecution.GetRunId()),
		tag.WorkflowScheduleID(request.GetScheduleId()))

	scope := metrics.HistoryRecordDecisionTaskStartedScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, workflowID)
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, workflowID)
	}

	if request.PollRequest == nil || request.PollRequest.TaskList.GetName() == "" {
		return nil, h.error(errTaskListNotSet, scope, domainID, workflowID)
	}

	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		h.GetLogger().Error("RecordDecisionTaskStarted failed.",
			tag.Error(err1),
			tag.WorkflowID(request.WorkflowExecution.GetWorkflowId()),
			tag.WorkflowRunID(request.WorkflowExecution.GetRunId()),
			tag.WorkflowScheduleID(request.GetScheduleId()),
		)
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	response, err2 := engine.RecordDecisionTaskStarted(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return response, nil
}

// RespondActivityTaskCompleted - records completion of an activity task
func (h *Handler) RespondActivityTaskCompleted(ctx context.Context, request *historyservice.RespondActivityTaskCompletedRequest) (_ *historyservice.RespondActivityTaskCompletedResponse, retError error) {

	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRespondActivityTaskCompletedScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	completeRequest := request.CompleteRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err0 != nil {
		return nil, h.error(errDeserializeTaskToken.MessageArgs(err0), scope, domainID, "")
	}

	err0 = validateTaskToken(taskToken)
	if err0 != nil {
		return nil, h.error(err0, scope, domainID, "")
	}
	workflowID := taskToken.GetWorkflowId()

	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.RespondActivityTaskCompleted(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return &historyservice.RespondActivityTaskCompletedResponse{}, nil
}

// RespondActivityTaskFailed - records failure of an activity task
func (h *Handler) RespondActivityTaskFailed(ctx context.Context, request *historyservice.RespondActivityTaskFailedRequest) (_ *historyservice.RespondActivityTaskFailedResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRespondActivityTaskFailedScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	failRequest := request.FailedRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(failRequest.TaskToken)
	if err0 != nil {
		return nil, h.error(errDeserializeTaskToken.MessageArgs(err0), scope, domainID, "")
	}

	err0 = validateTaskToken(taskToken)
	if err0 != nil {
		return nil, h.error(err0, scope, domainID, "")
	}
	workflowID := taskToken.GetWorkflowId()

	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.RespondActivityTaskFailed(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return &historyservice.RespondActivityTaskFailedResponse{}, nil
}

// RespondActivityTaskCanceled - records failure of an activity task
func (h *Handler) RespondActivityTaskCanceled(ctx context.Context, request *historyservice.RespondActivityTaskCanceledRequest) (_ *historyservice.RespondActivityTaskCanceledResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRespondActivityTaskCanceledScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	cancelRequest := request.CancelRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(cancelRequest.TaskToken)
	if err0 != nil {
		return nil, h.error(errDeserializeTaskToken.MessageArgs(err0), scope, domainID, "")
	}

	err0 = validateTaskToken(taskToken)
	if err0 != nil {
		return nil, h.error(err0, scope, domainID, "")
	}
	workflowID := taskToken.GetWorkflowId()

	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.RespondActivityTaskCanceled(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return &historyservice.RespondActivityTaskCanceledResponse{}, nil
}

// RespondDecisionTaskCompleted - records completion of a decision task
func (h *Handler) RespondDecisionTaskCompleted(ctx context.Context, request *historyservice.RespondDecisionTaskCompletedRequest) (_ *historyservice.RespondDecisionTaskCompletedResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRespondDecisionTaskCompletedScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	completeRequest := request.CompleteRequest
	if len(completeRequest.Decisions) == 0 {
		h.GetMetricsClient().IncCounter(scope, metrics.EmptyCompletionDecisionsCounter)
	}
	token, err0 := h.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err0 != nil {
		return nil, h.error(errDeserializeTaskToken.MessageArgs(err0), scope, domainID, "")
	}

	h.GetLogger().Debug("RespondDecisionTaskCompleted",
		tag.WorkflowDomainIDBytes(token.GetDomainId()),
		tag.WorkflowID(token.GetWorkflowId()),
		tag.WorkflowRunIDBytes(token.GetRunId()),
		tag.WorkflowScheduleID(token.GetScheduleId()))

	err0 = validateTaskToken(token)
	if err0 != nil {
		return nil, h.error(err0, scope, domainID, "")
	}
	workflowID := token.GetWorkflowId()

	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	response, err2 := engine.RespondDecisionTaskCompleted(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return response, nil
}

// RespondDecisionTaskFailed - failed response to decision task
func (h *Handler) RespondDecisionTaskFailed(ctx context.Context, request *historyservice.RespondDecisionTaskFailedRequest) (_ *historyservice.RespondDecisionTaskFailedResponse, retError error) {

	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRespondDecisionTaskFailedScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	failedRequest := request.FailedRequest
	token, err0 := h.tokenSerializer.Deserialize(failedRequest.TaskToken)
	if err0 != nil {
		return nil, h.error(errDeserializeTaskToken.MessageArgs(err0), scope, domainID, "")
	}

	h.GetLogger().Debug("RespondDecisionTaskFailed",
		tag.WorkflowDomainIDBytes(token.GetDomainId()),
		tag.WorkflowID(token.GetWorkflowId()),
		tag.WorkflowRunIDBytes(token.GetRunId()),
		tag.WorkflowScheduleID(token.GetScheduleId()))

	if failedRequest != nil && failedRequest.GetCause() == enums.DecisionTaskFailedCauseUnhandledDecision {
		h.GetLogger().Info("Non-Deterministic Error", tag.WorkflowDomainIDBytes(token.GetDomainId()), tag.WorkflowID(token.GetWorkflowId()), tag.WorkflowRunIDBytes(token.GetRunId()))
		domainName, err := h.GetDomainCache().GetDomainName(primitives.UUIDString(token.GetDomainId()))
		var domainTag metrics.Tag

		if err == nil {
			domainTag = metrics.DomainTag(domainName)
		} else {
			domainTag = metrics.DomainUnknownTag()
		}

		h.GetMetricsClient().Scope(scope, domainTag).IncCounter(metrics.ServiceErrNonDeterministicCounter)
	}
	err0 = validateTaskToken(token)
	if err0 != nil {
		return nil, h.error(err0, scope, domainID, "")
	}
	workflowID := token.GetWorkflowId()

	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.RespondDecisionTaskFailed(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return &historyservice.RespondDecisionTaskFailedResponse{}, nil
}

// StartWorkflowExecution - creates a new workflow execution
func (h *Handler) StartWorkflowExecution(ctx context.Context, request *historyservice.StartWorkflowExecutionRequest) (_ *historyservice.StartWorkflowExecutionResponse, retError error) {

	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryStartWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	startRequest := request.StartRequest
	workflowID := startRequest.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	response, err2 := engine.StartWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return response, nil
}

// DescribeHistoryHost returns information about the internal states of a history host
func (h *Handler) DescribeHistoryHost(_ context.Context, _ *historyservice.DescribeHistoryHostRequest) (_ *historyservice.DescribeHistoryHostResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
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

	resp := &historyservice.DescribeHistoryHostResponse{
		NumberOfShards: int32(h.controller.numShards()),
		ShardIDs:       h.controller.shardIDs(),
		DomainCache: &commonproto.DomainCacheInfo{
			NumOfItemsInCacheByID:   numOfItemsInCacheByID,
			NumOfItemsInCacheByName: numOfItemsInCacheByName,
		},
		ShardControllerStatus: status,
		Address:               h.GetHostInfo().GetAddress(),
	}
	return resp, nil
}

// RemoveTask returns information about the internal states of a history host
func (h *Handler) RemoveTask(_ context.Context, request *historyservice.RemoveTaskRequest) (_ *historyservice.RemoveTaskResponse, retError error) {
	executionMgr, err := h.GetExecutionManager(int(request.GetShardID()))
	if err != nil {
		return nil, err
	}
	deleteTaskRequest := &persistence.DeleteTaskRequest{
		TaskID:  request.GetTaskID(),
		Type:    int(request.GetType()),
		ShardID: int(request.GetShardID()),
	}
	err = executionMgr.DeleteTask(deleteTaskRequest)
	return &historyservice.RemoveTaskResponse{}, err
}

// CloseShard returns information about the internal states of a history host
func (h *Handler) CloseShard(_ context.Context, request *historyservice.CloseShardRequest) (_ *historyservice.CloseShardResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.controller.removeEngineForShard(int(request.GetShardID()))
	return &historyservice.CloseShardResponse{}, nil
}

// DescribeMutableState - returns the internal analysis of workflow execution state
func (h *Handler) DescribeMutableState(ctx context.Context, request *historyservice.DescribeMutableStateRequest) (_ *historyservice.DescribeMutableStateResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRecordActivityTaskHeartbeatScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
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
func (h *Handler) GetMutableState(ctx context.Context, request *historyservice.GetMutableStateRequest) (_ *historyservice.GetMutableStateResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryGetMutableStateScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	resp, err2 := engine.GetMutableState(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}
	return resp, nil
}

// PollMutableState - returns the id of the next event in the execution's history
func (h *Handler) PollMutableState(ctx context.Context, request *historyservice.PollMutableStateRequest) (_ *historyservice.PollMutableStateResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryPollMutableStateScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	resp, err2 := engine.PollMutableState(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}
	return resp, nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (h *Handler) DescribeWorkflowExecution(ctx context.Context, request *historyservice.DescribeWorkflowExecutionRequest) (_ *historyservice.DescribeWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryDescribeWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
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
func (h *Handler) RequestCancelWorkflowExecution(ctx context.Context, request *historyservice.RequestCancelWorkflowExecutionRequest) (_ *historyservice.RequestCancelWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRequestCancelWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" || request.CancelRequest.GetDomain() == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	cancelRequest := request.CancelRequest
	h.GetLogger().Debug("RequestCancelWorkflowExecution",
		tag.WorkflowDomainName(cancelRequest.GetDomain()),
		tag.WorkflowDomainID(request.GetDomainUUID()),
		tag.WorkflowID(cancelRequest.WorkflowExecution.GetWorkflowId()),
		tag.WorkflowRunID(cancelRequest.WorkflowExecution.GetRunId()))

	workflowID := cancelRequest.WorkflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.RequestCancelWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return &historyservice.RequestCancelWorkflowExecutionResponse{}, nil
}

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a decision task being created for the execution.
func (h *Handler) SignalWorkflowExecution(ctx context.Context, request *historyservice.SignalWorkflowExecutionRequest) (_ *historyservice.SignalWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistorySignalWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := request.SignalRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.SignalWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return &historyservice.SignalWorkflowExecutionResponse{}, nil
}

// SignalWithStartWorkflowExecution is used to ensure sending a signal event to a workflow execution.
// If workflow is running, this results in WorkflowExecutionSignaled event recorded in the history
// and a decision task being created for the execution.
// If workflow is not running or not found, this results in WorkflowExecutionStarted and WorkflowExecutionSignaled
// event recorded in history, and a decision task being created for the execution
func (h *Handler) SignalWithStartWorkflowExecution(ctx context.Context, request *historyservice.SignalWithStartWorkflowExecutionRequest) (_ *historyservice.SignalWithStartWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistorySignalWithStartWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	signalWithStartRequest := request.SignalWithStartRequest
	workflowID := signalWithStartRequest.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	resp, err2 := engine.SignalWithStartWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return resp, nil
}

// RemoveSignalMutableState is used to remove a signal request ID that was previously recorded.  This is currently
// used to clean execution info when signal decision finished.
func (h *Handler) RemoveSignalMutableState(ctx context.Context, request *historyservice.RemoveSignalMutableStateRequest) (_ *historyservice.RemoveSignalMutableStateResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRemoveSignalMutableStateScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.RemoveSignalMutableState(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return &historyservice.RemoveSignalMutableStateResponse{}, nil
}

// TerminateWorkflowExecution terminates an existing workflow execution by recording WorkflowExecutionTerminated event
// in the history and immediately terminating the execution instance.
func (h *Handler) TerminateWorkflowExecution(ctx context.Context, request *historyservice.TerminateWorkflowExecutionRequest) (_ *historyservice.TerminateWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryTerminateWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := request.TerminateRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.TerminateWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return &historyservice.TerminateWorkflowExecutionResponse{}, nil
}

// ResetWorkflowExecution reset an existing workflow execution
// in the history and immediately terminating the execution instance.
func (h *Handler) ResetWorkflowExecution(ctx context.Context, request *historyservice.ResetWorkflowExecutionRequest) (_ *historyservice.ResetWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryResetWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := request.ResetRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	resp, err2 := engine.ResetWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return resp, nil
}

// QueryWorkflow queries a workflow.
func (h *Handler) QueryWorkflow(ctx context.Context, request *historyservice.QueryWorkflowRequest) (_ *historyservice.QueryWorkflowResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryQueryWorkflowScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
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
func (h *Handler) ScheduleDecisionTask(ctx context.Context, request *historyservice.ScheduleDecisionTaskRequest) (_ *historyservice.ScheduleDecisionTaskResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryScheduleDecisionTaskScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	if request.WorkflowExecution == nil {
		return nil, h.error(errWorkflowExecutionNotSet, scope, domainID, "")
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.ScheduleDecisionTask(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return &historyservice.ScheduleDecisionTaskResponse{}, nil
}

// RecordChildExecutionCompleted is used for reporting the completion of child workflow execution to parent.
// This is mainly called by transfer queue processor during the processing of DeleteExecution task.
func (h *Handler) RecordChildExecutionCompleted(ctx context.Context, request *historyservice.RecordChildExecutionCompletedRequest) (_ *historyservice.RecordChildExecutionCompletedResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRecordChildExecutionCompletedScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	if request.WorkflowExecution == nil {
		return nil, h.error(errWorkflowExecutionNotSet, scope, domainID, "")
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.RecordChildExecutionCompleted(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return &historyservice.RecordChildExecutionCompletedResponse{}, nil
}

// ResetStickyTaskList reset the volatile information in mutable state of a given workflow.
// Volatile information are the information related to client, such as:
// 1. StickyTaskList
// 2. StickyScheduleToStartTimeout
// 3. ClientLibraryVersion
// 4. ClientFeatureVersion
// 5. ClientImpl
func (h *Handler) ResetStickyTaskList(ctx context.Context, request *historyservice.ResetStickyTaskListRequest) (_ *historyservice.ResetStickyTaskListResponse, retError error) {

	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryResetStickyTaskListScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowID := request.Execution.GetWorkflowId()
	engine, err := h.controller.GetEngine(workflowID)
	if err != nil {
		return nil, h.error(err, scope, domainID, workflowID)
	}

	resp, err := engine.ResetStickyTaskList(ctx, request)
	if err != nil {
		return nil, h.error(err, scope, domainID, workflowID)
	}

	return resp, nil
}

// ReplicateEvents is called by processor to replicate history events for passive domains
func (h *Handler) ReplicateEvents(ctx context.Context, request *historyservice.ReplicateEventsRequest) (_ *historyservice.ReplicateEventsResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryReplicateEventsScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.ReplicateEvents(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return &historyservice.ReplicateEventsResponse{}, nil
}

// ReplicateRawEvents is called by processor to replicate history raw events for passive domains
func (h *Handler) ReplicateRawEvents(ctx context.Context, request *historyservice.ReplicateRawEventsRequest) (_ *historyservice.ReplicateRawEventsResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryReplicateRawEventsScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.ReplicateRawEvents(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return &historyservice.ReplicateRawEventsResponse{}, nil
}

// ReplicateEventsV2 is called by processor to replicate history events for passive domains
func (h *Handler) ReplicateEventsV2(ctx context.Context, request *historyservice.ReplicateEventsV2Request) (_ *historyservice.ReplicateEventsV2Response, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryReplicateEventsV2Scope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	if domainID == "" {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, domainID, workflowID)
	}

	err2 := engine.ReplicateEventsV2(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, domainID, workflowID)
	}

	return &historyservice.ReplicateEventsV2Response{}, nil
}

// SyncShardStatus is called by processor to sync history shard information from another cluster
func (h *Handler) SyncShardStatus(ctx context.Context, request *historyservice.SyncShardStatusRequest) (_ *historyservice.SyncShardStatusResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistorySyncShardStatusScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, "", "")
	}

	if request.GetSourceCluster() == "" {
		return nil, h.error(errSourceClusterNotSet, scope, "", "")
	}

	if request.GetShardId() == 0 {
		return nil, h.error(errShardIDNotSet, scope, "", "")
	}

	if request.GetTimestamp() == 0 {
		return nil, h.error(errTimestampNotSet, scope, "", "")
	}

	// shard ID is already provided in the request
	engine, err := h.controller.getEngineForShard(int(request.GetShardId()))
	if err != nil {
		return nil, h.error(err, scope, "", "")
	}

	err = engine.SyncShardStatus(ctx, request)
	if err != nil {
		return nil, h.error(err, scope, "", "")
	}

	return &historyservice.SyncShardStatusResponse{}, nil
}

// SyncActivity is called by processor to sync activity
func (h *Handler) SyncActivity(ctx context.Context, request *historyservice.SyncActivityRequest) (_ *historyservice.SyncActivityResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistorySyncActivityScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainId()
	if request.GetDomainId() == "" || uuid.Parse(request.GetDomainId()) == nil {
		return nil, h.error(errDomainNotSet, scope, domainID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, domainID, "")
	}

	if request.GetWorkflowId() == "" {
		return nil, h.error(errWorkflowIDNotSet, scope, domainID, "")
	}

	if request.GetRunId() == "" || uuid.Parse(request.GetRunId()) == nil {
		return nil, h.error(errRunIDNotValid, scope, domainID, "")
	}

	workflowID := request.GetWorkflowId()
	engine, err := h.controller.GetEngine(workflowID)
	if err != nil {
		return nil, h.error(err, scope, domainID, workflowID)
	}

	err = engine.SyncActivity(ctx, request)
	if err != nil {
		return nil, h.error(err, scope, domainID, workflowID)
	}

	return &historyservice.SyncActivityResponse{}, nil
}

// GetReplicationMessages is called by remote peers to get replicated messages for cross DC replication
func (h *Handler) GetReplicationMessages(ctx context.Context, request *historyservice.GetReplicationMessagesRequest) (_ *historyservice.GetReplicationMessagesResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	h.GetLogger().Debug("Received GetReplicationMessages call.")

	scope := metrics.HistoryGetReplicationMessagesScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	var wg sync.WaitGroup
	wg.Add(len(request.Tokens))
	result := new(sync.Map)

	for _, token := range request.Tokens {
		go func(token *replication.ReplicationToken) {
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

	messagesByShard := make(map[int32]*replication.ReplicationMessages)
	result.Range(func(key, value interface{}) bool {
		shardID := key.(int32)
		tasks := value.(*replication.ReplicationMessages)
		messagesByShard[shardID] = tasks
		return true
	})

	h.GetLogger().Debug("GetReplicationMessages succeeded.")

	return &historyservice.GetReplicationMessagesResponse{MessagesByShard: messagesByShard}, nil
}

// GetDLQReplicationMessages is called by remote peers to get replicated messages for DLQ merging
func (h *Handler) GetDLQReplicationMessages(ctx context.Context, request *historyservice.GetDLQReplicationMessagesRequest) (_ *historyservice.GetDLQReplicationMessagesResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryGetDLQReplicationMessagesScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	taskInfoPerExecution := map[definition.WorkflowIdentifier][]*replication.ReplicationTaskInfo{}
	// do batch based on workflow ID and run ID
	for _, taskInfo := range request.GetTaskInfos() {
		identity := definition.NewWorkflowIdentifier(
			taskInfo.GetDomainId(),
			taskInfo.GetWorkflowId(),
			taskInfo.GetRunId(),
		)
		if _, ok := taskInfoPerExecution[identity]; !ok {
			taskInfoPerExecution[identity] = []*replication.ReplicationTaskInfo{}
		}
		taskInfoPerExecution[identity] = append(taskInfoPerExecution[identity], taskInfo)
	}

	var wg sync.WaitGroup
	wg.Add(len(taskInfoPerExecution))
	tasksChan := make(chan *replication.ReplicationTask, len(request.GetTaskInfos()))
	handleTaskInfoPerExecution := func(taskInfos []*replication.ReplicationTaskInfo) {
		defer wg.Done()
		if len(taskInfos) == 0 {
			return
		}

		engine, err := h.controller.GetEngine(
			taskInfos[0].GetWorkflowId(),
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

	replicationTasks := make([]*replication.ReplicationTask, len(tasksChan))
	for task := range tasksChan {
		replicationTasks = append(replicationTasks, task)
	}
	return &historyservice.GetDLQReplicationMessagesResponse{
		ReplicationTasks: replicationTasks,
	}, nil
}

// ReapplyEvents applies stale events to the current workflow and the current run
func (h *Handler) ReapplyEvents(ctx context.Context, request *historyservice.ReapplyEventsRequest) (_ *historyservice.ReapplyEventsResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryReapplyEventsScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	domainID := request.GetDomainUUID()
	workflowID := request.GetRequest().GetWorkflowExecution().GetWorkflowId()
	engine, err := h.controller.GetEngine(workflowID)
	if err != nil {
		return nil, h.error(err, scope, domainID, workflowID)
	}
	// deserialize history event object
	historyEvents, err := h.GetPayloadSerializer().DeserializeBatchEvents(&serialization.DataBlob{
		Encoding: common.EncodingTypeThriftRW,
		Data:     request.GetRequest().GetEvents().GetData(),
	})
	if err != nil {
		return nil, h.error(err, scope, domainID, workflowID)
	}

	execution := request.GetRequest().GetWorkflowExecution()
	if err := engine.ReapplyEvents(
		ctx,
		request.GetDomainUUID(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
		historyEvents,
	); err != nil {
		return nil, h.error(err, scope, domainID, workflowID)
	}
	return &historyservice.ReapplyEventsResponse{}, nil
}

func (h *Handler) ReadDLQMessages(ctx context.Context, request *historyservice.ReadDLQMessagesRequest) (_ *historyservice.ReadDLQMessagesResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)

	h.startWG.Wait()

	scope := metrics.HistoryReadDLQMessagesScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	engine, err := h.controller.getEngineForShard(int(request.GetShardID()))
	if err != nil {
		err = h.error(err, scope, "", "")
		return nil, err
	}

	resp, err := engine.ReadDLQMessages(ctx, request)
	if err != nil {
		err = h.error(err, scope, "", "")
		return nil, err
	}

	return resp, nil
}

func (h *Handler) PurgeDLQMessages(ctx context.Context, request *historyservice.PurgeDLQMessagesRequest) (_ *historyservice.PurgeDLQMessagesResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)

	h.startWG.Wait()

	scope := metrics.HistoryPurgeDLQMessagesScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	engine, err := h.controller.getEngineForShard(int(request.GetShardID()))
	if err != nil {
		err = h.error(err, scope, "", "")
		return nil, err
	}

	err = engine.PurgeDLQMessages(ctx, request)
	if err != nil {
		err = h.error(err, scope, "", "")
		return nil, err
	}
	return &historyservice.PurgeDLQMessagesResponse{}, nil
}

func (h *Handler) MergeDLQMessages(ctx context.Context, request *historyservice.MergeDLQMessagesRequest) (_ *historyservice.MergeDLQMessagesResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)

	h.startWG.Wait()

	scope := metrics.HistoryMergeDLQMessagesScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	engine, err := h.controller.getEngineForShard(int(request.GetShardID()))
	if err != nil {
		err = h.error(err, scope, "", "")
		return nil, err
	}

	resp, err := engine.MergeDLQMessages(ctx, request)
	if err != nil {
		err = h.error(err, scope, "", "")
		return nil, err
	}

	return resp, nil
}

func (h *Handler) RefreshWorkflowTasks(ctx context.Context, request *historyservice.RefreshWorkflowTasksRequest) (_ *historyservice.RefreshWorkflowTasksResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)

	h.startWG.Wait()

	scope := metrics.HistoryRefreshWorkflowTasksScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()
	domainID := request.GetDomainUUID()
	execution := request.GetRequest().GetExecution()
	workflowID := execution.GetWorkflowId()
	engine, err := h.controller.GetEngine(workflowID)
	if err != nil {
		err = h.error(err, scope, domainID, workflowID)
		return nil, err
	}

	err = engine.RefreshWorkflowTasks(
		ctx,
		domainID,
		commonproto.WorkflowExecution{
			WorkflowId: execution.WorkflowId,
			RunId:      execution.RunId,
		},
	)

	if err != nil {
		err = h.error(err, scope, domainID, workflowID)
		return nil, err
	}

	return &historyservice.RefreshWorkflowTasksResponse{}, nil
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
		return serviceerror.NewInternal(err.Msg)
	case *persistence.CurrentWorkflowConditionFailedError:
		err := err.(*persistence.CurrentWorkflowConditionFailedError)
		return serviceerror.NewInternal(err.Msg)
	case *persistence.TransactionSizeLimitError:
		err := err.(*persistence.TransactionSizeLimitError)
		return serviceerror.NewInvalidArgument(err.Msg)
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
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrContextTimeoutCounter)
		return
	}

	switch err := err.(type) {
	case *serviceerror.ShardOwnershipLost:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrShardOwnershipLostCounter)
	case *serviceerror.EventAlreadyStarted:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrEventAlreadyStartedCounter)
	case *serviceerror.InvalidArgument:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrInvalidArgumentCounter)
	case *serviceerror.DomainNotActive:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrInvalidArgumentCounter)
	case *serviceerror.WorkflowExecutionAlreadyStarted:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrExecutionAlreadyStartedCounter)
	case *serviceerror.NotFound:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrNotFoundCounter)
	case *serviceerror.CancellationAlreadyRequested:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrCancellationAlreadyRequestedCounter)
	case *serviceerror.ResourceExhausted:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrResourceExhaustedCounter)
	case *serviceerror.RetryTask:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrRetryTaskCounter)
	case *serviceerror.RetryTaskV2:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrRetryTaskCounter)
	case *serviceerror.DeadlineExceeded:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrContextTimeoutCounter)
	case *serviceerror.Internal:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceFailures)
		h.GetLogger().Error("Internal service error",
			tag.Error(err),
			tag.WorkflowID(workflowID),
			tag.WorkflowDomainID(domainID))
	default:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceFailures)
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
) *serviceerror.ShardOwnershipLost {

	return serviceerror.NewShardOwnershipLost(fmt.Sprintf("Shard is not owned by host: %v", currentHost), ownerHost)
}

func validateTaskToken(taskToken *token.Task) error {
	if taskToken.GetWorkflowId() == "" {
		return errWorkflowIDNotSet
	}
	return nil
}
