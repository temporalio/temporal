// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"sync"
	"sync/atomic"

	"go.temporal.io/server/common/convert"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	namespacespb "go.temporal.io/server/api/namespace/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/messaging"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resource"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/task"
)

type (

	// Handler - gRPC handler interface for historyservice
	Handler struct {
		resource.Resource

		shuttingDown            int32
		controller              *shardController
		tokenSerializer         common.TaskTokenSerializer
		startWG                 sync.WaitGroup
		config                  *Config
		historyEventNotifier    historyEventNotifier
		publisher               messaging.Producer
		rateLimiter             quotas.Limiter
		replicationTaskFetchers ReplicationTaskFetchers
		queueTaskProcessor      queueTaskProcessor
	}
)

const (
	serviceName = "temporal.api.workflowservice.v1.HistoryService"
)

var (
	_ EngineFactory                       = (*Handler)(nil)
	_ historyservice.HistoryServiceServer = (*Handler)(nil)

	errNamespaceNotSet         = serviceerror.NewInvalidArgument("Namespace not set on request.")
	errWorkflowExecutionNotSet = serviceerror.NewInvalidArgument("WorkflowExecution not set on request.")
	errTaskQueueNotSet         = serviceerror.NewInvalidArgument("Task queue not set.")
	errWorkflowIDNotSet        = serviceerror.NewInvalidArgument("WorkflowId is not set on request.")
	errRunIDNotValid           = serviceerror.NewInvalidArgument("RunId is not valid UUID.")
	errSourceClusterNotSet     = serviceerror.NewInvalidArgument("Source Cluster not set on request.")
	errShardIDNotSet           = serviceerror.NewInvalidArgument("ShardId not set on request.")
	errTimestampNotSet         = serviceerror.NewInvalidArgument("Timestamp not set on request.")
	errInvalidTaskType         = serviceerror.NewInvalidArgument("Invalid task type")
	errDeserializeTaskToken    = serviceerror.NewInvalidArgument("Error to deserialize task token. Error: %v.")

	errHistoryHostThrottle = serviceerror.NewResourceExhausted("History host RPS exceeded.")
	errShuttingDown        = serviceerror.NewInternal("Shutting down")
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
	if h.GetClusterMetadata().IsGlobalNamespaceEnabled() {
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
		h.GetClientBean(),
	)

	h.replicationTaskFetchers.Start()

	if h.config.EnablePriorityTaskProcessor() {
		var err error
		taskPriorityAssigner := newTaskPriorityAssigner(
			h.GetClusterMetadata().GetCurrentClusterName(),
			h.GetNamespaceCache(),
			h.GetLogger(),
			h.GetMetricsClient(),
			h.config,
		)

		schedulerType := task.SchedulerType(h.config.TaskSchedulerType())
		queueTaskProcessorOptions := &queueTaskProcessorOptions{
			schedulerType: schedulerType,
		}
		switch schedulerType {
		case task.SchedulerTypeFIFO:
			queueTaskProcessorOptions.fifoSchedulerOptions = &task.FIFOTaskSchedulerOptions{
				QueueSize:   h.config.TaskSchedulerQueueSize(),
				WorkerCount: h.config.TaskSchedulerWorkerCount(),
				RetryPolicy: common.CreatePersistanceRetryPolicy(),
			}
		case task.SchedulerTypeWRR:
			queueTaskProcessorOptions.wRRSchedulerOptions = &task.WeightedRoundRobinTaskSchedulerOptions{
				Weights:     h.config.TaskSchedulerRoundRobinWeights,
				QueueSize:   h.config.TaskSchedulerQueueSize(),
				WorkerCount: h.config.TaskSchedulerWorkerCount(),
				RetryPolicy: common.CreatePersistanceRetryPolicy(),
			}
		default:
			h.GetLogger().Fatal("Unknown task scheduler type", tag.Value(schedulerType))
		}
		h.queueTaskProcessor, err = newQueueTaskProcessor(
			taskPriorityAssigner,
			queueTaskProcessorOptions,
			h.GetLogger(),
			h.GetMetricsClient(),
		)
		if err != nil {
			h.GetLogger().Fatal("Creating priority task processor failed", tag.Error(err))
		}
		h.queueTaskProcessor.Start()
	}

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
	h.PrepareToStop()
	h.replicationTaskFetchers.Stop()
	if h.queueTaskProcessor != nil {
		h.queueTaskProcessor.Stop()
	}
	h.controller.Stop()
	h.historyEventNotifier.Stop()
}

// PrepareToStop starts graceful traffic drain in preparation for shutdown
func (h *Handler) PrepareToStop() {
	atomic.StoreInt32(&h.shuttingDown, 1)
}

func (h *Handler) isShuttingDown() bool {
	return atomic.LoadInt32(&h.shuttingDown) != 0
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
		h.queueTaskProcessor,
	)
}

// https://github.com/grpc/grpc/blob/master/doc/health-checking.md
func (h *Handler) Check(_ context.Context, request *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	h.GetLogger().Debug("History service health check endpoint (gRPC) reached.")

	h.startWG.Wait()

	if request.Service != serviceName {
		return &healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_SERVICE_UNKNOWN,
		}, nil
	}

	hs := &healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_SERVING,
	}
	return hs, nil
}
func (h *Handler) Watch(*healthpb.HealthCheckRequest, healthpb.Health_WatchServer) error {
	return serviceerror.NewUnimplemented("Watch is not implemented.")
}

// RecordActivityTaskHeartbeat - Record Activity Task Heart beat.
func (h *Handler) RecordActivityTaskHeartbeat(ctx context.Context, request *historyservice.RecordActivityTaskHeartbeatRequest) (_ *historyservice.RecordActivityTaskHeartbeatResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRecordActivityTaskHeartbeatScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	heartbeatRequest := request.HeartbeatRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(heartbeatRequest.TaskToken)
	if err0 != nil {
		return nil, h.error(errDeserializeTaskToken.MessageArgs(err0), scope, namespaceID, "")
	}

	err0 = validateTaskToken(taskToken)
	if err0 != nil {
		return nil, h.error(err0, scope, namespaceID, "")
	}
	workflowID := taskToken.GetWorkflowId()

	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	response, err2 := engine.RecordActivityTaskHeartbeat(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}

	return response, nil
}

// RecordActivityTaskStarted - Record Activity Task started.
func (h *Handler) RecordActivityTaskStarted(ctx context.Context, request *historyservice.RecordActivityTaskStartedRequest) (_ *historyservice.RecordActivityTaskStartedResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRecordActivityTaskStartedScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	namespaceID := request.GetNamespaceId()
	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	if request.GetNamespaceId() == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, workflowID)
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, workflowID)
	}

	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	response, err2 := engine.RecordActivityTaskStarted(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}

	return response, nil
}

// RecordWorkflowTaskStarted - Record Workflow Task started.
func (h *Handler) RecordWorkflowTaskStarted(ctx context.Context, request *historyservice.RecordWorkflowTaskStartedRequest) (_ *historyservice.RecordWorkflowTaskStartedResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()
	h.GetLogger().Debug("RecordWorkflowTaskStarted",
		tag.WorkflowNamespaceID(request.GetNamespaceId()),
		tag.WorkflowID(request.WorkflowExecution.GetWorkflowId()),
		tag.WorkflowRunID(request.WorkflowExecution.GetRunId()),
		tag.WorkflowScheduleID(request.GetScheduleId()))

	scope := metrics.HistoryRecordWorkflowTaskStartedScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	namespaceID := request.GetNamespaceId()
	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, workflowID)
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, workflowID)
	}

	if request.PollRequest == nil || request.PollRequest.TaskQueue.GetName() == "" {
		return nil, h.error(errTaskQueueNotSet, scope, namespaceID, workflowID)
	}

	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		h.GetLogger().Error("RecordWorkflowTaskStarted failed.",
			tag.Error(err1),
			tag.WorkflowID(request.WorkflowExecution.GetWorkflowId()),
			tag.WorkflowRunID(request.WorkflowExecution.GetRunId()),
			tag.WorkflowScheduleID(request.GetScheduleId()),
		)
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	response, err2 := engine.RecordWorkflowTaskStarted(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}

	return response, nil
}

// RespondActivityTaskCompleted - records completion of an activity task
func (h *Handler) RespondActivityTaskCompleted(ctx context.Context, request *historyservice.RespondActivityTaskCompletedRequest) (_ *historyservice.RespondActivityTaskCompletedResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRespondActivityTaskCompletedScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	completeRequest := request.CompleteRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err0 != nil {
		return nil, h.error(errDeserializeTaskToken.MessageArgs(err0), scope, namespaceID, "")
	}

	err0 = validateTaskToken(taskToken)
	if err0 != nil {
		return nil, h.error(err0, scope, namespaceID, "")
	}
	workflowID := taskToken.GetWorkflowId()

	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	err2 := engine.RespondActivityTaskCompleted(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}

	return &historyservice.RespondActivityTaskCompletedResponse{}, nil
}

// RespondActivityTaskFailed - records failure of an activity task
func (h *Handler) RespondActivityTaskFailed(ctx context.Context, request *historyservice.RespondActivityTaskFailedRequest) (_ *historyservice.RespondActivityTaskFailedResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRespondActivityTaskFailedScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	failRequest := request.FailedRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(failRequest.TaskToken)
	if err0 != nil {
		return nil, h.error(errDeserializeTaskToken.MessageArgs(err0), scope, namespaceID, "")
	}

	err0 = validateTaskToken(taskToken)
	if err0 != nil {
		return nil, h.error(err0, scope, namespaceID, "")
	}
	workflowID := taskToken.GetWorkflowId()

	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	err2 := engine.RespondActivityTaskFailed(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}

	return &historyservice.RespondActivityTaskFailedResponse{}, nil
}

// RespondActivityTaskCanceled - records failure of an activity task
func (h *Handler) RespondActivityTaskCanceled(ctx context.Context, request *historyservice.RespondActivityTaskCanceledRequest) (_ *historyservice.RespondActivityTaskCanceledResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRespondActivityTaskCanceledScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	cancelRequest := request.CancelRequest
	taskToken, err0 := h.tokenSerializer.Deserialize(cancelRequest.TaskToken)
	if err0 != nil {
		return nil, h.error(errDeserializeTaskToken.MessageArgs(err0), scope, namespaceID, "")
	}

	err0 = validateTaskToken(taskToken)
	if err0 != nil {
		return nil, h.error(err0, scope, namespaceID, "")
	}
	workflowID := taskToken.GetWorkflowId()

	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	err2 := engine.RespondActivityTaskCanceled(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}

	return &historyservice.RespondActivityTaskCanceledResponse{}, nil
}

// RespondWorkflowTaskCompleted - records completion of a workflow task
func (h *Handler) RespondWorkflowTaskCompleted(ctx context.Context, request *historyservice.RespondWorkflowTaskCompletedRequest) (_ *historyservice.RespondWorkflowTaskCompletedResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRespondWorkflowTaskCompletedScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	completeRequest := request.CompleteRequest
	if len(completeRequest.Commands) == 0 {
		h.GetMetricsClient().IncCounter(scope, metrics.EmptyCompletionCommandsCounter)
	}
	token, err0 := h.tokenSerializer.Deserialize(completeRequest.TaskToken)
	if err0 != nil {
		return nil, h.error(errDeserializeTaskToken.MessageArgs(err0), scope, namespaceID, "")
	}

	h.GetLogger().Debug("RespondWorkflowTaskCompleted",
		tag.WorkflowNamespaceID(token.GetNamespaceId()),
		tag.WorkflowID(token.GetWorkflowId()),
		tag.WorkflowRunID(token.GetRunId()),
		tag.WorkflowScheduleID(token.GetScheduleId()))

	err0 = validateTaskToken(token)
	if err0 != nil {
		return nil, h.error(err0, scope, namespaceID, "")
	}
	workflowID := token.GetWorkflowId()

	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	response, err2 := engine.RespondWorkflowTaskCompleted(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}

	return response, nil
}

// RespondWorkflowTaskFailed - failed response to workflow task
func (h *Handler) RespondWorkflowTaskFailed(ctx context.Context, request *historyservice.RespondWorkflowTaskFailedRequest) (_ *historyservice.RespondWorkflowTaskFailedResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRespondWorkflowTaskFailedScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	failedRequest := request.FailedRequest
	token, err0 := h.tokenSerializer.Deserialize(failedRequest.TaskToken)
	if err0 != nil {
		return nil, h.error(errDeserializeTaskToken.MessageArgs(err0), scope, namespaceID, "")
	}

	h.GetLogger().Debug("RespondWorkflowTaskFailed",
		tag.WorkflowNamespaceID(token.GetNamespaceId()),
		tag.WorkflowID(token.GetWorkflowId()),
		tag.WorkflowRunID(token.GetRunId()),
		tag.WorkflowScheduleID(token.GetScheduleId()))

	if failedRequest.GetCause() == enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND {
		h.GetLogger().Info("Non-Deterministic Error", tag.WorkflowNamespaceID(token.GetNamespaceId()), tag.WorkflowID(token.GetWorkflowId()), tag.WorkflowRunID(token.GetRunId()))
		namespace, err := h.GetNamespaceCache().GetNamespaceName(token.GetNamespaceId())
		var namespaceTag metrics.Tag

		if err == nil {
			namespaceTag = metrics.NamespaceTag(namespace)
		} else {
			namespaceTag = metrics.NamespaceUnknownTag()
		}

		h.GetMetricsClient().Scope(scope, namespaceTag).IncCounter(metrics.ServiceErrNonDeterministicCounter)
	}
	err0 = validateTaskToken(token)
	if err0 != nil {
		return nil, h.error(err0, scope, namespaceID, "")
	}
	workflowID := token.GetWorkflowId()

	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	err2 := engine.RespondWorkflowTaskFailed(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}

	return &historyservice.RespondWorkflowTaskFailedResponse{}, nil
}

// StartWorkflowExecution - creates a new workflow execution
func (h *Handler) StartWorkflowExecution(ctx context.Context, request *historyservice.StartWorkflowExecutionRequest) (_ *historyservice.StartWorkflowExecutionResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryStartWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	startRequest := request.StartRequest
	workflowID := startRequest.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	response, err2 := engine.StartWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}

	return response, nil
}

// DescribeHistoryHost returns information about the internal states of a history host
func (h *Handler) DescribeHistoryHost(_ context.Context, _ *historyservice.DescribeHistoryHostRequest) (_ *historyservice.DescribeHistoryHostResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	itemsInCacheByIDCount, itemsInCacheByNameCount := h.GetNamespaceCache().GetCacheSize()
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
		ShardsNumber: int32(h.controller.numShards()),
		ShardIds:     h.controller.shardIDs(),
		NamespaceCache: &namespacespb.NamespaceCacheInfo{
			ItemsInCacheByIdCount:   itemsInCacheByIDCount,
			ItemsInCacheByNameCount: itemsInCacheByNameCount,
		},
		ShardControllerStatus: status,
		Address:               h.GetHostInfo().GetAddress(),
	}
	return resp, nil
}

// RemoveTask returns information about the internal states of a history host
func (h *Handler) RemoveTask(_ context.Context, request *historyservice.RemoveTaskRequest) (_ *historyservice.RemoveTaskResponse, retError error) {
	executionMgr, err := h.GetExecutionManager(request.GetShardId())
	if err != nil {
		return nil, err
	}

	switch request.GetCategory() {
	case enumsspb.TASK_CATEGORY_TRANSFER:
		err = executionMgr.CompleteTransferTask(&persistence.CompleteTransferTaskRequest{
			TaskID: request.GetTaskId(),
		})
	case enumsspb.TASK_CATEGORY_TIMER:
		err = executionMgr.CompleteTimerTask(&persistence.CompleteTimerTaskRequest{
			VisibilityTimestamp: timestamp.TimeValue(request.GetVisibilityTime()),
			TaskID:              request.GetTaskId(),
		})
	case enumsspb.TASK_CATEGORY_REPLICATION:
		err = executionMgr.CompleteReplicationTask(&persistence.CompleteReplicationTaskRequest{
			TaskID: request.GetTaskId(),
		})
	default:
		err = errInvalidTaskType
	}

	return &historyservice.RemoveTaskResponse{}, err
}

// CloseShard closes a shard hosted by this instance
func (h *Handler) CloseShard(_ context.Context, request *historyservice.CloseShardRequest) (_ *historyservice.CloseShardResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.controller.removeEngineForShard(request.GetShardId(), nil)
	return &historyservice.CloseShardResponse{}, nil
}

// DescribeMutableState - returns the internal analysis of workflow execution state
func (h *Handler) DescribeMutableState(ctx context.Context, request *historyservice.DescribeMutableStateRequest) (_ *historyservice.DescribeMutableStateResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRecordActivityTaskHeartbeatScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	workflowExecution := request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	resp, err2 := engine.DescribeMutableState(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}
	return resp, nil
}

// GetMutableState - returns the id of the next event in the execution's history
func (h *Handler) GetMutableState(ctx context.Context, request *historyservice.GetMutableStateRequest) (_ *historyservice.GetMutableStateResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryGetMutableStateScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	workflowExecution := request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	resp, err2 := engine.GetMutableState(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}
	return resp, nil
}

// PollMutableState - returns the id of the next event in the execution's history
func (h *Handler) PollMutableState(ctx context.Context, request *historyservice.PollMutableStateRequest) (_ *historyservice.PollMutableStateResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryPollMutableStateScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	workflowExecution := request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	resp, err2 := engine.PollMutableState(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}
	return resp, nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (h *Handler) DescribeWorkflowExecution(ctx context.Context, request *historyservice.DescribeWorkflowExecutionRequest) (_ *historyservice.DescribeWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryDescribeWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	workflowExecution := request.Request.Execution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	resp, err2 := engine.DescribeWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}
	return resp, nil
}

// RequestCancelWorkflowExecution - requests cancellation of a workflow
func (h *Handler) RequestCancelWorkflowExecution(ctx context.Context, request *historyservice.RequestCancelWorkflowExecutionRequest) (_ *historyservice.RequestCancelWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRequestCancelWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" || request.CancelRequest.GetNamespace() == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	cancelRequest := request.CancelRequest
	h.GetLogger().Debug("RequestCancelWorkflowExecution",
		tag.WorkflowNamespace(cancelRequest.GetNamespace()),
		tag.WorkflowNamespaceID(request.GetNamespaceId()),
		tag.WorkflowID(cancelRequest.WorkflowExecution.GetWorkflowId()),
		tag.WorkflowRunID(cancelRequest.WorkflowExecution.GetRunId()))

	workflowID := cancelRequest.WorkflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	err2 := engine.RequestCancelWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}

	return &historyservice.RequestCancelWorkflowExecutionResponse{}, nil
}

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a workflow task being created for the execution.
func (h *Handler) SignalWorkflowExecution(ctx context.Context, request *historyservice.SignalWorkflowExecutionRequest) (_ *historyservice.SignalWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistorySignalWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	workflowExecution := request.SignalRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	err2 := engine.SignalWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}

	return &historyservice.SignalWorkflowExecutionResponse{}, nil
}

// SignalWithStartWorkflowExecution is used to ensure sending a signal event to a workflow execution.
// If workflow is running, this results in WorkflowExecutionSignaled event recorded in the history
// and a workflow task being created for the execution.
// If workflow is not running or not found, this results in WorkflowExecutionStarted and WorkflowExecutionSignaled
// event recorded in history, and a workflow task being created for the execution
func (h *Handler) SignalWithStartWorkflowExecution(ctx context.Context, request *historyservice.SignalWithStartWorkflowExecutionRequest) (_ *historyservice.SignalWithStartWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistorySignalWithStartWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	signalWithStartRequest := request.SignalWithStartRequest
	workflowID := signalWithStartRequest.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	for {
		resp, err2 := engine.SignalWithStartWorkflowExecution(ctx, request)
		if err2 == nil {
			return resp, nil
		}

		// Two simultaneous SignalWithStart requests might try to start a workflow at the same time.
		// This can result in one of the requests failing with one of two possible errors:
		//    1) If it is a brand new WF ID, one of the requests can fail with WorkflowExecutionAlreadyStartedError
		//       (createMode is persistence.CreateWorkflowModeBrandNew)
		//    2) If it an already existing WF ID, one of the requests can fail with a CurrentWorkflowConditionFailedError
		//       (createMode is persisetence.CreateWorkflowModeWorkflowIDReuse)
		// If either error occurs, just go ahead and retry. It should succeed on the subsequent attempt.
		// For simplicity, we keep trying unless the context finishes or we get an error that is not one of the
		// two mentioned above.
		_, isExecutionAlreadyStartedErr := err2.(*persistence.WorkflowExecutionAlreadyStartedError)
		_, isWorkflowConditionFailedErr := err2.(*persistence.CurrentWorkflowConditionFailedError)

		isContextDone := false
		select {
		case <-ctx.Done():
			isContextDone = true
			if ctxErr := ctx.Err(); ctxErr != nil {
				err2 = ctxErr
			}
		default:
		}

		if (!isExecutionAlreadyStartedErr && !isWorkflowConditionFailedErr) || isContextDone {
			return nil, h.error(err2, scope, namespaceID, workflowID)
		}
	}
}

// RemoveSignalMutableState is used to remove a signal request ID that was previously recorded.  This is currently
// used to clean execution info when signal workflow task finished.
func (h *Handler) RemoveSignalMutableState(ctx context.Context, request *historyservice.RemoveSignalMutableStateRequest) (_ *historyservice.RemoveSignalMutableStateResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRemoveSignalMutableStateScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	err2 := engine.RemoveSignalMutableState(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}

	return &historyservice.RemoveSignalMutableStateResponse{}, nil
}

// TerminateWorkflowExecution terminates an existing workflow execution by recording WorkflowExecutionTerminated event
// in the history and immediately terminating the execution instance.
func (h *Handler) TerminateWorkflowExecution(ctx context.Context, request *historyservice.TerminateWorkflowExecutionRequest) (_ *historyservice.TerminateWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryTerminateWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	workflowExecution := request.TerminateRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	err2 := engine.TerminateWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}

	return &historyservice.TerminateWorkflowExecutionResponse{}, nil
}

// ResetWorkflowExecution reset an existing workflow execution
// in the history and immediately terminating the execution instance.
func (h *Handler) ResetWorkflowExecution(ctx context.Context, request *historyservice.ResetWorkflowExecutionRequest) (_ *historyservice.ResetWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryResetWorkflowExecutionScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	workflowExecution := request.ResetRequest.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	resp, err2 := engine.ResetWorkflowExecution(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}

	return resp, nil
}

// QueryWorkflow queries a workflow.
func (h *Handler) QueryWorkflow(ctx context.Context, request *historyservice.QueryWorkflowRequest) (_ *historyservice.QueryWorkflowResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryQueryWorkflowScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	workflowID := request.GetRequest().GetExecution().GetWorkflowId()
	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	resp, err2 := engine.QueryWorkflow(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}

	return resp, nil
}

// ScheduleWorkflowTask is used for creating a workflow task for already started workflow execution.  This is mainly
// used by transfer queue processor during the processing of StartChildWorkflowExecution task, where it first starts
// child execution without creating the workflow task and then calls this API after updating the mutable state of
// parent execution.
func (h *Handler) ScheduleWorkflowTask(ctx context.Context, request *historyservice.ScheduleWorkflowTaskRequest) (_ *historyservice.ScheduleWorkflowTaskResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryScheduleWorkflowTaskScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	if request.WorkflowExecution == nil {
		return nil, h.error(errWorkflowExecutionNotSet, scope, namespaceID, "")
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	err2 := engine.ScheduleWorkflowTask(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}

	return &historyservice.ScheduleWorkflowTaskResponse{}, nil
}

// RecordChildExecutionCompleted is used for reporting the completion of child workflow execution to parent.
// This is mainly called by transfer queue processor during the processing of DeleteExecution task.
func (h *Handler) RecordChildExecutionCompleted(ctx context.Context, request *historyservice.RecordChildExecutionCompletedRequest) (_ *historyservice.RecordChildExecutionCompletedResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryRecordChildExecutionCompletedScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	if request.WorkflowExecution == nil {
		return nil, h.error(errWorkflowExecutionNotSet, scope, namespaceID, "")
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	err2 := engine.RecordChildExecutionCompleted(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}

	return &historyservice.RecordChildExecutionCompletedResponse{}, nil
}

// ResetStickyTaskQueue reset the volatile information in mutable state of a given workflow.
// Volatile information are the information related to client, such as:
// 1. StickyTaskQueue
// 2. StickyScheduleToStartTimeout
func (h *Handler) ResetStickyTaskQueue(ctx context.Context, request *historyservice.ResetStickyTaskQueueRequest) (_ *historyservice.ResetStickyTaskQueueResponse, retError error) {

	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryResetStickyTaskQueueScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	workflowID := request.Execution.GetWorkflowId()
	engine, err := h.controller.GetEngine(namespaceID, workflowID)
	if err != nil {
		return nil, h.error(err, scope, namespaceID, workflowID)
	}

	resp, err := engine.ResetStickyTaskQueue(ctx, request)
	if err != nil {
		return nil, h.error(err, scope, namespaceID, workflowID)
	}

	return resp, nil
}

// ReplicateEventsV2 is called by processor to replicate history events for passive namespaces
func (h *Handler) ReplicateEventsV2(ctx context.Context, request *historyservice.ReplicateEventsV2Request) (_ *historyservice.ReplicateEventsV2Response, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	scope := metrics.HistoryReplicateEventsV2Scope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	namespaceID := request.GetNamespaceId()
	if namespaceID == "" {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	workflowExecution := request.WorkflowExecution
	workflowID := workflowExecution.GetWorkflowId()
	engine, err1 := h.controller.GetEngine(namespaceID, workflowID)
	if err1 != nil {
		return nil, h.error(err1, scope, namespaceID, workflowID)
	}

	err2 := engine.ReplicateEventsV2(ctx, request)
	if err2 != nil {
		return nil, h.error(err2, scope, namespaceID, workflowID)
	}

	return &historyservice.ReplicateEventsV2Response{}, nil
}

// SyncShardStatus is called by processor to sync history shard information from another cluster
func (h *Handler) SyncShardStatus(ctx context.Context, request *historyservice.SyncShardStatusRequest) (_ *historyservice.SyncShardStatusResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistorySyncShardStatusScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, "", "")
	}

	if request.GetSourceCluster() == "" {
		return nil, h.error(errSourceClusterNotSet, scope, "", "")
	}

	if request.GetShardId() == 0 {
		return nil, h.error(errShardIDNotSet, scope, "", "")
	}

	if timestamp.TimeValue(request.GetStatusTime()).IsZero() {
		return nil, h.error(errTimestampNotSet, scope, "", "")
	}

	// shard ID is already provided in the request
	engine, err := h.controller.getEngineForShard(int32(request.GetShardId()))
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
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistorySyncActivityScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	namespaceID := request.GetNamespaceId()
	if request.GetNamespaceId() == "" || uuid.Parse(request.GetNamespaceId()) == nil {
		return nil, h.error(errNamespaceNotSet, scope, namespaceID, "")
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.error(errHistoryHostThrottle, scope, namespaceID, "")
	}

	if request.GetWorkflowId() == "" {
		return nil, h.error(errWorkflowIDNotSet, scope, namespaceID, "")
	}

	if request.GetRunId() == "" || uuid.Parse(request.GetRunId()) == nil {
		return nil, h.error(errRunIDNotValid, scope, namespaceID, "")
	}

	workflowID := request.GetWorkflowId()
	engine, err := h.controller.GetEngine(namespaceID, workflowID)
	if err != nil {
		return nil, h.error(err, scope, namespaceID, workflowID)
	}

	err = engine.SyncActivity(ctx, request)
	if err != nil {
		return nil, h.error(err, scope, namespaceID, workflowID)
	}

	return &historyservice.SyncActivityResponse{}, nil
}

// GetReplicationMessages is called by remote peers to get replicated messages for cross DC replication
func (h *Handler) GetReplicationMessages(ctx context.Context, request *historyservice.GetReplicationMessagesRequest) (_ *historyservice.GetReplicationMessagesResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	h.GetLogger().Debug("Received GetReplicationMessages call.")

	scope := metrics.HistoryGetReplicationMessagesScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	var wg sync.WaitGroup
	wg.Add(len(request.Tokens))
	result := new(sync.Map)

	for _, token := range request.Tokens {
		go func(token *replicationspb.ReplicationToken) {
			defer wg.Done()

			engine, err := h.controller.getEngineForShard(token.GetShardId())
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

			result.Store(token.GetShardId(), tasks)
		}(token)
	}

	wg.Wait()

	messagesByShard := make(map[int32]*replicationspb.ReplicationMessages)
	result.Range(func(key, value interface{}) bool {
		shardID := key.(int32)
		tasks := value.(*replicationspb.ReplicationMessages)
		messagesByShard[shardID] = tasks
		return true
	})

	h.GetLogger().Debug("GetReplicationMessages succeeded.")

	return &historyservice.GetReplicationMessagesResponse{ShardMessages: messagesByShard}, nil
}

// GetDLQReplicationMessages is called by remote peers to get replicated messages for DLQ merging
func (h *Handler) GetDLQReplicationMessages(ctx context.Context, request *historyservice.GetDLQReplicationMessagesRequest) (_ *historyservice.GetDLQReplicationMessagesResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryGetDLQReplicationMessagesScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	taskInfoPerExecution := map[definition.WorkflowIdentifier][]*replicationspb.ReplicationTaskInfo{}
	// do batch based on workflow ID and run ID
	for _, taskInfo := range request.GetTaskInfos() {
		identity := definition.NewWorkflowIdentifier(
			taskInfo.GetNamespaceId(),
			taskInfo.GetWorkflowId(),
			taskInfo.GetRunId(),
		)
		if _, ok := taskInfoPerExecution[identity]; !ok {
			taskInfoPerExecution[identity] = []*replicationspb.ReplicationTaskInfo{}
		}
		taskInfoPerExecution[identity] = append(taskInfoPerExecution[identity], taskInfo)
	}

	var wg sync.WaitGroup
	wg.Add(len(taskInfoPerExecution))
	tasksChan := make(chan *replicationspb.ReplicationTask, len(request.GetTaskInfos()))
	handleTaskInfoPerExecution := func(taskInfos []*replicationspb.ReplicationTaskInfo) {
		defer wg.Done()
		if len(taskInfos) == 0 {
			return
		}

		engine, err := h.controller.GetEngine(
			taskInfos[0].GetNamespaceId(),
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

	replicationTasks := make([]*replicationspb.ReplicationTask, len(tasksChan))
	for task := range tasksChan {
		replicationTasks = append(replicationTasks, task)
	}
	return &historyservice.GetDLQReplicationMessagesResponse{
		ReplicationTasks: replicationTasks,
	}, nil
}

// ReapplyEvents applies stale events to the current workflow and the current run
func (h *Handler) ReapplyEvents(ctx context.Context, request *historyservice.ReapplyEventsRequest) (_ *historyservice.ReapplyEventsResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	h.startWG.Wait()

	scope := metrics.HistoryReapplyEventsScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	namespaceID := request.GetNamespaceId()
	workflowID := request.GetRequest().GetWorkflowExecution().GetWorkflowId()
	engine, err := h.controller.GetEngine(namespaceID, workflowID)
	if err != nil {
		return nil, h.error(err, scope, namespaceID, workflowID)
	}
	// deserialize history event object
	historyEvents, err := h.GetPayloadSerializer().DeserializeBatchEvents(&serialization.DataBlob{
		Encoding: enumspb.ENCODING_TYPE_PROTO3,
		Data:     request.GetRequest().GetEvents().GetData(),
	})
	if err != nil {
		return nil, h.error(err, scope, namespaceID, workflowID)
	}

	execution := request.GetRequest().GetWorkflowExecution()
	if err := engine.ReapplyEvents(
		ctx,
		request.GetNamespaceId(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
		historyEvents,
	); err != nil {
		return nil, h.error(err, scope, namespaceID, workflowID)
	}
	return &historyservice.ReapplyEventsResponse{}, nil
}

func (h *Handler) GetDLQMessages(ctx context.Context, request *historyservice.GetDLQMessagesRequest) (_ *historyservice.GetDLQMessagesResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)

	h.startWG.Wait()

	scope := metrics.HistoryReadDLQMessagesScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	engine, err := h.controller.getEngineForShard(request.GetShardId())
	if err != nil {
		err = h.error(err, scope, "", "")
		return nil, err
	}

	resp, err := engine.GetDLQMessages(ctx, request)
	if err != nil {
		err = h.error(err, scope, "", "")
		return nil, err
	}

	return resp, nil
}

func (h *Handler) PurgeDLQMessages(ctx context.Context, request *historyservice.PurgeDLQMessagesRequest) (_ *historyservice.PurgeDLQMessagesResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)

	h.startWG.Wait()

	scope := metrics.HistoryPurgeDLQMessagesScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	engine, err := h.controller.getEngineForShard(request.GetShardId())
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
	defer log.CapturePanic(h.GetLogger(), &retError)

	h.startWG.Wait()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	scope := metrics.HistoryMergeDLQMessagesScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	engine, err := h.controller.getEngineForShard(request.GetShardId())
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
	defer log.CapturePanic(h.GetLogger(), &retError)

	h.startWG.Wait()

	scope := metrics.HistoryRefreshWorkflowTasksScope
	h.GetMetricsClient().IncCounter(scope, metrics.ServiceRequests)
	sw := h.GetMetricsClient().StartTimer(scope, metrics.ServiceLatency)
	defer sw.Stop()

	if h.isShuttingDown() {
		return nil, errShuttingDown
	}

	namespaceID := request.GetNamespaceId()
	execution := request.GetRequest().GetExecution()
	workflowID := execution.GetWorkflowId()
	engine, err := h.controller.GetEngine(namespaceID, workflowID)
	if err != nil {
		err = h.error(err, scope, namespaceID, workflowID)
		return nil, err
	}

	err = engine.RefreshWorkflowTasks(
		ctx,
		namespaceID,
		commonpb.WorkflowExecution{
			WorkflowId: execution.WorkflowId,
			RunId:      execution.RunId,
		},
	)

	if err != nil {
		err = h.error(err, scope, namespaceID, workflowID)
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
		info, err := h.GetHistoryServiceResolver().Lookup(convert.Int32ToString(shardID))
		if err == nil {
			return serviceerrors.NewShardOwnershipLost(h.GetHostInfo().GetAddress(), info.GetAddress())
		}
		return serviceerrors.NewShardOwnershipLost(h.GetHostInfo().GetAddress(), "<unknown>")
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
	namespaceID string,
	workflowID string,
	err error,
) {

	if common.IsContextTimeoutErr(err) {
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrContextTimeoutCounter)
		return
	}

	switch err := err.(type) {
	case *serviceerrors.ShardOwnershipLost:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrShardOwnershipLostCounter)
	case *serviceerrors.TaskAlreadyStarted:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrTaskAlreadyStartedCounter)
	case *serviceerror.InvalidArgument:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrInvalidArgumentCounter)
	case *serviceerror.NamespaceNotActive:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrInvalidArgumentCounter)
	case *serviceerror.WorkflowExecutionAlreadyStarted:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrExecutionAlreadyStartedCounter)
	case *serviceerror.NotFound:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrNotFoundCounter)
	case *serviceerror.CancellationAlreadyRequested:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrCancellationAlreadyRequestedCounter)
	case *serviceerror.ResourceExhausted:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrResourceExhaustedCounter)
	case *serviceerrors.RetryTask:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrRetryTaskCounter)
	case *serviceerrors.RetryTaskV2:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceErrRetryTaskCounter)
	case *serviceerror.Internal:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceFailures)
		h.GetLogger().Error("Internal service error",
			tag.Error(err),
			tag.WorkflowID(workflowID),
			tag.WorkflowNamespaceID(namespaceID))
	default:
		h.GetMetricsClient().IncCounter(scope, metrics.ServiceFailures)
		h.getLoggerWithTags(namespaceID, workflowID).Error("Uncategorized error", tag.Error(err))
	}
}

func (h *Handler) error(
	err error,
	scope int,
	namespaceID string,
	workflowID string,
) error {

	err = h.convertError(err)
	h.updateErrorMetric(scope, namespaceID, workflowID, err)

	return err
}

func (h *Handler) getLoggerWithTags(
	namespaceID string,
	workflowID string,
) log.Logger {

	logger := h.GetLogger()
	if namespaceID != "" {
		logger = logger.WithTags(tag.WorkflowNamespaceID(namespaceID))
	}

	if workflowID != "" {
		logger = logger.WithTags(tag.WorkflowID(workflowID))
	}

	return logger
}

func validateTaskToken(taskToken *tokenspb.Task) error {
	if taskToken.GetWorkflowId() == "" {
		return errWorkflowIDNotSet
	}
	return nil
}
