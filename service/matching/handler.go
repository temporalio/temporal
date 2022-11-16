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

package matching

import (
	"context"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
)

type (
	// Handler - gRPC handler interface for matchingservice
	Handler struct {
		engine            Engine
		config            *Config
		metricsHandler    metrics.MetricsHandler
		logger            log.Logger
		startWG           sync.WaitGroup
		throttledLogger   log.Logger
		namespaceRegistry namespace.Registry
	}
)

const (
	serviceName = "temporal.api.workflowservice.v1.MatchingService"
)

var (
	_ matchingservice.MatchingServiceServer = (*Handler)(nil)
)

// NewHandler creates a gRPC handler for the matchingservice
func NewHandler(
	config *Config,
	logger log.Logger,
	throttledLogger log.Logger,
	taskManager persistence.TaskManager,
	historyClient historyservice.HistoryServiceClient,
	matchingRawClient matchingservice.MatchingServiceClient,
	matchingServiceResolver membership.ServiceResolver,
	metricsHandler metrics.MetricsHandler,
	namespaceRegistry namespace.Registry,
	clusterMetadata cluster.Metadata,
) *Handler {
	handler := &Handler{
		config:          config,
		metricsHandler:  metricsHandler,
		logger:          logger,
		throttledLogger: throttledLogger,
		engine: NewEngine(
			taskManager,
			historyClient,
			matchingRawClient, // Use non retry client inside matching
			config,
			logger,
			metricsHandler,
			namespaceRegistry,
			matchingServiceResolver,
			clusterMetadata,
		),
		namespaceRegistry: namespaceRegistry,
	}

	// prevent from serving requests before matching engine is started and ready
	handler.startWG.Add(1)

	return handler
}

// Start starts the handler
func (h *Handler) Start() {
	h.startWG.Done()
}

// Stop stops the handler
func (h *Handler) Stop() {
	h.engine.Stop()
}

func (h *Handler) newHandlerContext(
	ctx context.Context,
	namespaceID namespace.ID,
	taskQueue *taskqueuepb.TaskQueue,
	operation string,
) *handlerContext {
	return newHandlerContext(
		ctx,
		h.namespaceName(namespaceID),
		taskQueue,
		h.metricsHandler,
		operation,
		h.logger,
	)
}

// AddActivityTask - adds an activity task.
func (h *Handler) AddActivityTask(
	ctx context.Context,
	request *matchingservice.AddActivityTaskRequest,
) (_ *matchingservice.AddActivityTaskResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	startT := time.Now().UTC()
	hCtx := h.newHandlerContext(
		ctx,
		namespace.ID(request.GetNamespaceId()),
		request.GetTaskQueue(),
		metrics.MatchingAddActivityTaskScope,
	)

	if request.GetForwardedSource() != "" {
		h.reportForwardedPerTaskQueueCounter(hCtx, namespace.ID(request.GetNamespaceId()))
	}

	syncMatch, err := h.engine.AddActivityTask(hCtx, request)
	if syncMatch {
		hCtx.metricsHandler.Timer(metrics.SyncMatchLatencyPerTaskQueue.GetMetricName()).Record(time.Since(startT))
	}

	return &matchingservice.AddActivityTaskResponse{}, err
}

// AddWorkflowTask - adds a workflow task.
func (h *Handler) AddWorkflowTask(
	ctx context.Context,
	request *matchingservice.AddWorkflowTaskRequest,
) (_ *matchingservice.AddWorkflowTaskResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	startT := time.Now().UTC()
	hCtx := h.newHandlerContext(
		ctx,
		namespace.ID(request.GetNamespaceId()),
		request.GetTaskQueue(),
		metrics.MatchingAddWorkflowTaskScope,
	)

	if request.GetForwardedSource() != "" {
		h.reportForwardedPerTaskQueueCounter(hCtx, namespace.ID(request.GetNamespaceId()))
	}

	syncMatch, err := h.engine.AddWorkflowTask(hCtx, request)
	if syncMatch {
		hCtx.metricsHandler.Timer(metrics.SyncMatchLatencyPerTaskQueue.GetMetricName()).Record(time.Since(startT))
	}
	return &matchingservice.AddWorkflowTaskResponse{}, err
}

// PollActivityTaskQueue - long poll for an activity task.
func (h *Handler) PollActivityTaskQueue(
	ctx context.Context,
	request *matchingservice.PollActivityTaskQueueRequest,
) (_ *matchingservice.PollActivityTaskQueueResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	hCtx := h.newHandlerContext(
		ctx,
		namespace.ID(request.GetNamespaceId()),
		request.GetPollRequest().GetTaskQueue(),
		metrics.MatchingPollActivityTaskQueueScope,
	)

	if request.GetForwardedSource() != "" {
		h.reportForwardedPerTaskQueueCounter(hCtx, namespace.ID(request.GetNamespaceId()))
	}

	if _, err := common.ValidateLongPollContextTimeoutIsSet(
		ctx,
		"PollActivityTaskQueue",
		h.throttledLogger,
	); err != nil {
		return nil, err
	}

	response, err := h.engine.PollActivityTaskQueue(hCtx, request)
	return response, err
}

// PollWorkflowTaskQueue - long poll for a workflow task.
func (h *Handler) PollWorkflowTaskQueue(
	ctx context.Context,
	request *matchingservice.PollWorkflowTaskQueueRequest,
) (_ *matchingservice.PollWorkflowTaskQueueResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	hCtx := h.newHandlerContext(
		ctx,
		namespace.ID(request.GetNamespaceId()),
		request.GetPollRequest().GetTaskQueue(),
		metrics.MatchingPollWorkflowTaskQueueScope,
	)

	if request.GetForwardedSource() != "" {
		h.reportForwardedPerTaskQueueCounter(hCtx, namespace.ID(request.GetNamespaceId()))
	}

	if _, err := common.ValidateLongPollContextTimeoutIsSet(
		ctx,
		"PollWorkflowTaskQueue",
		h.throttledLogger,
	); err != nil {
		return nil, err
	}

	response, err := h.engine.PollWorkflowTaskQueue(hCtx, request)
	return response, err
}

// QueryWorkflow queries a given workflow synchronously and return the query result.
func (h *Handler) QueryWorkflow(
	ctx context.Context,
	request *matchingservice.QueryWorkflowRequest,
) (_ *matchingservice.QueryWorkflowResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	hCtx := h.newHandlerContext(
		ctx,
		namespace.ID(request.GetNamespaceId()),
		request.GetTaskQueue(),
		metrics.MatchingQueryWorkflowScope,
	)

	if request.GetForwardedSource() != "" {
		h.reportForwardedPerTaskQueueCounter(hCtx, namespace.ID(request.GetNamespaceId()))
	}

	response, err := h.engine.QueryWorkflow(hCtx, request)
	return response, err
}

// RespondQueryTaskCompleted responds a query task completed
func (h *Handler) RespondQueryTaskCompleted(
	ctx context.Context,
	request *matchingservice.RespondQueryTaskCompletedRequest,
) (_ *matchingservice.RespondQueryTaskCompletedResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	hCtx := h.newHandlerContext(
		ctx,
		namespace.ID(request.GetNamespaceId()),
		request.GetTaskQueue(),
		metrics.MatchingRespondQueryTaskCompletedScope,
	)

	err := h.engine.RespondQueryTaskCompleted(hCtx, request)
	return &matchingservice.RespondQueryTaskCompletedResponse{}, err
}

// CancelOutstandingPoll is used to cancel outstanding pollers
func (h *Handler) CancelOutstandingPoll(ctx context.Context,
	request *matchingservice.CancelOutstandingPollRequest) (_ *matchingservice.CancelOutstandingPollResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	hCtx := h.newHandlerContext(
		ctx,
		namespace.ID(request.GetNamespaceId()),
		request.GetTaskQueue(),
		metrics.MatchingCancelOutstandingPollScope,
	)

	err := h.engine.CancelOutstandingPoll(hCtx, request)
	return &matchingservice.CancelOutstandingPollResponse{}, err
}

// DescribeTaskQueue returns information about the target task queue, right now this API returns the
// pollers which polled this task queue in last few minutes. If includeTaskQueueStatus field is true,
// it will also return status of task queue's ackManager (readLevel, ackLevel, backlogCountHint and taskIDBlock).
func (h *Handler) DescribeTaskQueue(
	ctx context.Context,
	request *matchingservice.DescribeTaskQueueRequest,
) (_ *matchingservice.DescribeTaskQueueResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	hCtx := h.newHandlerContext(
		ctx,
		namespace.ID(request.GetNamespaceId()),
		request.GetDescRequest().GetTaskQueue(),
		metrics.MatchingDescribeTaskQueueScope,
	)

	response, err := h.engine.DescribeTaskQueue(hCtx, request)
	return response, err
}

// ListTaskQueuePartitions returns information about partitions for a taskQueue
func (h *Handler) ListTaskQueuePartitions(
	ctx context.Context,
	request *matchingservice.ListTaskQueuePartitionsRequest,
) (_ *matchingservice.ListTaskQueuePartitionsResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	hCtx := newHandlerContext(
		ctx,
		namespace.Name(request.GetNamespace()),
		request.GetTaskQueue(),
		h.metricsHandler,
		metrics.MatchingListTaskQueuePartitionsScope,
		h.logger,
	)

	response, err := h.engine.ListTaskQueuePartitions(hCtx, request)
	return response, err
}

// UpdateWorkerBuildIdOrdering allows changing the worker versioning graph for a task queue
func (h *Handler) UpdateWorkerBuildIdOrdering(
	ctx context.Context,
	request *matchingservice.UpdateWorkerBuildIdOrderingRequest,
) (_ *matchingservice.UpdateWorkerBuildIdOrderingResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	hCtx := h.newHandlerContext(
		ctx,
		namespace.ID(request.GetNamespaceId()),
		&taskqueuepb.TaskQueue{
			Name: request.Request.GetTaskQueue(),
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		metrics.MatchingUpdateWorkerBuildIdOrderingScope,
	)

	return h.engine.UpdateWorkerBuildIdOrdering(hCtx, request)
}

// GetWorkerBuildIdOrdering fetches the worker versioning graph for a task queue
func (h *Handler) GetWorkerBuildIdOrdering(
	ctx context.Context,
	request *matchingservice.GetWorkerBuildIdOrderingRequest,
) (_ *matchingservice.GetWorkerBuildIdOrderingResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	hCtx := h.newHandlerContext(
		ctx,
		namespace.ID(request.GetNamespaceId()),
		&taskqueuepb.TaskQueue{
			Name: request.Request.GetTaskQueue(),
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		metrics.MatchingGetWorkerBuildIdOrderingScope,
	)

	return h.engine.GetWorkerBuildIdOrdering(hCtx, request)
}

// InvalidateTaskQueueMetadata notifies a task queue that some data has changed, and should be invalidated/refreshed
func (h *Handler) InvalidateTaskQueueMetadata(
	ctx context.Context,
	request *matchingservice.InvalidateTaskQueueMetadataRequest,
) (_ *matchingservice.InvalidateTaskQueueMetadataResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	hCtx := h.newHandlerContext(
		ctx,
		namespace.ID(request.GetNamespaceId()),
		&taskqueuepb.TaskQueue{
			Name: request.GetTaskQueue(),
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		metrics.MatchingInvalidateTaskQueueMetadataScope,
	)

	return h.engine.InvalidateTaskQueueMetadata(hCtx, request)
}

func (h *Handler) GetTaskQueueMetadata(
	ctx context.Context,
	request *matchingservice.GetTaskQueueMetadataRequest,
) (_ *matchingservice.GetTaskQueueMetadataResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	hCtx := h.newHandlerContext(
		ctx,
		namespace.ID(request.GetNamespaceId()),
		&taskqueuepb.TaskQueue{
			Name: request.GetTaskQueue(),
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		metrics.MatchingGetTaskQueueMetadataScope,
	)

	return h.engine.GetTaskQueueMetadata(hCtx, request)
}

func (h *Handler) namespaceName(id namespace.ID) namespace.Name {
	entry, err := h.namespaceRegistry.GetNamespaceByID(id)
	if err != nil {
		return ""
	}
	return entry.Name()
}

func (h *Handler) reportForwardedPerTaskQueueCounter(hCtx *handlerContext, namespaceId namespace.ID) {
	hCtx.metricsHandler.Counter(metrics.ForwardedPerTaskQueueCounter.GetMetricName()).Record(1)
	h.metricsHandler.Counter(metrics.MatchingClientForwardedCounter.GetMetricName()).
		Record(
			1,
			metrics.OperationTag(metrics.MatchingAddWorkflowTaskScope),
			metrics.NamespaceTag(h.namespaceName(namespaceId).String()),
			metrics.ServiceRoleTag(metrics.MatchingRoleTagValue))
}
