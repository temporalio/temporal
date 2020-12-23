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

	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resource"
)

type (
	// Handler - gRPC handler interface for matchingservice
	Handler struct {
		resource.Resource

		engine        Engine
		config        *Config
		metricsClient metrics.Client
		startWG       sync.WaitGroup
		rateLimiter   quotas.Limiter
	}
)

const (
	serviceName = "temporal.api.workflowservice.v1.MatchingService"
)

var (
	_ matchingservice.MatchingServiceServer = (*Handler)(nil)

	errMatchingHostThrottle = serviceerror.NewResourceExhausted("Matching host RPS exceeded.")
)

// NewHandler creates a gRPC handler for the matchingservice
func NewHandler(
	resource resource.Resource,
	config *Config,
) *Handler {
	handler := &Handler{
		Resource:      resource,
		config:        config,
		metricsClient: resource.GetMetricsClient(),
		rateLimiter: quotas.NewDefaultIncomingDynamicRateLimiter(
			func() float64 { return float64(config.RPS()) },
		),
		engine: NewEngine(
			resource.GetTaskManager(),
			resource.GetHistoryClient(),
			resource.GetMatchingRawClient(), // Use non retry client inside matching
			config,
			resource.GetLogger(),
			resource.GetMetricsClient(),
			resource.GetNamespaceCache(),
			resource.GetMatchingServiceResolver(),
		),
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

// https://github.com/grpc/grpc/blob/master/doc/health-checking.md
func (h *Handler) Check(_ context.Context, request *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	h.GetLogger().Debug("Matching service health check endpoint (gRPC) reached.")

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

func (h *Handler) newHandlerContext(
	ctx context.Context,
	namespaceID string,
	taskQueue *taskqueuepb.TaskQueue,
	scope int,
) *handlerContext {
	return newHandlerContext(
		ctx,
		h.namespaceName(namespaceID),
		taskQueue,
		h.metricsClient,
		scope,
	)
}

// AddActivityTask - adds an activity task.
func (h *Handler) AddActivityTask(
	ctx context.Context,
	request *matchingservice.AddActivityTaskRequest,
) (_ *matchingservice.AddActivityTaskResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	startT := time.Now().UTC()
	hCtx := h.newHandlerContext(
		ctx,
		request.GetNamespaceId(),
		request.GetTaskQueue(),
		metrics.MatchingAddActivityTaskScope,
	)

	sw := hCtx.startProfiling(&h.startWG)
	defer sw.Stop()

	if request.GetForwardedSource() != "" {
		h.reportForwardedPerTaskQueueCounter(hCtx, request.GetNamespaceId())
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return &matchingservice.AddActivityTaskResponse{}, hCtx.handleErr(errMatchingHostThrottle)
	}

	syncMatch, err := h.engine.AddActivityTask(hCtx, request)
	if syncMatch {
		hCtx.scope.RecordTimer(metrics.SyncMatchLatencyPerTaskQueue, time.Since(startT))
	}

	return &matchingservice.AddActivityTaskResponse{}, hCtx.handleErr(err)
}

// AddWorkflowTask - adds a workflow task.
func (h *Handler) AddWorkflowTask(
	ctx context.Context,
	request *matchingservice.AddWorkflowTaskRequest,
) (_ *matchingservice.AddWorkflowTaskResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	startT := time.Now().UTC()
	hCtx := h.newHandlerContext(
		ctx,
		request.GetNamespaceId(),
		request.GetTaskQueue(),
		metrics.MatchingAddWorkflowTaskScope,
	)

	sw := hCtx.startProfiling(&h.startWG)
	defer sw.Stop()

	if request.GetForwardedSource() != "" {
		h.reportForwardedPerTaskQueueCounter(hCtx, request.GetNamespaceId())
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return &matchingservice.AddWorkflowTaskResponse{}, hCtx.handleErr(errMatchingHostThrottle)
	}

	syncMatch, err := h.engine.AddWorkflowTask(hCtx, request)
	if syncMatch {
		hCtx.scope.RecordTimer(metrics.SyncMatchLatencyPerTaskQueue, time.Since(startT))
	}
	return &matchingservice.AddWorkflowTaskResponse{}, hCtx.handleErr(err)
}

// PollActivityTaskQueue - long poll for an activity task.
func (h *Handler) PollActivityTaskQueue(
	ctx context.Context,
	request *matchingservice.PollActivityTaskQueueRequest,
) (_ *matchingservice.PollActivityTaskQueueResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	hCtx := h.newHandlerContext(
		ctx,
		request.GetNamespaceId(),
		request.GetPollRequest().GetTaskQueue(),
		metrics.MatchingPollActivityTaskQueueScope,
	)

	sw := hCtx.startProfiling(&h.startWG)
	defer sw.Stop()

	if request.GetForwardedSource() != "" {
		h.reportForwardedPerTaskQueueCounter(hCtx, request.GetNamespaceId())
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, hCtx.handleErr(errMatchingHostThrottle)
	}

	if _, err := common.ValidateLongPollContextTimeoutIsSet(
		ctx,
		"PollActivityTaskQueue",
		h.Resource.GetThrottledLogger(),
	); err != nil {
		return nil, hCtx.handleErr(err)
	}

	response, err := h.engine.PollActivityTaskQueue(hCtx, request)
	return response, hCtx.handleErr(err)
}

// PollWorkflowTaskQueue - long poll for a workflow task.
func (h *Handler) PollWorkflowTaskQueue(
	ctx context.Context,
	request *matchingservice.PollWorkflowTaskQueueRequest,
) (_ *matchingservice.PollWorkflowTaskQueueResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	hCtx := h.newHandlerContext(
		ctx,
		request.GetNamespaceId(),
		request.GetPollRequest().GetTaskQueue(),
		metrics.MatchingPollWorkflowTaskQueueScope,
	)

	sw := hCtx.startProfiling(&h.startWG)
	defer sw.Stop()

	if request.GetForwardedSource() != "" {
		h.reportForwardedPerTaskQueueCounter(hCtx, request.GetNamespaceId())
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, hCtx.handleErr(errMatchingHostThrottle)
	}

	if _, err := common.ValidateLongPollContextTimeoutIsSet(
		ctx,
		"PollWorkflowTaskQueue",
		h.Resource.GetThrottledLogger(),
	); err != nil {
		return nil, hCtx.handleErr(err)
	}

	response, err := h.engine.PollWorkflowTaskQueue(hCtx, request)
	return response, hCtx.handleErr(err)
}

// QueryWorkflow queries a given workflow synchronously and return the query result.
func (h *Handler) QueryWorkflow(
	ctx context.Context,
	request *matchingservice.QueryWorkflowRequest,
) (_ *matchingservice.QueryWorkflowResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	hCtx := h.newHandlerContext(
		ctx,
		request.GetNamespaceId(),
		request.GetTaskQueue(),
		metrics.MatchingQueryWorkflowScope,
	)

	sw := hCtx.startProfiling(&h.startWG)
	defer sw.Stop()

	if request.GetForwardedSource() != "" {
		h.reportForwardedPerTaskQueueCounter(hCtx, request.GetNamespaceId())
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, hCtx.handleErr(errMatchingHostThrottle)
	}

	response, err := h.engine.QueryWorkflow(hCtx, request)
	return response, hCtx.handleErr(err)
}

// RespondQueryTaskCompleted responds a query task completed
func (h *Handler) RespondQueryTaskCompleted(
	ctx context.Context,
	request *matchingservice.RespondQueryTaskCompletedRequest,
) (_ *matchingservice.RespondQueryTaskCompletedResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	hCtx := h.newHandlerContext(
		ctx,
		request.GetNamespaceId(),
		request.GetTaskQueue(),
		metrics.MatchingRespondQueryTaskCompletedScope,
	)

	sw := hCtx.startProfiling(&h.startWG)
	defer sw.Stop()

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	h.rateLimiter.Allow()

	err := h.engine.RespondQueryTaskCompleted(hCtx, request)
	return &matchingservice.RespondQueryTaskCompletedResponse{}, hCtx.handleErr(err)
}

// CancelOutstandingPoll is used to cancel outstanding pollers
func (h *Handler) CancelOutstandingPoll(ctx context.Context,
	request *matchingservice.CancelOutstandingPollRequest) (_ *matchingservice.CancelOutstandingPollResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	hCtx := h.newHandlerContext(
		ctx,
		request.GetNamespaceId(),
		request.GetTaskQueue(),
		metrics.MatchingCancelOutstandingPollScope,
	)

	sw := hCtx.startProfiling(&h.startWG)
	defer sw.Stop()

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	h.rateLimiter.Allow()

	err := h.engine.CancelOutstandingPoll(hCtx, request)
	return &matchingservice.CancelOutstandingPollResponse{}, hCtx.handleErr(err)
}

// DescribeTaskQueue returns information about the target task queue, right now this API returns the
// pollers which polled this task queue in last few minutes. If includeTaskQueueStatus field is true,
// it will also return status of task queue's ackManager (readLevel, ackLevel, backlogCountHint and taskIDBlock).
func (h *Handler) DescribeTaskQueue(
	ctx context.Context,
	request *matchingservice.DescribeTaskQueueRequest,
) (_ *matchingservice.DescribeTaskQueueResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	hCtx := h.newHandlerContext(
		ctx,
		request.GetNamespaceId(),
		request.GetDescRequest().GetTaskQueue(),
		metrics.MatchingDescribeTaskQueueScope,
	)

	sw := hCtx.startProfiling(&h.startWG)
	defer sw.Stop()

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, hCtx.handleErr(errMatchingHostThrottle)
	}

	response, err := h.engine.DescribeTaskQueue(hCtx, request)
	return response, hCtx.handleErr(err)
}

// ListTaskQueuePartitions returns information about partitions for a taskQueue
func (h *Handler) ListTaskQueuePartitions(
	ctx context.Context,
	request *matchingservice.ListTaskQueuePartitionsRequest,
) (_ *matchingservice.ListTaskQueuePartitionsResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	hCtx := newHandlerContext(
		ctx,
		request.GetNamespace(),
		request.GetTaskQueue(),
		h.metricsClient,
		metrics.MatchingListTaskQueuePartitionsScope,
	)

	sw := hCtx.startProfiling(&h.startWG)
	defer sw.Stop()

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, hCtx.handleErr(errMatchingHostThrottle)
	}

	response, err := h.engine.ListTaskQueuePartitions(hCtx, request)
	return response, hCtx.handleErr(err)
}

func (h *Handler) namespaceName(id string) string {
	entry, err := h.GetNamespaceCache().GetNamespaceByID(id)
	if err != nil {
		return ""
	}
	return entry.GetInfo().Name
}

func (h *Handler) reportForwardedPerTaskQueueCounter(hCtx *handlerContext, namespaceId string) {
	hCtx.scope.IncCounter(metrics.ForwardedPerTaskQueueCounter)
	h.GetMetricsClient().
		Scope(metrics.MatchingAddWorkflowTaskScope).
		Tagged(metrics.NamespaceTag(h.namespaceName(namespaceId))).
		Tagged(metrics.ServiceRoleTag(metrics.MatchingRoleTagValue)).
		IncCounter(metrics.MatchingClientForwardedCounter)
}
