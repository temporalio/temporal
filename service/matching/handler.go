// Copyright (c) 2019 Temporal Technologies, Inc.
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

	"github.com/uber-go/tally"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/healthservice"
	"github.com/temporalio/temporal/.gen/proto/matchingservice"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/quotas"
	"github.com/temporalio/temporal/common/resource"
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
		rateLimiter: quotas.NewDynamicRateLimiter(func() float64 {
			return float64(config.RPS())
		}),
		engine: NewEngine(
			resource.GetTaskManager(),
			resource.GetHistoryClient(),
			resource.GetMatchingRawClient(), // Use non retry client inside matching
			config,
			resource.GetLogger(),
			resource.GetMetricsClient(),
			resource.GetDomainCache(),
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

// startRequestProfile initiates recording of request metrics
func (h *Handler) startRequestProfile(_ string, scope int) tally.Stopwatch {
	h.startWG.Wait()
	sw := h.metricsClient.StartTimer(scope, metrics.ServiceLatency)
	h.metricsClient.IncCounter(scope, metrics.ServiceRequests)
	return sw
}

// Health is for health check
func (h *Handler) Health(_ context.Context, _ *healthservice.HealthRequest) (_ *healthservice.HealthStatus, retError error) {
	h.startWG.Wait()
	h.GetLogger().Debug("Matching service health check endpoint (gRPC) reached.")
	hs := &healthservice.HealthStatus{Ok: true, Msg: "Matching service is healthy."}
	return hs, nil
}

// AddActivityTask - adds an activity task.
func (h *Handler) AddActivityTask(ctx context.Context, request *matchingservice.AddActivityTaskRequest) (_ *matchingservice.AddActivityTaskResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	startT := time.Now()
	scope := metrics.MatchingAddActivityTaskScope
	sw := h.startRequestProfile("AddActivityTask", scope)
	defer sw.Stop()

	if request.GetForwardedFrom() != "" {
		h.metricsClient.IncCounter(scope, metrics.ForwardedCounter)
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return &matchingservice.AddActivityTaskResponse{}, h.handleErr(errMatchingHostThrottle, scope)
	}

	syncMatch, err := h.engine.AddActivityTask(ctx, request)
	if syncMatch {
		h.metricsClient.RecordTimer(scope, metrics.SyncMatchLatency, time.Since(startT))
	}

	return &matchingservice.AddActivityTaskResponse{}, h.handleErr(err, scope)
}

// AddDecisionTask - adds a decision task.
func (h *Handler) AddDecisionTask(ctx context.Context, request *matchingservice.AddDecisionTaskRequest) (_ *matchingservice.AddDecisionTaskResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	startT := time.Now()
	scope := metrics.MatchingAddDecisionTaskScope
	sw := h.startRequestProfile("AddDecisionTask", scope)
	defer sw.Stop()

	if request.GetForwardedFrom() != "" {
		h.metricsClient.IncCounter(scope, metrics.ForwardedCounter)
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return &matchingservice.AddDecisionTaskResponse{}, h.handleErr(errMatchingHostThrottle, scope)
	}

	syncMatch, err := h.engine.AddDecisionTask(ctx, request)
	if syncMatch {
		h.metricsClient.RecordTimer(scope, metrics.SyncMatchLatency, time.Since(startT))
	}
	return &matchingservice.AddDecisionTaskResponse{}, h.handleErr(err, scope)
}

// PollForActivityTask - long poll for an activity task.
func (h *Handler) PollForActivityTask(ctx context.Context, request *matchingservice.PollForActivityTaskRequest) (_ *matchingservice.PollForActivityTaskResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)

	scope := metrics.MatchingPollForActivityTaskScope
	sw := h.startRequestProfile("PollForActivityTask", scope)
	defer sw.Stop()

	if request.GetForwardedFrom() != "" {
		h.metricsClient.IncCounter(scope, metrics.ForwardedCounter)
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.handleErr(errMatchingHostThrottle, scope)
	}

	if _, err := common.ValidateLongPollContextTimeoutIsSet(
		ctx,
		"PollForActivityTask",
		h.Resource.GetThrottledLogger(),
	); err != nil {
		return nil, h.handleErr(err, scope)
	}

	response, err := h.engine.PollForActivityTask(ctx, request)
	return response, h.handleErr(err, scope)
}

// PollForDecisionTask - long poll for a decision task.
func (h *Handler) PollForDecisionTask(ctx context.Context, request *matchingservice.PollForDecisionTaskRequest) (_ *matchingservice.PollForDecisionTaskResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)

	scope := metrics.MatchingPollForDecisionTaskScope
	sw := h.startRequestProfile("PollForDecisionTask", scope)
	defer sw.Stop()

	if request.GetForwardedFrom() != "" {
		h.metricsClient.IncCounter(scope, metrics.ForwardedCounter)
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.handleErr(errMatchingHostThrottle, scope)
	}

	if _, err := common.ValidateLongPollContextTimeoutIsSet(
		ctx,
		"PollForDecisionTask",
		h.Resource.GetThrottledLogger(),
	); err != nil {
		return nil, h.handleErr(err, scope)
	}

	response, err := h.engine.PollForDecisionTask(ctx, request)
	return response, h.handleErr(err, scope)
}

// QueryWorkflow queries a given workflow synchronously and return the query result.
func (h *Handler) QueryWorkflow(ctx context.Context, request *matchingservice.QueryWorkflowRequest) (_ *matchingservice.QueryWorkflowResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	scope := metrics.MatchingQueryWorkflowScope
	sw := h.startRequestProfile("QueryWorkflow", scope)
	defer sw.Stop()

	if request.GetForwardedFrom() != "" {
		h.metricsClient.IncCounter(scope, metrics.ForwardedCounter)
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.handleErr(errMatchingHostThrottle, scope)
	}

	response, err := h.engine.QueryWorkflow(ctx, request)
	return response, h.handleErr(err, scope)
}

// RespondQueryTaskCompleted responds a query task completed
func (h *Handler) RespondQueryTaskCompleted(ctx context.Context, request *matchingservice.RespondQueryTaskCompletedRequest) (_ *matchingservice.RespondQueryTaskCompletedResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	scope := metrics.MatchingRespondQueryTaskCompletedScope
	sw := h.startRequestProfile("RespondQueryTaskCompleted", scope)
	defer sw.Stop()

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	h.rateLimiter.Allow()

	err := h.engine.RespondQueryTaskCompleted(ctx, request)
	return &matchingservice.RespondQueryTaskCompletedResponse{}, h.handleErr(err, scope)
}

// CancelOutstandingPoll is used to cancel outstanding pollers
func (h *Handler) CancelOutstandingPoll(ctx context.Context, request *matchingservice.CancelOutstandingPollRequest) (_ *matchingservice.CancelOutstandingPollResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	scope := metrics.MatchingCancelOutstandingPollScope
	sw := h.startRequestProfile("CancelOutstandingPoll", scope)
	defer sw.Stop()

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	h.rateLimiter.Allow()

	err := h.engine.CancelOutstandingPoll(ctx, request)
	return &matchingservice.CancelOutstandingPollResponse{}, h.handleErr(err, scope)
}

// DescribeTaskList returns information about the target task list, right now this API returns the
// pollers which polled this task list in last few minutes. If includeTaskListStatus field is true,
// it will also return status of task list's ackManager (readLevel, ackLevel, backlogCountHint and taskIDBlock).
func (h *Handler) DescribeTaskList(ctx context.Context, request *matchingservice.DescribeTaskListRequest) (_ *matchingservice.DescribeTaskListResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	scope := metrics.MatchingDescribeTaskListScope
	sw := h.startRequestProfile("DescribeTaskList", scope)
	defer sw.Stop()

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.handleErr(errMatchingHostThrottle, scope)
	}

	response, err := h.engine.DescribeTaskList(ctx, request)
	return response, h.handleErr(err, scope)
}

// ListTaskListPartitions returns information about partitions for a taskList
func (h *Handler) ListTaskListPartitions(ctx context.Context, request *matchingservice.ListTaskListPartitionsRequest) (_ *matchingservice.ListTaskListPartitionsResponse, retError error) {
	defer log.CapturePanicGRPC(h.GetLogger(), &retError)
	scope := metrics.MatchingListTaskListPartitionsScope
	sw := h.startRequestProfile("ListTaskListPartitions", scope)
	defer sw.Stop()

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, h.handleErr(errMatchingHostThrottle, scope)
	}

	response, err := h.engine.ListTaskListPartitions(ctx, request)
	return response, h.handleErr(err, scope)
}

func (h *Handler) handleErr(err error, scope int) error {

	if err == nil {
		return nil
	}

	switch err.(type) {
	case *serviceerror.Internal:
		h.metricsClient.IncCounter(scope, metrics.ServiceFailures)
		return err
	case *serviceerror.InvalidArgument:
		h.metricsClient.IncCounter(scope, metrics.ServiceErrInvalidArgumentCounter)
		return err
	case *serviceerror.NotFound:
		h.metricsClient.IncCounter(scope, metrics.ServiceErrNotFoundCounter)
		return err
	case *serviceerror.WorkflowExecutionAlreadyStarted:
		h.metricsClient.IncCounter(scope, metrics.ServiceErrExecutionAlreadyStartedCounter)
		return err
	case *serviceerror.DomainAlreadyExists:
		h.metricsClient.IncCounter(scope, metrics.ServiceErrDomainAlreadyExistsCounter)
		return err
	case *serviceerror.QueryFailed:
		h.metricsClient.IncCounter(scope, metrics.ServiceErrQueryFailedCounter)
		return err
	case *serviceerror.ResourceExhausted:
		h.metricsClient.IncCounter(scope, metrics.ServiceErrResourceExhaustedCounter)
		return err
	case *serviceerror.DomainNotActive:
		h.metricsClient.IncCounter(scope, metrics.ServiceErrDomainNotActiveCounter)
		return err
	default:
		h.metricsClient.IncCounter(scope, metrics.ServiceFailures)
		return serviceerror.NewInternal(err.Error())
	}
}
