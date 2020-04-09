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

package matching

import (
	"context"
	"sync"
	"time"

	"github.com/uber/cadence/.gen/go/health"
	"github.com/uber/cadence/.gen/go/health/metaserver"
	m "github.com/uber/cadence/.gen/go/matching"
	"github.com/uber/cadence/.gen/go/matching/matchingserviceserver"
	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/quotas"
	"github.com/uber/cadence/common/resource"
)

var _ matchingserviceserver.Interface = (*Handler)(nil)

// Handler - Thrift handler interface for history service
type Handler struct {
	resource.Resource

	engine        Engine
	config        *Config
	metricsClient metrics.Client
	startWG       sync.WaitGroup
	rateLimiter   quotas.Limiter
}

var (
	errMatchingHostThrottle = &gen.ServiceBusyError{Message: "Matching host rps exceeded"}
)

// NewHandler creates a thrift handler for the history service
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
	// prevent us from trying to serve requests before matching engine is started and ready
	handler.startWG.Add(1)
	return handler
}

// RegisterHandler register this handler, must be called before Start()
func (h *Handler) RegisterHandler() {
	h.Resource.GetDispatcher().Register(matchingserviceserver.New(h))
	h.Resource.GetDispatcher().Register(metaserver.New(h))
}

// Start starts the handler
func (h *Handler) Start() {
	h.startWG.Done()
}

// Stop stops the handler
func (h *Handler) Stop() {
	h.engine.Stop()
}

// Health is for health check
func (h *Handler) Health(ctx context.Context) (*health.HealthStatus, error) {
	h.startWG.Wait()
	h.GetLogger().Debug("Matching service health check endpoint reached.")
	hs := &health.HealthStatus{Ok: true, Msg: common.StringPtr("matching good")}
	return hs, nil
}

func (h *Handler) newHandlerContext(
	ctx context.Context,
	domainID string,
	taskList *gen.TaskList,
	scope int,
) *handlerContext {
	return newHandlerContext(
		ctx,
		h.domainName(domainID),
		taskList,
		h.metricsClient,
		scope,
	)
}

// AddActivityTask - adds an activity task.
func (h *Handler) AddActivityTask(
	ctx context.Context,
	request *m.AddActivityTaskRequest,
) (retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	startT := time.Now()
	hCtx := h.newHandlerContext(
		ctx,
		request.GetDomainUUID(),
		request.GetTaskList(),
		metrics.MatchingAddActivityTaskScope,
	)

	sw := hCtx.startProfiling(&h.startWG)
	defer sw.Stop()

	if request.GetForwardedFrom() != "" {
		hCtx.scope.IncCounter(metrics.ForwardedPerTaskListCounter)
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return hCtx.handleErr(errMatchingHostThrottle)
	}

	syncMatch, err := h.engine.AddActivityTask(hCtx, request)
	if syncMatch {
		hCtx.scope.RecordTimer(metrics.SyncMatchLatencyPerTaskList, time.Since(startT))
	}

	return hCtx.handleErr(err)
}

// AddDecisionTask - adds a decision task.
func (h *Handler) AddDecisionTask(
	ctx context.Context,
	request *m.AddDecisionTaskRequest,
) (retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	startT := time.Now()
	hCtx := h.newHandlerContext(
		ctx,
		request.GetDomainUUID(),
		request.GetTaskList(),
		metrics.MatchingAddDecisionTaskScope,
	)

	sw := hCtx.startProfiling(&h.startWG)
	defer sw.Stop()

	if request.GetForwardedFrom() != "" {
		hCtx.scope.IncCounter(metrics.ForwardedPerTaskListCounter)
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return hCtx.handleErr(errMatchingHostThrottle)
	}

	syncMatch, err := h.engine.AddDecisionTask(hCtx, request)
	if syncMatch {
		hCtx.scope.RecordTimer(metrics.SyncMatchLatencyPerTaskList, time.Since(startT))
	}
	return hCtx.handleErr(err)
}

// PollForActivityTask - long poll for an activity task.
func (h *Handler) PollForActivityTask(
	ctx context.Context,
	request *m.PollForActivityTaskRequest,
) (resp *gen.PollForActivityTaskResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	hCtx := h.newHandlerContext(
		ctx,
		request.GetDomainUUID(),
		request.GetPollRequest().GetTaskList(),
		metrics.MatchingPollForActivityTaskScope,
	)

	sw := hCtx.startProfiling(&h.startWG)
	defer sw.Stop()

	if request.GetForwardedFrom() != "" {
		hCtx.scope.IncCounter(metrics.ForwardedPerTaskListCounter)
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, hCtx.handleErr(errMatchingHostThrottle)
	}

	if _, err := common.ValidateLongPollContextTimeoutIsSet(
		ctx,
		"PollForActivityTask",
		h.Resource.GetThrottledLogger(),
	); err != nil {
		return nil, hCtx.handleErr(err)
	}

	response, err := h.engine.PollForActivityTask(hCtx, request)
	return response, hCtx.handleErr(err)
}

// PollForDecisionTask - long poll for a decision task.
func (h *Handler) PollForDecisionTask(
	ctx context.Context,
	request *m.PollForDecisionTaskRequest,
) (resp *m.PollForDecisionTaskResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	hCtx := h.newHandlerContext(
		ctx,
		request.GetDomainUUID(),
		request.GetPollRequest().GetTaskList(),
		metrics.MatchingPollForDecisionTaskScope,
	)

	sw := hCtx.startProfiling(&h.startWG)
	defer sw.Stop()

	if request.GetForwardedFrom() != "" {
		hCtx.scope.IncCounter(metrics.ForwardedPerTaskListCounter)
	}

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, hCtx.handleErr(errMatchingHostThrottle)
	}

	if _, err := common.ValidateLongPollContextTimeoutIsSet(
		ctx,
		"PollForDecisionTask",
		h.Resource.GetThrottledLogger(),
	); err != nil {
		return nil, hCtx.handleErr(err)
	}

	response, err := h.engine.PollForDecisionTask(hCtx, request)
	return response, hCtx.handleErr(err)
}

// QueryWorkflow queries a given workflow synchronously and return the query result.
func (h *Handler) QueryWorkflow(
	ctx context.Context,
	request *m.QueryWorkflowRequest,
) (resp *gen.QueryWorkflowResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	hCtx := h.newHandlerContext(
		ctx,
		request.GetDomainUUID(),
		request.GetTaskList(),
		metrics.MatchingQueryWorkflowScope,
	)

	sw := hCtx.startProfiling(&h.startWG)
	defer sw.Stop()

	if request.GetForwardedFrom() != "" {
		hCtx.scope.IncCounter(metrics.ForwardedPerTaskListCounter)
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
	request *m.RespondQueryTaskCompletedRequest,
) (retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	hCtx := h.newHandlerContext(
		ctx,
		request.GetDomainUUID(),
		request.GetTaskList(),
		metrics.MatchingRespondQueryTaskCompletedScope,
	)

	sw := hCtx.startProfiling(&h.startWG)
	defer sw.Stop()

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	h.rateLimiter.Allow()

	err := h.engine.RespondQueryTaskCompleted(hCtx, request)
	return hCtx.handleErr(err)
}

// CancelOutstandingPoll is used to cancel outstanding pollers
func (h *Handler) CancelOutstandingPoll(ctx context.Context,
	request *m.CancelOutstandingPollRequest) (retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	hCtx := h.newHandlerContext(
		ctx,
		request.GetDomainUUID(),
		request.GetTaskList(),
		metrics.MatchingCancelOutstandingPollScope,
	)

	sw := hCtx.startProfiling(&h.startWG)
	defer sw.Stop()

	// Count the request in the RPS, but we still accept it even if RPS is exceeded
	h.rateLimiter.Allow()

	err := h.engine.CancelOutstandingPoll(hCtx, request)
	return hCtx.handleErr(err)
}

// DescribeTaskList returns information about the target tasklist, right now this API returns the
// pollers which polled this tasklist in last few minutes. If includeTaskListStatus field is true,
// it will also return status of tasklist's ackManager (readLevel, ackLevel, backlogCountHint and taskIDBlock).
func (h *Handler) DescribeTaskList(
	ctx context.Context,
	request *m.DescribeTaskListRequest,
) (resp *gen.DescribeTaskListResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	hCtx := h.newHandlerContext(
		ctx,
		request.GetDomainUUID(),
		request.GetDescRequest().GetTaskList(),
		metrics.MatchingDescribeTaskListScope,
	)

	sw := hCtx.startProfiling(&h.startWG)
	defer sw.Stop()

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, hCtx.handleErr(errMatchingHostThrottle)
	}

	response, err := h.engine.DescribeTaskList(hCtx, request)
	return response, hCtx.handleErr(err)
}

// ListTaskListPartitions returns information about partitions for a taskList
func (h *Handler) ListTaskListPartitions(
	ctx context.Context,
	request *m.ListTaskListPartitionsRequest,
) (resp *gen.ListTaskListPartitionsResponse, retError error) {
	defer log.CapturePanic(h.GetLogger(), &retError)
	hCtx := newHandlerContext(
		ctx,
		request.GetDomain(),
		request.GetTaskList(),
		h.metricsClient,
		metrics.MatchingListTaskListPartitionsScope,
	)

	sw := hCtx.startProfiling(&h.startWG)
	defer sw.Stop()

	if ok := h.rateLimiter.Allow(); !ok {
		return nil, hCtx.handleErr(errMatchingHostThrottle)
	}

	response, err := h.engine.ListTaskListPartitions(hCtx, request)
	return response, hCtx.handleErr(err)
}

func (h *Handler) domainName(id string) string {
	entry, err := h.GetDomainCache().GetDomainByID(id)
	if err != nil {
		return ""
	}
	return entry.GetInfo().Name
}
