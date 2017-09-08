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

	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/health"
	m "github.com/uber/cadence/.gen/go/matching"
	"github.com/uber/cadence/.gen/go/matching/matchingserviceserver"
	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
)

var _ matchingserviceserver.Interface = (*Handler)(nil)

// Handler - Thrift handler inteface for history service
type Handler struct {
	taskPersistence persistence.TaskManager
	engine          Engine
	config          *Config
	metricsClient   metrics.Client
	startWG         sync.WaitGroup
	service.Service
}

// NewHandler creates a thrift handler for the history service
func NewHandler(sVice service.Service, config *Config, taskPersistence persistence.TaskManager) *Handler {
	handler := &Handler{
		Service:         sVice,
		taskPersistence: taskPersistence,
		config:          config,
	}
	// prevent us from trying to serve requests before matching engine is started and ready
	handler.startWG.Add(1)
	return handler
}

// Start starts the handler
func (h *Handler) Start() error {
	h.Service.GetDispatcher().Register(matchingserviceserver.New(h))
	h.Service.Start()
	history, err := h.Service.GetClientFactory().NewHistoryClient()
	if err != nil {
		return err
	}
	h.metricsClient = h.Service.GetMetricsClient()
	h.engine = NewEngine(h.taskPersistence, history, h.config, h.Service.GetLogger(), h.Service.GetMetricsClient())
	h.startWG.Done()
	return nil
}

// Stop stops the handler
func (h *Handler) Stop() {
	h.engine.Stop()
	h.taskPersistence.Close()
	h.Service.Stop()
}

// Health is for health check
func (h *Handler) Health(ctx context.Context) (*health.HealthStatus, error) {
	h.startWG.Wait()
	h.GetLogger().Debug("Matching service health check endpoint reached.")
	hs := &health.HealthStatus{Ok: true, Msg: common.StringPtr("matching good")}
	return hs, nil
}

// startRequestProfile initiates recording of request metrics
func (h *Handler) startRequestProfile(api string, scope int) tally.Stopwatch {
	h.startWG.Wait()
	sw := h.metricsClient.StartTimer(scope, metrics.CadenceLatency)
	h.Service.GetLogger().WithField("api", api).Debug("Received new request")
	h.metricsClient.IncCounter(scope, metrics.CadenceRequests)
	return sw
}

// AddActivityTask - adds an activity task.
func (h *Handler) AddActivityTask(ctx context.Context, addRequest *m.AddActivityTaskRequest) error {
	scope := metrics.MatchingAddActivityTaskScope
	sw := h.startRequestProfile("AddActivityTask", scope)
	defer sw.Stop()
	return h.handleErr(h.engine.AddActivityTask(addRequest), scope)
}

// AddDecisionTask - adds a decision task.
func (h *Handler) AddDecisionTask(ctx context.Context, addRequest *m.AddDecisionTaskRequest) error {
	scope := metrics.MatchingAddDecisionTaskScope
	sw := h.startRequestProfile("AddDecisionTask", scope)
	defer sw.Stop()
	return h.handleErr(h.engine.AddDecisionTask(addRequest), scope)
}

// PollForActivityTask - long poll for an activity task.
func (h *Handler) PollForActivityTask(ctx context.Context,
	pollRequest *m.PollForActivityTaskRequest) (*gen.PollForActivityTaskResponse, error) {

	scope := metrics.MatchingPollForActivityTaskScope
	sw := h.startRequestProfile("PollForActivityTask", scope)
	defer sw.Stop()

	response, error := h.engine.PollForActivityTask(ctx, pollRequest)
	h.Service.GetLogger().Debug("Engine returned from PollForActivityTask")
	return response, h.handleErr(error, scope)

}

// PollForDecisionTask - long poll for a decision task.
func (h *Handler) PollForDecisionTask(ctx context.Context,
	pollRequest *m.PollForDecisionTaskRequest) (*m.PollForDecisionTaskResponse, error) {

	scope := metrics.MatchingPollForDecisionTaskScope
	sw := h.startRequestProfile("PollForDecisionTask", scope)
	defer sw.Stop()

	response, error := h.engine.PollForDecisionTask(ctx, pollRequest)
	h.Service.GetLogger().Debug("Engine returned from PollForDecisionTask")
	return response, h.handleErr(error, scope)
}

func (h *Handler) handleErr(err error, scope int) error {

	if err == nil {
		return nil
	}

	switch err.(type) {
	case *gen.InternalServiceError:
		h.metricsClient.IncCounter(scope, metrics.CadenceFailures)
		return err
	case *gen.BadRequestError:
		h.metricsClient.IncCounter(scope, metrics.CadenceErrBadRequestCounter)
		return err
	case *gen.EntityNotExistsError:
		h.metricsClient.IncCounter(scope, metrics.CadenceErrEntityNotExistsCounter)
		return err
	case *gen.WorkflowExecutionAlreadyStartedError:
		h.metricsClient.IncCounter(scope, metrics.CadenceErrExecutionAlreadyStartedCounter)
		return err
	case *gen.DomainAlreadyExistsError:
		h.metricsClient.IncCounter(scope, metrics.CadenceErrDomainAlreadyExistsCounter)
		return err
	default:
		h.metricsClient.IncCounter(scope, metrics.CadenceFailures)
		return &gen.InternalServiceError{Message: err.Error()}
	}
}
