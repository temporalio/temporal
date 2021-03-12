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

package persistence

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
)

type visibilitySamplingClient struct {
	rateLimitersForOpen   quotas.NamespaceRateLimiter
	rateLimitersForClosed quotas.NamespaceRateLimiter
	rateLimitersForList   quotas.NamespaceRateLimiter
	persistence           VisibilityManager
	config                *config.VisibilityConfig
	metricClient          metrics.Client
	logger                log.Logger
}

var _ VisibilityManager = (*visibilitySamplingClient)(nil)

// NewVisibilitySamplingClient creates a client to manage visibility with sampling
func NewVisibilitySamplingClient(
	persistence VisibilityManager,
	config *config.VisibilityConfig,
	metricClient metrics.Client,
	logger log.Logger,
) VisibilityManager {

	var persistenceRateLimiter []quotas.RateLimiter
	if config.MaxQPS != nil {
		persistenceRateLimiter = append(persistenceRateLimiter, quotas.NewDefaultIncomingDynamicRateLimiter(
			func() float64 { return float64(config.MaxQPS()) },
		))
	}

	return &visibilitySamplingClient{
		persistence: persistence,
		rateLimitersForOpen: quotas.NewNamespaceMultiStageRateLimiter(
			func(namespace string) quotas.RateLimiter {
				return quotas.NewDefaultOutgoingDynamicRateLimiter(
					func() float64 { return float64(config.VisibilityOpenMaxQPS(namespace)) },
				)
			},
			persistenceRateLimiter,
		),
		rateLimitersForClosed: quotas.NewNamespaceMultiStageRateLimiter(
			func(namespace string) quotas.RateLimiter {
				return quotas.NewDefaultOutgoingDynamicRateLimiter(
					func() float64 { return float64(config.VisibilityClosedMaxQPS(namespace)) },
				)
			},
			persistenceRateLimiter,
		),
		rateLimitersForList: quotas.NewNamespaceMultiStageRateLimiter(
			func(namespace string) quotas.RateLimiter {
				return quotas.NewDefaultOutgoingDynamicRateLimiter(
					func() float64 { return float64(config.VisibilityListMaxQPS(namespace)) },
				)
			},
			persistenceRateLimiter,
		),
		config:       config,
		metricClient: metricClient,
		logger:       logger,
	}
}

func (p *visibilitySamplingClient) RecordWorkflowExecutionStarted(request *RecordWorkflowExecutionStartedRequest) error {
	namespace := request.Namespace
	namespaceID := request.NamespaceID

	if ok := p.rateLimitersForOpen.Allow(namespace); ok {
		return p.persistence.RecordWorkflowExecutionStarted(request)
	}

	p.logger.Info("Request for open workflow is sampled",
		tag.WorkflowNamespaceID(namespaceID),
		tag.WorkflowNamespace(namespace),
		tag.WorkflowType(request.WorkflowTypeName),
		tag.WorkflowID(request.Execution.GetWorkflowId()),
		tag.WorkflowRunID(request.Execution.GetRunId()),
	)
	p.metricClient.IncCounter(metrics.PersistenceRecordWorkflowExecutionStartedScope, metrics.PersistenceSampledCounter)
	return nil
}

func (p *visibilitySamplingClient) RecordWorkflowExecutionClosed(request *RecordWorkflowExecutionClosedRequest) error {
	namespace := request.Namespace
	namespaceID := request.NamespaceID

	if ok := p.rateLimitersForClosed.Allow(namespace); ok {
		return p.persistence.RecordWorkflowExecutionClosed(request)
	}

	p.logger.Info("Request for closed workflow is sampled",
		tag.WorkflowNamespaceID(namespaceID),
		tag.WorkflowNamespace(namespace),
		tag.WorkflowType(request.WorkflowTypeName),
		tag.WorkflowID(request.Execution.GetWorkflowId()),
		tag.WorkflowRunID(request.Execution.GetRunId()),
	)
	p.metricClient.IncCounter(metrics.PersistenceRecordWorkflowExecutionClosedScope, metrics.PersistenceSampledCounter)
	return nil
}

func (p *visibilitySamplingClient) UpsertWorkflowExecution(request *UpsertWorkflowExecutionRequest) error {
	namespace := request.Namespace
	namespaceID := request.NamespaceID

	if ok := p.rateLimitersForClosed.Allow(namespace); ok {
		return p.persistence.UpsertWorkflowExecution(request)
	}

	p.logger.Info("Request for upsert workflow is sampled",
		tag.WorkflowNamespaceID(namespaceID),
		tag.WorkflowNamespace(namespace),
		tag.WorkflowType(request.WorkflowTypeName),
		tag.WorkflowID(request.Execution.GetWorkflowId()),
		tag.WorkflowRunID(request.Execution.GetRunId()),
	)
	p.metricClient.IncCounter(metrics.PersistenceUpsertWorkflowExecutionScope, metrics.PersistenceSampledCounter)
	return nil
}

func (p *visibilitySamplingClient) ListOpenWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	namespace := request.Namespace

	if ok := p.rateLimitersForList.Allow(namespace); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListOpenWorkflowExecutions(request)
}

func (p *visibilitySamplingClient) ListClosedWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	namespace := request.Namespace

	if ok := p.rateLimitersForList.Allow(namespace); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListClosedWorkflowExecutions(request)
}

func (p *visibilitySamplingClient) ListOpenWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	namespace := request.Namespace

	if ok := p.rateLimitersForList.Allow(namespace); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListOpenWorkflowExecutionsByType(request)
}

func (p *visibilitySamplingClient) ListClosedWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	namespace := request.Namespace

	if ok := p.rateLimitersForList.Allow(namespace); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListClosedWorkflowExecutionsByType(request)
}

func (p *visibilitySamplingClient) ListOpenWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	namespace := request.Namespace

	if ok := p.rateLimitersForList.Allow(namespace); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListOpenWorkflowExecutionsByWorkflowID(request)
}

func (p *visibilitySamplingClient) ListClosedWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	namespace := request.Namespace

	if ok := p.rateLimitersForList.Allow(namespace); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListClosedWorkflowExecutionsByWorkflowID(request)
}

func (p *visibilitySamplingClient) ListClosedWorkflowExecutionsByStatus(request *ListClosedWorkflowExecutionsByStatusRequest) (*ListWorkflowExecutionsResponse, error) {
	namespace := request.Namespace

	if ok := p.rateLimitersForList.Allow(namespace); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListClosedWorkflowExecutionsByStatus(request)
}

func (p *visibilitySamplingClient) GetClosedWorkflowExecution(request *GetClosedWorkflowExecutionRequest) (*GetClosedWorkflowExecutionResponse, error) {
	return p.persistence.GetClosedWorkflowExecution(request)
}

func (p *visibilitySamplingClient) DeleteWorkflowExecution(request *VisibilityDeleteWorkflowExecutionRequest) error {
	return p.persistence.DeleteWorkflowExecution(request)
}

func (p *visibilitySamplingClient) ListWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*ListWorkflowExecutionsResponse, error) {
	return p.persistence.ListWorkflowExecutions(request)
}

func (p *visibilitySamplingClient) ScanWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*ListWorkflowExecutionsResponse, error) {
	return p.persistence.ScanWorkflowExecutions(request)
}

func (p *visibilitySamplingClient) CountWorkflowExecutions(request *CountWorkflowExecutionsRequest) (*CountWorkflowExecutionsResponse, error) {
	return p.persistence.CountWorkflowExecutions(request)
}

func (p *visibilitySamplingClient) Close() {
	p.persistence.Close()
}

func (p *visibilitySamplingClient) GetName() string {
	return p.persistence.GetName()
}
