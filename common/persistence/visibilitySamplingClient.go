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
	"sync"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/service/config"
	"go.temporal.io/server/common/tokenbucket"
)

const (
	// To sample visibility request, open has only 1 bucket, closed has 2
	numOfPriorityForOpen   = 1
	numOfPriorityForClosed = 2
	numOfPriorityForList   = 1
)

type visibilitySamplingClient struct {
	rateLimitersForOpen   *namespaceToBucketMap
	rateLimitersForClosed *namespaceToBucketMap
	rateLimitersForList   *namespaceToBucketMap
	persistence           VisibilityManager
	config                *config.VisibilityConfig
	metricClient          metrics.Client
	logger                log.Logger
}

var _ VisibilityManager = (*visibilitySamplingClient)(nil)

// NewVisibilitySamplingClient creates a client to manage visibility with sampling
func NewVisibilitySamplingClient(persistence VisibilityManager, config *config.VisibilityConfig, metricClient metrics.Client, logger log.Logger) VisibilityManager {
	return &visibilitySamplingClient{
		persistence:           persistence,
		rateLimitersForOpen:   newNamespaceToBucketMap(),
		rateLimitersForClosed: newNamespaceToBucketMap(),
		rateLimitersForList:   newNamespaceToBucketMap(),
		config:                config,
		metricClient:          metricClient,
		logger:                logger,
	}
}

type namespaceToBucketMap struct {
	sync.RWMutex
	mappings map[string]tokenbucket.PriorityTokenBucket
}

func newNamespaceToBucketMap() *namespaceToBucketMap {
	return &namespaceToBucketMap{
		mappings: make(map[string]tokenbucket.PriorityTokenBucket),
	}
}

func (m *namespaceToBucketMap) getRateLimiter(namespace string, numOfPriority, qps int) tokenbucket.PriorityTokenBucket {
	m.RLock()
	rateLimiter, exist := m.mappings[namespace]
	m.RUnlock()

	if exist {
		return rateLimiter
	}

	m.Lock()
	if rateLimiter, ok := m.mappings[namespace]; ok { // read again to ensure no duplicate create
		m.Unlock()
		return rateLimiter
	}
	rateLimiter = tokenbucket.NewFullPriorityTokenBucket(numOfPriority, qps, clock.NewRealTimeSource())
	m.mappings[namespace] = rateLimiter
	m.Unlock()
	return rateLimiter
}

func (p *visibilitySamplingClient) RecordWorkflowExecutionStarted(request *RecordWorkflowExecutionStartedRequest) error {
	namespace := request.Namespace
	namespaceID := request.NamespaceID

	rateLimiter := p.rateLimitersForOpen.getRateLimiter(namespace, numOfPriorityForOpen, p.config.VisibilityOpenMaxQPS(namespace))
	if ok, _ := rateLimiter.GetToken(0, 1); ok {
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
	priority := getRequestPriority(request)

	rateLimiter := p.rateLimitersForClosed.getRateLimiter(namespace, numOfPriorityForClosed, p.config.VisibilityClosedMaxQPS(namespace))
	if ok, _ := rateLimiter.GetToken(priority, 1); ok {
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

	rateLimiter := p.rateLimitersForClosed.getRateLimiter(namespace, numOfPriorityForClosed, p.config.VisibilityClosedMaxQPS(namespace))
	if ok, _ := rateLimiter.GetToken(0, 1); ok {
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

func (p *visibilitySamplingClient) UpsertWorkflowExecutionV2(request *UpsertWorkflowExecutionRequestV2) error {
	namespace := request.Namespace
	namespaceID := request.NamespaceID

	rateLimiter := p.rateLimitersForClosed.getRateLimiter(namespace, numOfPriorityForClosed, p.config.VisibilityClosedMaxQPS(namespace))
	if ok, _ := rateLimiter.GetToken(0, 1); ok {
		return p.persistence.UpsertWorkflowExecutionV2(request)
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

	rateLimiter := p.rateLimitersForList.getRateLimiter(namespace, numOfPriorityForList, p.config.VisibilityListMaxQPS(namespace))
	if ok, _ := rateLimiter.GetToken(0, 1); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListOpenWorkflowExecutions(request)
}

func (p *visibilitySamplingClient) ListClosedWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	namespace := request.Namespace

	rateLimiter := p.rateLimitersForList.getRateLimiter(namespace, numOfPriorityForList, p.config.VisibilityListMaxQPS(namespace))
	if ok, _ := rateLimiter.GetToken(0, 1); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListClosedWorkflowExecutions(request)
}

func (p *visibilitySamplingClient) ListOpenWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	namespace := request.Namespace

	rateLimiter := p.rateLimitersForList.getRateLimiter(namespace, numOfPriorityForList, p.config.VisibilityListMaxQPS(namespace))
	if ok, _ := rateLimiter.GetToken(0, 1); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListOpenWorkflowExecutionsByType(request)
}

func (p *visibilitySamplingClient) ListClosedWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	namespace := request.Namespace

	rateLimiter := p.rateLimitersForList.getRateLimiter(namespace, numOfPriorityForList, p.config.VisibilityListMaxQPS(namespace))
	if ok, _ := rateLimiter.GetToken(0, 1); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListClosedWorkflowExecutionsByType(request)
}

func (p *visibilitySamplingClient) ListOpenWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	namespace := request.Namespace

	rateLimiter := p.rateLimitersForList.getRateLimiter(namespace, numOfPriorityForList, p.config.VisibilityListMaxQPS(namespace))
	if ok, _ := rateLimiter.GetToken(0, 1); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListOpenWorkflowExecutionsByWorkflowID(request)
}

func (p *visibilitySamplingClient) ListClosedWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	namespace := request.Namespace

	rateLimiter := p.rateLimitersForList.getRateLimiter(namespace, numOfPriorityForList, p.config.VisibilityListMaxQPS(namespace))
	if ok, _ := rateLimiter.GetToken(0, 1); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListClosedWorkflowExecutionsByWorkflowID(request)
}

func (p *visibilitySamplingClient) ListClosedWorkflowExecutionsByStatus(request *ListClosedWorkflowExecutionsByStatusRequest) (*ListWorkflowExecutionsResponse, error) {
	namespace := request.Namespace

	rateLimiter := p.rateLimitersForList.getRateLimiter(namespace, numOfPriorityForList, p.config.VisibilityListMaxQPS(namespace))
	if ok, _ := rateLimiter.GetToken(0, 1); !ok {
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

func getRequestPriority(request *RecordWorkflowExecutionClosedRequest) int {
	priority := 0
	if request.Status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		priority = 1 // low priority for completed workflows
	}
	return priority
}
