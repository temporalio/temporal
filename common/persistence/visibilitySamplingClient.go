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

package persistence

import (
	"sync"

	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service/config"
)

const (
	// To sample visibility request, open has only 1 bucket, closed has 2
	numOfPriorityForOpen   = 1
	numOfPriorityForClosed = 2
	numOfPriorityForList   = 1
)

type visibilitySamplingClient struct {
	rateLimitersForOpen   *domainToBucketMap
	rateLimitersForClosed *domainToBucketMap
	rateLimitersForList   *domainToBucketMap
	persistence           VisibilityManager
	config                *config.VisibilityConfig
	metricClient          metrics.Client
	logger                bark.Logger
}

var _ VisibilityManager = (*visibilitySamplingClient)(nil)

// NewVisibilitySamplingClient creates a client to manage visibility with sampling
func NewVisibilitySamplingClient(persistence VisibilityManager, config *config.VisibilityConfig, metricClient metrics.Client, logger bark.Logger) VisibilityManager {
	return &visibilitySamplingClient{
		persistence:           persistence,
		rateLimitersForOpen:   newDomainToBucketMap(),
		rateLimitersForClosed: newDomainToBucketMap(),
		rateLimitersForList:   newDomainToBucketMap(),
		config:                config,
		metricClient:          metricClient,
		logger:                logger,
	}
}

type domainToBucketMap struct {
	sync.RWMutex
	mappings map[string]common.PriorityTokenBucket
}

func newDomainToBucketMap() *domainToBucketMap {
	return &domainToBucketMap{
		mappings: make(map[string]common.PriorityTokenBucket),
	}
}

func (m *domainToBucketMap) getRateLimiter(domain string, numOfPriority, qps int) common.PriorityTokenBucket {
	m.RLock()
	rateLimiter, exist := m.mappings[domain]
	m.RUnlock()

	if exist {
		return rateLimiter
	}

	m.Lock()
	if rateLimiter, ok := m.mappings[domain]; ok { // read again to ensure no duplicate create
		m.Unlock()
		return rateLimiter
	}
	rateLimiter = common.NewFullPriorityTokenBucket(numOfPriority, qps, common.NewRealTimeSource())
	m.mappings[domain] = rateLimiter
	m.Unlock()
	return rateLimiter
}

func (p *visibilitySamplingClient) RecordWorkflowExecutionStarted(request *RecordWorkflowExecutionStartedRequest) error {
	domain := request.Domain

	rateLimiter := p.rateLimitersForOpen.getRateLimiter(domain, numOfPriorityForOpen, p.config.VisibilityOpenMaxQPS(domain))
	if ok, _ := rateLimiter.GetToken(0, 1); ok {
		return p.persistence.RecordWorkflowExecutionStarted(request)
	}

	logging.LogOpenWorkflowSampled(p.logger, domain, request.Execution.GetWorkflowId(), request.Execution.GetRunId(), request.WorkflowTypeName)
	p.metricClient.IncCounter(metrics.PersistenceRecordWorkflowExecutionStartedScope, metrics.PersistenceSampledCounter)
	return nil
}

func (p *visibilitySamplingClient) RecordWorkflowExecutionClosed(request *RecordWorkflowExecutionClosedRequest) error {
	domain := request.Domain
	priority := getRequestPriority(request)

	rateLimiter := p.rateLimitersForClosed.getRateLimiter(domain, numOfPriorityForClosed, p.config.VisibilityClosedMaxQPS(domain))
	if ok, _ := rateLimiter.GetToken(priority, 1); ok {
		return p.persistence.RecordWorkflowExecutionClosed(request)
	}

	logging.LogClosedWorkflowSampled(p.logger, domain, request.Execution.GetWorkflowId(), request.Execution.GetRunId(), request.WorkflowTypeName)
	p.metricClient.IncCounter(metrics.PersistenceRecordWorkflowExecutionClosedScope, metrics.PersistenceSampledCounter)
	return nil
}

func (p *visibilitySamplingClient) ListOpenWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	domain := request.Domain

	rateLimiter := p.rateLimitersForList.getRateLimiter(domain, numOfPriorityForList, p.config.VisibilityListMaxQPS(domain))
	if ok, _ := rateLimiter.GetToken(0, 1); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListOpenWorkflowExecutions(request)
}

func (p *visibilitySamplingClient) ListClosedWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	domain := request.Domain

	rateLimiter := p.rateLimitersForList.getRateLimiter(domain, numOfPriorityForList, p.config.VisibilityListMaxQPS(domain))
	if ok, _ := rateLimiter.GetToken(0, 1); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListClosedWorkflowExecutions(request)
}

func (p *visibilitySamplingClient) ListOpenWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	domain := request.Domain

	rateLimiter := p.rateLimitersForList.getRateLimiter(domain, numOfPriorityForList, p.config.VisibilityListMaxQPS(domain))
	if ok, _ := rateLimiter.GetToken(0, 1); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListOpenWorkflowExecutionsByType(request)
}

func (p *visibilitySamplingClient) ListClosedWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	domain := request.Domain

	rateLimiter := p.rateLimitersForList.getRateLimiter(domain, numOfPriorityForList, p.config.VisibilityListMaxQPS(domain))
	if ok, _ := rateLimiter.GetToken(0, 1); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListClosedWorkflowExecutionsByType(request)
}

func (p *visibilitySamplingClient) ListOpenWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	domain := request.Domain

	rateLimiter := p.rateLimitersForList.getRateLimiter(domain, numOfPriorityForList, p.config.VisibilityListMaxQPS(domain))
	if ok, _ := rateLimiter.GetToken(0, 1); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListOpenWorkflowExecutionsByWorkflowID(request)
}

func (p *visibilitySamplingClient) ListClosedWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	domain := request.Domain

	rateLimiter := p.rateLimitersForList.getRateLimiter(domain, numOfPriorityForList, p.config.VisibilityListMaxQPS(domain))
	if ok, _ := rateLimiter.GetToken(0, 1); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListClosedWorkflowExecutionsByWorkflowID(request)
}

func (p *visibilitySamplingClient) ListClosedWorkflowExecutionsByStatus(request *ListClosedWorkflowExecutionsByStatusRequest) (*ListWorkflowExecutionsResponse, error) {
	domain := request.Domain

	rateLimiter := p.rateLimitersForList.getRateLimiter(domain, numOfPriorityForList, p.config.VisibilityListMaxQPS(domain))
	if ok, _ := rateLimiter.GetToken(0, 1); !ok {
		return nil, ErrPersistenceLimitExceededForList
	}

	return p.persistence.ListClosedWorkflowExecutionsByStatus(request)
}

func (p *visibilitySamplingClient) GetClosedWorkflowExecution(request *GetClosedWorkflowExecutionRequest) (*GetClosedWorkflowExecutionResponse, error) {
	return p.persistence.GetClosedWorkflowExecution(request)
}

func (p *visibilitySamplingClient) Close() {
	p.persistence.Close()
}

func (p *visibilitySamplingClient) GetName() string {
	return p.persistence.GetName()
}

func getRequestPriority(request *RecordWorkflowExecutionClosedRequest) int {
	priority := 0
	if request.Status == workflow.WorkflowExecutionCloseStatusCompleted {
		priority = 1 // low priority for completed workflows
	}
	return priority
}
