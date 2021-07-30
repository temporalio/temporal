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

package visibility

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
)

type (
	visibilityRateLimitedPersistenceClient struct {
		rateLimiter quotas.RateLimiter
		persistence VisibilityManager
		logger      log.Logger
	}
)

var _ VisibilityManager = (*visibilityRateLimitedPersistenceClient)(nil)

// NewVisibilityPersistenceRateLimitedClient creates a client to manage visibility
func NewVisibilityPersistenceRateLimitedClient(persistence VisibilityManager, rateLimiter quotas.RateLimiter, logger log.Logger) VisibilityManager {
	return &visibilityRateLimitedPersistenceClient{
		persistence: persistence,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

func (p *visibilityRateLimitedPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *visibilityRateLimitedPersistenceClient) RecordWorkflowExecutionStarted(request *RecordWorkflowExecutionStartedRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return persistence.ErrPersistenceLimitExceeded
	}

	err := p.persistence.RecordWorkflowExecutionStarted(request)
	return err
}

func (p *visibilityRateLimitedPersistenceClient) RecordWorkflowExecutionClosed(request *RecordWorkflowExecutionClosedRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return persistence.ErrPersistenceLimitExceeded
	}

	err := p.persistence.RecordWorkflowExecutionClosed(request)
	return err
}

func (p *visibilityRateLimitedPersistenceClient) UpsertWorkflowExecution(request *UpsertWorkflowExecutionRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return persistence.ErrPersistenceLimitExceeded
	}

	err := p.persistence.UpsertWorkflowExecution(request)
	return err
}

func (p *visibilityRateLimitedPersistenceClient) ListOpenWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListOpenWorkflowExecutions(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) ListClosedWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListClosedWorkflowExecutions(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) ListOpenWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListOpenWorkflowExecutionsByType(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) ListClosedWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListClosedWorkflowExecutionsByType(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) ListOpenWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListOpenWorkflowExecutionsByWorkflowID(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) ListClosedWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListClosedWorkflowExecutionsByWorkflowID(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) ListClosedWorkflowExecutionsByStatus(request *ListClosedWorkflowExecutionsByStatusRequest) (*ListWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}

	response, err := p.persistence.ListClosedWorkflowExecutionsByStatus(request)
	return response, err
}

func (p *visibilityRateLimitedPersistenceClient) DeleteWorkflowExecution(request *VisibilityDeleteWorkflowExecutionRequest) error {
	if ok := p.rateLimiter.Allow(); !ok {
		return persistence.ErrPersistenceLimitExceeded
	}
	return p.persistence.DeleteWorkflowExecution(request)
}

func (p *visibilityRateLimitedPersistenceClient) ListWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*ListWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}
	return p.persistence.ListWorkflowExecutions(request)
}

func (p *visibilityRateLimitedPersistenceClient) ScanWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*ListWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}
	return p.persistence.ScanWorkflowExecutions(request)
}

func (p *visibilityRateLimitedPersistenceClient) CountWorkflowExecutions(request *CountWorkflowExecutionsRequest) (*CountWorkflowExecutionsResponse, error) {
	if ok := p.rateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}
	return p.persistence.CountWorkflowExecutions(request)
}

func (p *visibilityRateLimitedPersistenceClient) Close() {
	p.persistence.Close()
}
