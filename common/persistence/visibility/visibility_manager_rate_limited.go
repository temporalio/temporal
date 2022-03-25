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
	"context"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
)

var _ manager.VisibilityManager = (*visibilityManagerRateLimited)(nil)

type visibilityManagerRateLimited struct {
	delegate         manager.VisibilityManager
	readRateLimiter  quotas.RateLimiter
	writeRateLimiter quotas.RateLimiter
}

func NewVisibilityManagerRateLimited(
	delegate manager.VisibilityManager,
	readMaxQPS dynamicconfig.IntPropertyFn,
	writeMaxQPS dynamicconfig.IntPropertyFn,
) *visibilityManagerRateLimited {
	readRateLimiter := quotas.NewDefaultOutgoingRateLimiter(
		func() float64 { return float64(readMaxQPS()) },
	)
	writeRateLimiter := quotas.NewDefaultOutgoingRateLimiter(
		func() float64 { return float64(writeMaxQPS()) },
	)
	return &visibilityManagerRateLimited{
		delegate:         delegate,
		readRateLimiter:  readRateLimiter,
		writeRateLimiter: writeRateLimiter,
	}
}

func (m *visibilityManagerRateLimited) Close() {
	m.delegate.Close()
}

func (m *visibilityManagerRateLimited) GetName() string {
	return m.delegate.GetName()
}

// Below are write APIs.

func (m *visibilityManagerRateLimited) RecordWorkflowExecutionStarted(
	ctx context.Context,
	request *manager.RecordWorkflowExecutionStartedRequest,
) error {
	if ok := m.writeRateLimiter.Allow(); !ok {
		return persistence.ErrPersistenceLimitExceeded
	}
	return m.delegate.RecordWorkflowExecutionStarted(ctx, request)
}

func (m *visibilityManagerRateLimited) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *manager.RecordWorkflowExecutionClosedRequest,
) error {
	if ok := m.writeRateLimiter.Allow(); !ok {
		return persistence.ErrPersistenceLimitExceeded
	}
	return m.delegate.RecordWorkflowExecutionClosed(ctx, request)
}

func (m *visibilityManagerRateLimited) UpsertWorkflowExecution(
	ctx context.Context,
	request *manager.UpsertWorkflowExecutionRequest,
) error {
	if ok := m.writeRateLimiter.Allow(); !ok {
		return persistence.ErrPersistenceLimitExceeded
	}
	return m.delegate.UpsertWorkflowExecution(ctx, request)
}

func (m *visibilityManagerRateLimited) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	if ok := m.writeRateLimiter.Allow(); !ok {
		return persistence.ErrPersistenceLimitExceeded
	}
	return m.delegate.DeleteWorkflowExecution(ctx, request)
}

// Below are read APIs.

func (m *visibilityManagerRateLimited) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := m.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}
	return m.delegate.ListOpenWorkflowExecutions(ctx, request)
}

func (m *visibilityManagerRateLimited) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := m.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}
	return m.delegate.ListClosedWorkflowExecutions(ctx, request)
}

func (m *visibilityManagerRateLimited) ListOpenWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := m.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}
	return m.delegate.ListOpenWorkflowExecutionsByType(ctx, request)
}

func (m *visibilityManagerRateLimited) ListClosedWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := m.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}
	return m.delegate.ListClosedWorkflowExecutionsByType(ctx, request)
}

func (m *visibilityManagerRateLimited) ListOpenWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := m.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}
	return m.delegate.ListOpenWorkflowExecutionsByWorkflowID(ctx, request)
}

func (m *visibilityManagerRateLimited) ListClosedWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := m.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}
	return m.delegate.ListClosedWorkflowExecutionsByWorkflowID(ctx, request)
}

func (m *visibilityManagerRateLimited) ListClosedWorkflowExecutionsByStatus(
	ctx context.Context,
	request *manager.ListClosedWorkflowExecutionsByStatusRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := m.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}
	return m.delegate.ListClosedWorkflowExecutionsByStatus(ctx, request)
}

func (m *visibilityManagerRateLimited) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := m.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}
	return m.delegate.ListWorkflowExecutions(ctx, request)
}

func (m *visibilityManagerRateLimited) ScanWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := m.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}
	return m.delegate.ScanWorkflowExecutions(ctx, request)
}

func (m *visibilityManagerRateLimited) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	if ok := m.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}
	return m.delegate.CountWorkflowExecutions(ctx, request)
}
