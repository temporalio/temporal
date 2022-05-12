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

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

var _ manager.VisibilityManager = (*visibilityManagerMetrics)(nil)

type visibilityManagerMetrics struct {
	metricClient             metrics.Client
	logger                   log.Logger
	delegate                 manager.VisibilityManager
	visibilityTypeMetricsTag metrics.Tag
}

func NewVisibilityManagerMetrics(
	delegate manager.VisibilityManager,
	metricClient metrics.Client,
	logger log.Logger,
	visibilityTypeMetricsTag metrics.Tag,
) *visibilityManagerMetrics {
	return &visibilityManagerMetrics{
		metricClient: metricClient,
		logger:       logger,
		delegate:     delegate,

		visibilityTypeMetricsTag: visibilityTypeMetricsTag,
	}
}

func (m *visibilityManagerMetrics) Close() {
	m.delegate.Close()
}

func (m *visibilityManagerMetrics) GetName() string {
	return m.delegate.GetName()
}

func (m *visibilityManagerMetrics) RecordWorkflowExecutionStarted(
	ctx context.Context,
	request *manager.RecordWorkflowExecutionStartedRequest,
) error {
	scope, sw := m.tagScope(metrics.VisibilityPersistenceRecordWorkflowExecutionStartedScope)
	err := m.delegate.RecordWorkflowExecutionStarted(ctx, request)
	sw.Stop()
	return m.updateErrorMetric(scope, err)
}

func (m *visibilityManagerMetrics) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *manager.RecordWorkflowExecutionClosedRequest,
) error {
	scope, sw := m.tagScope(metrics.VisibilityPersistenceRecordWorkflowExecutionClosedScope)
	err := m.delegate.RecordWorkflowExecutionClosed(ctx, request)
	sw.Stop()
	return m.updateErrorMetric(scope, err)
}

func (m *visibilityManagerMetrics) UpsertWorkflowExecution(
	ctx context.Context,
	request *manager.UpsertWorkflowExecutionRequest,
) error {
	scope, sw := m.tagScope(metrics.VisibilityPersistenceUpsertWorkflowExecutionScope)
	err := m.delegate.UpsertWorkflowExecution(ctx, request)
	sw.Stop()
	return m.updateErrorMetric(scope, err)
}

func (m *visibilityManagerMetrics) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	scope, sw := m.tagScope(metrics.VisibilityPersistenceDeleteWorkflowExecutionScope)
	err := m.delegate.DeleteWorkflowExecution(ctx, request)
	sw.Stop()
	return m.updateErrorMetric(scope, err)
}

func (m *visibilityManagerMetrics) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	scope, sw := m.tagScope(metrics.VisibilityPersistenceListOpenWorkflowExecutionsScope)
	response, err := m.delegate.ListOpenWorkflowExecutions(ctx, request)
	sw.Stop()
	return response, m.updateErrorMetric(scope, err)
}

func (m *visibilityManagerMetrics) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	scope, sw := m.tagScope(metrics.VisibilityPersistenceListClosedWorkflowExecutionsScope)
	response, err := m.delegate.ListClosedWorkflowExecutions(ctx, request)
	sw.Stop()
	return response, m.updateErrorMetric(scope, err)
}

func (m *visibilityManagerMetrics) ListOpenWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	scope, sw := m.tagScope(metrics.VisibilityPersistenceListOpenWorkflowExecutionsByTypeScope)
	response, err := m.delegate.ListOpenWorkflowExecutionsByType(ctx, request)
	sw.Stop()
	return response, m.updateErrorMetric(scope, err)
}

func (m *visibilityManagerMetrics) ListClosedWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	scope, sw := m.tagScope(metrics.VisibilityPersistenceListClosedWorkflowExecutionsByTypeScope)
	response, err := m.delegate.ListClosedWorkflowExecutionsByType(ctx, request)
	sw.Stop()
	return response, m.updateErrorMetric(scope, err)
}

func (m *visibilityManagerMetrics) ListOpenWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	scope, sw := m.tagScope(metrics.VisibilityPersistenceListOpenWorkflowExecutionsByWorkflowIDScope)
	response, err := m.delegate.ListOpenWorkflowExecutionsByWorkflowID(ctx, request)
	sw.Stop()
	return response, m.updateErrorMetric(scope, err)
}

func (m *visibilityManagerMetrics) ListClosedWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	scope, sw := m.tagScope(metrics.VisibilityPersistenceListClosedWorkflowExecutionsByWorkflowIDScope)
	response, err := m.delegate.ListClosedWorkflowExecutionsByWorkflowID(ctx, request)
	sw.Stop()
	return response, m.updateErrorMetric(scope, err)
}

func (m *visibilityManagerMetrics) ListClosedWorkflowExecutionsByStatus(
	ctx context.Context,
	request *manager.ListClosedWorkflowExecutionsByStatusRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	scope, sw := m.tagScope(metrics.VisibilityPersistenceListClosedWorkflowExecutionsByStatusScope)
	response, err := m.delegate.ListClosedWorkflowExecutionsByStatus(ctx, request)
	sw.Stop()
	return response, m.updateErrorMetric(scope, err)
}

func (m *visibilityManagerMetrics) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	scope, sw := m.tagScope(metrics.VisibilityPersistenceListWorkflowExecutionsScope)
	response, err := m.delegate.ListWorkflowExecutions(ctx, request)
	sw.Stop()
	return response, m.updateErrorMetric(scope, err)
}

func (m *visibilityManagerMetrics) ScanWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	scope, sw := m.tagScope(metrics.VisibilityPersistenceScanWorkflowExecutionsScope)
	response, err := m.delegate.ScanWorkflowExecutions(ctx, request)
	sw.Stop()
	return response, m.updateErrorMetric(scope, err)
}

func (m *visibilityManagerMetrics) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	scope, sw := m.tagScope(metrics.VisibilityPersistenceCountWorkflowExecutionsScope)
	response, err := m.delegate.CountWorkflowExecutions(ctx, request)
	sw.Stop()
	return response, m.updateErrorMetric(scope, err)
}

func (m *visibilityManagerMetrics) tagScope(scope int) (metrics.Scope, metrics.Stopwatch) {
	taggedScope := m.metricClient.Scope(scope, m.visibilityTypeMetricsTag)
	taggedScope.IncCounter(metrics.VisibilityPersistenceRequests)
	return taggedScope, taggedScope.StartTimer(metrics.VisibilityPersistenceLatency)
}

func (m *visibilityManagerMetrics) updateErrorMetric(scope metrics.Scope, err error) error {
	switch err.(type) {
	case nil:
		return nil
	case *serviceerror.InvalidArgument:
		scope.IncCounter(metrics.VisibilityPersistenceInvalidArgument)
		scope.IncCounter(metrics.VisibilityPersistenceFailures)
	case *persistence.TimeoutError:
		scope.IncCounter(metrics.VisibilityPersistenceTimeout)
		scope.IncCounter(metrics.VisibilityPersistenceFailures)
	case *serviceerror.ResourceExhausted:
		scope.Tagged(metrics.ResourceExhaustedCauseTag(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED)).
			IncCounter(metrics.VisibilityPersistenceResourceExhausted)
		scope.IncCounter(metrics.VisibilityPersistenceFailures)
	case *serviceerror.Internal:
		scope.IncCounter(metrics.VisibilityPersistenceInternal)
		scope.IncCounter(metrics.VisibilityPersistenceFailures)
	case *serviceerror.Unavailable:
		scope.IncCounter(metrics.VisibilityPersistenceUnavailable)
		scope.IncCounter(metrics.VisibilityPersistenceFailures)
	case *persistence.ConditionFailedError:
		scope.IncCounter(metrics.VisibilityPersistenceConditionFailed)
	case *serviceerror.NotFound:
		scope.IncCounter(metrics.VisibilityPersistenceNotFound)
	default:
		m.logger.Error("Operation failed with an error.", tag.Error(err))
		scope.IncCounter(metrics.VisibilityPersistenceFailures)
	}

	return err
}
