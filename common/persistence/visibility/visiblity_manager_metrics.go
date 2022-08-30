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
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

var _ manager.VisibilityManager = (*visibilityManagerMetrics)(nil)

type visibilityManagerMetrics struct {
	metricsHandler           metrics.Handler
	logger                   log.Logger
	delegate                 manager.VisibilityManager
	visibilityTypeMetricsTag metrics.Tag
}

func NewVisibilityManagerMetrics(
	delegate manager.VisibilityManager,
	metricsHandler metrics.Handler,
	logger log.Logger,
	visibilityTypeMetricsTag metrics.Tag,
) *visibilityManagerMetrics {
	return &visibilityManagerMetrics{
		metricsHandler: metricsHandler,
		logger:         logger,
		delegate:       delegate,

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
	metricsHandler := m.tagMetricsHandler(metrics.VisibilityPersistenceRecordWorkflowExecutionStartedOperation)
	startTime := time.Now()
	err := m.delegate.RecordWorkflowExecutionStarted(ctx, request)
	metricsHandler.Timer(metrics.VisibilityPersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	return m.updateErrorMetric(metricsHandler, err)
}

func (m *visibilityManagerMetrics) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *manager.RecordWorkflowExecutionClosedRequest,
) error {
	metricsHandler := m.tagMetricsHandler(metrics.VisibilityPersistenceRecordWorkflowExecutionClosedOperation)
	startTime := time.Now()
	err := m.delegate.RecordWorkflowExecutionClosed(ctx, request)
	metricsHandler.Timer(metrics.VisibilityPersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	return m.updateErrorMetric(metricsHandler, err)
}

func (m *visibilityManagerMetrics) UpsertWorkflowExecution(
	ctx context.Context,
	request *manager.UpsertWorkflowExecutionRequest,
) error {
	metricsHandler := m.tagMetricsHandler(metrics.VisibilityPersistenceUpsertWorkflowExecutionOperation)
	startTime := time.Now()
	err := m.delegate.UpsertWorkflowExecution(ctx, request)
	metricsHandler.Timer(metrics.VisibilityPersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	return m.updateErrorMetric(metricsHandler, err)
}

func (m *visibilityManagerMetrics) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	metricsHandler := m.tagMetricsHandler(metrics.VisibilityPersistenceDeleteWorkflowExecutionOperation)
	startTime := time.Now()
	err := m.delegate.DeleteWorkflowExecution(ctx, request)
	metricsHandler.Timer(metrics.VisibilityPersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	return m.updateErrorMetric(metricsHandler, err)
}

func (m *visibilityManagerMetrics) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	metricsHandler := m.tagMetricsHandler(metrics.VisibilityPersistenceListOpenWorkflowExecutionsOperation)
	startTime := time.Now()
	response, err := m.delegate.ListOpenWorkflowExecutions(ctx, request)
	metricsHandler.Timer(metrics.VisibilityPersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	return response, m.updateErrorMetric(metricsHandler, err)
}

func (m *visibilityManagerMetrics) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	metricsHandler := m.tagMetricsHandler(metrics.VisibilityPersistenceListClosedWorkflowExecutionsOperation)
	startTime := time.Now()
	response, err := m.delegate.ListClosedWorkflowExecutions(ctx, request)
	metricsHandler.Timer(metrics.VisibilityPersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	return response, m.updateErrorMetric(metricsHandler, err)
}

func (m *visibilityManagerMetrics) ListOpenWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	metricsHandler := m.tagMetricsHandler(metrics.VisibilityPersistenceListOpenWorkflowExecutionsByTypeOperation)
	startTime := time.Now()
	response, err := m.delegate.ListOpenWorkflowExecutionsByType(ctx, request)
	metricsHandler.Timer(metrics.VisibilityPersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	return response, m.updateErrorMetric(metricsHandler, err)
}

func (m *visibilityManagerMetrics) ListClosedWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	metricsHandler := m.tagMetricsHandler(metrics.VisibilityPersistenceListClosedWorkflowExecutionsByTypeOperation)
	startTime := time.Now()
	response, err := m.delegate.ListClosedWorkflowExecutionsByType(ctx, request)
	metricsHandler.Timer(metrics.VisibilityPersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	return response, m.updateErrorMetric(metricsHandler, err)
}

func (m *visibilityManagerMetrics) ListOpenWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	metricsHandler := m.tagMetricsHandler(metrics.VisibilityPersistenceListOpenWorkflowExecutionsByWorkflowIDOperation)
	startTime := time.Now()
	response, err := m.delegate.ListOpenWorkflowExecutionsByWorkflowID(ctx, request)
	metricsHandler.Timer(metrics.VisibilityPersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	return response, m.updateErrorMetric(metricsHandler, err)
}

func (m *visibilityManagerMetrics) ListClosedWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	metricsHandler := m.tagMetricsHandler(metrics.VisibilityPersistenceListClosedWorkflowExecutionsByWorkflowIDOperation)
	startTime := time.Now()
	response, err := m.delegate.ListClosedWorkflowExecutionsByWorkflowID(ctx, request)
	metricsHandler.Timer(metrics.VisibilityPersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	return response, m.updateErrorMetric(metricsHandler, err)
}

func (m *visibilityManagerMetrics) ListClosedWorkflowExecutionsByStatus(
	ctx context.Context,
	request *manager.ListClosedWorkflowExecutionsByStatusRequest,
) (*manager.ListWorkflowExecutionsResponse, error) {
	metricsHandler := m.tagMetricsHandler(metrics.VisibilityPersistenceListClosedWorkflowExecutionsByStatusOperation)
	startTime := time.Now()
	response, err := m.delegate.ListClosedWorkflowExecutionsByStatus(ctx, request)
	metricsHandler.Timer(metrics.VisibilityPersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	return response, m.updateErrorMetric(metricsHandler, err)
}

func (m *visibilityManagerMetrics) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	metricsHandler := m.tagMetricsHandler(metrics.VisibilityPersistenceListWorkflowExecutionsOperation)
	startTime := time.Now()
	response, err := m.delegate.ListWorkflowExecutions(ctx, request)
	metricsHandler.Timer(metrics.VisibilityPersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	return response, m.updateErrorMetric(metricsHandler, err)
}

func (m *visibilityManagerMetrics) ScanWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	metricsHandler := m.tagMetricsHandler(metrics.VisibilityPersistenceScanWorkflowExecutionsOperation)
	startTime := time.Now()
	response, err := m.delegate.ScanWorkflowExecutions(ctx, request)
	metricsHandler.Timer(metrics.VisibilityPersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	return response, m.updateErrorMetric(metricsHandler, err)
}

func (m *visibilityManagerMetrics) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	metricsHandler := m.tagMetricsHandler(metrics.VisibilityPersistenceCountWorkflowExecutionsOperation)
	startTime := time.Now()
	response, err := m.delegate.CountWorkflowExecutions(ctx, request)
	metricsHandler.Timer(metrics.VisibilityPersistenceLatency.MetricName.String()).Record(time.Since(startTime))
	return response, m.updateErrorMetric(metricsHandler, err)
}

func (m *visibilityManagerMetrics) tagMetricsHandler(operation string) metrics.Handler {
	mHandler := m.metricsHandler.WithTags(
		metrics.OperationTag(operation),
		m.visibilityTypeMetricsTag,
	)
	mHandler.Counter(metrics.VisibilityPersistenceRequests.MetricName.String()).Record(1)
	return mHandler
}

func (m *visibilityManagerMetrics) updateErrorMetric(metricsHandler metrics.Handler, err error) error {
	if err == nil {
		return nil
	}

	metricsHandler.Counter(metrics.VisibilityPersistenceErrorWithType.MetricName.String()).Record(1, metrics.ServiceErrorTypeTag(err))
	switch err := err.(type) {
	case *serviceerror.InvalidArgument,
		*persistence.TimeoutError,
		*persistence.ConditionFailedError,
		*serviceerror.NotFound:
		// no-op

	case *serviceerror.ResourceExhausted:
		metricsHandler.Counter(metrics.VisibilityPersistenceResourceExhausted.MetricName.String()).Record(1, metrics.ResourceExhaustedCauseTag(err.Cause))
	default:
		m.logger.Error("Operation failed with an error.", tag.Error(err))
		metricsHandler.Counter(metrics.VisibilityPersistenceFailures.MetricName.String()).Record(1)
	}

	return err
}
