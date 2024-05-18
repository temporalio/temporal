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
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

var _ manager.VisibilityManager = (*visibilityManagerMetrics)(nil)

type visibilityManagerMetrics struct {
	metricHandler metrics.Handler
	logger        log.Logger
	delegate      manager.VisibilityManager

	visibilityPluginNameMetricsTag metrics.Tag
}

func NewVisibilityManagerMetrics(
	delegate manager.VisibilityManager,
	metricHandler metrics.Handler,
	logger log.Logger,
	visibilityPluginNameMetricsTag metrics.Tag,
) *visibilityManagerMetrics {
	return &visibilityManagerMetrics{
		metricHandler: metricHandler,
		logger:        logger,
		delegate:      delegate,

		visibilityPluginNameMetricsTag: visibilityPluginNameMetricsTag,
	}
}

func (m *visibilityManagerMetrics) Close() {
	m.delegate.Close()
}

func (m *visibilityManagerMetrics) GetReadStoreName(nsName namespace.Name) string {
	return m.delegate.GetReadStoreName(nsName)
}

func (m *visibilityManagerMetrics) GetStoreNames() []string {
	return m.delegate.GetStoreNames()
}

func (m *visibilityManagerMetrics) HasStoreName(stName string) bool {
	return m.delegate.HasStoreName(stName)
}

func (m *visibilityManagerMetrics) GetIndexName() string {
	return m.delegate.GetIndexName()
}

func (m *visibilityManagerMetrics) ValidateCustomSearchAttributes(
	searchAttributes map[string]any,
) (map[string]any, error) {
	return m.delegate.ValidateCustomSearchAttributes(searchAttributes)
}

func (m *visibilityManagerMetrics) RecordWorkflowExecutionStarted(
	ctx context.Context,
	request *manager.RecordWorkflowExecutionStartedRequest,
) error {
	handler, startTime := m.tagScope(metrics.VisibilityPersistenceRecordWorkflowExecutionStartedScope)
	err := m.delegate.RecordWorkflowExecutionStarted(ctx, request)
	metrics.VisibilityPersistenceLatency.With(handler).Record(time.Since(startTime))
	return m.updateErrorMetric(handler, err)
}

func (m *visibilityManagerMetrics) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *manager.RecordWorkflowExecutionClosedRequest,
) error {
	handler, startTime := m.tagScope(metrics.VisibilityPersistenceRecordWorkflowExecutionClosedScope)
	err := m.delegate.RecordWorkflowExecutionClosed(ctx, request)
	metrics.VisibilityPersistenceLatency.With(handler).Record(time.Since(startTime))
	return m.updateErrorMetric(handler, err)
}

func (m *visibilityManagerMetrics) UpsertWorkflowExecution(
	ctx context.Context,
	request *manager.UpsertWorkflowExecutionRequest,
) error {
	handler, startTime := m.tagScope(metrics.VisibilityPersistenceUpsertWorkflowExecutionScope)
	err := m.delegate.UpsertWorkflowExecution(ctx, request)
	metrics.VisibilityPersistenceLatency.With(handler).Record(time.Since(startTime))
	return m.updateErrorMetric(handler, err)
}

func (m *visibilityManagerMetrics) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	handler, startTime := m.tagScope(metrics.VisibilityPersistenceDeleteWorkflowExecutionScope)
	err := m.delegate.DeleteWorkflowExecution(ctx, request)
	metrics.VisibilityPersistenceLatency.With(handler).Record(time.Since(startTime))
	return m.updateErrorMetric(handler, err)
}

func (m *visibilityManagerMetrics) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	handler, startTime := m.tagScope(metrics.VisibilityPersistenceListWorkflowExecutionsScope)
	response, err := m.delegate.ListWorkflowExecutions(ctx, request)
	metrics.VisibilityPersistenceLatency.With(handler).Record(time.Since(startTime))
	return response, m.updateErrorMetric(handler, err)
}

func (m *visibilityManagerMetrics) ScanWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	handler, startTime := m.tagScope(metrics.VisibilityPersistenceScanWorkflowExecutionsScope)
	response, err := m.delegate.ScanWorkflowExecutions(ctx, request)
	metrics.VisibilityPersistenceLatency.With(handler).Record(time.Since(startTime))
	return response, m.updateErrorMetric(handler, err)
}

func (m *visibilityManagerMetrics) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	handler, startTime := m.tagScope(metrics.VisibilityPersistenceCountWorkflowExecutionsScope)
	response, err := m.delegate.CountWorkflowExecutions(ctx, request)
	metrics.VisibilityPersistenceLatency.With(handler).Record(time.Since(startTime))
	return response, m.updateErrorMetric(handler, err)
}

func (m *visibilityManagerMetrics) GetWorkflowExecution(
	ctx context.Context,
	request *manager.GetWorkflowExecutionRequest,
) (*manager.GetWorkflowExecutionResponse, error) {
	handler, startTime := m.tagScope(metrics.VisibilityPersistenceGetWorkflowExecutionScope)
	response, err := m.delegate.GetWorkflowExecution(ctx, request)
	metrics.VisibilityPersistenceLatency.With(handler).Record(time.Since(startTime))
	return response, m.updateErrorMetric(handler, err)
}

func (m *visibilityManagerMetrics) tagScope(operation string) (metrics.Handler, time.Time) {
	taggedHandler := m.metricHandler.WithTags(metrics.OperationTag(operation), m.visibilityPluginNameMetricsTag)
	metrics.VisibilityPersistenceRequests.With(taggedHandler).Record(1)
	return taggedHandler, time.Now().UTC()
}

func (m *visibilityManagerMetrics) updateErrorMetric(handler metrics.Handler, err error) error {
	if err == nil {
		return nil
	}

	metrics.VisibilityPersistenceErrorWithType.With(handler).Record(1, metrics.ServiceErrorTypeTag(err))
	switch err := err.(type) {
	case *serviceerror.InvalidArgument,
		*persistence.TimeoutError,
		*persistence.ConditionFailedError,
		*serviceerror.NotFound:
		// no-op

	case *serviceerror.ResourceExhausted:
		metrics.VisibilityPersistenceResourceExhausted.With(handler).Record(1, metrics.ResourceExhaustedCauseTag(err.Cause))
	default:
		m.logger.Error("Operation failed with an error.", tag.Error(err))
		metrics.VisibilityPersistenceFailures.With(handler).Record(1)
	}

	return err
}
