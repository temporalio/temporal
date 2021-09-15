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

package elasticsearch

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/visibility"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

type visibilityManagerMetrics struct {
	metricClient             metrics.Client
	persistence              visibility.VisibilityManager
	logger                   log.Logger
	visibilityTypeMetricsTag metrics.Tag
}

var _ visibility.VisibilityManager = (*visibilityManagerMetrics)(nil)

// NewVisibilityManagerMetrics wrap visibility client with metrics
func NewVisibilityManagerMetrics(persistence visibility.VisibilityManager, metricClient metrics.Client, logger log.Logger) visibility.VisibilityManager {
	return &visibilityManagerMetrics{
		persistence:              persistence,
		metricClient:             metricClient,
		logger:                   logger,
		visibilityTypeMetricsTag: metrics.AdvancedVisibilityTypeTag(),
	}
}

func (m *visibilityManagerMetrics) GetName() string {
	return m.persistence.GetName()
}

func (m *visibilityManagerMetrics) RecordWorkflowExecutionStarted(request *visibility.RecordWorkflowExecutionStartedRequest) error {
	scope := m.metricClient.Scope(metrics.PersistenceRecordWorkflowExecutionStartedScope, m.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	err := m.persistence.RecordWorkflowExecutionStarted(request)

	m.updateErrorMetric(scope, err)

	return err
}

func (m *visibilityManagerMetrics) RecordWorkflowExecutionClosed(request *visibility.RecordWorkflowExecutionClosedRequest) error {
	scope := m.metricClient.Scope(metrics.PersistenceRecordWorkflowExecutionClosedScope, m.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	err := m.persistence.RecordWorkflowExecutionClosed(request)

	m.updateErrorMetric(scope, err)

	return err
}

func (m *visibilityManagerMetrics) UpsertWorkflowExecution(request *visibility.UpsertWorkflowExecutionRequest) error {
	scope := m.metricClient.Scope(metrics.PersistenceUpsertWorkflowExecutionScope, m.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	err := m.persistence.UpsertWorkflowExecution(request)

	m.updateErrorMetric(scope, err)

	return err
}

func (m *visibilityManagerMetrics) ListOpenWorkflowExecutions(request *visibility.ListWorkflowExecutionsRequest) (*visibility.ListWorkflowExecutionsResponse, error) {
	scope := m.metricClient.Scope(metrics.PersistenceListOpenWorkflowExecutionsScope, m.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := m.persistence.ListOpenWorkflowExecutions(request)

	m.updateErrorMetric(scope, err)

	return response, err
}

func (m *visibilityManagerMetrics) ListClosedWorkflowExecutions(request *visibility.ListWorkflowExecutionsRequest) (*visibility.ListWorkflowExecutionsResponse, error) {
	scope := m.metricClient.Scope(metrics.PersistenceListClosedWorkflowExecutionsScope, m.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := m.persistence.ListClosedWorkflowExecutions(request)

	m.updateErrorMetric(scope, err)

	return response, err
}

func (m *visibilityManagerMetrics) ListOpenWorkflowExecutionsByType(request *visibility.ListWorkflowExecutionsByTypeRequest) (*visibility.ListWorkflowExecutionsResponse, error) {
	scope := m.metricClient.Scope(metrics.PersistenceListOpenWorkflowExecutionsByTypeScope, m.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := m.persistence.ListOpenWorkflowExecutionsByType(request)

	m.updateErrorMetric(scope, err)

	return response, err
}

func (m *visibilityManagerMetrics) ListClosedWorkflowExecutionsByType(request *visibility.ListWorkflowExecutionsByTypeRequest) (*visibility.ListWorkflowExecutionsResponse, error) {
	scope := m.metricClient.Scope(metrics.PersistenceListClosedWorkflowExecutionsScope, m.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := m.persistence.ListClosedWorkflowExecutionsByType(request)

	m.updateErrorMetric(scope, err)

	return response, err
}

func (m *visibilityManagerMetrics) ListOpenWorkflowExecutionsByWorkflowID(request *visibility.ListWorkflowExecutionsByWorkflowIDRequest) (*visibility.ListWorkflowExecutionsResponse, error) {
	scope := m.metricClient.Scope(metrics.PersistenceListOpenWorkflowExecutionsByWorkflowIDScope, m.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := m.persistence.ListOpenWorkflowExecutionsByWorkflowID(request)

	m.updateErrorMetric(scope, err)

	return response, err
}

func (m *visibilityManagerMetrics) ListClosedWorkflowExecutionsByWorkflowID(request *visibility.ListWorkflowExecutionsByWorkflowIDRequest) (*visibility.ListWorkflowExecutionsResponse, error) {
	scope := m.metricClient.Scope(metrics.PersistenceListClosedWorkflowExecutionsByWorkflowIDScope, m.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := m.persistence.ListClosedWorkflowExecutionsByWorkflowID(request)

	m.updateErrorMetric(scope, err)

	return response, err
}

func (m *visibilityManagerMetrics) ListClosedWorkflowExecutionsByStatus(request *visibility.ListClosedWorkflowExecutionsByStatusRequest) (*visibility.ListWorkflowExecutionsResponse, error) {
	scope := m.metricClient.Scope(metrics.PersistenceListClosedWorkflowExecutionsByStatusScope, m.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := m.persistence.ListClosedWorkflowExecutionsByStatus(request)

	m.updateErrorMetric(scope, err)

	return response, err
}

func (m *visibilityManagerMetrics) ListWorkflowExecutions(request *visibility.ListWorkflowExecutionsRequestV2) (*visibility.ListWorkflowExecutionsResponse, error) {
	scope := m.metricClient.Scope(metrics.PersistenceListWorkflowExecutionsScope, m.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := m.persistence.ListWorkflowExecutions(request)

	m.updateErrorMetric(scope, err)

	return response, err
}

func (m *visibilityManagerMetrics) ScanWorkflowExecutions(request *visibility.ListWorkflowExecutionsRequestV2) (*visibility.ListWorkflowExecutionsResponse, error) {
	scope := m.metricClient.Scope(metrics.PersistenceScanWorkflowExecutionsScope, m.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := m.persistence.ScanWorkflowExecutions(request)

	m.updateErrorMetric(scope, err)

	return response, err
}

func (m *visibilityManagerMetrics) CountWorkflowExecutions(request *visibility.CountWorkflowExecutionsRequest) (*visibility.CountWorkflowExecutionsResponse, error) {
	scope := m.metricClient.Scope(metrics.PersistenceCountWorkflowExecutionsScope, m.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := m.persistence.CountWorkflowExecutions(request)

	m.updateErrorMetric(scope, err)

	return response, err
}

func (m *visibilityManagerMetrics) DeleteWorkflowExecution(request *visibility.VisibilityDeleteWorkflowExecutionRequest) error {
	scope := m.metricClient.Scope(metrics.PersistenceVisibilityDeleteWorkflowExecutionScope, m.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	err := m.persistence.DeleteWorkflowExecution(request)

	m.updateErrorMetric(scope, err)

	return err
}

func (m *visibilityManagerMetrics) updateErrorMetric(scope metrics.Scope, err error) {
	if err == nil {
		return
	}
	switch err.(type) {
	case *serviceerror.InvalidArgument:
		scope.IncCounter(metrics.VisibilityPersistenceInvalidArgument)
		scope.IncCounter(metrics.VisibilityPersistenceFailures)
	case *serviceerror.ResourceExhausted:
		scope.IncCounter(metrics.VisibilityPersistenceResourceExhausted)
		scope.IncCounter(metrics.VisibilityPersistenceFailures)
	default:
		m.logger.Error("Operation failed with an error.", tag.Error(err))
		scope.IncCounter(metrics.VisibilityPersistenceFailures)
	}
}

func (m *visibilityManagerMetrics) Close() {
	m.persistence.Close()
}
