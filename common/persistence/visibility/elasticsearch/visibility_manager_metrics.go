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
	metricClient metrics.Client
	persistence  visibility.VisibilityManager
	logger       log.Logger
}

var _ visibility.VisibilityManager = (*visibilityManagerMetrics)(nil)

// NewVisibilityManagerMetrics wrap visibility client with metrics
func NewVisibilityManagerMetrics(persistence visibility.VisibilityManager, metricClient metrics.Client, logger log.Logger) visibility.VisibilityManager {
	return &visibilityManagerMetrics{
		persistence:  persistence,
		metricClient: metricClient,
		logger:       logger,
	}
}

func (m *visibilityManagerMetrics) GetName() string {
	return m.persistence.GetName()
}

func (m *visibilityManagerMetrics) RecordWorkflowExecutionStarted(request *visibility.RecordWorkflowExecutionStartedRequest) error {
	m.metricClient.IncCounter(metrics.ElasticsearchRecordWorkflowExecutionStartedScope, metrics.ElasticsearchRequests)

	sw := m.metricClient.StartTimer(metrics.ElasticsearchRecordWorkflowExecutionStartedScope, metrics.ElasticsearchLatency)
	err := m.persistence.RecordWorkflowExecutionStarted(request)
	sw.Stop()

	if err != nil {
		m.updateErrorMetric(metrics.ElasticsearchRecordWorkflowExecutionStartedScope, err)
	}

	return err
}

func (m *visibilityManagerMetrics) RecordWorkflowExecutionClosed(request *visibility.RecordWorkflowExecutionClosedRequest) error {
	m.metricClient.IncCounter(metrics.ElasticsearchRecordWorkflowExecutionClosedScope, metrics.ElasticsearchRequests)

	sw := m.metricClient.StartTimer(metrics.ElasticsearchRecordWorkflowExecutionClosedScope, metrics.ElasticsearchLatency)
	err := m.persistence.RecordWorkflowExecutionClosed(request)
	sw.Stop()

	if err != nil {
		m.updateErrorMetric(metrics.ElasticsearchRecordWorkflowExecutionClosedScope, err)
	}

	return err
}

func (m *visibilityManagerMetrics) UpsertWorkflowExecution(request *visibility.UpsertWorkflowExecutionRequest) error {
	m.metricClient.IncCounter(metrics.ElasticsearchUpsertWorkflowExecutionScope, metrics.ElasticsearchRequests)

	sw := m.metricClient.StartTimer(metrics.ElasticsearchUpsertWorkflowExecutionScope, metrics.ElasticsearchLatency)
	err := m.persistence.UpsertWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		m.updateErrorMetric(metrics.ElasticsearchUpsertWorkflowExecutionScope, err)
	}

	return err
}

func (m *visibilityManagerMetrics) ListOpenWorkflowExecutions(request *visibility.ListWorkflowExecutionsRequest) (*visibility.ListWorkflowExecutionsResponse, error) {
	m.metricClient.IncCounter(metrics.ElasticsearchListOpenWorkflowExecutionsScope, metrics.ElasticsearchRequests)

	sw := m.metricClient.StartTimer(metrics.ElasticsearchListOpenWorkflowExecutionsScope, metrics.ElasticsearchLatency)
	response, err := m.persistence.ListOpenWorkflowExecutions(request)
	sw.Stop()

	if err != nil {
		m.updateErrorMetric(metrics.ElasticsearchListOpenWorkflowExecutionsScope, err)
	}

	return response, err
}

func (m *visibilityManagerMetrics) ListClosedWorkflowExecutions(request *visibility.ListWorkflowExecutionsRequest) (*visibility.ListWorkflowExecutionsResponse, error) {
	m.metricClient.IncCounter(metrics.ElasticsearchListClosedWorkflowExecutionsScope, metrics.ElasticsearchRequests)

	sw := m.metricClient.StartTimer(metrics.ElasticsearchListClosedWorkflowExecutionsScope, metrics.ElasticsearchLatency)
	response, err := m.persistence.ListClosedWorkflowExecutions(request)
	sw.Stop()

	if err != nil {
		m.updateErrorMetric(metrics.ElasticsearchListClosedWorkflowExecutionsScope, err)
	}

	return response, err
}

func (m *visibilityManagerMetrics) ListOpenWorkflowExecutionsByType(request *visibility.ListWorkflowExecutionsByTypeRequest) (*visibility.ListWorkflowExecutionsResponse, error) {
	m.metricClient.IncCounter(metrics.ElasticsearchListOpenWorkflowExecutionsByTypeScope, metrics.ElasticsearchRequests)

	sw := m.metricClient.StartTimer(metrics.ElasticsearchListOpenWorkflowExecutionsByTypeScope, metrics.ElasticsearchLatency)
	response, err := m.persistence.ListOpenWorkflowExecutionsByType(request)
	sw.Stop()

	if err != nil {
		m.updateErrorMetric(metrics.ElasticsearchListOpenWorkflowExecutionsByTypeScope, err)
	}

	return response, err
}

func (m *visibilityManagerMetrics) ListClosedWorkflowExecutionsByType(request *visibility.ListWorkflowExecutionsByTypeRequest) (*visibility.ListWorkflowExecutionsResponse, error) {
	m.metricClient.IncCounter(metrics.ElasticsearchListClosedWorkflowExecutionsByTypeScope, metrics.ElasticsearchRequests)

	sw := m.metricClient.StartTimer(metrics.ElasticsearchListClosedWorkflowExecutionsByTypeScope, metrics.ElasticsearchLatency)
	response, err := m.persistence.ListClosedWorkflowExecutionsByType(request)
	sw.Stop()

	if err != nil {
		m.updateErrorMetric(metrics.ElasticsearchListClosedWorkflowExecutionsByTypeScope, err)
	}

	return response, err
}

func (m *visibilityManagerMetrics) ListOpenWorkflowExecutionsByWorkflowID(request *visibility.ListWorkflowExecutionsByWorkflowIDRequest) (*visibility.ListWorkflowExecutionsResponse, error) {
	m.metricClient.IncCounter(metrics.ElasticsearchListOpenWorkflowExecutionsByWorkflowIDScope, metrics.ElasticsearchRequests)

	sw := m.metricClient.StartTimer(metrics.ElasticsearchListOpenWorkflowExecutionsByWorkflowIDScope, metrics.ElasticsearchLatency)
	response, err := m.persistence.ListOpenWorkflowExecutionsByWorkflowID(request)
	sw.Stop()

	if err != nil {
		m.updateErrorMetric(metrics.ElasticsearchListOpenWorkflowExecutionsByWorkflowIDScope, err)
	}

	return response, err
}

func (m *visibilityManagerMetrics) ListClosedWorkflowExecutionsByWorkflowID(request *visibility.ListWorkflowExecutionsByWorkflowIDRequest) (*visibility.ListWorkflowExecutionsResponse, error) {
	m.metricClient.IncCounter(metrics.ElasticsearchListClosedWorkflowExecutionsByWorkflowIDScope, metrics.ElasticsearchRequests)

	sw := m.metricClient.StartTimer(metrics.ElasticsearchListClosedWorkflowExecutionsByWorkflowIDScope, metrics.ElasticsearchLatency)
	response, err := m.persistence.ListClosedWorkflowExecutionsByWorkflowID(request)
	sw.Stop()

	if err != nil {
		m.updateErrorMetric(metrics.ElasticsearchListClosedWorkflowExecutionsByWorkflowIDScope, err)
	}

	return response, err
}

func (m *visibilityManagerMetrics) ListClosedWorkflowExecutionsByStatus(request *visibility.ListClosedWorkflowExecutionsByStatusRequest) (*visibility.ListWorkflowExecutionsResponse, error) {
	m.metricClient.IncCounter(metrics.ElasticsearchListClosedWorkflowExecutionsByStatusScope, metrics.ElasticsearchRequests)

	sw := m.metricClient.StartTimer(metrics.ElasticsearchListClosedWorkflowExecutionsByStatusScope, metrics.ElasticsearchLatency)
	response, err := m.persistence.ListClosedWorkflowExecutionsByStatus(request)
	sw.Stop()

	if err != nil {
		m.updateErrorMetric(metrics.ElasticsearchListClosedWorkflowExecutionsByStatusScope, err)
	}

	return response, err
}

func (m *visibilityManagerMetrics) GetClosedWorkflowExecution(request *visibility.GetClosedWorkflowExecutionRequest) (*visibility.GetClosedWorkflowExecutionResponse, error) {
	m.metricClient.IncCounter(metrics.ElasticsearchGetClosedWorkflowExecutionScope, metrics.ElasticsearchRequests)

	sw := m.metricClient.StartTimer(metrics.ElasticsearchGetClosedWorkflowExecutionScope, metrics.ElasticsearchLatency)
	response, err := m.persistence.GetClosedWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		m.updateErrorMetric(metrics.ElasticsearchGetClosedWorkflowExecutionScope, err)
	}

	return response, err
}

func (m *visibilityManagerMetrics) ListWorkflowExecutions(request *visibility.ListWorkflowExecutionsRequestV2) (*visibility.ListWorkflowExecutionsResponse, error) {
	m.metricClient.IncCounter(metrics.ElasticsearchListWorkflowExecutionsScope, metrics.ElasticsearchRequests)

	sw := m.metricClient.StartTimer(metrics.ElasticsearchListWorkflowExecutionsScope, metrics.ElasticsearchLatency)
	response, err := m.persistence.ListWorkflowExecutions(request)
	sw.Stop()

	if err != nil {
		m.updateErrorMetric(metrics.ElasticsearchListWorkflowExecutionsScope, err)
	}

	return response, err
}

func (m *visibilityManagerMetrics) ScanWorkflowExecutions(request *visibility.ListWorkflowExecutionsRequestV2) (*visibility.ListWorkflowExecutionsResponse, error) {
	m.metricClient.IncCounter(metrics.ElasticsearchScanWorkflowExecutionsScope, metrics.ElasticsearchRequests)

	sw := m.metricClient.StartTimer(metrics.ElasticsearchScanWorkflowExecutionsScope, metrics.ElasticsearchLatency)
	response, err := m.persistence.ScanWorkflowExecutions(request)
	sw.Stop()

	if err != nil {
		m.updateErrorMetric(metrics.ElasticsearchScanWorkflowExecutionsScope, err)
	}

	return response, err
}

func (m *visibilityManagerMetrics) CountWorkflowExecutions(request *visibility.CountWorkflowExecutionsRequest) (*visibility.CountWorkflowExecutionsResponse, error) {
	m.metricClient.IncCounter(metrics.ElasticsearchCountWorkflowExecutionsScope, metrics.ElasticsearchRequests)

	sw := m.metricClient.StartTimer(metrics.ElasticsearchCountWorkflowExecutionsScope, metrics.ElasticsearchLatency)
	response, err := m.persistence.CountWorkflowExecutions(request)
	sw.Stop()

	if err != nil {
		m.updateErrorMetric(metrics.ElasticsearchCountWorkflowExecutionsScope, err)
	}

	return response, err
}

func (m *visibilityManagerMetrics) DeleteWorkflowExecution(request *visibility.VisibilityDeleteWorkflowExecutionRequest) error {
	m.metricClient.IncCounter(metrics.ElasticsearchDeleteWorkflowExecutionsScope, metrics.ElasticsearchRequests)

	sw := m.metricClient.StartTimer(metrics.ElasticsearchDeleteWorkflowExecutionsScope, metrics.ElasticsearchLatency)
	err := m.persistence.DeleteWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		m.updateErrorMetric(metrics.ElasticsearchDeleteWorkflowExecutionsScope, err)
	}

	return err
}

func (m *visibilityManagerMetrics) updateErrorMetric(scope int, err error) {
	switch err.(type) {
	case *serviceerror.InvalidArgument:
		m.metricClient.IncCounter(scope, metrics.ElasticsearchErrBadRequestCounter)
		m.metricClient.IncCounter(scope, metrics.ElasticsearchFailures)
	case *serviceerror.ResourceExhausted:
		m.metricClient.IncCounter(scope, metrics.ElasticsearchErrBusyCounter)
		m.metricClient.IncCounter(scope, metrics.ElasticsearchFailures)
	default:
		m.logger.Error("Operation failed with internal error.", tag.MetricScope(scope), tag.Error(err))
		m.metricClient.IncCounter(scope, metrics.ElasticsearchFailures)
	}
}

func (m *visibilityManagerMetrics) Close() {
	m.persistence.Close()
}
