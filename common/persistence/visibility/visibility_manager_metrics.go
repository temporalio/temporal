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
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

type (
	visibilityPersistenceClient struct {
		metricClient             metrics.Client
		persistence              VisibilityManager
		logger                   log.Logger
		visibilityTypeMetricsTag metrics.Tag
	}
)

var _ VisibilityManager = (*visibilityPersistenceClient)(nil)

// NewVisibilityPersistenceMetricsClient creates a client to manage visibility
func NewVisibilityPersistenceMetricsClient(persistence VisibilityManager, metricClient metrics.Client, logger log.Logger) VisibilityManager {
	return &visibilityPersistenceClient{
		persistence:              persistence,
		metricClient:             metricClient,
		logger:                   logger,
		visibilityTypeMetricsTag: metrics.StandardVisibilityTypeTag(),
	}
}

func (p *visibilityPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *visibilityPersistenceClient) RecordWorkflowExecutionStarted(request *RecordWorkflowExecutionStartedRequest) error {
	scope := p.metricClient.Scope(metrics.VisibilityPersistenceRecordWorkflowExecutionStartedScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	err := p.persistence.RecordWorkflowExecutionStarted(request)

	p.updateErrorMetric(scope, err)

	return err
}

func (p *visibilityPersistenceClient) RecordWorkflowExecutionClosed(request *RecordWorkflowExecutionClosedRequest) error {
	scope := p.metricClient.Scope(metrics.VisibilityPersistenceRecordWorkflowExecutionClosedScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	err := p.persistence.RecordWorkflowExecutionClosed(request)

	p.updateErrorMetric(scope, err)

	return err
}

func (p *visibilityPersistenceClient) UpsertWorkflowExecution(request *UpsertWorkflowExecutionRequest) error {
	scope := p.metricClient.Scope(metrics.VisibilityPersistenceUpsertWorkflowExecutionScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	err := p.persistence.UpsertWorkflowExecution(request)

	p.updateErrorMetric(scope, err)

	return err
}

func (p *visibilityPersistenceClient) ListOpenWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	scope := p.metricClient.Scope(metrics.VisibilityPersistenceListOpenWorkflowExecutionsScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.persistence.ListOpenWorkflowExecutions(request)

	p.updateErrorMetric(scope, err)

	return response, err
}

func (p *visibilityPersistenceClient) ListClosedWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	scope := p.metricClient.Scope(metrics.VisibilityPersistenceListClosedWorkflowExecutionsScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.persistence.ListClosedWorkflowExecutions(request)

	p.updateErrorMetric(scope, err)

	return response, err
}

func (p *visibilityPersistenceClient) ListOpenWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	scope := p.metricClient.Scope(metrics.VisibilityPersistenceListOpenWorkflowExecutionsByTypeScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.persistence.ListOpenWorkflowExecutionsByType(request)

	p.updateErrorMetric(scope, err)

	return response, err
}

func (p *visibilityPersistenceClient) ListClosedWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	scope := p.metricClient.Scope(metrics.VisibilityPersistenceListClosedWorkflowExecutionsScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.persistence.ListClosedWorkflowExecutionsByType(request)

	p.updateErrorMetric(scope, err)

	return response, err
}

func (p *visibilityPersistenceClient) ListOpenWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	scope := p.metricClient.Scope(metrics.VisibilityPersistenceListOpenWorkflowExecutionsByWorkflowIDScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.persistence.ListOpenWorkflowExecutionsByWorkflowID(request)

	p.updateErrorMetric(scope, err)

	return response, err
}

func (p *visibilityPersistenceClient) ListClosedWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	scope := p.metricClient.Scope(metrics.VisibilityPersistenceListClosedWorkflowExecutionsByWorkflowIDScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.persistence.ListClosedWorkflowExecutionsByWorkflowID(request)

	p.updateErrorMetric(scope, err)

	return response, err
}

func (p *visibilityPersistenceClient) ListClosedWorkflowExecutionsByStatus(request *ListClosedWorkflowExecutionsByStatusRequest) (*ListWorkflowExecutionsResponse, error) {
	scope := p.metricClient.Scope(metrics.VisibilityPersistenceListClosedWorkflowExecutionsByStatusScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.persistence.ListClosedWorkflowExecutionsByStatus(request)

	p.updateErrorMetric(scope, err)

	return response, err
}

func (p *visibilityPersistenceClient) ListWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*ListWorkflowExecutionsResponse, error) {
	scope := p.metricClient.Scope(metrics.VisibilityPersistenceListWorkflowExecutionsScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.persistence.ListWorkflowExecutions(request)

	p.updateErrorMetric(scope, err)

	return response, err
}

func (p *visibilityPersistenceClient) ScanWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*ListWorkflowExecutionsResponse, error) {
	scope := p.metricClient.Scope(metrics.VisibilityPersistenceScanWorkflowExecutionsScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.persistence.ScanWorkflowExecutions(request)

	p.updateErrorMetric(scope, err)

	return response, err
}

func (p *visibilityPersistenceClient) CountWorkflowExecutions(request *CountWorkflowExecutionsRequest) (*CountWorkflowExecutionsResponse, error) {
	scope := p.metricClient.Scope(metrics.VisibilityPersistenceCountWorkflowExecutionsScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.persistence.CountWorkflowExecutions(request)

	p.updateErrorMetric(scope, err)

	return response, err
}

func (p *visibilityPersistenceClient) DeleteWorkflowExecution(request *VisibilityDeleteWorkflowExecutionRequest) error {
	scope := p.metricClient.Scope(metrics.VisibilityPersistenceDeleteWorkflowExecutionScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	err := p.persistence.DeleteWorkflowExecution(request)

	p.updateErrorMetric(scope, err)

	return err
}

func (p *visibilityPersistenceClient) updateErrorMetric(scope metrics.Scope, err error) {
	switch err.(type) {
	case *persistence.ConditionFailedError:
		scope.IncCounter(metrics.VisibilityPersistenceConditionFailed)
	case *persistence.TimeoutError:
		scope.IncCounter(metrics.VisibilityPersistenceTimeout)
		scope.IncCounter(metrics.VisibilityPersistenceFailures)
	case *serviceerror.NotFound:
		scope.IncCounter(metrics.VisibilityPersistenceNotFound)
	case *serviceerror.ResourceExhausted:
		scope.IncCounter(metrics.VisibilityPersistenceResourceExhausted)
		scope.IncCounter(metrics.VisibilityPersistenceFailures)
	default:
		p.logger.Error("Operation failed with an error.", tag.Error(err))
		scope.IncCounter(metrics.VisibilityPersistenceFailures)
	}
}

func (p *visibilityPersistenceClient) Close() {
	p.persistence.Close()
}
