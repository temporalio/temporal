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

package elasticsearch

import (
	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	p "github.com/uber/cadence/common/persistence"
)

type visibilityMetricsClient struct {
	metricClient metrics.Client
	persistence  p.VisibilityManager
	logger       bark.Logger
}

var _ p.VisibilityManager = (*visibilityMetricsClient)(nil)

// NewVisibilityMetricsClient wrap visibility client with metrics
func NewVisibilityMetricsClient(persistence p.VisibilityManager, metricClient metrics.Client, logger bark.Logger) p.VisibilityManager {
	return &visibilityMetricsClient{
		persistence:  persistence,
		metricClient: metricClient,
		logger:       logger,
	}
}

func (p *visibilityMetricsClient) GetName() string {
	return p.persistence.GetName()
}

func (p *visibilityMetricsClient) RecordWorkflowExecutionStarted(request *p.RecordWorkflowExecutionStartedRequest) error {
	p.metricClient.IncCounter(metrics.ElasticsearchRecordWorkflowExecutionStartedScope, metrics.ElasticsearchRequests)

	sw := p.metricClient.StartTimer(metrics.ElasticsearchRecordWorkflowExecutionStartedScope, metrics.ElasticsearchLatency)
	err := p.persistence.RecordWorkflowExecutionStarted(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.ElasticsearchRecordWorkflowExecutionStartedScope, err)
	}

	return err
}

func (p *visibilityMetricsClient) RecordWorkflowExecutionClosed(request *p.RecordWorkflowExecutionClosedRequest) error {
	p.metricClient.IncCounter(metrics.ElasticsearchRecordWorkflowExecutionClosedScope, metrics.ElasticsearchRequests)

	sw := p.metricClient.StartTimer(metrics.ElasticsearchRecordWorkflowExecutionClosedScope, metrics.ElasticsearchLatency)
	err := p.persistence.RecordWorkflowExecutionClosed(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.ElasticsearchRecordWorkflowExecutionClosedScope, err)
	}

	return err
}

func (p *visibilityMetricsClient) ListOpenWorkflowExecutions(request *p.ListWorkflowExecutionsRequest) (*p.ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.ElasticsearchListOpenWorkflowExecutionsScope, metrics.ElasticsearchRequests)

	sw := p.metricClient.StartTimer(metrics.ElasticsearchListOpenWorkflowExecutionsScope, metrics.ElasticsearchLatency)
	response, err := p.persistence.ListOpenWorkflowExecutions(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.ElasticsearchListOpenWorkflowExecutionsScope, err)
	}

	return response, err
}

func (p *visibilityMetricsClient) ListClosedWorkflowExecutions(request *p.ListWorkflowExecutionsRequest) (*p.ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.ElasticsearchListClosedWorkflowExecutionsScope, metrics.ElasticsearchRequests)

	sw := p.metricClient.StartTimer(metrics.ElasticsearchListClosedWorkflowExecutionsScope, metrics.ElasticsearchLatency)
	response, err := p.persistence.ListClosedWorkflowExecutions(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.ElasticsearchListClosedWorkflowExecutionsScope, err)
	}

	return response, err
}

func (p *visibilityMetricsClient) ListOpenWorkflowExecutionsByType(request *p.ListWorkflowExecutionsByTypeRequest) (*p.ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.ElasticsearchListOpenWorkflowExecutionsByTypeScope, metrics.ElasticsearchRequests)

	sw := p.metricClient.StartTimer(metrics.ElasticsearchListOpenWorkflowExecutionsByTypeScope, metrics.ElasticsearchLatency)
	response, err := p.persistence.ListOpenWorkflowExecutionsByType(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.ElasticsearchListOpenWorkflowExecutionsByTypeScope, err)
	}

	return response, err
}

func (p *visibilityMetricsClient) ListClosedWorkflowExecutionsByType(request *p.ListWorkflowExecutionsByTypeRequest) (*p.ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.ElasticsearchListClosedWorkflowExecutionsByTypeScope, metrics.ElasticsearchRequests)

	sw := p.metricClient.StartTimer(metrics.ElasticsearchListClosedWorkflowExecutionsByTypeScope, metrics.ElasticsearchLatency)
	response, err := p.persistence.ListClosedWorkflowExecutionsByType(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.ElasticsearchListClosedWorkflowExecutionsByTypeScope, err)
	}

	return response, err
}

func (p *visibilityMetricsClient) ListOpenWorkflowExecutionsByWorkflowID(request *p.ListWorkflowExecutionsByWorkflowIDRequest) (*p.ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.ElasticsearchListOpenWorkflowExecutionsByWorkflowIDScope, metrics.ElasticsearchRequests)

	sw := p.metricClient.StartTimer(metrics.ElasticsearchListOpenWorkflowExecutionsByWorkflowIDScope, metrics.ElasticsearchLatency)
	response, err := p.persistence.ListOpenWorkflowExecutionsByWorkflowID(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.ElasticsearchListOpenWorkflowExecutionsByWorkflowIDScope, err)
	}

	return response, err
}

func (p *visibilityMetricsClient) ListClosedWorkflowExecutionsByWorkflowID(request *p.ListWorkflowExecutionsByWorkflowIDRequest) (*p.ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.ElasticsearchListClosedWorkflowExecutionsByWorkflowIDScope, metrics.ElasticsearchRequests)

	sw := p.metricClient.StartTimer(metrics.ElasticsearchListClosedWorkflowExecutionsByWorkflowIDScope, metrics.ElasticsearchLatency)
	response, err := p.persistence.ListClosedWorkflowExecutionsByWorkflowID(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.ElasticsearchListClosedWorkflowExecutionsByWorkflowIDScope, err)
	}

	return response, err
}

func (p *visibilityMetricsClient) ListClosedWorkflowExecutionsByStatus(request *p.ListClosedWorkflowExecutionsByStatusRequest) (*p.ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.ElasticsearchListClosedWorkflowExecutionsByStatusScope, metrics.ElasticsearchRequests)

	sw := p.metricClient.StartTimer(metrics.ElasticsearchListClosedWorkflowExecutionsByStatusScope, metrics.ElasticsearchLatency)
	response, err := p.persistence.ListClosedWorkflowExecutionsByStatus(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.ElasticsearchListClosedWorkflowExecutionsByStatusScope, err)
	}

	return response, err
}

func (p *visibilityMetricsClient) GetClosedWorkflowExecution(request *p.GetClosedWorkflowExecutionRequest) (*p.GetClosedWorkflowExecutionResponse, error) {
	p.metricClient.IncCounter(metrics.ElasticsearchGetClosedWorkflowExecutionScope, metrics.ElasticsearchRequests)

	sw := p.metricClient.StartTimer(metrics.ElasticsearchGetClosedWorkflowExecutionScope, metrics.ElasticsearchLatency)
	response, err := p.persistence.GetClosedWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.ElasticsearchGetClosedWorkflowExecutionScope, err)
	}

	return response, err
}

func (p *visibilityMetricsClient) updateErrorMetric(scope int, err error) {
	switch err.(type) {
	case *workflow.BadRequestError:
		p.metricClient.IncCounter(scope, metrics.ElasticsearchErrBadRequestCounter)
		p.metricClient.IncCounter(scope, metrics.ElasticsearchFailures)
	case *workflow.ServiceBusyError:
		p.metricClient.IncCounter(scope, metrics.ElasticsearchErrBusyCounter)
		p.metricClient.IncCounter(scope, metrics.ElasticsearchFailures)
	default:
		p.logger.WithFields(bark.Fields{
			logging.TagScope: scope,
			logging.TagErr:   err,
		}).Error("Operation failed with internal error.")
		p.metricClient.IncCounter(scope, metrics.ElasticsearchFailures)
	}
}

func (p *visibilityMetricsClient) Close() {
	p.persistence.Close()
}
