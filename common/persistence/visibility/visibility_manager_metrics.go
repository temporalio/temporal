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
		metricClient metrics.Client
		persistence  VisibilityManager
		logger       log.Logger
	}
)

var _ VisibilityManager = (*visibilityPersistenceClient)(nil)

// NewVisibilityPersistenceMetricsClient creates a client to manage visibility
func NewVisibilityPersistenceMetricsClient(persistence VisibilityManager, metricClient metrics.Client, logger log.Logger) VisibilityManager {
	return &visibilityPersistenceClient{
		persistence:  persistence,
		metricClient: metricClient,
		logger:       logger,
	}
}

func (p *visibilityPersistenceClient) GetName() string {
	return p.persistence.GetName()
}

func (p *visibilityPersistenceClient) RecordWorkflowExecutionStarted(request *RecordWorkflowExecutionStartedRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceRecordWorkflowExecutionStartedScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceRecordWorkflowExecutionStartedScope, metrics.PersistenceLatency)
	err := p.persistence.RecordWorkflowExecutionStarted(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceRecordWorkflowExecutionStartedScope, err)
	}

	return err
}

func (p *visibilityPersistenceClient) RecordWorkflowExecutionClosed(request *RecordWorkflowExecutionClosedRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceRecordWorkflowExecutionClosedScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceRecordWorkflowExecutionClosedScope, metrics.PersistenceLatency)
	err := p.persistence.RecordWorkflowExecutionClosed(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceRecordWorkflowExecutionClosedScope, err)
	}

	return err
}

func (p *visibilityPersistenceClient) UpsertWorkflowExecution(request *UpsertWorkflowExecutionRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceUpsertWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceUpsertWorkflowExecutionScope, metrics.PersistenceLatency)
	err := p.persistence.UpsertWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceUpsertWorkflowExecutionScope, err)
	}

	return err
}

func (p *visibilityPersistenceClient) ListOpenWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListOpenWorkflowExecutionsScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListOpenWorkflowExecutionsScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListOpenWorkflowExecutions(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListOpenWorkflowExecutionsScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) ListClosedWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListClosedWorkflowExecutionsScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListClosedWorkflowExecutionsScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListClosedWorkflowExecutions(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListClosedWorkflowExecutionsScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) ListOpenWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListOpenWorkflowExecutionsByTypeScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListOpenWorkflowExecutionsByTypeScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListOpenWorkflowExecutionsByType(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListOpenWorkflowExecutionsByTypeScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) ListClosedWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListClosedWorkflowExecutionsByTypeScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListClosedWorkflowExecutionsByTypeScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListClosedWorkflowExecutionsByType(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListClosedWorkflowExecutionsByTypeScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) ListOpenWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListOpenWorkflowExecutionsByWorkflowIDScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListOpenWorkflowExecutionsByWorkflowIDScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListOpenWorkflowExecutionsByWorkflowID(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListOpenWorkflowExecutionsByWorkflowIDScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) ListClosedWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListClosedWorkflowExecutionsByWorkflowIDScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListClosedWorkflowExecutionsByWorkflowIDScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListClosedWorkflowExecutionsByWorkflowID(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListClosedWorkflowExecutionsByWorkflowIDScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) ListClosedWorkflowExecutionsByStatus(request *ListClosedWorkflowExecutionsByStatusRequest) (*ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListClosedWorkflowExecutionsByStatusScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListClosedWorkflowExecutionsByStatusScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListClosedWorkflowExecutionsByStatus(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListClosedWorkflowExecutionsByStatusScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) GetClosedWorkflowExecution(request *GetClosedWorkflowExecutionRequest) (*GetClosedWorkflowExecutionResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceGetClosedWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceGetClosedWorkflowExecutionScope, metrics.PersistenceLatency)
	response, err := p.persistence.GetClosedWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceGetClosedWorkflowExecutionScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) DeleteWorkflowExecution(request *VisibilityDeleteWorkflowExecutionRequest) error {
	p.metricClient.IncCounter(metrics.PersistenceVisibilityDeleteWorkflowExecutionScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceVisibilityDeleteWorkflowExecutionScope, metrics.PersistenceLatency)
	err := p.persistence.DeleteWorkflowExecution(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceVisibilityDeleteWorkflowExecutionScope, err)
	}

	return err
}

func (p *visibilityPersistenceClient) ListWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceListWorkflowExecutionsScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceListWorkflowExecutionsScope, metrics.PersistenceLatency)
	response, err := p.persistence.ListWorkflowExecutions(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceListWorkflowExecutionsScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) ScanWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*ListWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceScanWorkflowExecutionsScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceScanWorkflowExecutionsScope, metrics.PersistenceLatency)
	response, err := p.persistence.ScanWorkflowExecutions(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceScanWorkflowExecutionsScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) CountWorkflowExecutions(request *CountWorkflowExecutionsRequest) (*CountWorkflowExecutionsResponse, error) {
	p.metricClient.IncCounter(metrics.PersistenceCountWorkflowExecutionsScope, metrics.PersistenceRequests)

	sw := p.metricClient.StartTimer(metrics.PersistenceCountWorkflowExecutionsScope, metrics.PersistenceLatency)
	response, err := p.persistence.CountWorkflowExecutions(request)
	sw.Stop()

	if err != nil {
		p.updateErrorMetric(metrics.PersistenceCountWorkflowExecutionsScope, err)
	}

	return response, err
}

func (p *visibilityPersistenceClient) updateErrorMetric(scope int, err error) {
	switch err.(type) {
	case *persistence.ConditionFailedError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrConditionFailedCounter)
	case *persistence.TimeoutError:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrTimeoutCounter)
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	case *serviceerror.NotFound:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrEntityNotExistsCounter)
	case *serviceerror.ResourceExhausted:
		p.metricClient.IncCounter(scope, metrics.PersistenceErrBusyCounter)
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	default:
		p.logger.Error("Operation failed with internal error.",
			tag.Error(err), tag.MetricScope(scope))
		p.metricClient.IncCounter(scope, metrics.PersistenceFailures)
	}
}

func (p *visibilityPersistenceClient) Close() {
	p.persistence.Close()
}
