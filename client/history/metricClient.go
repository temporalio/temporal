package history

import (
	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/tchannel-go/thrift"
)

var _ Client = (*metricClient)(nil)

type metricClient struct {
	client        Client
	metricsClient metrics.Client
}

// NewMetricClient creates a new instance of Client that emits metrics
func NewMetricClient(client Client, metricsClient metrics.Client) Client {
	return &metricClient{
		client:        client,
		metricsClient: metricsClient,
	}
}

func (c *metricClient) StartWorkflowExecution(context thrift.Context,
	request *workflow.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientStartWorkflowExecutionScope, metrics.CadenceRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientStartWorkflowExecutionScope, metrics.CadenceLatency)
	resp, err := c.client.StartWorkflowExecution(context, request)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientStartWorkflowExecutionScope, metrics.CadenceFailures)
	}

	return resp, err
}

func (c *metricClient) GetWorkflowExecutionHistory(context thrift.Context,
	request *workflow.GetWorkflowExecutionHistoryRequest) (*workflow.GetWorkflowExecutionHistoryResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientGetWorkflowExecutionHistoryScope, metrics.CadenceRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientGetWorkflowExecutionHistoryScope, metrics.CadenceLatency)
	resp, err := c.client.GetWorkflowExecutionHistory(context, request)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientGetWorkflowExecutionHistoryScope, metrics.CadenceFailures)
	}

	return resp, err
}

func (c *metricClient) RecordDecisionTaskStarted(context thrift.Context,
	request *h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRecordDecisionTaskStartedScope, metrics.CadenceRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRecordDecisionTaskStartedScope, metrics.CadenceLatency)
	resp, err := c.client.RecordDecisionTaskStarted(context, request)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRecordDecisionTaskStartedScope, metrics.CadenceFailures)
	}

	return resp, err
}

func (c *metricClient) RecordActivityTaskStarted(context thrift.Context,
	request *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRecordActivityTaskStartedScope, metrics.CadenceRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRecordActivityTaskStartedScope, metrics.CadenceLatency)
	resp, err := c.client.RecordActivityTaskStarted(context, request)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRecordActivityTaskStartedScope, metrics.CadenceFailures)
	}

	return resp, err
}

func (c *metricClient) RespondDecisionTaskCompleted(context thrift.Context,
	request *workflow.RespondDecisionTaskCompletedRequest) error {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondDecisionTaskCompletedScope, metrics.CadenceRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondDecisionTaskCompletedScope, metrics.CadenceLatency)
	err := c.client.RespondDecisionTaskCompleted(context, request)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondDecisionTaskCompletedScope, metrics.CadenceFailures)
	}

	return err
}

func (c *metricClient) RespondActivityTaskCompleted(context thrift.Context,
	request *workflow.RespondActivityTaskCompletedRequest) error {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskCompletedScope, metrics.CadenceRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondActivityTaskCompletedScope, metrics.CadenceLatency)
	err := c.client.RespondActivityTaskCompleted(context, request)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskCompletedScope, metrics.CadenceFailures)
	}

	return err
}

func (c *metricClient) RespondActivityTaskFailed(context thrift.Context,
	request *workflow.RespondActivityTaskFailedRequest) error {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskFailedScope, metrics.CadenceRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondActivityTaskFailedScope, metrics.CadenceLatency)
	err := c.client.RespondActivityTaskFailed(context, request)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskFailedScope, metrics.CadenceFailures)
	}

	return err
}

func (c *metricClient) RespondActivityTaskCanceled(context thrift.Context,
	request *workflow.RespondActivityTaskCanceledRequest) error {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskCanceledScope, metrics.CadenceRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondActivityTaskCanceledScope, metrics.CadenceLatency)
	err := c.client.RespondActivityTaskCanceled(context, request)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskCanceledScope, metrics.CadenceFailures)
	}

	return err
}

func (c *metricClient) RecordActivityTaskHeartbeat(context thrift.Context,
	request *workflow.RecordActivityTaskHeartbeatRequest) (*workflow.RecordActivityTaskHeartbeatResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRecordActivityTaskHeartbeatScope, metrics.CadenceRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRecordActivityTaskHeartbeatScope, metrics.CadenceLatency)
	resp, err := c.client.RecordActivityTaskHeartbeat(context, request)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRecordActivityTaskHeartbeatScope, metrics.CadenceFailures)
	}

	return resp, err
}
