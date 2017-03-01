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
	c.metricsClient.IncCounter(metrics.HistoryClientStartWorkflowExecutionScope, metrics.WorkflowRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientStartWorkflowExecutionScope, metrics.WorkflowLatency)
	resp, err := c.client.StartWorkflowExecution(context, request)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientStartWorkflowExecutionScope, metrics.WorkflowFailures)
	}

	return resp, err
}

func (c *metricClient) GetWorkflowExecutionHistory(context thrift.Context,
	request *workflow.GetWorkflowExecutionHistoryRequest) (*workflow.GetWorkflowExecutionHistoryResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientGetWorkflowExecutionHistoryScope, metrics.WorkflowRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientGetWorkflowExecutionHistoryScope, metrics.WorkflowLatency)
	resp, err := c.client.GetWorkflowExecutionHistory(context, request)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientGetWorkflowExecutionHistoryScope, metrics.WorkflowFailures)
	}

	return resp, err
}

func (c *metricClient) RecordDecisionTaskStarted(context thrift.Context,
	request *h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRecordDecisionTaskStartedScope, metrics.WorkflowRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRecordDecisionTaskStartedScope, metrics.WorkflowLatency)
	resp, err := c.client.RecordDecisionTaskStarted(context, request)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRecordDecisionTaskStartedScope, metrics.WorkflowFailures)
	}

	return resp, err
}

func (c *metricClient) RecordActivityTaskStarted(context thrift.Context,
	request *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRecordActivityTaskStartedScope, metrics.WorkflowRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRecordActivityTaskStartedScope, metrics.WorkflowLatency)
	resp, err := c.client.RecordActivityTaskStarted(context, request)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRecordActivityTaskStartedScope, metrics.WorkflowFailures)
	}

	return resp, err
}

func (c *metricClient) RespondDecisionTaskCompleted(context thrift.Context,
	request *workflow.RespondDecisionTaskCompletedRequest) error {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondDecisionTaskCompletedScope, metrics.WorkflowRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondDecisionTaskCompletedScope, metrics.WorkflowLatency)
	err := c.client.RespondDecisionTaskCompleted(context, request)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondDecisionTaskCompletedScope, metrics.WorkflowFailures)
	}

	return err
}

func (c *metricClient) RespondActivityTaskCompleted(context thrift.Context,
	request *workflow.RespondActivityTaskCompletedRequest) error {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskCompletedScope, metrics.WorkflowRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondActivityTaskCompletedScope, metrics.WorkflowLatency)
	err := c.client.RespondActivityTaskCompleted(context, request)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskCompletedScope, metrics.WorkflowFailures)
	}

	return err
}

func (c *metricClient) RespondActivityTaskFailed(context thrift.Context,
	request *workflow.RespondActivityTaskFailedRequest) error {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskFailedScope, metrics.WorkflowRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondActivityTaskFailedScope, metrics.WorkflowLatency)
	err := c.client.RespondActivityTaskFailed(context, request)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskFailedScope, metrics.WorkflowFailures)
	}

	return err
}

func (c *metricClient) RespondActivityTaskCanceled(context thrift.Context,
	request *workflow.RespondActivityTaskCanceledRequest) error {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskCanceledScope, metrics.WorkflowRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondActivityTaskCanceledScope, metrics.WorkflowLatency)
	err := c.client.RespondActivityTaskCanceled(context, request)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskCanceledScope, metrics.WorkflowFailures)
	}

	return err
}

func (c *metricClient) RecordActivityTaskHeartbeat(context thrift.Context,
	request *workflow.RecordActivityTaskHeartbeatRequest) (*workflow.RecordActivityTaskHeartbeatResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRecordActivityTaskHeartbeatScope, metrics.WorkflowRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRecordActivityTaskHeartbeatScope, metrics.WorkflowLatency)
	resp, err := c.client.RecordActivityTaskHeartbeat(context, request)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRecordActivityTaskHeartbeatScope, metrics.WorkflowFailures)
	}

	return resp, err
}
