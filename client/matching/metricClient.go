package matching

import (
	m "github.com/uber/cadence/.gen/go/matching"
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

func (c *metricClient) AddActivityTask(context thrift.Context, addRequest *m.AddActivityTaskRequest) error {
	c.metricsClient.IncCounter(metrics.MatchingClientAddActivityTaskScope, metrics.WorkflowRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientAddActivityTaskScope, metrics.WorkflowLatency)
	err := c.client.AddActivityTask(context, addRequest)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientAddActivityTaskScope, metrics.WorkflowFailures)
	}

	return err
}

func (c *metricClient) AddDecisionTask(context thrift.Context, addRequest *m.AddDecisionTaskRequest) error {
	c.metricsClient.IncCounter(metrics.MatchingClientAddDecisionTaskScope, metrics.WorkflowRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientAddDecisionTaskScope, metrics.WorkflowLatency)
	err := c.client.AddDecisionTask(context, addRequest)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientAddDecisionTaskScope, metrics.WorkflowFailures)
	}

	return err
}

func (c *metricClient) PollForActivityTask(context thrift.Context,
	pollRequest *workflow.PollForActivityTaskRequest) (*workflow.PollForActivityTaskResponse, error) {
	c.metricsClient.IncCounter(metrics.MatchingClientPollForActivityTaskScope, metrics.WorkflowRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientPollForActivityTaskScope, metrics.WorkflowLatency)
	resp, err := c.client.PollForActivityTask(context, pollRequest)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientPollForActivityTaskScope, metrics.WorkflowFailures)
	}

	return resp, err
}

func (c *metricClient) PollForDecisionTask(context thrift.Context,
	pollRequest *workflow.PollForDecisionTaskRequest) (*workflow.PollForDecisionTaskResponse, error) {
	c.metricsClient.IncCounter(metrics.MatchingClientPollForDecisionTaskScope, metrics.WorkflowRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientPollForDecisionTaskScope, metrics.WorkflowLatency)
	resp, err := c.client.PollForDecisionTask(context, pollRequest)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientPollForDecisionTaskScope, metrics.WorkflowFailures)
	}

	return resp, err
}
