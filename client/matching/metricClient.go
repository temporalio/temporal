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

package matching

import (
	"context"

	m "github.com/uber/cadence/.gen/go/matching"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/metrics"
	"go.uber.org/yarpc"
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

func (c *metricClient) AddActivityTask(
	ctx context.Context,
	addRequest *m.AddActivityTaskRequest,
	opts ...yarpc.CallOption) error {
	c.metricsClient.IncCounter(metrics.MatchingClientAddActivityTaskScope, metrics.CadenceRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientAddActivityTaskScope, metrics.CadenceLatency)
	err := c.client.AddActivityTask(ctx, addRequest, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientAddActivityTaskScope, metrics.MatchingFailures)
	}

	return err
}

func (c *metricClient) AddDecisionTask(
	ctx context.Context,
	addRequest *m.AddDecisionTaskRequest,
	opts ...yarpc.CallOption) error {
	c.metricsClient.IncCounter(metrics.MatchingClientAddDecisionTaskScope, metrics.CadenceRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientAddDecisionTaskScope, metrics.CadenceLatency)
	err := c.client.AddDecisionTask(ctx, addRequest, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientAddDecisionTaskScope, metrics.MatchingFailures)
	}

	return err
}

func (c *metricClient) PollForActivityTask(
	ctx context.Context,
	pollRequest *m.PollForActivityTaskRequest,
	opts ...yarpc.CallOption) (*workflow.PollForActivityTaskResponse, error) {
	c.metricsClient.IncCounter(metrics.MatchingClientPollForActivityTaskScope, metrics.CadenceRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientPollForActivityTaskScope, metrics.CadenceLatency)
	resp, err := c.client.PollForActivityTask(ctx, pollRequest, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientPollForActivityTaskScope, metrics.MatchingFailures)
	}

	return resp, err
}

func (c *metricClient) PollForDecisionTask(
	ctx context.Context,
	pollRequest *m.PollForDecisionTaskRequest,
	opts ...yarpc.CallOption) (*m.PollForDecisionTaskResponse, error) {
	c.metricsClient.IncCounter(metrics.MatchingClientPollForDecisionTaskScope, metrics.CadenceRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientPollForDecisionTaskScope, metrics.CadenceLatency)
	resp, err := c.client.PollForDecisionTask(ctx, pollRequest, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientPollForDecisionTaskScope, metrics.MatchingFailures)
	}

	return resp, err
}

func (c *metricClient) QueryWorkflow(
	ctx context.Context,
	queryRequest *m.QueryWorkflowRequest,
	opts ...yarpc.CallOption) (*workflow.QueryWorkflowResponse, error) {
	c.metricsClient.IncCounter(metrics.MatchingClientQueryWorkflowScope, metrics.CadenceRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientQueryWorkflowScope, metrics.CadenceLatency)
	resp, err := c.client.QueryWorkflow(ctx, queryRequest, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientQueryWorkflowScope, metrics.MatchingFailures)
	}

	return resp, err
}

func (c *metricClient) RespondQueryTaskCompleted(
	ctx context.Context,
	request *m.RespondQueryTaskCompletedRequest,
	opts ...yarpc.CallOption) error {
	c.metricsClient.IncCounter(metrics.MatchingClientRespondQueryTaskCompletedScope, metrics.CadenceRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientRespondQueryTaskCompletedScope, metrics.CadenceLatency)
	err := c.client.RespondQueryTaskCompleted(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientRespondQueryTaskCompletedScope, metrics.MatchingFailures)
	}

	return err
}

func (c *metricClient) CancelOutstandingPoll(
	ctx context.Context,
	request *m.CancelOutstandingPollRequest,
	opts ...yarpc.CallOption) error {
	c.metricsClient.IncCounter(metrics.MatchingClientCancelOutstandingPollScope, metrics.CadenceRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientCancelOutstandingPollScope, metrics.CadenceLatency)
	err := c.client.CancelOutstandingPoll(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientCancelOutstandingPollScope, metrics.MatchingFailures)
	}

	return err
}
