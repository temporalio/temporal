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
	"strings"

	"go.uber.org/yarpc"

	m "github.com/uber/cadence/.gen/go/matching"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/metrics"
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
	request *m.AddActivityTaskRequest,
	opts ...yarpc.CallOption) error {
	c.metricsClient.IncCounter(metrics.MatchingClientAddActivityTaskScope, metrics.CadenceClientRequests)
	sw := c.metricsClient.StartTimer(metrics.MatchingClientAddActivityTaskScope, metrics.CadenceClientLatency)

	c.emitForwardedFromStats(
		metrics.MatchingClientAddActivityTaskScope,
		request.GetForwardedFrom(),
		request.TaskList,
	)

	err := c.client.AddActivityTask(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientAddActivityTaskScope, metrics.CadenceClientFailures)
	}

	return err
}

func (c *metricClient) AddDecisionTask(
	ctx context.Context,
	request *m.AddDecisionTaskRequest,
	opts ...yarpc.CallOption) error {
	c.metricsClient.IncCounter(metrics.MatchingClientAddDecisionTaskScope, metrics.CadenceClientRequests)
	sw := c.metricsClient.StartTimer(metrics.MatchingClientAddDecisionTaskScope, metrics.CadenceClientLatency)

	c.emitForwardedFromStats(
		metrics.MatchingClientAddDecisionTaskScope,
		request.GetForwardedFrom(),
		request.TaskList,
	)

	err := c.client.AddDecisionTask(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientAddDecisionTaskScope, metrics.CadenceClientFailures)
	}

	return err
}

func (c *metricClient) PollForActivityTask(
	ctx context.Context,
	request *m.PollForActivityTaskRequest,
	opts ...yarpc.CallOption) (*workflow.PollForActivityTaskResponse, error) {
	c.metricsClient.IncCounter(metrics.MatchingClientPollForActivityTaskScope, metrics.CadenceClientRequests)
	sw := c.metricsClient.StartTimer(metrics.MatchingClientPollForActivityTaskScope, metrics.CadenceClientLatency)

	if request.PollRequest != nil {
		c.emitForwardedFromStats(
			metrics.MatchingClientPollForActivityTaskScope,
			request.GetForwardedFrom(),
			request.PollRequest.TaskList,
		)
	}

	resp, err := c.client.PollForActivityTask(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientPollForActivityTaskScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClient) PollForDecisionTask(
	ctx context.Context,
	request *m.PollForDecisionTaskRequest,
	opts ...yarpc.CallOption) (*m.PollForDecisionTaskResponse, error) {
	c.metricsClient.IncCounter(metrics.MatchingClientPollForDecisionTaskScope, metrics.CadenceClientRequests)
	sw := c.metricsClient.StartTimer(metrics.MatchingClientPollForDecisionTaskScope, metrics.CadenceClientLatency)

	if request.PollRequest != nil {
		c.emitForwardedFromStats(
			metrics.MatchingClientPollForDecisionTaskScope,
			request.GetForwardedFrom(),
			request.PollRequest.TaskList,
		)
	}

	resp, err := c.client.PollForDecisionTask(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientPollForDecisionTaskScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClient) QueryWorkflow(
	ctx context.Context,
	request *m.QueryWorkflowRequest,
	opts ...yarpc.CallOption) (*workflow.QueryWorkflowResponse, error) {
	c.metricsClient.IncCounter(metrics.MatchingClientQueryWorkflowScope, metrics.CadenceClientRequests)
	sw := c.metricsClient.StartTimer(metrics.MatchingClientQueryWorkflowScope, metrics.CadenceClientLatency)

	c.emitForwardedFromStats(
		metrics.MatchingClientQueryWorkflowScope,
		request.GetForwardedFrom(),
		request.TaskList,
	)

	resp, err := c.client.QueryWorkflow(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientQueryWorkflowScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClient) RespondQueryTaskCompleted(
	ctx context.Context,
	request *m.RespondQueryTaskCompletedRequest,
	opts ...yarpc.CallOption) error {
	c.metricsClient.IncCounter(metrics.MatchingClientRespondQueryTaskCompletedScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientRespondQueryTaskCompletedScope, metrics.CadenceClientLatency)
	err := c.client.RespondQueryTaskCompleted(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientRespondQueryTaskCompletedScope, metrics.CadenceClientFailures)
	}

	return err
}

func (c *metricClient) CancelOutstandingPoll(
	ctx context.Context,
	request *m.CancelOutstandingPollRequest,
	opts ...yarpc.CallOption) error {
	c.metricsClient.IncCounter(metrics.MatchingClientCancelOutstandingPollScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientCancelOutstandingPollScope, metrics.CadenceClientLatency)
	err := c.client.CancelOutstandingPoll(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientCancelOutstandingPollScope, metrics.CadenceClientFailures)
	}

	return err
}

func (c *metricClient) DescribeTaskList(
	ctx context.Context,
	request *m.DescribeTaskListRequest,
	opts ...yarpc.CallOption) (*workflow.DescribeTaskListResponse, error) {
	c.metricsClient.IncCounter(metrics.MatchingClientDescribeTaskListScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientDescribeTaskListScope, metrics.CadenceClientLatency)
	resp, err := c.client.DescribeTaskList(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientDescribeTaskListScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClient) ListTaskListPartitions(
	ctx context.Context,
	request *m.ListTaskListPartitionsRequest,
	opts ...yarpc.CallOption) (*workflow.ListTaskListPartitionsResponse, error) {
	c.metricsClient.IncCounter(metrics.MatchingClientListTaskListPartitionsScope, metrics.CadenceClientRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientListTaskListPartitionsScope, metrics.CadenceClientLatency)
	resp, err := c.client.ListTaskListPartitions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientListTaskListPartitionsScope, metrics.CadenceClientFailures)
	}

	return resp, err
}

func (c *metricClient) emitForwardedFromStats(scope int, forwardedFrom string, taskList *workflow.TaskList) {
	if taskList == nil {
		return
	}
	isChildPartition := strings.HasPrefix(taskList.GetName(), taskListPartitionPrefix)
	switch {
	case forwardedFrom != "":
		c.metricsClient.IncCounter(scope, metrics.MatchingClientForwardedCounter)
	default:
		if isChildPartition {
			c.metricsClient.IncCounter(scope, metrics.MatchingClientInvalidTaskListName)
		}
	}
}
