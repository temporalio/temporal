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

package matching

import (
	"context"
	"strings"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/metrics"
)

var _ matchingservice.MatchingServiceClient = (*metricClient)(nil)

type metricClient struct {
	client        matchingservice.MatchingServiceClient
	metricsClient metrics.Client
}

// NewMetricClient creates a new instance of matchingservice.MatchingServiceClient that emits metrics
func NewMetricClient(client matchingservice.MatchingServiceClient, metricsClient metrics.Client) matchingservice.MatchingServiceClient {
	return &metricClient{
		client:        client,
		metricsClient: metricsClient,
	}
}

func (c *metricClient) AddActivityTask(
	ctx context.Context,
	request *matchingservice.AddActivityTaskRequest,
	opts ...grpc.CallOption) (*matchingservice.AddActivityTaskResponse, error) {

	c.metricsClient.IncCounter(metrics.MatchingClientAddActivityTaskScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.MatchingClientAddActivityTaskScope, metrics.ClientLatency)

	c.emitForwardedSourceStats(
		metrics.MatchingClientAddActivityTaskScope,
		request.GetForwardedSource(),
		request.TaskQueue,
	)

	resp, err := c.client.AddActivityTask(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientAddActivityTaskScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) AddWorkflowTask(
	ctx context.Context,
	request *matchingservice.AddWorkflowTaskRequest,
	opts ...grpc.CallOption) (*matchingservice.AddWorkflowTaskResponse, error) {

	c.metricsClient.IncCounter(metrics.MatchingClientAddWorkflowTaskScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.MatchingClientAddWorkflowTaskScope, metrics.ClientLatency)

	c.emitForwardedSourceStats(
		metrics.MatchingClientAddWorkflowTaskScope,
		request.GetForwardedSource(),
		request.TaskQueue,
	)

	resp, err := c.client.AddWorkflowTask(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientAddWorkflowTaskScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) PollActivityTaskQueue(
	ctx context.Context,
	request *matchingservice.PollActivityTaskQueueRequest,
	opts ...grpc.CallOption) (*matchingservice.PollActivityTaskQueueResponse, error) {

	c.metricsClient.IncCounter(metrics.MatchingClientPollActivityTaskQueueScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.MatchingClientPollActivityTaskQueueScope, metrics.ClientLatency)

	if request.PollRequest != nil {
		c.emitForwardedSourceStats(
			metrics.MatchingClientPollActivityTaskQueueScope,
			request.GetForwardedSource(),
			request.PollRequest.TaskQueue,
		)
	}

	resp, err := c.client.PollActivityTaskQueue(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientPollActivityTaskQueueScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) PollWorkflowTaskQueue(
	ctx context.Context,
	request *matchingservice.PollWorkflowTaskQueueRequest,
	opts ...grpc.CallOption) (*matchingservice.PollWorkflowTaskQueueResponse, error) {

	c.metricsClient.IncCounter(metrics.MatchingClientPollWorkflowTaskQueueScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.MatchingClientPollWorkflowTaskQueueScope, metrics.ClientLatency)

	if request.PollRequest != nil {
		c.emitForwardedSourceStats(
			metrics.MatchingClientPollWorkflowTaskQueueScope,
			request.GetForwardedSource(),
			request.PollRequest.TaskQueue,
		)
	}

	resp, err := c.client.PollWorkflowTaskQueue(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientPollWorkflowTaskQueueScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) QueryWorkflow(
	ctx context.Context,
	request *matchingservice.QueryWorkflowRequest,
	opts ...grpc.CallOption) (*matchingservice.QueryWorkflowResponse, error) {

	c.metricsClient.IncCounter(metrics.MatchingClientQueryWorkflowScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.MatchingClientQueryWorkflowScope, metrics.ClientLatency)

	c.emitForwardedSourceStats(
		metrics.MatchingClientQueryWorkflowScope,
		request.GetForwardedSource(),
		request.TaskQueue,
	)

	resp, err := c.client.QueryWorkflow(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientQueryWorkflowScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) RespondQueryTaskCompleted(
	ctx context.Context,
	request *matchingservice.RespondQueryTaskCompletedRequest,
	opts ...grpc.CallOption) (*matchingservice.RespondQueryTaskCompletedResponse, error) {

	c.metricsClient.IncCounter(metrics.MatchingClientRespondQueryTaskCompletedScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientRespondQueryTaskCompletedScope, metrics.ClientLatency)
	resp, err := c.client.RespondQueryTaskCompleted(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientRespondQueryTaskCompletedScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) CancelOutstandingPoll(
	ctx context.Context,
	request *matchingservice.CancelOutstandingPollRequest,
	opts ...grpc.CallOption) (*matchingservice.CancelOutstandingPollResponse, error) {

	c.metricsClient.IncCounter(metrics.MatchingClientCancelOutstandingPollScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientCancelOutstandingPollScope, metrics.ClientLatency)
	resp, err := c.client.CancelOutstandingPoll(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientCancelOutstandingPollScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) DescribeTaskQueue(
	ctx context.Context,
	request *matchingservice.DescribeTaskQueueRequest,
	opts ...grpc.CallOption) (*matchingservice.DescribeTaskQueueResponse, error) {

	c.metricsClient.IncCounter(metrics.MatchingClientDescribeTaskQueueScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientDescribeTaskQueueScope, metrics.ClientLatency)
	resp, err := c.client.DescribeTaskQueue(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientDescribeTaskQueueScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) ListTaskQueuePartitions(
	ctx context.Context,
	request *matchingservice.ListTaskQueuePartitionsRequest,
	opts ...grpc.CallOption) (*matchingservice.ListTaskQueuePartitionsResponse, error) {

	c.metricsClient.IncCounter(metrics.MatchingClientListTaskQueuePartitionsScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.MatchingClientListTaskQueuePartitionsScope, metrics.ClientLatency)
	resp, err := c.client.ListTaskQueuePartitions(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingClientListTaskQueuePartitionsScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) emitForwardedSourceStats(scope int, forwardedFrom string, taskQueue *taskqueuepb.TaskQueue) {
	if taskQueue == nil {
		return
	}
	isChildPartition := strings.HasPrefix(taskQueue.GetName(), taskQueuePartitionPrefix)
	switch {
	case forwardedFrom != "":
		c.metricsClient.IncCounter(scope, metrics.MatchingClientForwardedCounter)
	default:
		if isChildPartition {
			c.metricsClient.IncCounter(scope, metrics.MatchingClientInvalidTaskQueueName)
		}
	}
}
