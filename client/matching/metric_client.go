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
	"time"

	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/headers"
	serviceerrors "go.temporal.io/server/common/serviceerror"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

var _ matchingservice.MatchingServiceClient = (*metricClient)(nil)

type metricClient struct {
	client          matchingservice.MatchingServiceClient
	metricsHandler  metrics.MetricsHandler
	logger          log.Logger
	throttledLogger log.Logger
}

// NewMetricClient creates a new instance of matchingservice.MatchingServiceClient that emits metrics
func NewMetricClient(
	client matchingservice.MatchingServiceClient,
	metricsHandler metrics.MetricsHandler,
	logger log.Logger,
	throttledLogger log.Logger,
) matchingservice.MatchingServiceClient {
	return &metricClient{
		client:          client,
		metricsHandler:  metricsHandler,
		logger:          logger,
		throttledLogger: throttledLogger,
	}
}

func (c *metricClient) AddActivityTask(
	ctx context.Context,
	request *matchingservice.AddActivityTaskRequest,
	opts ...grpc.CallOption,
) (_ *matchingservice.AddActivityTaskResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(ctx, metrics.MatchingClientAddActivityTaskScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	c.emitForwardedSourceStats(
		scope,
		request.GetForwardedSource(),
		request.TaskQueue,
	)

	return c.client.AddActivityTask(ctx, request, opts...)
}

func (c *metricClient) AddWorkflowTask(
	ctx context.Context,
	request *matchingservice.AddWorkflowTaskRequest,
	opts ...grpc.CallOption,
) (_ *matchingservice.AddWorkflowTaskResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(ctx, metrics.MatchingClientAddWorkflowTaskScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	c.emitForwardedSourceStats(
		scope,
		request.GetForwardedSource(),
		request.TaskQueue,
	)

	return c.client.AddWorkflowTask(ctx, request, opts...)
}

func (c *metricClient) PollActivityTaskQueue(
	ctx context.Context,
	request *matchingservice.PollActivityTaskQueueRequest,
	opts ...grpc.CallOption,
) (_ *matchingservice.PollActivityTaskQueueResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(ctx, metrics.MatchingClientPollActivityTaskQueueScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	if request.PollRequest != nil {
		c.emitForwardedSourceStats(
			scope,
			request.GetForwardedSource(),
			request.PollRequest.TaskQueue,
		)
	}

	return c.client.PollActivityTaskQueue(ctx, request, opts...)
}

func (c *metricClient) PollWorkflowTaskQueue(
	ctx context.Context,
	request *matchingservice.PollWorkflowTaskQueueRequest,
	opts ...grpc.CallOption,
) (_ *matchingservice.PollWorkflowTaskQueueResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(ctx, metrics.MatchingClientPollWorkflowTaskQueueScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	if request.PollRequest != nil {
		c.emitForwardedSourceStats(
			scope,
			request.GetForwardedSource(),
			request.PollRequest.TaskQueue,
		)
	}

	return c.client.PollWorkflowTaskQueue(ctx, request, opts...)
}

func (c *metricClient) QueryWorkflow(
	ctx context.Context,
	request *matchingservice.QueryWorkflowRequest,
	opts ...grpc.CallOption,
) (_ *matchingservice.QueryWorkflowResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(ctx, metrics.MatchingClientQueryWorkflowScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	c.emitForwardedSourceStats(
		scope,
		request.GetForwardedSource(),
		request.TaskQueue,
	)

	return c.client.QueryWorkflow(ctx, request, opts...)
}

func (c *metricClient) emitForwardedSourceStats(
	handler metrics.MetricsHandler,
	forwardedFrom string,
	taskQueue *taskqueuepb.TaskQueue,
) {
	if taskQueue == nil {
		return
	}

	isChildPartition := strings.HasPrefix(taskQueue.GetName(), taskQueuePartitionPrefix)
	switch {
	case forwardedFrom != "":
		handler.Counter(metrics.MatchingClientForwardedCounter.GetMetricName()).Record(1)
	default:
		if isChildPartition {
			handler.Counter(metrics.MatchingClientInvalidTaskQueueName.GetMetricName()).Record(1)
		}
	}
}

func (c *metricClient) startMetricsRecording(
	ctx context.Context,
	operation string,
) (metrics.MetricsHandler, time.Time) {
	caller := headers.GetCallerInfo(ctx).CallerName
	handler := c.metricsHandler.WithTags(metrics.OperationTag(operation), metrics.NamespaceTag(caller), metrics.ServiceRoleTag(metrics.MatchingRoleTagValue))
	handler.Counter(metrics.ClientRequests.GetMetricName()).Record(1)
	return handler, time.Now().UTC()
}

func (c *metricClient) finishMetricsRecording(
	handler metrics.MetricsHandler,
	startTime time.Time,
	err error,
) {
	if err != nil {
		switch err.(type) {
		case *serviceerrors.StickyWorkerUnavailable,
			*serviceerror.Canceled,
			*serviceerror.DeadlineExceeded,
			*serviceerror.NotFound,
			*serviceerror.QueryFailed,
			*serviceerror.NamespaceNotFound,
			*serviceerror.WorkflowExecutionAlreadyStarted:
			// noop - not interest and too many logs
		default:
			c.throttledLogger.Info("matching client encountered error", tag.Error(err), tag.ErrorType(err))
		}
		handler.Counter(metrics.ClientFailures.GetMetricName()).Record(1, metrics.ServiceErrorTypeTag(err))
	}
	handler.Timer(metrics.ClientLatency.GetMetricName()).Record(time.Since(startTime))
}
