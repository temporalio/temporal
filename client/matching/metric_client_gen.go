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

// Code generated by cmd/tools/rpcwrappers. DO NOT EDIT.

package matching

import (
	"context"

	"go.temporal.io/server/api/matchingservice/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/metrics"
)

func (c *metricClient) ApplyTaskQueueUserDataReplicationEvent(
	ctx context.Context,
	request *matchingservice.ApplyTaskQueueUserDataReplicationEventRequest,
	opts ...grpc.CallOption,
) (_ *matchingservice.ApplyTaskQueueUserDataReplicationEventResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.MatchingClientApplyTaskQueueUserDataReplicationEventScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ApplyTaskQueueUserDataReplicationEvent(ctx, request, opts...)
}

func (c *metricClient) CancelOutstandingPoll(
	ctx context.Context,
	request *matchingservice.CancelOutstandingPollRequest,
	opts ...grpc.CallOption,
) (_ *matchingservice.CancelOutstandingPollResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.MatchingClientCancelOutstandingPollScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.CancelOutstandingPoll(ctx, request, opts...)
}

func (c *metricClient) DescribeTaskQueue(
	ctx context.Context,
	request *matchingservice.DescribeTaskQueueRequest,
	opts ...grpc.CallOption,
) (_ *matchingservice.DescribeTaskQueueResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.MatchingClientDescribeTaskQueueScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DescribeTaskQueue(ctx, request, opts...)
}

func (c *metricClient) ForceUnloadTaskQueue(
	ctx context.Context,
	request *matchingservice.ForceUnloadTaskQueueRequest,
	opts ...grpc.CallOption,
) (_ *matchingservice.ForceUnloadTaskQueueResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.MatchingClientForceUnloadTaskQueueScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ForceUnloadTaskQueue(ctx, request, opts...)
}

func (c *metricClient) GetBuildIdTaskQueueMapping(
	ctx context.Context,
	request *matchingservice.GetBuildIdTaskQueueMappingRequest,
	opts ...grpc.CallOption,
) (_ *matchingservice.GetBuildIdTaskQueueMappingResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.MatchingClientGetBuildIdTaskQueueMappingScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetBuildIdTaskQueueMapping(ctx, request, opts...)
}

func (c *metricClient) GetTaskQueueUserData(
	ctx context.Context,
	request *matchingservice.GetTaskQueueUserDataRequest,
	opts ...grpc.CallOption,
) (_ *matchingservice.GetTaskQueueUserDataResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.MatchingClientGetTaskQueueUserDataScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetTaskQueueUserData(ctx, request, opts...)
}

func (c *metricClient) GetWorkerBuildIdCompatibility(
	ctx context.Context,
	request *matchingservice.GetWorkerBuildIdCompatibilityRequest,
	opts ...grpc.CallOption,
) (_ *matchingservice.GetWorkerBuildIdCompatibilityResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.MatchingClientGetWorkerBuildIdCompatibilityScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetWorkerBuildIdCompatibility(ctx, request, opts...)
}

func (c *metricClient) ListTaskQueuePartitions(
	ctx context.Context,
	request *matchingservice.ListTaskQueuePartitionsRequest,
	opts ...grpc.CallOption,
) (_ *matchingservice.ListTaskQueuePartitionsResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.MatchingClientListTaskQueuePartitionsScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ListTaskQueuePartitions(ctx, request, opts...)
}

func (c *metricClient) ReplicateTaskQueueUserData(
	ctx context.Context,
	request *matchingservice.ReplicateTaskQueueUserDataRequest,
	opts ...grpc.CallOption,
) (_ *matchingservice.ReplicateTaskQueueUserDataResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.MatchingClientReplicateTaskQueueUserDataScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ReplicateTaskQueueUserData(ctx, request, opts...)
}

func (c *metricClient) RespondQueryTaskCompleted(
	ctx context.Context,
	request *matchingservice.RespondQueryTaskCompletedRequest,
	opts ...grpc.CallOption,
) (_ *matchingservice.RespondQueryTaskCompletedResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.MatchingClientRespondQueryTaskCompletedScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RespondQueryTaskCompleted(ctx, request, opts...)
}

func (c *metricClient) UpdateTaskQueueUserData(
	ctx context.Context,
	request *matchingservice.UpdateTaskQueueUserDataRequest,
	opts ...grpc.CallOption,
) (_ *matchingservice.UpdateTaskQueueUserDataResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.MatchingClientUpdateTaskQueueUserDataScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.UpdateTaskQueueUserData(ctx, request, opts...)
}

func (c *metricClient) UpdateWorkerBuildIdCompatibility(
	ctx context.Context,
	request *matchingservice.UpdateWorkerBuildIdCompatibilityRequest,
	opts ...grpc.CallOption,
) (_ *matchingservice.UpdateWorkerBuildIdCompatibilityResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.MatchingClientUpdateWorkerBuildIdCompatibilityScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.UpdateWorkerBuildIdCompatibility(ctx, request, opts...)
}
