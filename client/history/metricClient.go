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

package history

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

var _ historyservice.HistoryServiceClient = (*metricClient)(nil)

type metricClient struct {
	client          historyservice.HistoryServiceClient
	metricsClient   metrics.Client
	logger          log.Logger
	throttledLogger log.Logger
}

// NewMetricClient creates a new instance of historyservice.HistoryServiceClient that emits metrics
func NewMetricClient(
	client historyservice.HistoryServiceClient,
	metricsClient metrics.Client,
	logger log.Logger,
	throttledLogger log.Logger,
) historyservice.HistoryServiceClient {
	return &metricClient{
		client:          client,
		metricsClient:   metricsClient,
		logger:          logger,
		throttledLogger: throttledLogger,
	}
}

func (c *metricClient) StartWorkflowExecution(
	context context.Context,
	request *historyservice.StartWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.StartWorkflowExecutionResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientStartWorkflowExecutionScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.StartWorkflowExecution(context, request, opts...)
}

func (c *metricClient) DescribeHistoryHost(
	context context.Context,
	request *historyservice.DescribeHistoryHostRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.DescribeHistoryHostResponse, retError error) {
	resp, err := c.client.DescribeHistoryHost(context, request, opts...)

	return resp, err
}

func (c *metricClient) RemoveTask(
	context context.Context,
	request *historyservice.RemoveTaskRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RemoveTaskResponse, retError error) {
	resp, err := c.client.RemoveTask(context, request, opts...)

	return resp, err
}

func (c *metricClient) CloseShard(
	context context.Context,
	request *historyservice.CloseShardRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.CloseShardResponse, retError error) {
	resp, err := c.client.CloseShard(context, request, opts...)

	return resp, err
}

func (c *metricClient) GetShard(
	context context.Context,
	request *historyservice.GetShardRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.GetShardResponse, retError error) {
	resp, err := c.client.GetShard(context, request, opts...)

	return resp, err
}

func (c *metricClient) RebuildMutableState(
	context context.Context,
	request *historyservice.RebuildMutableStateRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RebuildMutableStateResponse, retError error) {
	resp, err := c.client.RebuildMutableState(context, request, opts...)

	return resp, err
}

func (c *metricClient) DescribeMutableState(
	context context.Context,
	request *historyservice.DescribeMutableStateRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.DescribeMutableStateResponse, retError error) {
	resp, err := c.client.DescribeMutableState(context, request, opts...)

	return resp, err
}

func (c *metricClient) GetMutableState(
	context context.Context,
	request *historyservice.GetMutableStateRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.GetMutableStateResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientGetMutableStateScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.GetMutableState(context, request, opts...)
}

func (c *metricClient) PollMutableState(
	context context.Context,
	request *historyservice.PollMutableStateRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.PollMutableStateResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientPollMutableStateScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.PollMutableState(context, request, opts...)
}

func (c *metricClient) ResetStickyTaskQueue(
	context context.Context,
	request *historyservice.ResetStickyTaskQueueRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.ResetStickyTaskQueueResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientResetStickyTaskQueueScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.ResetStickyTaskQueue(context, request, opts...)
}

func (c *metricClient) DescribeWorkflowExecution(
	context context.Context,
	request *historyservice.DescribeWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.DescribeWorkflowExecutionResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientDescribeWorkflowExecutionScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.DescribeWorkflowExecution(context, request, opts...)
}

func (c *metricClient) RecordWorkflowTaskStarted(
	context context.Context,
	request *historyservice.RecordWorkflowTaskStartedRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RecordWorkflowTaskStartedResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientRecordWorkflowTaskStartedScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.RecordWorkflowTaskStarted(context, request, opts...)
}

func (c *metricClient) RecordActivityTaskStarted(
	context context.Context,
	request *historyservice.RecordActivityTaskStartedRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RecordActivityTaskStartedResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientRecordActivityTaskStartedScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.RecordActivityTaskStarted(context, request, opts...)
}

func (c *metricClient) RespondWorkflowTaskCompleted(
	context context.Context,
	request *historyservice.RespondWorkflowTaskCompletedRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RespondWorkflowTaskCompletedResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientRespondWorkflowTaskCompletedScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.RespondWorkflowTaskCompleted(context, request, opts...)
}

func (c *metricClient) RespondWorkflowTaskFailed(
	context context.Context,
	request *historyservice.RespondWorkflowTaskFailedRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RespondWorkflowTaskFailedResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientRespondWorkflowTaskFailedScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.RespondWorkflowTaskFailed(context, request, opts...)
}

func (c *metricClient) RespondActivityTaskCompleted(
	context context.Context,
	request *historyservice.RespondActivityTaskCompletedRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RespondActivityTaskCompletedResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientRespondActivityTaskCompletedScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.RespondActivityTaskCompleted(context, request, opts...)
}

func (c *metricClient) RespondActivityTaskFailed(
	context context.Context,
	request *historyservice.RespondActivityTaskFailedRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RespondActivityTaskFailedResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientRespondActivityTaskFailedScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.RespondActivityTaskFailed(context, request, opts...)
}

func (c *metricClient) RespondActivityTaskCanceled(
	context context.Context,
	request *historyservice.RespondActivityTaskCanceledRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RespondActivityTaskCanceledResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientRespondActivityTaskCanceledScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.RespondActivityTaskCanceled(context, request, opts...)
}

func (c *metricClient) RecordActivityTaskHeartbeat(
	context context.Context,
	request *historyservice.RecordActivityTaskHeartbeatRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RecordActivityTaskHeartbeatResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientRecordActivityTaskHeartbeatScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.RecordActivityTaskHeartbeat(context, request, opts...)
}

func (c *metricClient) RequestCancelWorkflowExecution(
	context context.Context,
	request *historyservice.RequestCancelWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RequestCancelWorkflowExecutionResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientRequestCancelWorkflowExecutionScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.RequestCancelWorkflowExecution(context, request, opts...)
}

func (c *metricClient) SignalWorkflowExecution(
	context context.Context,
	request *historyservice.SignalWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.SignalWorkflowExecutionResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientSignalWorkflowExecutionScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.SignalWorkflowExecution(context, request, opts...)
}

func (c *metricClient) SignalWithStartWorkflowExecution(
	context context.Context,
	request *historyservice.SignalWithStartWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.SignalWithStartWorkflowExecutionResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientSignalWithStartWorkflowExecutionScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.SignalWithStartWorkflowExecution(context, request, opts...)
}

func (c *metricClient) RemoveSignalMutableState(
	context context.Context,
	request *historyservice.RemoveSignalMutableStateRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RemoveSignalMutableStateResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientRemoveSignalMutableStateScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.RemoveSignalMutableState(context, request, opts...)
}

func (c *metricClient) TerminateWorkflowExecution(
	context context.Context,
	request *historyservice.TerminateWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.TerminateWorkflowExecutionResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientTerminateWorkflowExecutionScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.TerminateWorkflowExecution(context, request, opts...)
}

func (c *metricClient) DeleteWorkflowExecution(
	context context.Context,
	request *historyservice.DeleteWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.DeleteWorkflowExecutionResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientDeleteWorkflowExecutionScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientDeleteWorkflowExecutionScope, metrics.ClientLatency)
	resp, err := c.client.DeleteWorkflowExecution(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientDeleteWorkflowExecutionScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) ResetWorkflowExecution(
	context context.Context,
	request *historyservice.ResetWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.ResetWorkflowExecutionResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientResetWorkflowExecutionScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.ResetWorkflowExecution(context, request, opts...)
}

func (c *metricClient) ScheduleWorkflowTask(
	context context.Context,
	request *historyservice.ScheduleWorkflowTaskRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.ScheduleWorkflowTaskResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientScheduleWorkflowTaskScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.ScheduleWorkflowTask(context, request, opts...)
}

func (c *metricClient) RecordChildExecutionCompleted(
	context context.Context,
	request *historyservice.RecordChildExecutionCompletedRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RecordChildExecutionCompletedResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientRecordChildExecutionCompletedScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.RecordChildExecutionCompleted(context, request, opts...)
}

func (c *metricClient) ReplicateEventsV2(
	context context.Context,
	request *historyservice.ReplicateEventsV2Request,
	opts ...grpc.CallOption,
) (_ *historyservice.ReplicateEventsV2Response, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientReplicateEventsV2Scope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.ReplicateEventsV2(context, request, opts...)
}

func (c *metricClient) SyncShardStatus(
	context context.Context,
	request *historyservice.SyncShardStatusRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.SyncShardStatusResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientSyncShardStatusScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.SyncShardStatus(context, request, opts...)
}

func (c *metricClient) SyncActivity(
	context context.Context,
	request *historyservice.SyncActivityRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.SyncActivityResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientSyncActivityScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.SyncActivity(context, request, opts...)
}

func (c *metricClient) GetReplicationMessages(
	context context.Context,
	request *historyservice.GetReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.GetReplicationMessagesResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientGetReplicationTasksScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.GetReplicationMessages(context, request, opts...)
}

func (c *metricClient) GetDLQReplicationMessages(
	context context.Context,
	request *historyservice.GetDLQReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.GetDLQReplicationMessagesResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientGetDLQReplicationTasksScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.GetDLQReplicationMessages(context, request, opts...)
}

func (c *metricClient) QueryWorkflow(
	context context.Context,
	request *historyservice.QueryWorkflowRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.QueryWorkflowResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientQueryWorkflowScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.QueryWorkflow(context, request, opts...)
}

func (c *metricClient) ReapplyEvents(
	context context.Context,
	request *historyservice.ReapplyEventsRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.ReapplyEventsResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientReapplyEventsScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.ReapplyEvents(context, request, opts...)
}

func (c *metricClient) GetDLQMessages(
	ctx context.Context,
	request *historyservice.GetDLQMessagesRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.GetDLQMessagesResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientGetDLQMessagesScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.GetDLQMessages(ctx, request, opts...)
}

func (c *metricClient) PurgeDLQMessages(
	ctx context.Context,
	request *historyservice.PurgeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.PurgeDLQMessagesResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientPurgeDLQMessagesScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.PurgeDLQMessages(ctx, request, opts...)
}

func (c *metricClient) MergeDLQMessages(
	ctx context.Context,
	request *historyservice.MergeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.MergeDLQMessagesResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientMergeDLQMessagesScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.MergeDLQMessages(ctx, request, opts...)
}

func (c *metricClient) RefreshWorkflowTasks(
	ctx context.Context,
	request *historyservice.RefreshWorkflowTasksRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RefreshWorkflowTasksResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientRefreshWorkflowTasksScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.RefreshWorkflowTasks(ctx, request, opts...)
}

func (c *metricClient) GenerateLastHistoryReplicationTasks(
	ctx context.Context,
	request *historyservice.GenerateLastHistoryReplicationTasksRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.GenerateLastHistoryReplicationTasksResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientGenerateLastHistoryReplicationTasksScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.GenerateLastHistoryReplicationTasks(ctx, request, opts...)
}

func (c *metricClient) GetReplicationStatus(
	ctx context.Context,
	request *historyservice.GetReplicationStatusRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.GetReplicationStatusResponse, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.HistoryClientGetReplicationStatusScope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.GetReplicationStatus(ctx, request, opts...)
}

func (c *metricClient) startMetricsRecording(
	metricScope int,
) (metrics.Scope, metrics.Stopwatch) {
	scope := c.metricsClient.Scope(metricScope)
	scope.IncCounter(metrics.ClientRequests)
	stopwatch := scope.StartTimer(metrics.ClientLatency)
	return scope, stopwatch
}

func (c *metricClient) finishMetricsRecording(
	scope metrics.Scope,
	stopwatch metrics.Stopwatch,
	err error,
) {
	if err != nil {
		switch err.(type) {
		case *serviceerror.Canceled,
			*serviceerror.DeadlineExceeded,
			*serviceerror.NotFound,
			*serviceerror.WorkflowExecutionAlreadyStarted:
			// noop - not interest and too many logs
		default:
			c.throttledLogger.Info("history client encountered error", tag.Error(err), tag.ErrorType(err))
		}
		scope.Tagged(metrics.ServiceErrorTypeTag(err)).IncCounter(metrics.ClientFailures)
	}
	stopwatch.Stop()
}
