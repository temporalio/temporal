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

	"google.golang.org/grpc"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/metrics"
)

var _ historyservice.HistoryServiceClient = (*metricClient)(nil)

type metricClient struct {
	client        historyservice.HistoryServiceClient
	metricsClient metrics.Client
}

// NewMetricClient creates a new instance of historyservice.HistoryServiceClient that emits metrics
func NewMetricClient(client historyservice.HistoryServiceClient, metricsClient metrics.Client) historyservice.HistoryServiceClient {
	return &metricClient{
		client:        client,
		metricsClient: metricsClient,
	}
}

func (c *metricClient) StartWorkflowExecution(
	context context.Context,
	request *historyservice.StartWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.StartWorkflowExecutionResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientStartWorkflowExecutionScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientStartWorkflowExecutionScope, metrics.ClientLatency)
	resp, err := c.client.StartWorkflowExecution(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientStartWorkflowExecutionScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) DescribeHistoryHost(
	context context.Context,
	request *historyservice.DescribeHistoryHostRequest,
	opts ...grpc.CallOption) (*historyservice.DescribeHistoryHostResponse, error) {
	resp, err := c.client.DescribeHistoryHost(context, request, opts...)

	return resp, err
}

func (c *metricClient) RemoveTask(
	context context.Context,
	request *historyservice.RemoveTaskRequest,
	opts ...grpc.CallOption) (*historyservice.RemoveTaskResponse, error) {
	resp, err := c.client.RemoveTask(context, request, opts...)

	return resp, err
}

func (c *metricClient) CloseShard(
	context context.Context,
	request *historyservice.CloseShardRequest,
	opts ...grpc.CallOption) (*historyservice.CloseShardResponse, error) {
	resp, err := c.client.CloseShard(context, request, opts...)

	return resp, err
}

func (c *metricClient) GetShard(
	context context.Context,
	request *historyservice.GetShardRequest,
	opts ...grpc.CallOption) (*historyservice.GetShardResponse, error) {
	resp, err := c.client.GetShard(context, request, opts...)

	return resp, err
}

func (c *metricClient) DescribeMutableState(
	context context.Context,
	request *historyservice.DescribeMutableStateRequest,
	opts ...grpc.CallOption) (*historyservice.DescribeMutableStateResponse, error) {
	resp, err := c.client.DescribeMutableState(context, request, opts...)

	return resp, err
}

func (c *metricClient) GetMutableState(
	context context.Context,
	request *historyservice.GetMutableStateRequest,
	opts ...grpc.CallOption) (*historyservice.GetMutableStateResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientGetMutableStateScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientGetMutableStateScope, metrics.ClientLatency)
	resp, err := c.client.GetMutableState(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientGetMutableStateScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) PollMutableState(
	context context.Context,
	request *historyservice.PollMutableStateRequest,
	opts ...grpc.CallOption) (*historyservice.PollMutableStateResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientPollMutableStateScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientPollMutableStateScope, metrics.ClientLatency)
	resp, err := c.client.PollMutableState(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientPollMutableStateScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) ResetStickyTaskQueue(
	context context.Context,
	request *historyservice.ResetStickyTaskQueueRequest,
	opts ...grpc.CallOption) (*historyservice.ResetStickyTaskQueueResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientResetStickyTaskQueueScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientResetStickyTaskQueueScope, metrics.ClientLatency)
	resp, err := c.client.ResetStickyTaskQueue(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientResetStickyTaskQueueScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) DescribeWorkflowExecution(
	context context.Context,
	request *historyservice.DescribeWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.DescribeWorkflowExecutionResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientDescribeWorkflowExecutionScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientDescribeWorkflowExecutionScope, metrics.ClientLatency)
	resp, err := c.client.DescribeWorkflowExecution(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientDescribeWorkflowExecutionScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) RecordWorkflowTaskStarted(
	context context.Context,
	request *historyservice.RecordWorkflowTaskStartedRequest,
	opts ...grpc.CallOption) (*historyservice.RecordWorkflowTaskStartedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRecordWorkflowTaskStartedScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRecordWorkflowTaskStartedScope, metrics.ClientLatency)
	resp, err := c.client.RecordWorkflowTaskStarted(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRecordWorkflowTaskStartedScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) RecordActivityTaskStarted(
	context context.Context,
	request *historyservice.RecordActivityTaskStartedRequest,
	opts ...grpc.CallOption) (*historyservice.RecordActivityTaskStartedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRecordActivityTaskStartedScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRecordActivityTaskStartedScope, metrics.ClientLatency)
	resp, err := c.client.RecordActivityTaskStarted(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRecordActivityTaskStartedScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) RespondWorkflowTaskCompleted(
	context context.Context,
	request *historyservice.RespondWorkflowTaskCompletedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondWorkflowTaskCompletedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondWorkflowTaskCompletedScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondWorkflowTaskCompletedScope, metrics.ClientLatency)
	response, err := c.client.RespondWorkflowTaskCompleted(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondWorkflowTaskCompletedScope, metrics.ClientFailures)
	}

	return response, err
}

func (c *metricClient) RespondWorkflowTaskFailed(
	context context.Context,
	request *historyservice.RespondWorkflowTaskFailedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondWorkflowTaskFailedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondWorkflowTaskFailedScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondWorkflowTaskFailedScope, metrics.ClientLatency)
	resp, err := c.client.RespondWorkflowTaskFailed(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondWorkflowTaskFailedScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) RespondActivityTaskCompleted(
	context context.Context,
	request *historyservice.RespondActivityTaskCompletedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondActivityTaskCompletedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskCompletedScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondActivityTaskCompletedScope, metrics.ClientLatency)
	resp, err := c.client.RespondActivityTaskCompleted(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskCompletedScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) RespondActivityTaskFailed(
	context context.Context,
	request *historyservice.RespondActivityTaskFailedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondActivityTaskFailedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskFailedScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondActivityTaskFailedScope, metrics.ClientLatency)
	resp, err := c.client.RespondActivityTaskFailed(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskFailedScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) RespondActivityTaskCanceled(
	context context.Context,
	request *historyservice.RespondActivityTaskCanceledRequest,
	opts ...grpc.CallOption) (*historyservice.RespondActivityTaskCanceledResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskCanceledScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondActivityTaskCanceledScope, metrics.ClientLatency)
	resp, err := c.client.RespondActivityTaskCanceled(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondActivityTaskCanceledScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) RecordActivityTaskHeartbeat(
	context context.Context,
	request *historyservice.RecordActivityTaskHeartbeatRequest,
	opts ...grpc.CallOption) (*historyservice.RecordActivityTaskHeartbeatResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRecordActivityTaskHeartbeatScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRecordActivityTaskHeartbeatScope, metrics.ClientLatency)
	resp, err := c.client.RecordActivityTaskHeartbeat(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRecordActivityTaskHeartbeatScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) RequestCancelWorkflowExecution(
	context context.Context,
	request *historyservice.RequestCancelWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRequestCancelWorkflowExecutionScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRequestCancelWorkflowExecutionScope, metrics.ClientLatency)
	resp, err := c.client.RequestCancelWorkflowExecution(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRequestCancelWorkflowExecutionScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) SignalWorkflowExecution(
	context context.Context,
	request *historyservice.SignalWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.SignalWorkflowExecutionResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientSignalWorkflowExecutionScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientSignalWorkflowExecutionScope, metrics.ClientLatency)
	resp, err := c.client.SignalWorkflowExecution(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientSignalWorkflowExecutionScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) SignalWithStartWorkflowExecution(
	context context.Context,
	request *historyservice.SignalWithStartWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.SignalWithStartWorkflowExecutionResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientSignalWithStartWorkflowExecutionScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientSignalWithStartWorkflowExecutionScope, metrics.ClientLatency)
	resp, err := c.client.SignalWithStartWorkflowExecution(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientSignalWithStartWorkflowExecutionScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) RemoveSignalMutableState(
	context context.Context,
	request *historyservice.RemoveSignalMutableStateRequest,
	opts ...grpc.CallOption) (*historyservice.RemoveSignalMutableStateResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRemoveSignalMutableStateScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRemoveSignalMutableStateScope, metrics.ClientLatency)
	resp, err := c.client.RemoveSignalMutableState(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRemoveSignalMutableStateScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) TerminateWorkflowExecution(
	context context.Context,
	request *historyservice.TerminateWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.TerminateWorkflowExecutionResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientTerminateWorkflowExecutionScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientTerminateWorkflowExecutionScope, metrics.ClientLatency)
	resp, err := c.client.TerminateWorkflowExecution(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientTerminateWorkflowExecutionScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) ResetWorkflowExecution(
	context context.Context,
	request *historyservice.ResetWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.ResetWorkflowExecutionResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientResetWorkflowExecutionScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientResetWorkflowExecutionScope, metrics.ClientLatency)
	resp, err := c.client.ResetWorkflowExecution(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientResetWorkflowExecutionScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) ScheduleWorkflowTask(
	context context.Context,
	request *historyservice.ScheduleWorkflowTaskRequest,
	opts ...grpc.CallOption) (*historyservice.ScheduleWorkflowTaskResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientScheduleWorkflowTaskScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientScheduleWorkflowTaskScope, metrics.ClientLatency)
	resp, err := c.client.ScheduleWorkflowTask(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientScheduleWorkflowTaskScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) RecordChildExecutionCompleted(
	context context.Context,
	request *historyservice.RecordChildExecutionCompletedRequest,
	opts ...grpc.CallOption) (*historyservice.RecordChildExecutionCompletedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRecordChildExecutionCompletedScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRecordChildExecutionCompletedScope, metrics.ClientLatency)
	resp, err := c.client.RecordChildExecutionCompleted(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRecordChildExecutionCompletedScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) ReplicateEventsV2(
	context context.Context,
	request *historyservice.ReplicateEventsV2Request,
	opts ...grpc.CallOption) (*historyservice.ReplicateEventsV2Response, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientReplicateEventsV2Scope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientReplicateEventsV2Scope, metrics.ClientLatency)
	resp, err := c.client.ReplicateEventsV2(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientReplicateEventsV2Scope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) SyncShardStatus(
	context context.Context,
	request *historyservice.SyncShardStatusRequest,
	opts ...grpc.CallOption) (*historyservice.SyncShardStatusResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientSyncShardStatusScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientSyncShardStatusScope, metrics.ClientLatency)
	resp, err := c.client.SyncShardStatus(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientSyncShardStatusScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) SyncActivity(
	context context.Context,
	request *historyservice.SyncActivityRequest,
	opts ...grpc.CallOption) (*historyservice.SyncActivityResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientSyncActivityScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientSyncActivityScope, metrics.ClientLatency)
	resp, err := c.client.SyncActivity(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientSyncActivityScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) GetReplicationMessages(
	context context.Context,
	request *historyservice.GetReplicationMessagesRequest,
	opts ...grpc.CallOption) (*historyservice.GetReplicationMessagesResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientGetReplicationTasksScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientGetReplicationTasksScope, metrics.ClientLatency)
	resp, err := c.client.GetReplicationMessages(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientGetReplicationTasksScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) GetDLQReplicationMessages(
	context context.Context,
	request *historyservice.GetDLQReplicationMessagesRequest,
	opts ...grpc.CallOption) (*historyservice.GetDLQReplicationMessagesResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientGetDLQReplicationTasksScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientGetDLQReplicationTasksScope, metrics.ClientLatency)
	resp, err := c.client.GetDLQReplicationMessages(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientGetDLQReplicationTasksScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) QueryWorkflow(
	context context.Context,
	request *historyservice.QueryWorkflowRequest,
	opts ...grpc.CallOption) (*historyservice.QueryWorkflowResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientQueryWorkflowScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientQueryWorkflowScope, metrics.ClientLatency)
	resp, err := c.client.QueryWorkflow(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientQueryWorkflowScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) ReapplyEvents(
	context context.Context,
	request *historyservice.ReapplyEventsRequest,
	opts ...grpc.CallOption) (*historyservice.ReapplyEventsResponse, error) {

	c.metricsClient.IncCounter(metrics.HistoryClientReapplyEventsScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.HistoryClientReapplyEventsScope, metrics.ClientLatency)
	resp, err := c.client.ReapplyEvents(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientReapplyEventsScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetDLQMessages(
	ctx context.Context,
	request *historyservice.GetDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*historyservice.GetDLQMessagesResponse, error) {

	c.metricsClient.IncCounter(metrics.HistoryClientGetDLQMessagesScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.HistoryClientGetDLQMessagesScope, metrics.ClientLatency)
	resp, err := c.client.GetDLQMessages(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientGetDLQMessagesScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) PurgeDLQMessages(
	ctx context.Context,
	request *historyservice.PurgeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*historyservice.PurgeDLQMessagesResponse, error) {

	c.metricsClient.IncCounter(metrics.HistoryClientPurgeDLQMessagesScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.HistoryClientPurgeDLQMessagesScope, metrics.ClientLatency)
	resp, err := c.client.PurgeDLQMessages(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientPurgeDLQMessagesScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) MergeDLQMessages(
	ctx context.Context,
	request *historyservice.MergeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*historyservice.MergeDLQMessagesResponse, error) {

	c.metricsClient.IncCounter(metrics.HistoryClientMergeDLQMessagesScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.HistoryClientMergeDLQMessagesScope, metrics.ClientLatency)
	resp, err := c.client.MergeDLQMessages(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientMergeDLQMessagesScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) RefreshWorkflowTasks(
	ctx context.Context,
	request *historyservice.RefreshWorkflowTasksRequest,
	opts ...grpc.CallOption,
) (*historyservice.RefreshWorkflowTasksResponse, error) {

	c.metricsClient.IncCounter(metrics.HistoryClientRefreshWorkflowTasksScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.HistoryClientRefreshWorkflowTasksScope, metrics.ClientLatency)
	resp, err := c.client.RefreshWorkflowTasks(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRefreshWorkflowTasksScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) GenerateLastHistoryReplicationTasks(
	ctx context.Context,
	request *historyservice.GenerateLastHistoryReplicationTasksRequest,
	opts ...grpc.CallOption,
) (*historyservice.GenerateLastHistoryReplicationTasksResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientGenerateLastHistoryReplicationTasksScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.HistoryClientGenerateLastHistoryReplicationTasksScope, metrics.ClientLatency)
	resp, err := c.client.GenerateLastHistoryReplicationTasks(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientGenerateLastHistoryReplicationTasksScope, metrics.ClientFailures)
	}
	return resp, err
}

func (c *metricClient) GetReplicationStatus(
	ctx context.Context,
	request *historyservice.GetReplicationStatusRequest,
	opts ...grpc.CallOption,
) (*historyservice.GetReplicationStatusResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientGetReplicationStatusScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.HistoryClientGetReplicationStatusScope, metrics.ClientLatency)
	resp, err := c.client.GetReplicationStatus(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientGetReplicationStatusScope, metrics.ClientFailures)
	}
	return resp, err
}
