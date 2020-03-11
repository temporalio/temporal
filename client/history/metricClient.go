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

package history

import (
	"context"

	"google.golang.org/grpc"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/common/metrics"
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

func (c *metricClient) ResetStickyTaskList(
	context context.Context,
	request *historyservice.ResetStickyTaskListRequest,
	opts ...grpc.CallOption) (*historyservice.ResetStickyTaskListResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientResetStickyTaskListScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientResetStickyTaskListScope, metrics.ClientLatency)
	resp, err := c.client.ResetStickyTaskList(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientResetStickyTaskListScope, metrics.ClientFailures)
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

func (c *metricClient) RecordDecisionTaskStarted(
	context context.Context,
	request *historyservice.RecordDecisionTaskStartedRequest,
	opts ...grpc.CallOption) (*historyservice.RecordDecisionTaskStartedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRecordDecisionTaskStartedScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRecordDecisionTaskStartedScope, metrics.ClientLatency)
	resp, err := c.client.RecordDecisionTaskStarted(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRecordDecisionTaskStartedScope, metrics.ClientFailures)
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

func (c *metricClient) RespondDecisionTaskCompleted(
	context context.Context,
	request *historyservice.RespondDecisionTaskCompletedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondDecisionTaskCompletedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondDecisionTaskCompletedScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondDecisionTaskCompletedScope, metrics.ClientLatency)
	response, err := c.client.RespondDecisionTaskCompleted(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondDecisionTaskCompletedScope, metrics.ClientFailures)
	}

	return response, err
}

func (c *metricClient) RespondDecisionTaskFailed(
	context context.Context,
	request *historyservice.RespondDecisionTaskFailedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondDecisionTaskFailedResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientRespondDecisionTaskFailedScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientRespondDecisionTaskFailedScope, metrics.ClientLatency)
	resp, err := c.client.RespondDecisionTaskFailed(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientRespondDecisionTaskFailedScope, metrics.ClientFailures)
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

func (c *metricClient) ScheduleDecisionTask(
	context context.Context,
	request *historyservice.ScheduleDecisionTaskRequest,
	opts ...grpc.CallOption) (*historyservice.ScheduleDecisionTaskResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientScheduleDecisionTaskScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientScheduleDecisionTaskScope, metrics.ClientLatency)
	resp, err := c.client.ScheduleDecisionTask(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientScheduleDecisionTaskScope, metrics.ClientFailures)
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

func (c *metricClient) ReplicateEvents(
	context context.Context,
	request *historyservice.ReplicateEventsRequest,
	opts ...grpc.CallOption) (*historyservice.ReplicateEventsResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientReplicateEventsScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientReplicateEventsScope, metrics.ClientLatency)
	resp, err := c.client.ReplicateEvents(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientReplicateEventsScope, metrics.ClientFailures)
	}

	return resp, err
}

func (c *metricClient) ReplicateRawEvents(
	context context.Context,
	request *historyservice.ReplicateRawEventsRequest,
	opts ...grpc.CallOption) (*historyservice.ReplicateRawEventsResponse, error) {
	c.metricsClient.IncCounter(metrics.HistoryClientReplicateRawEventsScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.HistoryClientReplicateRawEventsScope, metrics.ClientLatency)
	resp, err := c.client.ReplicateRawEvents(context, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientReplicateRawEventsScope, metrics.ClientFailures)
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

func (c *metricClient) ReadDLQMessages(
	ctx context.Context,
	request *historyservice.ReadDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*historyservice.ReadDLQMessagesResponse, error) {

	c.metricsClient.IncCounter(metrics.HistoryClientReadDLQMessagesScope, metrics.ClientRequests)
	sw := c.metricsClient.StartTimer(metrics.HistoryClientReadDLQMessagesScope, metrics.ClientLatency)
	resp, err := c.client.ReadDLQMessages(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryClientReadDLQMessagesScope, metrics.ClientFailures)
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
