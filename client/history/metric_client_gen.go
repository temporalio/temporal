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

package history

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/metrics"
)

func (c *metricClient) CloseShard(
	ctx context.Context,
	request *historyservice.CloseShardRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.CloseShardResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientCloseShardScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.CloseShard(ctx, request, opts...)
}

func (c *metricClient) DeleteHistoryBranch(
	ctx context.Context,
	request *historyservice.DeleteHistoryBranchRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.DeleteHistoryBranchResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientDeleteHistoryBranchScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DeleteHistoryBranch(ctx, request, opts...)
}

func (c *metricClient) DeleteWorkflowExecution(
	ctx context.Context,
	request *historyservice.DeleteWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.DeleteWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientDeleteWorkflowExecutionScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DeleteWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) DeleteWorkflowVisibilityRecord(
	ctx context.Context,
	request *historyservice.DeleteWorkflowVisibilityRecordRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.DeleteWorkflowVisibilityRecordResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientDeleteWorkflowVisibilityRecordScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DeleteWorkflowVisibilityRecord(ctx, request, opts...)
}

func (c *metricClient) DescribeHistoryHost(
	ctx context.Context,
	request *historyservice.DescribeHistoryHostRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.DescribeHistoryHostResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientDescribeHistoryHostScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DescribeHistoryHost(ctx, request, opts...)
}

func (c *metricClient) DescribeMutableState(
	ctx context.Context,
	request *historyservice.DescribeMutableStateRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.DescribeMutableStateResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientDescribeMutableStateScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DescribeMutableState(ctx, request, opts...)
}

func (c *metricClient) DescribeWorkflowExecution(
	ctx context.Context,
	request *historyservice.DescribeWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.DescribeWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientDescribeWorkflowExecutionScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.DescribeWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) GenerateLastHistoryReplicationTasks(
	ctx context.Context,
	request *historyservice.GenerateLastHistoryReplicationTasksRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.GenerateLastHistoryReplicationTasksResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientGenerateLastHistoryReplicationTasksScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GenerateLastHistoryReplicationTasks(ctx, request, opts...)
}

func (c *metricClient) GetDLQMessages(
	ctx context.Context,
	request *historyservice.GetDLQMessagesRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.GetDLQMessagesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientGetDLQMessagesScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetDLQMessages(ctx, request, opts...)
}

func (c *metricClient) GetDLQReplicationMessages(
	ctx context.Context,
	request *historyservice.GetDLQReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.GetDLQReplicationMessagesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientGetDLQReplicationMessagesScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetDLQReplicationMessages(ctx, request, opts...)
}

func (c *metricClient) GetMutableState(
	ctx context.Context,
	request *historyservice.GetMutableStateRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.GetMutableStateResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientGetMutableStateScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetMutableState(ctx, request, opts...)
}

func (c *metricClient) GetReplicationMessages(
	ctx context.Context,
	request *historyservice.GetReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.GetReplicationMessagesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientGetReplicationMessagesScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetReplicationMessages(ctx, request, opts...)
}

func (c *metricClient) GetReplicationStatus(
	ctx context.Context,
	request *historyservice.GetReplicationStatusRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.GetReplicationStatusResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientGetReplicationStatusScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetReplicationStatus(ctx, request, opts...)
}

func (c *metricClient) GetShard(
	ctx context.Context,
	request *historyservice.GetShardRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.GetShardResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientGetShardScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.GetShard(ctx, request, opts...)
}

func (c *metricClient) MergeDLQMessages(
	ctx context.Context,
	request *historyservice.MergeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.MergeDLQMessagesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientMergeDLQMessagesScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.MergeDLQMessages(ctx, request, opts...)
}

func (c *metricClient) PollMutableState(
	ctx context.Context,
	request *historyservice.PollMutableStateRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.PollMutableStateResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientPollMutableStateScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.PollMutableState(ctx, request, opts...)
}

func (c *metricClient) PurgeDLQMessages(
	ctx context.Context,
	request *historyservice.PurgeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.PurgeDLQMessagesResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientPurgeDLQMessagesScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.PurgeDLQMessages(ctx, request, opts...)
}

func (c *metricClient) QueryWorkflow(
	ctx context.Context,
	request *historyservice.QueryWorkflowRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.QueryWorkflowResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientQueryWorkflowScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.QueryWorkflow(ctx, request, opts...)
}

func (c *metricClient) ReadHistoryBranch(
	ctx context.Context,
	request *historyservice.ReadHistoryBranchRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.ReadHistoryBranchResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientReadHistoryBranchScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ReadHistoryBranch(ctx, request, opts...)
}

func (c *metricClient) ReadHistoryBranchReverse(
	ctx context.Context,
	request *historyservice.ReadHistoryBranchReverseRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.ReadHistoryBranchReverseResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientReadHistoryBranchReverseScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ReadHistoryBranchReverse(ctx, request, opts...)
}

func (c *metricClient) ReadRawHistoryBranch(
	ctx context.Context,
	request *historyservice.ReadRawHistoryBranchRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.ReadRawHistoryBranchResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientReadRawHistoryBranchScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ReadRawHistoryBranch(ctx, request, opts...)
}

func (c *metricClient) ReapplyEvents(
	ctx context.Context,
	request *historyservice.ReapplyEventsRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.ReapplyEventsResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientReapplyEventsScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ReapplyEvents(ctx, request, opts...)
}

func (c *metricClient) RebuildMutableState(
	ctx context.Context,
	request *historyservice.RebuildMutableStateRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RebuildMutableStateResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientRebuildMutableStateScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RebuildMutableState(ctx, request, opts...)
}

func (c *metricClient) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *historyservice.RecordActivityTaskHeartbeatRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RecordActivityTaskHeartbeatResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientRecordActivityTaskHeartbeatScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RecordActivityTaskHeartbeat(ctx, request, opts...)
}

func (c *metricClient) RecordActivityTaskStarted(
	ctx context.Context,
	request *historyservice.RecordActivityTaskStartedRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RecordActivityTaskStartedResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientRecordActivityTaskStartedScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RecordActivityTaskStarted(ctx, request, opts...)
}

func (c *metricClient) RecordChildExecutionCompleted(
	ctx context.Context,
	request *historyservice.RecordChildExecutionCompletedRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RecordChildExecutionCompletedResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientRecordChildExecutionCompletedScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RecordChildExecutionCompleted(ctx, request, opts...)
}

func (c *metricClient) RecordWorkflowTaskStarted(
	ctx context.Context,
	request *historyservice.RecordWorkflowTaskStartedRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RecordWorkflowTaskStartedResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientRecordWorkflowTaskStartedScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RecordWorkflowTaskStarted(ctx, request, opts...)
}

func (c *metricClient) RefreshWorkflowTasks(
	ctx context.Context,
	request *historyservice.RefreshWorkflowTasksRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RefreshWorkflowTasksResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientRefreshWorkflowTasksScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RefreshWorkflowTasks(ctx, request, opts...)
}

func (c *metricClient) RemoveSignalMutableState(
	ctx context.Context,
	request *historyservice.RemoveSignalMutableStateRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RemoveSignalMutableStateResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientRemoveSignalMutableStateScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RemoveSignalMutableState(ctx, request, opts...)
}

func (c *metricClient) RemoveTask(
	ctx context.Context,
	request *historyservice.RemoveTaskRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RemoveTaskResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientRemoveTaskScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RemoveTask(ctx, request, opts...)
}

func (c *metricClient) ReplicateEventsV2(
	ctx context.Context,
	request *historyservice.ReplicateEventsV2Request,
	opts ...grpc.CallOption,
) (_ *historyservice.ReplicateEventsV2Response, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientReplicateEventsV2Scope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ReplicateEventsV2(ctx, request, opts...)
}

func (c *metricClient) ReplicateWorkflowState(
	ctx context.Context,
	request *historyservice.ReplicateWorkflowStateRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.ReplicateWorkflowStateResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientReplicateWorkflowStateScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ReplicateWorkflowState(ctx, request, opts...)
}

func (c *metricClient) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *historyservice.RequestCancelWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RequestCancelWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientRequestCancelWorkflowExecutionScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RequestCancelWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) ResetStickyTaskQueue(
	ctx context.Context,
	request *historyservice.ResetStickyTaskQueueRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.ResetStickyTaskQueueResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientResetStickyTaskQueueScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ResetStickyTaskQueue(ctx, request, opts...)
}

func (c *metricClient) ResetWorkflowExecution(
	ctx context.Context,
	request *historyservice.ResetWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.ResetWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientResetWorkflowExecutionScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ResetWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) RespondActivityTaskCanceled(
	ctx context.Context,
	request *historyservice.RespondActivityTaskCanceledRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RespondActivityTaskCanceledResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientRespondActivityTaskCanceledScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RespondActivityTaskCanceled(ctx, request, opts...)
}

func (c *metricClient) RespondActivityTaskCompleted(
	ctx context.Context,
	request *historyservice.RespondActivityTaskCompletedRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RespondActivityTaskCompletedResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientRespondActivityTaskCompletedScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RespondActivityTaskCompleted(ctx, request, opts...)
}

func (c *metricClient) RespondActivityTaskFailed(
	ctx context.Context,
	request *historyservice.RespondActivityTaskFailedRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RespondActivityTaskFailedResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientRespondActivityTaskFailedScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RespondActivityTaskFailed(ctx, request, opts...)
}

func (c *metricClient) RespondWorkflowTaskCompleted(
	ctx context.Context,
	request *historyservice.RespondWorkflowTaskCompletedRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RespondWorkflowTaskCompletedResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientRespondWorkflowTaskCompletedScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RespondWorkflowTaskCompleted(ctx, request, opts...)
}

func (c *metricClient) RespondWorkflowTaskFailed(
	ctx context.Context,
	request *historyservice.RespondWorkflowTaskFailedRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.RespondWorkflowTaskFailedResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientRespondWorkflowTaskFailedScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.RespondWorkflowTaskFailed(ctx, request, opts...)
}

func (c *metricClient) ScheduleWorkflowTask(
	ctx context.Context,
	request *historyservice.ScheduleWorkflowTaskRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.ScheduleWorkflowTaskResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientScheduleWorkflowTaskScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.ScheduleWorkflowTask(ctx, request, opts...)
}

func (c *metricClient) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *historyservice.SignalWithStartWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.SignalWithStartWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientSignalWithStartWorkflowExecutionScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.SignalWithStartWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) SignalWorkflowExecution(
	ctx context.Context,
	request *historyservice.SignalWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.SignalWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientSignalWorkflowExecutionScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.SignalWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) StartWorkflowExecution(
	ctx context.Context,
	request *historyservice.StartWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.StartWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientStartWorkflowExecutionScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.StartWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) SyncActivity(
	ctx context.Context,
	request *historyservice.SyncActivityRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.SyncActivityResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientSyncActivityScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.SyncActivity(ctx, request, opts...)
}

func (c *metricClient) SyncShardStatus(
	ctx context.Context,
	request *historyservice.SyncShardStatusRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.SyncShardStatusResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientSyncShardStatusScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.SyncShardStatus(ctx, request, opts...)
}

func (c *metricClient) TerminateWorkflowExecution(
	ctx context.Context,
	request *historyservice.TerminateWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.TerminateWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientTerminateWorkflowExecutionScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.TerminateWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) TrimHistoryBranch(
	ctx context.Context,
	request *historyservice.TrimHistoryBranchRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.TrimHistoryBranchResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientTrimHistoryBranchScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.TrimHistoryBranch(ctx, request, opts...)
}

func (c *metricClient) UpdateWorkflowExecution(
	ctx context.Context,
	request *historyservice.UpdateWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.UpdateWorkflowExecutionResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientUpdateWorkflowExecutionScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.UpdateWorkflowExecution(ctx, request, opts...)
}

func (c *metricClient) VerifyChildExecutionCompletionRecorded(
	ctx context.Context,
	request *historyservice.VerifyChildExecutionCompletionRecordedRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.VerifyChildExecutionCompletionRecordedResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientVerifyChildExecutionCompletionRecordedScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.VerifyChildExecutionCompletionRecorded(ctx, request, opts...)
}

func (c *metricClient) VerifyFirstWorkflowTaskScheduled(
	ctx context.Context,
	request *historyservice.VerifyFirstWorkflowTaskScheduledRequest,
	opts ...grpc.CallOption,
) (_ *historyservice.VerifyFirstWorkflowTaskScheduledResponse, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientVerifyFirstWorkflowTaskScheduledScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.VerifyFirstWorkflowTaskScheduled(ctx, request, opts...)
}
