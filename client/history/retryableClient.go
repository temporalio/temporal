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
	"go.temporal.io/server/common/backoff"
)

var _ Client = (*retryableClient)(nil)

type retryableClient struct {
	client      Client
	policy      backoff.RetryPolicy
	isRetryable backoff.IsRetryable
}

// NewRetryableClient creates a new instance of Client with retry policy
func NewRetryableClient(client Client, policy backoff.RetryPolicy, isRetryable backoff.IsRetryable) Client {
	return &retryableClient{
		client:      client,
		policy:      policy,
		isRetryable: isRetryable,
	}
}

func (c *retryableClient) StartWorkflowExecution(
	ctx context.Context,
	request *historyservice.StartWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.StartWorkflowExecutionResponse, error) {

	var resp *historyservice.StartWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.StartWorkflowExecution(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeHistoryHost(
	ctx context.Context,
	request *historyservice.DescribeHistoryHostRequest,
	opts ...grpc.CallOption) (*historyservice.DescribeHistoryHostResponse, error) {

	var resp *historyservice.DescribeHistoryHostResponse
	op := func() error {
		var err error
		resp, err = c.client.DescribeHistoryHost(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) CloseShard(
	ctx context.Context,
	request *historyservice.CloseShardRequest,
	opts ...grpc.CallOption) (*historyservice.CloseShardResponse, error) {

	var resp *historyservice.CloseShardResponse
	op := func() error {
		var err error
		resp, err = c.client.CloseShard(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RemoveTask(
	ctx context.Context,
	request *historyservice.RemoveTaskRequest,
	opts ...grpc.CallOption) (*historyservice.RemoveTaskResponse, error) {

	var resp *historyservice.RemoveTaskResponse
	op := func() error {
		var err error
		resp, err = c.client.RemoveTask(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeMutableState(
	ctx context.Context,
	request *historyservice.DescribeMutableStateRequest,
	opts ...grpc.CallOption) (*historyservice.DescribeMutableStateResponse, error) {

	var resp *historyservice.DescribeMutableStateResponse
	op := func() error {
		var err error
		resp, err = c.client.DescribeMutableState(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetMutableState(
	ctx context.Context,
	request *historyservice.GetMutableStateRequest,
	opts ...grpc.CallOption) (*historyservice.GetMutableStateResponse, error) {

	var resp *historyservice.GetMutableStateResponse
	op := func() error {
		var err error
		resp, err = c.client.GetMutableState(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PollMutableState(
	ctx context.Context,
	request *historyservice.PollMutableStateRequest,
	opts ...grpc.CallOption) (*historyservice.PollMutableStateResponse, error) {

	var resp *historyservice.PollMutableStateResponse
	op := func() error {
		var err error
		resp, err = c.client.PollMutableState(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ResetStickyTaskQueue(
	ctx context.Context,
	request *historyservice.ResetStickyTaskQueueRequest,
	opts ...grpc.CallOption) (*historyservice.ResetStickyTaskQueueResponse, error) {

	var resp *historyservice.ResetStickyTaskQueueResponse
	op := func() error {
		var err error
		resp, err = c.client.ResetStickyTaskQueue(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeWorkflowExecution(
	ctx context.Context,
	request *historyservice.DescribeWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.DescribeWorkflowExecutionResponse, error) {

	var resp *historyservice.DescribeWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.DescribeWorkflowExecution(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RecordWorkflowTaskStarted(
	ctx context.Context,
	request *historyservice.RecordWorkflowTaskStartedRequest,
	opts ...grpc.CallOption) (*historyservice.RecordWorkflowTaskStartedResponse, error) {

	var resp *historyservice.RecordWorkflowTaskStartedResponse
	op := func() error {
		var err error
		resp, err = c.client.RecordWorkflowTaskStarted(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RecordActivityTaskStarted(
	ctx context.Context,
	request *historyservice.RecordActivityTaskStartedRequest,
	opts ...grpc.CallOption) (*historyservice.RecordActivityTaskStartedResponse, error) {

	var resp *historyservice.RecordActivityTaskStartedResponse
	op := func() error {
		var err error
		resp, err = c.client.RecordActivityTaskStarted(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondWorkflowTaskCompleted(
	ctx context.Context,
	request *historyservice.RespondWorkflowTaskCompletedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondWorkflowTaskCompletedResponse, error) {

	var resp *historyservice.RespondWorkflowTaskCompletedResponse
	op := func() error {
		var err error
		resp, err = c.client.RespondWorkflowTaskCompleted(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondWorkflowTaskFailed(
	ctx context.Context,
	request *historyservice.RespondWorkflowTaskFailedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondWorkflowTaskFailedResponse, error) {

	var resp *historyservice.RespondWorkflowTaskFailedResponse
	op := func() error {
		var err error
		resp, err = c.client.RespondWorkflowTaskFailed(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondActivityTaskCompleted(
	ctx context.Context,
	request *historyservice.RespondActivityTaskCompletedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondActivityTaskCompletedResponse, error) {

	var resp *historyservice.RespondActivityTaskCompletedResponse
	op := func() error {
		var err error
		resp, err = c.client.RespondActivityTaskCompleted(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondActivityTaskFailed(
	ctx context.Context,
	request *historyservice.RespondActivityTaskFailedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondActivityTaskFailedResponse, error) {

	var resp *historyservice.RespondActivityTaskFailedResponse
	op := func() error {
		var err error
		resp, err = c.client.RespondActivityTaskFailed(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondActivityTaskCanceled(
	ctx context.Context,
	request *historyservice.RespondActivityTaskCanceledRequest,
	opts ...grpc.CallOption) (*historyservice.RespondActivityTaskCanceledResponse, error) {

	var resp *historyservice.RespondActivityTaskCanceledResponse
	op := func() error {
		var err error
		resp, err = c.client.RespondActivityTaskCanceled(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *historyservice.RecordActivityTaskHeartbeatRequest,
	opts ...grpc.CallOption) (*historyservice.RecordActivityTaskHeartbeatResponse, error) {

	var resp *historyservice.RecordActivityTaskHeartbeatResponse
	op := func() error {
		var err error
		resp, err = c.client.RecordActivityTaskHeartbeat(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *historyservice.RequestCancelWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.RequestCancelWorkflowExecutionResponse, error) {

	var resp *historyservice.RequestCancelWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.RequestCancelWorkflowExecution(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) SignalWorkflowExecution(
	ctx context.Context,
	request *historyservice.SignalWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.SignalWorkflowExecutionResponse, error) {

	var resp *historyservice.SignalWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.SignalWorkflowExecution(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *historyservice.SignalWithStartWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.SignalWithStartWorkflowExecutionResponse, error) {

	var resp *historyservice.SignalWithStartWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.SignalWithStartWorkflowExecution(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RemoveSignalMutableState(
	ctx context.Context,
	request *historyservice.RemoveSignalMutableStateRequest,
	opts ...grpc.CallOption) (*historyservice.RemoveSignalMutableStateResponse, error) {

	var resp *historyservice.RemoveSignalMutableStateResponse
	op := func() error {
		var err error
		resp, err = c.client.RemoveSignalMutableState(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) TerminateWorkflowExecution(
	ctx context.Context,
	request *historyservice.TerminateWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.TerminateWorkflowExecutionResponse, error) {

	var resp *historyservice.TerminateWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.TerminateWorkflowExecution(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ResetWorkflowExecution(
	ctx context.Context,
	request *historyservice.ResetWorkflowExecutionRequest,
	opts ...grpc.CallOption) (*historyservice.ResetWorkflowExecutionResponse, error) {

	var resp *historyservice.ResetWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.ResetWorkflowExecution(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ScheduleWorkflowTask(
	ctx context.Context,
	request *historyservice.ScheduleWorkflowTaskRequest,
	opts ...grpc.CallOption) (*historyservice.ScheduleWorkflowTaskResponse, error) {

	var resp *historyservice.ScheduleWorkflowTaskResponse
	op := func() error {
		var err error
		resp, err = c.client.ScheduleWorkflowTask(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RecordChildExecutionCompleted(
	ctx context.Context,
	request *historyservice.RecordChildExecutionCompletedRequest,
	opts ...grpc.CallOption) (*historyservice.RecordChildExecutionCompletedResponse, error) {

	var resp *historyservice.RecordChildExecutionCompletedResponse
	op := func() error {
		var err error
		resp, err = c.client.RecordChildExecutionCompleted(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ReplicateEvents(
	ctx context.Context,
	request *historyservice.ReplicateEventsRequest,
	opts ...grpc.CallOption) (*historyservice.ReplicateEventsResponse, error) {

	var resp *historyservice.ReplicateEventsResponse
	op := func() error {
		var err error
		resp, err = c.client.ReplicateEvents(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ReplicateRawEvents(
	ctx context.Context,
	request *historyservice.ReplicateRawEventsRequest,
	opts ...grpc.CallOption) (*historyservice.ReplicateRawEventsResponse, error) {

	var resp *historyservice.ReplicateRawEventsResponse
	op := func() error {
		var err error
		resp, err = c.client.ReplicateRawEvents(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ReplicateEventsV2(
	ctx context.Context,
	request *historyservice.ReplicateEventsV2Request,
	opts ...grpc.CallOption) (*historyservice.ReplicateEventsV2Response, error) {

	var resp *historyservice.ReplicateEventsV2Response
	op := func() error {
		var err error
		resp, err = c.client.ReplicateEventsV2(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) SyncShardStatus(
	ctx context.Context,
	request *historyservice.SyncShardStatusRequest,
	opts ...grpc.CallOption) (*historyservice.SyncShardStatusResponse, error) {

	var resp *historyservice.SyncShardStatusResponse
	op := func() error {
		var err error
		resp, err = c.client.SyncShardStatus(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) SyncActivity(
	ctx context.Context,
	request *historyservice.SyncActivityRequest,
	opts ...grpc.CallOption) (*historyservice.SyncActivityResponse, error) {

	var resp *historyservice.SyncActivityResponse
	op := func() error {
		var err error
		resp, err = c.client.SyncActivity(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetReplicationMessages(
	ctx context.Context,
	request *historyservice.GetReplicationMessagesRequest,
	opts ...grpc.CallOption) (*historyservice.GetReplicationMessagesResponse, error) {
	var resp *historyservice.GetReplicationMessagesResponse
	op := func() error {
		var err error
		resp, err = c.client.GetReplicationMessages(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetDLQReplicationMessages(
	ctx context.Context,
	request *historyservice.GetDLQReplicationMessagesRequest,
	opts ...grpc.CallOption) (*historyservice.GetDLQReplicationMessagesResponse, error) {
	var resp *historyservice.GetDLQReplicationMessagesResponse
	op := func() error {
		var err error
		resp, err = c.client.GetDLQReplicationMessages(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) QueryWorkflow(
	ctx context.Context,
	request *historyservice.QueryWorkflowRequest,
	opts ...grpc.CallOption) (*historyservice.QueryWorkflowResponse, error) {
	var resp *historyservice.QueryWorkflowResponse
	op := func() error {
		var err error
		resp, err = c.client.QueryWorkflow(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ReapplyEvents(
	ctx context.Context,
	request *historyservice.ReapplyEventsRequest,
	opts ...grpc.CallOption) (*historyservice.ReapplyEventsResponse, error) {

	var resp *historyservice.ReapplyEventsResponse
	op := func() error {
		var err error
		resp, err = c.client.ReapplyEvents(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetDLQMessages(
	ctx context.Context,
	request *historyservice.GetDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*historyservice.GetDLQMessagesResponse, error) {

	var resp *historyservice.GetDLQMessagesResponse
	op := func() error {
		var err error
		resp, err = c.client.GetDLQMessages(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PurgeDLQMessages(
	ctx context.Context,
	request *historyservice.PurgeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*historyservice.PurgeDLQMessagesResponse, error) {

	var resp *historyservice.PurgeDLQMessagesResponse
	op := func() error {
		var err error
		resp, err = c.client.PurgeDLQMessages(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) MergeDLQMessages(
	ctx context.Context,
	request *historyservice.MergeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*historyservice.MergeDLQMessagesResponse, error) {

	var resp *historyservice.MergeDLQMessagesResponse
	op := func() error {
		var err error
		resp, err = c.client.MergeDLQMessages(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RefreshWorkflowTasks(
	ctx context.Context,
	request *historyservice.RefreshWorkflowTasksRequest,
	opts ...grpc.CallOption,
) (*historyservice.RefreshWorkflowTasksResponse, error) {

	var resp *historyservice.RefreshWorkflowTasksResponse
	op := func() error {
		var err error
		resp, err = c.client.RefreshWorkflowTasks(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}
