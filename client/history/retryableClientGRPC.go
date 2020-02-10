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
	"github.com/temporalio/temporal/common/backoff"
)

var _ ClientGRPC = (*retryableClientGRPC)(nil)

type retryableClientGRPC struct {
	client      ClientGRPC
	policy      backoff.RetryPolicy
	isRetryable backoff.IsRetryable
}

// NewRetryableClientGRPC creates a new instance of Client with retry policy
func NewRetryableClientGRPC(client ClientGRPC, policy backoff.RetryPolicy, isRetryable backoff.IsRetryable) ClientGRPC {
	return &retryableClientGRPC{
		client:      client,
		policy:      policy,
		isRetryable: isRetryable,
	}
}

func (c *retryableClientGRPC) StartWorkflowExecution(
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

func (c *retryableClientGRPC) DescribeHistoryHost(
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

func (c *retryableClientGRPC) CloseShard(
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

func (c *retryableClientGRPC) RemoveTask(
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

func (c *retryableClientGRPC) DescribeMutableState(
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

func (c *retryableClientGRPC) GetMutableState(
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

func (c *retryableClientGRPC) PollMutableState(
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

func (c *retryableClientGRPC) ResetStickyTaskList(
	ctx context.Context,
	request *historyservice.ResetStickyTaskListRequest,
	opts ...grpc.CallOption) (*historyservice.ResetStickyTaskListResponse, error) {

	var resp *historyservice.ResetStickyTaskListResponse
	op := func() error {
		var err error
		resp, err = c.client.ResetStickyTaskList(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClientGRPC) DescribeWorkflowExecution(
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

func (c *retryableClientGRPC) RecordDecisionTaskStarted(
	ctx context.Context,
	request *historyservice.RecordDecisionTaskStartedRequest,
	opts ...grpc.CallOption) (*historyservice.RecordDecisionTaskStartedResponse, error) {

	var resp *historyservice.RecordDecisionTaskStartedResponse
	op := func() error {
		var err error
		resp, err = c.client.RecordDecisionTaskStarted(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClientGRPC) RecordActivityTaskStarted(
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

func (c *retryableClientGRPC) RespondDecisionTaskCompleted(
	ctx context.Context,
	request *historyservice.RespondDecisionTaskCompletedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondDecisionTaskCompletedResponse, error) {

	var resp *historyservice.RespondDecisionTaskCompletedResponse
	op := func() error {
		var err error
		resp, err = c.client.RespondDecisionTaskCompleted(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClientGRPC) RespondDecisionTaskFailed(
	ctx context.Context,
	request *historyservice.RespondDecisionTaskFailedRequest,
	opts ...grpc.CallOption) (*historyservice.RespondDecisionTaskFailedResponse, error) {

	var resp *historyservice.RespondDecisionTaskFailedResponse
	op := func() error {
		var err error
		resp, err = c.client.RespondDecisionTaskFailed(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClientGRPC) RespondActivityTaskCompleted(
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

func (c *retryableClientGRPC) RespondActivityTaskFailed(
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

func (c *retryableClientGRPC) RespondActivityTaskCanceled(
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

func (c *retryableClientGRPC) RecordActivityTaskHeartbeat(
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

func (c *retryableClientGRPC) RequestCancelWorkflowExecution(
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

func (c *retryableClientGRPC) SignalWorkflowExecution(
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

func (c *retryableClientGRPC) SignalWithStartWorkflowExecution(
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

func (c *retryableClientGRPC) RemoveSignalMutableState(
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

func (c *retryableClientGRPC) TerminateWorkflowExecution(
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

func (c *retryableClientGRPC) ResetWorkflowExecution(
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

func (c *retryableClientGRPC) ScheduleDecisionTask(
	ctx context.Context,
	request *historyservice.ScheduleDecisionTaskRequest,
	opts ...grpc.CallOption) (*historyservice.ScheduleDecisionTaskResponse, error) {

	var resp *historyservice.ScheduleDecisionTaskResponse
	op := func() error {
		var err error
		resp, err = c.client.ScheduleDecisionTask(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClientGRPC) RecordChildExecutionCompleted(
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

func (c *retryableClientGRPC) ReplicateEvents(
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

func (c *retryableClientGRPC) ReplicateRawEvents(
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

func (c *retryableClientGRPC) ReplicateEventsV2(
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

func (c *retryableClientGRPC) SyncShardStatus(
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

func (c *retryableClientGRPC) SyncActivity(
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

func (c *retryableClientGRPC) GetReplicationMessages(
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

func (c *retryableClientGRPC) GetDLQReplicationMessages(
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

func (c *retryableClientGRPC) QueryWorkflow(
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

func (c *retryableClientGRPC) ReapplyEvents(
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
