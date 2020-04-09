package matching

import (
	"context"

	"google.golang.org/grpc"

	"github.com/temporalio/temporal/.gen/proto/matchingservice"
	"github.com/temporalio/temporal/common/backoff"
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

func (c *retryableClient) AddActivityTask(
	ctx context.Context,
	addRequest *matchingservice.AddActivityTaskRequest,
	opts ...grpc.CallOption) (*matchingservice.AddActivityTaskResponse, error) {

	var resp *matchingservice.AddActivityTaskResponse
	op := func() error {
		var err error
		resp, err = c.client.AddActivityTask(ctx, addRequest, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) AddDecisionTask(
	ctx context.Context,
	addRequest *matchingservice.AddDecisionTaskRequest,
	opts ...grpc.CallOption) (*matchingservice.AddDecisionTaskResponse, error) {

	var resp *matchingservice.AddDecisionTaskResponse
	op := func() error {
		var err error
		resp, err = c.client.AddDecisionTask(ctx, addRequest, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PollForActivityTask(
	ctx context.Context,
	pollRequest *matchingservice.PollForActivityTaskRequest,
	opts ...grpc.CallOption) (*matchingservice.PollForActivityTaskResponse, error) {

	var resp *matchingservice.PollForActivityTaskResponse
	op := func() error {
		var err error
		resp, err = c.client.PollForActivityTask(ctx, pollRequest, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PollForDecisionTask(
	ctx context.Context,
	pollRequest *matchingservice.PollForDecisionTaskRequest,
	opts ...grpc.CallOption) (*matchingservice.PollForDecisionTaskResponse, error) {

	var resp *matchingservice.PollForDecisionTaskResponse
	op := func() error {
		var err error
		resp, err = c.client.PollForDecisionTask(ctx, pollRequest, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) QueryWorkflow(
	ctx context.Context,
	queryRequest *matchingservice.QueryWorkflowRequest,
	opts ...grpc.CallOption) (*matchingservice.QueryWorkflowResponse, error) {

	var resp *matchingservice.QueryWorkflowResponse
	op := func() error {
		var err error
		resp, err = c.client.QueryWorkflow(ctx, queryRequest, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RespondQueryTaskCompleted(
	ctx context.Context,
	request *matchingservice.RespondQueryTaskCompletedRequest,
	opts ...grpc.CallOption) (*matchingservice.RespondQueryTaskCompletedResponse, error) {

	var resp *matchingservice.RespondQueryTaskCompletedResponse
	op := func() error {
		var err error
		resp, err = c.client.RespondQueryTaskCompleted(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) CancelOutstandingPoll(
	ctx context.Context,
	request *matchingservice.CancelOutstandingPollRequest,
	opts ...grpc.CallOption) (*matchingservice.CancelOutstandingPollResponse, error) {

	var resp *matchingservice.CancelOutstandingPollResponse
	op := func() error {
		var err error
		resp, err = c.client.CancelOutstandingPoll(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeTaskList(
	ctx context.Context,
	request *matchingservice.DescribeTaskListRequest,
	opts ...grpc.CallOption) (*matchingservice.DescribeTaskListResponse, error) {

	var resp *matchingservice.DescribeTaskListResponse
	op := func() error {
		var err error
		resp, err = c.client.DescribeTaskList(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ListTaskListPartitions(
	ctx context.Context,
	request *matchingservice.ListTaskListPartitionsRequest,
	opts ...grpc.CallOption) (*matchingservice.ListTaskListPartitionsResponse, error) {

	var resp *matchingservice.ListTaskListPartitionsResponse
	op := func() error {
		var err error
		resp, err = c.client.ListTaskListPartitions(ctx, request, opts...)
		return err
	}

	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}
