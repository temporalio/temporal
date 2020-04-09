package admin

import (
	"context"

	"google.golang.org/grpc"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
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

func (c *retryableClient) AddSearchAttribute(
	ctx context.Context,
	request *adminservice.AddSearchAttributeRequest,
	opts ...grpc.CallOption,
) (*adminservice.AddSearchAttributeResponse, error) {

	var resp *adminservice.AddSearchAttributeResponse
	op := func() error {
		var err error
		resp, err = c.client.AddSearchAttribute(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeHistoryHost(
	ctx context.Context,
	request *adminservice.DescribeHistoryHostRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeHistoryHostResponse, error) {

	var resp *adminservice.DescribeHistoryHostResponse
	op := func() error {
		var err error
		resp, err = c.client.DescribeHistoryHost(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) RemoveTask(
	ctx context.Context,
	request *adminservice.RemoveTaskRequest,
	opts ...grpc.CallOption,
) (*adminservice.RemoveTaskResponse, error) {

	var resp *adminservice.RemoveTaskResponse
	op := func() error {
		var err error
		resp, err = c.client.RemoveTask(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) CloseShard(
	ctx context.Context,
	request *adminservice.CloseShardRequest,
	opts ...grpc.CallOption,
) (*adminservice.CloseShardResponse, error) {

	var resp *adminservice.CloseShardResponse
	op := func() error {
		var err error
		resp, err = c.client.CloseShard(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeWorkflowExecution(
	ctx context.Context,
	request *adminservice.DescribeWorkflowExecutionRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeWorkflowExecutionResponse, error) {

	var resp *adminservice.DescribeWorkflowExecutionResponse
	op := func() error {
		var err error
		resp, err = c.client.DescribeWorkflowExecution(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetWorkflowExecutionRawHistory(
	ctx context.Context,
	request *adminservice.GetWorkflowExecutionRawHistoryRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetWorkflowExecutionRawHistoryResponse, error) {

	var resp *adminservice.GetWorkflowExecutionRawHistoryResponse
	op := func() error {
		var err error
		resp, err = c.client.GetWorkflowExecutionRawHistory(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetWorkflowExecutionRawHistoryV2(
	ctx context.Context,
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	opts ...grpc.CallOption,
) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {

	var resp *adminservice.GetWorkflowExecutionRawHistoryV2Response
	op := func() error {
		var err error
		resp, err = c.client.GetWorkflowExecutionRawHistoryV2(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) DescribeCluster(
	ctx context.Context,
	request *adminservice.DescribeClusterRequest,
	opts ...grpc.CallOption,
) (*adminservice.DescribeClusterResponse, error) {

	var resp *adminservice.DescribeClusterResponse
	op := func() error {
		var err error
		resp, err = c.client.DescribeCluster(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetReplicationMessages(
	ctx context.Context,
	request *adminservice.GetReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetReplicationMessagesResponse, error) {
	var resp *adminservice.GetReplicationMessagesResponse
	op := func() error {
		var err error
		resp, err = c.client.GetReplicationMessages(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetNamespaceReplicationMessages(
	ctx context.Context,
	request *adminservice.GetNamespaceReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetNamespaceReplicationMessagesResponse, error) {
	var resp *adminservice.GetNamespaceReplicationMessagesResponse
	op := func() error {
		var err error
		resp, err = c.client.GetNamespaceReplicationMessages(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) GetDLQReplicationMessages(
	ctx context.Context,
	request *adminservice.GetDLQReplicationMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.GetDLQReplicationMessagesResponse, error) {
	var resp *adminservice.GetDLQReplicationMessagesResponse
	op := func() error {
		var err error
		resp, err = c.client.GetDLQReplicationMessages(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ReapplyEvents(
	ctx context.Context,
	request *adminservice.ReapplyEventsRequest,
	opts ...grpc.CallOption,
) (*adminservice.ReapplyEventsResponse, error) {
	var resp *adminservice.ReapplyEventsResponse
	op := func() error {
		var err error
		resp, err = c.client.ReapplyEvents(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) ReadDLQMessages(
	ctx context.Context,
	request *adminservice.ReadDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.ReadDLQMessagesResponse, error) {

	var resp *adminservice.ReadDLQMessagesResponse
	op := func() error {
		var err error
		resp, err = c.client.ReadDLQMessages(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}

func (c *retryableClient) PurgeDLQMessages(
	ctx context.Context,
	request *adminservice.PurgeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.PurgeDLQMessagesResponse, error) {

	var resp *adminservice.PurgeDLQMessagesResponse
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
	request *adminservice.MergeDLQMessagesRequest,
	opts ...grpc.CallOption,
) (*adminservice.MergeDLQMessagesResponse, error) {

	var resp *adminservice.MergeDLQMessagesResponse
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
	request *adminservice.RefreshWorkflowTasksRequest,
	opts ...grpc.CallOption,
) (*adminservice.RefreshWorkflowTasksResponse, error) {

	var resp *adminservice.RefreshWorkflowTasksResponse
	op := func() error {
		var err error
		resp, err = c.client.RefreshWorkflowTasks(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}
