package history

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/grpc"
)

var _ historyservice.HistoryServiceClient = (*retryableClient)(nil)

type retryableClient struct {
	client      historyservice.HistoryServiceClient
	policy      backoff.RetryPolicy
	isRetryable backoff.IsRetryable
}

// NewRetryableClient creates a new instance of historyservice.HistoryServiceClient with retry policy
func NewRetryableClient(client historyservice.HistoryServiceClient, policy backoff.RetryPolicy, isRetryable backoff.IsRetryable) historyservice.HistoryServiceClient {
	return &retryableClient{
		client:      client,
		policy:      policy,
		isRetryable: isRetryable,
	}
}

func (c *retryableClient) StreamWorkflowReplicationMessages(
	ctx context.Context,
	opts ...grpc.CallOption,
) (historyservice.HistoryService_StreamWorkflowReplicationMessagesClient, error) {
	var resp historyservice.HistoryService_StreamWorkflowReplicationMessagesClient
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.StreamWorkflowReplicationMessages(ctx, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}
