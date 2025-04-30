package frontend

import (
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/backoff"
)

var _ workflowservice.WorkflowServiceClient = (*retryableClient)(nil)

type retryableClient struct {
	client      workflowservice.WorkflowServiceClient
	policy      backoff.RetryPolicy
	isRetryable backoff.IsRetryable
}

// NewRetryableClient creates a new instance of workflowservice.WorkflowServiceClient with retry policy
func NewRetryableClient(client workflowservice.WorkflowServiceClient, policy backoff.RetryPolicy, isRetryable backoff.IsRetryable) workflowservice.WorkflowServiceClient {
	return &retryableClient{
		client:      client,
		policy:      policy,
		isRetryable: isRetryable,
	}
}
