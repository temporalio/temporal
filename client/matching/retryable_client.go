package matching

import (
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/backoff"
)

var _ matchingservice.MatchingServiceClient = (*retryableClient)(nil)

type retryableClient struct {
	client      matchingservice.MatchingServiceClient
	policy      backoff.RetryPolicy
	pollPolicy  backoff.RetryPolicy
	isRetryable backoff.IsRetryable
}

// NewRetryableClient creates a new instance of matchingservice.MatchingServiceClient with retry policy
func NewRetryableClient(
	client matchingservice.MatchingServiceClient,
	policy,
	pollPolicy backoff.RetryPolicy,
	isRetryable backoff.IsRetryable,
) matchingservice.MatchingServiceClient {
	return &retryableClient{
		client:      client,
		policy:      policy,
		pollPolicy:  pollPolicy,
		isRetryable: isRetryable,
	}
}
