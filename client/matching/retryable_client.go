package matching

import (
	"errors"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/tqid"
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

func (c *retryableClient) Route(p tqid.Partition) (string, error) {
	// Ideally we wouldn't do a type-check here and require c.client to have
	// Route, but it would require changing too many types all over the place.
	// This isn't called in a hot path.
	rc, ok := c.client.(RoutingClient)
	if !ok {
		return "", errors.New("not routing client")
	}
	return rc.Route(p)
}
