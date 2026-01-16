package matching

import (
	"context"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/backoff"
)

type retryHint struct{ bits *uint64 }
type retryHintCtxKey struct{}

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

func withRetryHint(ctx context.Context) context.Context {
	return context.WithValue(ctx, retryHintCtxKey{}, retryHint{bits: new(uint64)})
}

func getRetryHint(ctx context.Context) retryHint {
	hint, _ := ctx.Value(retryHintCtxKey{}).(retryHint)
	return hint
}

func (r retryHint) used(i int) bool {
	if r.bits != nil && i >= 0 && i < 64 {
		return (*r.bits)&(1<<i) != 0
	}
	return false
}

func (r retryHint) set(i int) {
	if r.bits != nil && i >= 0 && i < 64 {
		*r.bits |= 1 << i
	}
}
