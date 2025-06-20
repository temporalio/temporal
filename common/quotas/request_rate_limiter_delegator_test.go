package quotas_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/quotas"
)

// blockingRequestRateLimiter is a rate limiter that blocks in its Wait method until the context is canceled.
type blockingRequestRateLimiter struct {
	// RateLimiter is an embedded field so that blockingRequestRateLimiter implements the RateLimiter interface. It doesn't
	// actually delegate to this rate limiter, and this field should be left nil.
	quotas.RequestRateLimiter
	// waitStarted is a channel which is sent to as soon as Wait is called.
	waitStarted chan struct{}
}

func (b *blockingRequestRateLimiter) Wait(ctx context.Context, _ quotas.Request) error {
	b.waitStarted <- struct{}{}
	<-ctx.Done()
	return ctx.Err()
}

// TestRateLimiterDelegator_Wait verifies that the RequestRateLimiterDelegator.Wait method can be called concurrently even if
// the rate limiter it delegates to is switched while the method is being called. The same condition should hold for
// all methods on RequestRateLimiterDelegator, but we only test Wait here for simplicity.
func TestRateLimiterDelegator_Wait(t *testing.T) {
	t.Parallel()

	blockingRateLimiter := &blockingRequestRateLimiter{
		waitStarted: make(chan struct{}),
	}
	delegator := &quotas.RequestRateLimiterDelegator{}
	delegator.SetRateLimiter(blockingRateLimiter)

	ctx, cancel := context.WithCancel(context.Background())
	waitErrs := make(chan error)

	go func() {
		waitErrs <- delegator.Wait(ctx, quotas.Request{})
	}()
	<-blockingRateLimiter.waitStarted
	delegator.SetRateLimiter(quotas.NoopRequestRateLimiter)
	assert.NoError(t, delegator.Wait(ctx, quotas.Request{}))
	select {
	case err := <-waitErrs:
		t.Fatal("Wait returned before context was canceled:", err)
	default:
	}
	cancel()
	assert.ErrorIs(t, <-waitErrs, context.Canceled)
}

func TestRateLimiterDelegator_SetRateLimiter(t *testing.T) {
	t.Parallel()

	delegator := &quotas.RequestRateLimiterDelegator{}
	delegator.SetRateLimiter(quotas.NoopRequestRateLimiter)
	testNoopRequestRateLimiterImpl(t, delegator)
}
