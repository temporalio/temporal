package quotas_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/quotas"
)

// disallowingRateLimiter is a rate limiter whose Allow method always returns false.
type disallowingRateLimiter struct {
	// RequestRateLimiter is an embedded field so that disallowingRateLimiter implements that interface. It doesn't
	// actually delegate to this rate limiter, and this field should be left nil.
	quotas.RequestRateLimiter
}

func (rl disallowingRateLimiter) Allow(time.Time, quotas.Request) bool {
	return false
}

func TestNewDelayedRequestRateLimiter_NegativeDelay(t *testing.T) {
	t.Parallel()

	_, err := quotas.NewDelayedRequestRateLimiter(
		quotas.NoopRequestRateLimiter,
		-time.Nanosecond,
		clock.NewRealTimeSource(),
	)
	assert.ErrorIs(t, err, quotas.ErrNegativeDelay)
}

func TestNewDelayedRequestRateLimiter_ZeroDelay(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	drl, err := quotas.NewDelayedRequestRateLimiter(disallowingRateLimiter{}, 0, timeSource)
	require.NoError(t, err)
	assert.False(t, drl.Allow(time.Time{}, quotas.Request{}), "expected Allow to return false because we "+
		"immediately switched to the disallowing rate limiter due to the zero delay")
}

func TestDelayedRequestRateLimiter_Allow(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	drl, err := quotas.NewDelayedRequestRateLimiter(disallowingRateLimiter{}, time.Second, timeSource)
	require.NoError(t, err)
	timeSource.Advance(time.Second - time.Nanosecond)
	assert.True(t, drl.Allow(time.Time{}, quotas.Request{}), "expected Allow to return true because the "+
		"timer hasn't expired yet")
	timeSource.Advance(time.Nanosecond)
	assert.False(t, drl.Allow(time.Time{}, quotas.Request{}), "expected Allow to return false because the "+
		"timer expired, and we switched to the disallowing rate limiter")
}

func TestDelayedRequestRateLimiter_Cancel(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	drl, err := quotas.NewDelayedRequestRateLimiter(disallowingRateLimiter{}, time.Second, timeSource)
	require.NoError(t, err)
	timeSource.Advance(time.Second - time.Nanosecond)
	assert.True(t, drl.Cancel(), "expected Cancel to return true because the timer was stopped before it "+
		"expired")
	timeSource.Advance(time.Nanosecond)
	assert.True(t, drl.Allow(time.Time{}, quotas.Request{}), "expected Allow to return true because the "+
		"timer was stopped before it could expire")
	assert.False(t, drl.Cancel(), "expected Cancel to return false because the timer was already stopped")
}
