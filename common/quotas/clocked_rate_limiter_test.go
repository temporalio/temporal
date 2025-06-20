package quotas_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/quotas"
	"golang.org/x/time/rate"
)

func TestClockedRateLimiter_Allow_NoQuota(t *testing.T) {
	t.Parallel()

	ts := clock.NewRealTimeSource()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(0, 0), ts)
	assert.False(t, rl.Allow())
}

func TestClockedRateLimiter_Allow_OneBurst(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(0, 1), ts)
	assert.True(t, rl.Allow())
	assert.False(t, rl.Allow())
}

func TestClockedRateLimiter_Allow_RPS_TooHigh(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(1, 1), ts)
	assert.True(t, rl.Allow())
	ts.Advance(999 * time.Millisecond)
	assert.False(t, rl.Allow())
}

func TestClockedRateLimiter_Allow_RPS_Ok(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(1, 1), ts)
	assert.True(t, rl.Allow())
	ts.Advance(time.Second)
	assert.True(t, rl.Allow())
}

func TestClockedRateLimiter_AllowN_Ok(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(0, 10), ts)
	assert.True(t, rl.AllowN(ts.Now(), 10))
}

func TestClockedRateLimiter_AllowN_NotOk(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(0, 10), ts)
	assert.False(t, rl.AllowN(ts.Now(), 11))
}

func TestClockedRateLimiter_Wait_NoBurst(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(1, 0), ts)
	ctx := context.Background()
	assert.ErrorIs(t, rl.Wait(ctx), quotas.ErrRateLimiterReservationCannotBeMade)
}

func TestClockedRateLimiter_Wait_Ok(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(1, 1), ts)
	ctx := context.Background()
	assert.NoError(t, rl.Wait(ctx))

	go func() {
		ts.Advance(time.Second)
	}()
	assert.NoError(t, rl.Wait(ctx))
}

func TestClockedRateLimiter_Wait_Canceled(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(1, 1), ts)
	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)
	assert.NoError(t, rl.Wait(ctx))

	go func() {
		ts.Advance(time.Millisecond * 999)
		cancel()
	}()
	assert.ErrorIs(t, rl.Wait(ctx), quotas.ErrRateLimiterWaitInterrupted)
}

func TestClockedRateLimiter_Reserve(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(1, 1), ts)
	rl.Allow()
	reservation := rl.Reserve()
	assert.Equal(t, time.Second, reservation.DelayFrom(ts.Now()))
}

func TestClockedRateLimiter_Wait_DeadlineWouldExceed(t *testing.T) {
	t.Parallel()

	ts := clock.NewEventTimeSource()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(1, 1), ts)
	rl.Allow()

	ctx := context.Background()

	ctx, cancel := context.WithDeadline(ctx, ts.Now().Add(500*time.Millisecond))
	t.Cleanup(cancel)
	assert.ErrorIs(t, rl.Wait(ctx), quotas.ErrRateLimiterReservationWouldExceedContextDeadline)
}

// test that reservations for 1 token ARE unblocked by RecycleToken
func TestClockedRateLimiter_Wait_Recycle(t *testing.T) {
	t.Parallel()
	ts := clock.NewEventTimeSource()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(1, 1), ts)
	ctx := context.Background()

	// take first token
	assert.NoError(t, rl.Wait(ctx))

	// wait for next token and report when success
	var asserted atomic.Bool
	asserted.Store(false)
	go func() {
		assert.NoError(t, rl.Wait(ctx))
		asserted.Store(true)
	}()
	// wait for rl.Wait() to start and get to the select statement
	time.Sleep(10 * time.Millisecond) // nolint

	// once a waiter exists, recycle the token instead of advancing time
	rl.RecycleToken()

	// wait until done so we know assert.NoError was called
	assert.Eventually(t, func() bool { return asserted.Load() }, time.Second, time.Millisecond)
}

// test that reservations for >1 token are NOT unblocked by RecycleToken
func TestClockedRateLimiter_WaitN_NoRecycle(t *testing.T) {
	t.Parallel()
	ts := clock.NewEventTimeSource()

	// set burst to 2 so that the reservation succeeds and WaitN gets to the select statement
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(1, 2), ts)
	ctx, cancel := context.WithCancel(context.Background())

	// take first token
	assert.NoError(t, rl.Wait(ctx))

	// wait for 2 tokens, which will never get a recycle
	// expect a context cancel error instead once we advance time
	// wait for next token and report when success
	var asserted atomic.Bool
	asserted.Store(false)
	go func() {
		err := rl.WaitN(ctx, 2)
		assert.ErrorContains(t, err, quotas.ErrRateLimiterWaitInterrupted.Error())
		asserted.Store(true)
	}()
	// wait for rl.Wait() to start and get to the select statement
	time.Sleep(10 * time.Millisecond) // nolint

	// once a waiter exists, recycle the token instead of advancing time
	rl.RecycleToken()

	// cancel the context so that we return an error
	cancel()

	// wait until done so we know assert.NoError was called
	assert.Eventually(t, func() bool { return asserted.Load() }, time.Second, time.Millisecond)
}
