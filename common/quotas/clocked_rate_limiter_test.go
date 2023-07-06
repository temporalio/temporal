// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package quotas_test

import (
	"context"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/quotas"
	"golang.org/x/time/rate"
)

func TestClockedRateLimiter_Allow_NoQuota(t *testing.T) {
	t.Parallel()

	clock := clockwork.NewRealClock()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(0, 0), clock)
	assert.False(t, rl.Allow())
}

func TestClockedRateLimiter_Allow_OneBurst(t *testing.T) {
	t.Parallel()

	clock := clockwork.NewFakeClock()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(0, 1), clock)
	assert.True(t, rl.Allow())
	assert.False(t, rl.Allow())
}

func TestClockedRateLimiter_Allow_RPS_TooHigh(t *testing.T) {
	t.Parallel()

	clock := clockwork.NewFakeClock()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(1, 1), clock)
	assert.True(t, rl.Allow())
	clock.Advance(999 * time.Millisecond)
	assert.False(t, rl.Allow())
}

func TestClockedRateLimiter_Allow_RPS_Ok(t *testing.T) {
	t.Parallel()

	clock := clockwork.NewFakeClock()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(1, 1), clock)
	assert.True(t, rl.Allow())
	clock.Advance(time.Second)
	assert.True(t, rl.Allow())
}

func TestClockedRateLimiter_AllowN_Ok(t *testing.T) {
	t.Parallel()

	clock := clockwork.NewFakeClock()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(0, 10), clock)
	assert.True(t, rl.AllowN(clock.Now(), 10))
}

func TestClockedRateLimiter_AllowN_NotOk(t *testing.T) {
	t.Parallel()

	clock := clockwork.NewFakeClock()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(0, 10), clock)
	assert.False(t, rl.AllowN(clock.Now(), 11))
}

func TestClockedRateLimiter_Wait_NoBurst(t *testing.T) {
	t.Parallel()

	clock := clockwork.NewFakeClock()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(1, 0), clock)
	ctx := context.Background()
	assert.ErrorIs(t, rl.Wait(ctx), quotas.ErrRateLimiterReservationCannotBeMade)
}

func TestClockedRateLimiter_Wait_Ok(t *testing.T) {
	t.Parallel()

	clock := clockwork.NewFakeClock()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(1, 1), clock)
	ctx := context.Background()
	assert.NoError(t, rl.Wait(ctx))

	go func() {
		clock.BlockUntil(1)
		clock.Advance(time.Second)
	}()
	assert.NoError(t, rl.Wait(ctx))
}

func TestClockedRateLimiter_Wait_Canceled(t *testing.T) {
	t.Parallel()

	clock := clockwork.NewFakeClock()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(1, 1), clock)
	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)
	assert.NoError(t, rl.Wait(ctx))

	go func() {
		clock.BlockUntil(1)
		clock.Advance(time.Millisecond * 999)
		clock.BlockUntil(1)
		cancel()
	}()
	assert.ErrorIs(t, rl.Wait(ctx), quotas.ErrRateLimiterWaitInterrupted)
}

func TestClockedRateLimiter_Reserve(t *testing.T) {
	t.Parallel()

	clock := clockwork.NewFakeClock()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(1, 1), clock)
	rl.Allow()
	reservation := rl.Reserve()
	assert.Equal(t, time.Second, reservation.DelayFrom(clock.Now()))
}

func TestClockedRateLimiter_Wait_DeadlineWouldExceed(t *testing.T) {
	t.Parallel()

	clock := clockwork.NewFakeClock()
	rl := quotas.NewClockedRateLimiter(rate.NewLimiter(1, 1), clock)
	rl.Allow()

	ctx := context.Background()

	ctx, cancel := context.WithDeadline(ctx, clock.Now().Add(500*time.Millisecond))
	t.Cleanup(cancel)
	assert.ErrorIs(t, rl.Wait(ctx), quotas.ErrRateLimiterReservationWouldExceedContextDeadline)
}
