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
	waiting := make(chan struct{})
	done := make(chan struct{})
	go func() {
		go func() {
			assert.NoError(t, rl.Wait(ctx))
			done <- struct{}{}
		}()

		// The waiting channel isn't enough by itself to confirm waiting, because
		// the inner go func can be called without the rl.Wait() having finished.
		// Use time.Sleep to allow rl.Wait() to complete before signalling `waiting`.
		// 1-millisecond sleep succeeded >50,000x, whereas 1-nanosecond sleep deadlocks.
		time.Sleep(time.Millisecond)
		waiting <- struct{}{}
	}()

	// once a waiter exists, recycle the token instead of advancing time
	<-waiting
	rl.RecycleToken()

	// wait until done so we know assert.NoError was called
	<-done
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
	waiting := make(chan struct{})
	done := make(chan struct{})
	go func() {
		go func() {
			err := rl.WaitN(ctx, 2)
			assert.ErrorContains(t, err, quotas.ErrRateLimiterWaitInterrupted.Error())
			done <- struct{}{}
		}()

		// The waiting channel isn't enough by itself to confirm waiting, because
		// the inner go func can be called without the rl.Wait() having finished.
		// Use time.Sleep to allow rl.Wait() to complete before signalling `waiting`.
		// 1-millisecond sleep succeeded >50,000x, whereas 1-nanosecond sleep deadlocks.
		time.Sleep(time.Millisecond)
		waiting <- struct{}{}
	}()

	// Once a waiter exists, recycle the token instead of advancing time.
	// This should be a no-op. If it works unexpectedly, assert.Error will fail.
	<-waiting
	rl.RecycleToken()

	// cancel the context so that we return an error
	cancel()

	// wait until done so we know assert.Error was called
	<-done
}
