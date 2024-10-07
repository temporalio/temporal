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

package quotas

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/server/common/clock"
	"golang.org/x/time/rate"
)

// ClockedRateLimiter wraps a rate.Limiter with a clock.TimeSource. It is used to ensure that the rate limiter respects
// the time determined by the timeSource.
type ClockedRateLimiter struct {
	rateLimiter *rate.Limiter
	timeSource  clock.TimeSource
	recycleCh   chan struct{}
}

var (
	ErrRateLimiterWaitInterrupted                       = errors.New("rate limiter wait interrupted")
	ErrRateLimiterReservationCannotBeMade               = errors.New("rate limiter reservation cannot be made due to insufficient quota")
	ErrRateLimiterReservationWouldExceedContextDeadline = errors.New("rate limiter reservation would exceed context deadline")
)

func NewClockedRateLimiter(rateLimiter *rate.Limiter, timeSource clock.TimeSource) ClockedRateLimiter {
	return ClockedRateLimiter{
		rateLimiter: rateLimiter,
		timeSource:  timeSource,
		recycleCh:   make(chan struct{}),
	}
}

func (l ClockedRateLimiter) Allow() bool {
	return l.AllowN(l.timeSource.Now(), 1)
}

func (l ClockedRateLimiter) AllowN(now time.Time, token int) bool {
	return l.rateLimiter.AllowN(now, token)
}

// ClockedReservation wraps a rate.Reservation with a clockwork.Clock. It is used to ensure that the reservation
// respects the time determined by the timeSource.
type ClockedReservation struct {
	reservation *rate.Reservation
	timeSource  clock.TimeSource
}

func (r ClockedReservation) OK() bool {
	return r.reservation.OK()
}

func (r ClockedReservation) Delay() time.Duration {
	return r.DelayFrom(r.timeSource.Now())
}

func (r ClockedReservation) DelayFrom(t time.Time) time.Duration {
	return r.reservation.DelayFrom(t)
}

func (r ClockedReservation) Cancel() {
	r.CancelAt(r.timeSource.Now())
}

func (r ClockedReservation) CancelAt(t time.Time) {
	r.reservation.CancelAt(t)
}

func (l ClockedRateLimiter) Reserve() ClockedReservation {
	return l.ReserveN(l.timeSource.Now(), 1)
}

func (l ClockedRateLimiter) ReserveN(now time.Time, token int) ClockedReservation {
	reservation := l.rateLimiter.ReserveN(now, token)
	return ClockedReservation{reservation, l.timeSource}
}

func (l ClockedRateLimiter) Wait(ctx context.Context) error {
	return l.WaitN(ctx, 1)
}

// WaitN is the only method that is different from rate.Limiter. We need to fully reimplement this method because
// the original method uses time.Now(), and does not allow us to pass in a time.Time. Fortunately, it can be built on
// top of ReserveN. However, there are some optimizations that we can make.
func (l ClockedRateLimiter) WaitN(ctx context.Context, token int) error {
	reservation := ClockedReservation{l.rateLimiter.ReserveN(l.timeSource.Now(), token), l.timeSource}
	if !reservation.OK() {
		return fmt.Errorf("%w: WaitN(n=%d)", ErrRateLimiterReservationCannotBeMade, token)
	}

	waitDuration := reservation.Delay()

	// Optimization: if the waitDuration is 0, we don't need to start a timer.
	if waitDuration <= 0 {
		return nil
	}

	// Optimization: if the waitDuration is longer than the context deadline, we can immediately return an error.
	if deadline, ok := ctx.Deadline(); ok {
		if l.timeSource.Now().Add(waitDuration).After(deadline) {
			reservation.Cancel()
			return fmt.Errorf("%w: WaitN(n=%d)", ErrRateLimiterReservationWouldExceedContextDeadline, token)
		}
	}

	waitExpired := make(chan struct{})
	timer := l.timeSource.AfterFunc(waitDuration, func() {
		close(waitExpired)
	})
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			reservation.Cancel()
			return fmt.Errorf("%w: %v", ErrRateLimiterWaitInterrupted, ctx.Err())
		case <-waitExpired:
			return nil
		case <-l.recycleCh:
			if token > 1 {
				break
			}
			reservation.Cancel()
			return nil
		}
	}
}

func (l ClockedRateLimiter) SetLimitAt(t time.Time, newLimit rate.Limit) {
	l.rateLimiter.SetLimitAt(t, newLimit)
}

func (l ClockedRateLimiter) SetBurstAt(t time.Time, newBurst int) {
	l.rateLimiter.SetBurstAt(t, newBurst)
}

func (l ClockedRateLimiter) TokensAt(t time.Time) int {
	return int(l.rateLimiter.TokensAt(t))
}

// RecycleToken should be called when the action being rate limited was not completed
// for some reason (i.e. a task is not dispatched because it was invalid).
// In this case, we want to immediately unblock another process that is waiting for one token
// so that the actual rate of completed actions is as close to the intended rate limit as possible.
// If no process is waiting for a token when RecycleToken is called, this is a no-op.
//
// Since we don't know how many tokens were reserved by the process calling recycle, we will only unblock
// new reservations that are for one token (otherwise we could recycle a 1-token-reservation and unblock
// a 100-token-reservation). If all waiting processes are waiting for >1 tokens, this is a no-op.
//
// Because recycleCh is an unbuffered channel, the token will be reused for the next waiter as long
// as there exists a waiter at the time RecycleToken is called. Usually the attempted rate is consistently
// above or below the limit for a period of time, so if rate limiting is in effect and recycling matters,
// most likely there will be a waiter. If the actual rate is erratically bouncing to either side of the
// rate limit AND there are many invalid tasks, we can make it a buffered channel instead.
//
// Our goal is to ensure that each token in our bucket is used every second, meaning the time between
// taking and successfully using a token must be <= 1s. For this to be true, we must have:
//
//	time_to_recycle * number_of_recycles_per_second <= 1s
//	time_to_recycle * probability_of_recycle * number_of_attempts_per_second <= 1s
//
// Therefore, it is also possible for this strategy to be inaccurate if the delay between taking and
// successfully using a token is greater than one second.
//
// Currently, RecycleToken is called when we take a token to attempt a matching task dispatch and
// then later find out (usually via RPC to History) that the task should not be dispatched.
// If history rpc takes 10ms --> 100 opportunities for the token to be used that second --> 99% recycle probability is ok.
// If recycle probability is 50% --> need at least 2 opportunities for token to be used --> 500ms history rpc time is ok.
func (l ClockedRateLimiter) RecycleToken() {
	select {
	case l.recycleCh <- struct{}{}:
	default:
	}
}
