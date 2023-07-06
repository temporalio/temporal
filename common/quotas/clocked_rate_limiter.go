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

	"github.com/jonboulle/clockwork"
	"golang.org/x/time/rate"
)

// ClockedRateLimiter wraps a rate.Limiter with a clockwork.Clock. It is used to ensure that the rate limiter respects
// the time determined by the clock.
type ClockedRateLimiter struct {
	rateLimiter *rate.Limiter
	clock       clockwork.Clock
}

var (
	ErrRateLimiterWaitInterrupted                       = errors.New("rate limiter wait interrupted")
	ErrRateLimiterReservationCannotBeMade               = errors.New("rate limiter reservation cannot be made due to insufficient quota")
	ErrRateLimiterReservationWouldExceedContextDeadline = errors.New("rate limiter reservation would exceed context deadline")
)

func NewClockedRateLimiter(rateLimiter *rate.Limiter, clock clockwork.Clock) ClockedRateLimiter {
	return ClockedRateLimiter{
		rateLimiter: rateLimiter,
		clock:       clock,
	}
}

func (l ClockedRateLimiter) Allow() bool {
	return l.AllowN(l.clock.Now(), 1)
}

func (l ClockedRateLimiter) AllowN(now time.Time, token int) bool {
	return l.rateLimiter.AllowN(now, token)
}

// ClockedReservation wraps a rate.Reservation with a clockwork.Clock. It is used to ensure that the reservation
// respects the time determined by the clock.
type ClockedReservation struct {
	reservation *rate.Reservation
	clock       clockwork.Clock
}

func (r ClockedReservation) OK() bool {
	return r.reservation.OK()
}

func (r ClockedReservation) Delay() time.Duration {
	return r.DelayFrom(r.clock.Now())
}

func (r ClockedReservation) DelayFrom(t time.Time) time.Duration {
	return r.reservation.DelayFrom(t)
}

func (r ClockedReservation) Cancel() {
	r.CancelAt(r.clock.Now())
}

func (r ClockedReservation) CancelAt(t time.Time) {
	r.reservation.CancelAt(t)
}

func (l ClockedRateLimiter) Reserve() ClockedReservation {
	return l.ReserveN(l.clock.Now(), 1)
}

func (l ClockedRateLimiter) ReserveN(now time.Time, token int) ClockedReservation {
	reservation := l.rateLimiter.ReserveN(now, token)
	return ClockedReservation{reservation, l.clock}
}

func (l ClockedRateLimiter) Wait(ctx context.Context) error {
	return l.WaitN(ctx, 1)
}

// WaitN is the only method that is different from rate.Limiter. We need to fully reimplement this method because
// the original method uses time.Now(), and does not allow us to pass in a time.Time. Fortunately, it can be built on
// top of ReserveN. However, there are some optimizations that we can make.
func (l ClockedRateLimiter) WaitN(ctx context.Context, token int) error {
	reservation := ClockedReservation{l.rateLimiter.ReserveN(l.clock.Now(), token), l.clock}
	if !reservation.OK() {
		return fmt.Errorf(
			"%w: reservation would delay for %v",
			ErrRateLimiterReservationCannotBeMade,
			reservation.Delay(),
		)
	}

	waitDuration := reservation.Delay()

	// Optimization: if the waitDuration is 0, we don't need to start a timer.
	if waitDuration <= 0 {
		return nil
	}

	// Optimization: if the waitDuration is longer than the context deadline, we can immediately return an error.
	if deadline, ok := ctx.Deadline(); ok {
		if l.clock.Now().Add(waitDuration).After(deadline) {
			reservation.Cancel()
			return fmt.Errorf(
				"%w: reservation would delay for %v",
				ErrRateLimiterReservationWouldExceedContextDeadline,
				reservation.Delay(),
			)
		}
	}

	timer := l.clock.NewTimer(waitDuration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		reservation.Cancel()
		return fmt.Errorf("%w: %v", ErrRateLimiterWaitInterrupted, ctx.Err())
	case <-timer.Chan():
		return nil
	}
}

func (l ClockedRateLimiter) SetLimitAt(t time.Time, newLimit rate.Limit) {
	l.rateLimiter.SetLimitAt(t, newLimit)
}

func (l ClockedRateLimiter) SetBurstAt(t time.Time, newBurst int) {
	l.rateLimiter.SetBurstAt(t, newBurst)
}
