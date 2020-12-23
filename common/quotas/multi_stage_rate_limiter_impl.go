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
	"fmt"
	"time"

	"golang.org/x/time/rate"
)

const (
	InfDuration = rate.InfDuration
)

type (
	// MultiStageRateLimiterImpl is a wrapper around the limiter interface
	MultiStageRateLimiterImpl struct {
		rateLimiters []RateLimiter
	}

	MultiStageReservationImpl struct {
		ok           bool
		reservations []Reservation
	}
)

var _ RateLimiter = (*MultiStageRateLimiterImpl)(nil)
var _ Reservation = (*MultiStageReservationImpl)(nil)

// NewMultiStageRateLimiter returns a new rate limiter that have multiple stage
func NewMultiStageRateLimiter(
	rateLimiters []RateLimiter,
) *MultiStageRateLimiterImpl {
	return &MultiStageRateLimiterImpl{
		rateLimiters: rateLimiters,
	}
}

func NewMultiStageReservation(
	ok bool,
	reservations []Reservation,
) *MultiStageReservationImpl {
	return &MultiStageReservationImpl{
		ok:           ok,
		reservations: reservations,
	}
}

// Allow immediately returns with true or false indicating if a rate limit
// token is available or not
func (rl *MultiStageRateLimiterImpl) Allow() bool {
	return rl.AllowN(time.Now(), 1)
}

// AllowN immediately returns with true or false indicating if n rate limit
// token is available or not
func (rl *MultiStageRateLimiterImpl) AllowN(now time.Time, numToken int) bool {
	length := len(rl.rateLimiters)
	reservations := make([]Reservation, 0, length)

	for _, rateLimiter := range rl.rateLimiters {
		reservation := rateLimiter.ReserveN(now, numToken)
		if !reservation.OK() {
			// cancel all existing reservation
			for _, reservation := range reservations {
				reservation.CancelAt(now)
			}
			return false
		}
		reservations = append(reservations, reservation)
	}

	return true
}

// Reserve returns a Reservation that indicates how long the caller
// must wait before event happen.
func (rl *MultiStageRateLimiterImpl) Reserve() Reservation {
	return rl.ReserveN(time.Now(), 1)
}

// ReserveN returns a Reservation that indicates how long the caller
// must wait before event happen.
func (rl *MultiStageRateLimiterImpl) ReserveN(now time.Time, numToken int) Reservation {
	length := len(rl.rateLimiters)
	reservations := make([]Reservation, 0, length)

	for _, rateLimiter := range rl.rateLimiters {
		reservation := rateLimiter.ReserveN(now, numToken)
		if !reservation.OK() {
			// cancel all existing reservation
			for _, reservation := range reservations {
				reservation.CancelAt(now)
			}
			return NewMultiStageReservation(false, nil)
		}
		reservations = append(reservations, reservation)
	}

	return NewMultiStageReservation(true, reservations)
}

// Wait waits up till deadline for a rate limit token
func (rl *MultiStageRateLimiterImpl) Wait(ctx context.Context) error {
	return rl.WaitN(ctx, 1)
}

// WaitN waits up till deadline for n rate limit token
func (rl *MultiStageRateLimiterImpl) WaitN(ctx context.Context, numToken int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	now := time.Now().UTC()
	reservation := rl.ReserveN(now, numToken)
	if !reservation.OK() {
		return fmt.Errorf("rate: Wait(n=%d) would exceed context deadline", numToken)
	}

	delay := reservation.DelayFrom(now)
	if delay == 0 {
		return nil
	}
	waitLimit := InfDuration
	if deadline, ok := ctx.Deadline(); ok {
		waitLimit = deadline.Sub(now)
	}
	if waitLimit < delay {
		reservation.CancelAt(now)
		return fmt.Errorf("rate: Wait(n=%d) would exceed context deadline", numToken)
	}

	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C:
		return nil

	case <-ctx.Done():
		reservation.CancelAt(time.Now().UTC())
		return ctx.Err()
	}
}

// Rate returns the rate per second for this rate limiter
func (rl *MultiStageRateLimiterImpl) Rate() float64 {
	var result *float64
	for _, rateLimiter := range rl.rateLimiters {
		rate := rateLimiter.Rate()
		if result == nil || *result > rate {
			result = &rate
		}
	}
	// assuming at least one rate limiter within
	// if not, fail fast
	return *result
}

// Burst returns the burst for this rate limiter
func (rl *MultiStageRateLimiterImpl) Burst() int {
	var result *int
	for _, rateLimiter := range rl.rateLimiters {
		burst := rateLimiter.Burst()
		if result == nil || *result > burst {
			result = &burst
		}
	}
	// assuming at least one rate limiter within
	// if not, fail fast
	return *result
}

// OK returns whether the limiter can provide the requested number of tokens
func (r *MultiStageReservationImpl) OK() bool {
	return r.ok
}

// Cancel indicates that the reservation holder will not perform the reserved action
// and reverses the effects of this Reservation on the rate limit as much as possible
func (r *MultiStageReservationImpl) Cancel() {
	r.CancelAt(time.Now().UTC())
}

// Cancel indicates that the reservation holder will not perform the reserved action
// and reverses the effects of this Reservation on the rate limit as much as possible
func (r *MultiStageReservationImpl) CancelAt(now time.Time) {
	if !r.ok {
		return
	}

	for _, reservation := range r.reservations {
		reservation.CancelAt(now)
	}
}

// Delay returns the duration for which the reservation holder must wait
// before taking the reserved action.  Zero duration means act immediately.
func (r *MultiStageReservationImpl) Delay() time.Duration {
	return r.DelayFrom(time.Now().UTC())
}

// Delay returns the duration for which the reservation holder must wait
// before taking the reserved action.  Zero duration means act immediately.
func (r *MultiStageReservationImpl) DelayFrom(now time.Time) time.Duration {
	if !r.ok {
		return InfDuration
	}

	var result *time.Duration
	for _, reservation := range r.reservations {
		duration := reservation.DelayFrom(now)
		if result == nil || *result < duration {
			result = &duration
		}
	}
	// assuming at least one rate limiter within
	// if not, fail fast
	return *result
}
