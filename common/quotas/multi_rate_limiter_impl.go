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
	// MultiRateLimiterImpl is a wrapper around the limiter interface
	MultiRateLimiterImpl struct {
		rateLimiters []RateLimiter
	}
)

var _ RateLimiter = (*MultiRateLimiterImpl)(nil)

// NewMultiRateLimiter returns a new rate limiter that have multiple stage
func NewMultiRateLimiter(
	rateLimiters []RateLimiter,
) *MultiRateLimiterImpl {
	if len(rateLimiters) == 0 {
		panic("expect at least one rate limiter")
	}
	return &MultiRateLimiterImpl{
		rateLimiters: rateLimiters,
	}
}

// Allow immediately returns with true or false indicating if a rate limit
// token is available or not
func (rl *MultiRateLimiterImpl) Allow() bool {
	return rl.AllowN(time.Now(), 1)
}

// AllowN immediately returns with true or false indicating if n rate limit
// token is available or not
func (rl *MultiRateLimiterImpl) AllowN(now time.Time, numToken int) bool {
	length := len(rl.rateLimiters)
	reservations := make([]Reservation, 0, length)

	for _, rateLimiter := range rl.rateLimiters {
		reservation := rateLimiter.ReserveN(now, numToken)
		if !reservation.OK() || reservation.DelayFrom(now) > 0 {
			if reservation.OK() {
				reservation.CancelAt(now)
			}

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
func (rl *MultiRateLimiterImpl) Reserve() Reservation {
	return rl.ReserveN(time.Now(), 1)
}

// ReserveN returns a Reservation that indicates how long the caller
// must wait before event happen.
func (rl *MultiRateLimiterImpl) ReserveN(now time.Time, numToken int) Reservation {
	length := len(rl.rateLimiters)
	reservations := make([]Reservation, 0, length)

	for _, rateLimiter := range rl.rateLimiters {
		reservation := rateLimiter.ReserveN(now, numToken)
		if !reservation.OK() {
			// cancel all existing reservation
			for _, reservation := range reservations {
				reservation.CancelAt(now)
			}
			return NewMultiReservation(false, nil)
		}
		reservations = append(reservations, reservation)
	}

	return NewMultiReservation(true, reservations)
}

// Wait waits up till deadline for a rate limit token
func (rl *MultiRateLimiterImpl) Wait(ctx context.Context) error {
	return rl.WaitN(ctx, 1)
}

// WaitN waits up till deadline for n rate limit token
func (rl *MultiRateLimiterImpl) WaitN(ctx context.Context, numToken int) error {
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
		reservation.CancelAt(time.Now())
		return ctx.Err()
	}
}

// Rate returns the rate per second for this rate limiter
func (rl *MultiRateLimiterImpl) Rate() float64 {
	result := rl.rateLimiters[0].Rate()
	for _, rateLimiter := range rl.rateLimiters {
		newRate := rateLimiter.Rate()
		if result > newRate {
			result = newRate
		}
	}
	return result
}

// Burst returns the burst for this rate limiter
func (rl *MultiRateLimiterImpl) Burst() int {
	result := rl.rateLimiters[0].Burst()
	for _, rateLimiter := range rl.rateLimiters {
		newBurst := rateLimiter.Burst()
		if result > newBurst {
			result = newBurst
		}
	}
	return result
}
