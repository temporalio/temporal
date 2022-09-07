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
)

var _ RequestRateLimiter = (*MultiRequestRateLimiterImpl)(nil)

type (
	MultiRequestRateLimiterImpl struct {
		requestRateLimiters []RequestRateLimiter
	}
)

func NewMultiRequestRateLimiter(
	requestRateLimiters ...RequestRateLimiter,
) *MultiRequestRateLimiterImpl {
	if len(requestRateLimiters) == 0 {
		panic("expect at least one rate limiter")
	}
	return &MultiRequestRateLimiterImpl{
		requestRateLimiters: requestRateLimiters,
	}
}

func (rl *MultiRequestRateLimiterImpl) Allow(now time.Time, request Request) bool {
	length := len(rl.requestRateLimiters)
	reservations := make([]Reservation, 0, length)

	for _, requestRateLimiter := range rl.requestRateLimiters {
		reservation := requestRateLimiter.Reserve(now, request)
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

func (rl *MultiRequestRateLimiterImpl) Reserve(now time.Time, request Request) Reservation {
	length := len(rl.requestRateLimiters)
	reservations := make([]Reservation, 0, length)

	for _, requestRateLimiter := range rl.requestRateLimiters {
		reservation := requestRateLimiter.Reserve(now, request)
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

func (rl *MultiRequestRateLimiterImpl) Wait(ctx context.Context, request Request) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	now := time.Now().UTC()
	reservation := rl.Reserve(now, request)
	if !reservation.OK() {
		return fmt.Errorf("rate: Wait(n=%d) would exceed context deadline", request.Token)
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
		return fmt.Errorf("rate: Wait(n=%d) would exceed context deadline", request.Token)
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
