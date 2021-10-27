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
	"time"
)

const (
	defaultRefreshInterval        = time.Minute
	defaultIncomingRateBurstRatio = float64(2)
	defaultOutgoingRateBurstRatio = float64(1)
)

type (
	// DynamicRateLimiterImpl implements a dynamic config wrapper around the rate limiter
	DynamicRateLimiterImpl struct {
		rateBurstFn     RateBurst
		refreshInterval time.Duration

		refreshTimer *time.Timer
		rateLimiter  *RateLimiterImpl
	}
)

var _ RateLimiter = (*DynamicRateLimiterImpl)(nil)

// NewDynamicRateLimiter returns a rate limiter which handles dynamic config
func NewDynamicRateLimiter(
	rateBurstFn RateBurst,
	refreshInterval time.Duration,
) *DynamicRateLimiterImpl {
	rateLimiter := &DynamicRateLimiterImpl{
		rateBurstFn:     rateBurstFn,
		refreshInterval: refreshInterval,

		refreshTimer: time.NewTimer(refreshInterval),
		rateLimiter:  NewRateLimiter(rateBurstFn.Rate(), rateBurstFn.Burst()),
	}
	return rateLimiter
}

// NewDefaultIncomingRateLimiter returns a default rate limiter
// for incoming traffic
func NewDefaultIncomingRateLimiter(
	rateFn RateFn,
) *DynamicRateLimiterImpl {
	return NewDynamicRateLimiter(
		NewDefaultIncomingRateBurst(rateFn),
		defaultRefreshInterval,
	)
}

// NewDefaultOutgoingRateLimiter returns a default rate limiter
// for outgoing traffic
func NewDefaultOutgoingRateLimiter(
	rateFn RateFn,
) *DynamicRateLimiterImpl {
	return NewDynamicRateLimiter(
		NewDefaultOutgoingRateBurst(rateFn),
		defaultRefreshInterval,
	)
}

// Allow immediately returns with true or false indicating if a rate limit
// token is available or not
func (d *DynamicRateLimiterImpl) Allow() bool {
	d.maybeRefresh()
	return d.rateLimiter.Allow()
}

// AllowN immediately returns with true or false indicating if n rate limit
// token is available or not
func (d *DynamicRateLimiterImpl) AllowN(now time.Time, numToken int) bool {
	d.maybeRefresh()
	return d.rateLimiter.AllowN(now, numToken)
}

// Reserve reserves a rate limit token
func (d *DynamicRateLimiterImpl) Reserve() Reservation {
	d.maybeRefresh()
	return d.rateLimiter.Reserve()
}

// ReserveN reserves n rate limit token
func (d *DynamicRateLimiterImpl) ReserveN(now time.Time, numToken int) Reservation {
	d.maybeRefresh()
	return d.rateLimiter.ReserveN(now, numToken)
}

// Wait waits up till deadline for a rate limit token
func (d *DynamicRateLimiterImpl) Wait(ctx context.Context) error {
	d.maybeRefresh()
	return d.rateLimiter.Wait(ctx)
}

// WaitN waits up till deadline for n rate limit token
func (d *DynamicRateLimiterImpl) WaitN(ctx context.Context, numToken int) error {
	d.maybeRefresh()
	return d.rateLimiter.WaitN(ctx, numToken)
}

// Rate returns the rate per second for this rate limiter
func (d *DynamicRateLimiterImpl) Rate() float64 {
	return d.rateLimiter.Rate()
}

// Burst returns the burst for this rate limiter
func (d *DynamicRateLimiterImpl) Burst() int {
	return d.rateLimiter.Burst()
}

func (d *DynamicRateLimiterImpl) maybeRefresh() {
	select {
	case <-d.refreshTimer.C:
		d.refreshTimer.Reset(d.refreshInterval)
		d.rateLimiter.SetRateBurst(d.rateBurstFn.Rate(), d.rateBurstFn.Burst())

	default:
		// noop
	}
}
