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

	"golang.org/x/time/rate"
)

const (
	defaultRefreshInterval = time.Minute
	defaultRateBurstRatio  = 2
)

type (
	// DynamicRateLimiter implements a dynamic config wrapper around the rate limiter
	DynamicRateLimiter struct {
		rateFn          RateFn
		burstFn         BurstFn
		refreshInterval time.Duration

		refreshTimer *time.Timer
		rateLimiter  *RateLimiter
	}
)

// NewDynamicRateLimiter returns a rate limiter which handles dynamic config
func NewDynamicRateLimiter(
	rateFn RateFn,
	burstFn BurstFn,
	refreshInterval time.Duration,
) *DynamicRateLimiter {
	rateLimiter := &DynamicRateLimiter{
		rateFn:          rateFn,
		burstFn:         burstFn,
		refreshInterval: refreshInterval,

		refreshTimer: time.NewTimer(refreshInterval),
		rateLimiter:  NewRateLimiter(rateFn(), burstFn()),
	}
	return rateLimiter
}

// NewDefaultIncomingDynamicRateLimiter returns a default rate limiter
// for incoming traffic
func NewDefaultIncomingDynamicRateLimiter(
	rateFn RateFn,
) *DynamicRateLimiter {
	return NewDynamicRateLimiter(
		rateFn,
		func() int { return defaultRateBurstRatio * int(rateFn()) },
		defaultRefreshInterval,
	)
}

// NewDefaultOutgoingDynamicRateLimiter returns a default rate limiter
// for outgoing traffic
func NewDefaultOutgoingDynamicRateLimiter(
	rateFn RateFn,
) *DynamicRateLimiter {
	return NewDynamicRateLimiter(
		rateFn,
		func() int { return int(rateFn()) },
		defaultRefreshInterval,
	)
}

// Allow immediately returns with true or false indicating if a rate limit
// token is available or not
func (d *DynamicRateLimiter) Allow() bool {
	d.maybeRefresh()
	return d.rateLimiter.Allow()
}

// Reserve reserves a rate limit token
func (d *DynamicRateLimiter) Reserve() *rate.Reservation {
	d.maybeRefresh()
	return d.rateLimiter.Reserve()
}

// Wait waits up till deadline for a rate limit token
func (d *DynamicRateLimiter) Wait(ctx context.Context) error {
	d.maybeRefresh()
	return d.rateLimiter.Wait(ctx)
}

// Rate returns the rate per second for this rate limiter
func (d *DynamicRateLimiter) Rate() float64 {
	return d.rateLimiter.Rate()
}

// Burst returns the burst for this rate limiter
func (d *DynamicRateLimiter) Burst() int {
	return d.rateLimiter.Burst()
}

func (d *DynamicRateLimiter) maybeRefresh() {
	select {
	case <-d.refreshTimer.C:
		d.refreshTimer.Reset(d.refreshInterval)
		d.rateLimiter.SetRateBurst(d.rateFn(), d.burstFn())

	default:
		// noop
	}
}
