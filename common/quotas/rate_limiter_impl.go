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
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type (
	// RateLimiterImpl is a wrapper around the golang rate limiter
	RateLimiterImpl struct {
		sync.RWMutex
		rate          float64
		burst         int
		goRateLimiter *rate.Limiter
	}
)

var _ RateLimiter = (*RateLimiterImpl)(nil)

// NewRateLimiter returns a new rate limiter that can handle dynamic
// configuration updates
func NewRateLimiter(newRate float64, newBurst int) *RateLimiterImpl {
	rl := &RateLimiterImpl{
		rate:          newRate,
		burst:         newBurst,
		goRateLimiter: rate.NewLimiter(rate.Limit(newRate), newBurst),
	}

	return rl
}

// SetRate set the rate of the rate limiter
func (rl *RateLimiterImpl) SetRate(rate float64) {
	rl.refreshInternalRateLimiterImpl(&rate, nil)
}

// SetBurst set the burst of the rate limiter
func (rl *RateLimiterImpl) SetBurst(burst int) {
	rl.refreshInternalRateLimiterImpl(nil, &burst)
}

// SetRateBurst set the rate & burst of the rate limiter
func (rl *RateLimiterImpl) SetRateBurst(rate float64, burst int) {
	rl.refreshInternalRateLimiterImpl(&rate, &burst)
}

// Allow immediately returns with true or false indicating if a rate limit
// token is available or not
func (rl *RateLimiterImpl) Allow() bool {
	return rl.goRateLimiter.Allow()
}

// AllowN immediately returns with true or false indicating if n rate limit
// token is available or not
func (rl *RateLimiterImpl) AllowN(now time.Time, numToken int) bool {
	return rl.goRateLimiter.AllowN(now, numToken)
}

// Reserve reserves a rate limit token
func (rl *RateLimiterImpl) Reserve() Reservation {
	return rl.goRateLimiter.Reserve()
}

// ReserveN reserves n rate limit token
func (rl *RateLimiterImpl) ReserveN(now time.Time, numToken int) Reservation {
	return rl.goRateLimiter.ReserveN(now, numToken)
}

// Wait waits up till deadline for a rate limit token
func (rl *RateLimiterImpl) Wait(ctx context.Context) error {
	return rl.goRateLimiter.Wait(ctx)
}

// WaitN waits up till deadline for n rate limit token
func (rl *RateLimiterImpl) WaitN(ctx context.Context, numToken int) error {
	return rl.goRateLimiter.WaitN(ctx, numToken)
}

// Rate returns the rate per second for this rate limiter
func (rl *RateLimiterImpl) Rate() float64 {
	rl.Lock()
	defer rl.Unlock()
	return rl.rate
}

// Burst returns the burst for this rate limiter
func (rl *RateLimiterImpl) Burst() int {
	rl.Lock()
	defer rl.Unlock()
	return rl.burst
}

func (rl *RateLimiterImpl) refreshInternalRateLimiterImpl(
	newRate *float64,
	newBurst *int,
) {
	rl.Lock()
	defer rl.Unlock()

	refresh := false
	if newRate != nil && rl.rate != *newRate {
		rl.rate = *newRate
		refresh = true
	}
	if newBurst != nil && rl.burst != *newBurst {
		rl.burst = *newBurst
		refresh = true
	}

	if refresh {
		now := time.Now()
		rl.goRateLimiter.SetLimitAt(now, rate.Limit(rl.rate))
		rl.goRateLimiter.SetBurstAt(now, rl.burst)
	}
}
