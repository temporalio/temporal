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
	"sync/atomic"

	"golang.org/x/time/rate"
)

type (
	// RateLimiter is a wrapper around the golang rate limiter
	RateLimiter struct {
		sync.Mutex
		rate          float64
		burst         int
		goRateLimiter atomic.Value // type *rate.Limiter
	}
)

// NewRateLimiter returns a new rate limiter that can handle dynamic
// configuration updates
func NewRateLimiter(newRate float64, newBurst int) *RateLimiter {
	rl := &RateLimiter{
		rate:  newRate,
		burst: newBurst,
	}
	rl.goRateLimiter.Store(rate.NewLimiter(rate.Limit(rl.rate), rl.burst))
	return rl
}

// SetRate set the rate of the rate limiter
func (rl *RateLimiter) SetRate(rate float64) {
	rl.refreshInternalRateLimiter(&rate, nil)
}

// Burst set the burst of the rate limiter
func (rl *RateLimiter) SetBurst(burst int) {
	rl.refreshInternalRateLimiter(nil, &burst)
}

// SetRateBurst set the rate & burst of the rate limiter
func (rl *RateLimiter) SetRateBurst(rate float64, burst int) {
	rl.refreshInternalRateLimiter(&rate, &burst)
}

// Allow immediately returns with true or false indicating if a rate limit
// token is available or not
func (rl *RateLimiter) Allow() bool {
	limiter := rl.goRateLimiter.Load().(*rate.Limiter)
	return limiter.Allow()
}

// Reserve reserves a rate limit token
func (rl *RateLimiter) Reserve() *rate.Reservation {
	limiter := rl.goRateLimiter.Load().(*rate.Limiter)
	return limiter.Reserve()
}

// Wait waits up till deadline for a rate limit token
func (rl *RateLimiter) Wait(ctx context.Context) error {
	limiter := rl.goRateLimiter.Load().(*rate.Limiter)
	return limiter.Wait(ctx)
}

// Rate returns the rate per second for this rate limiter
func (rl *RateLimiter) Rate() float64 {
	rl.Lock()
	defer rl.Unlock()
	return rl.rate
}

// Burst returns the burst for this rate limiter
func (rl *RateLimiter) Burst() int {
	rl.Lock()
	defer rl.Unlock()
	return rl.burst
}

func (rl *RateLimiter) refreshInternalRateLimiter(
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
		limiter := rate.NewLimiter(rate.Limit(rl.rate), rl.burst)
		rl.goRateLimiter.Store(limiter)
	}
}
