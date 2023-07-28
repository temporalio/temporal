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
	"sync/atomic"
	"time"
)

// RequestRateLimiterDelegator is a request rate limiter that delegates to another rate limiter. The delegate can be
// changed at runtime by calling SetRateLimiter. This rate limiter is useful for cases where you want to substitute one
// rate limiter implementation for another at runtime. All methods of this type are thread-safe.
type RequestRateLimiterDelegator struct {
	// delegate is an atomic.Value so that it can be safely read from and written to concurrently. It stores the rate
	// limiter that this rate limiter delegates to as a monomorphicRequestRateLimiter.
	delegate atomic.Value
}

// monomorphicRequestRateLimiter is a workaround for the fact that the value stored in an atomic.Value must always be
// the same type, but we want to allow the rate limiter delegate to be any type that implements the RequestRateLimiter
// interface.
type monomorphicRequestRateLimiter struct {
	RequestRateLimiter
}

// SetRateLimiter sets the rate limiter to delegate to.
func (d *RequestRateLimiterDelegator) SetRateLimiter(rl RequestRateLimiter) {
	d.delegate.Store(monomorphicRequestRateLimiter{rl})
}

// loadDelegate returns the rate limiter that this rate limiter delegates to.
func (d *RequestRateLimiterDelegator) loadDelegate() RequestRateLimiter {
	return d.delegate.Load().(RequestRateLimiter)
}

// The following methods just delegate to the underlying rate limiter.

func (d *RequestRateLimiterDelegator) Allow(now time.Time, request Request) bool {
	return d.loadDelegate().Allow(now, request)
}

func (d *RequestRateLimiterDelegator) Reserve(now time.Time, request Request) Reservation {
	return d.loadDelegate().Reserve(now, request)
}

func (d *RequestRateLimiterDelegator) Wait(ctx context.Context, request Request) error {
	return d.loadDelegate().Wait(ctx, request)
}
