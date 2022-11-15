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
)

type (
	// RoutingRequestRateLimiterFn returns generate a specific rate limiter
	RoutingRequestRateLimiterFn[T Request] func(req T) RequestRateLimiter[T]
	// RoutingRequestKeyFn returns the routing key for a rate limiter request
	RoutingRequestKeyFn[T Request] func(req T) string

	// RoutingRateLimiterImpl is a rate limiter special built for multi-tenancy
	RoutingRateLimiterImpl[T Request] struct {
		routingRequestRateLimiterFn RoutingRequestRateLimiterFn[T]
		routingReuqestKeyFn         RoutingRequestKeyFn[T]

		sync.RWMutex
		routingRateLimiters map[string]RequestRateLimiter[T]
	}
)

// var _ RequestRateLimiter = (*RoutingRateLimiterImpl)(nil)

func NewRoutingRateLimiter[T Request](
	routingRateLimiterFn RoutingRequestRateLimiterFn[T],
	routingReuqestKeyFn RoutingRequestKeyFn[T],
) *RoutingRateLimiterImpl[T] {
	return &RoutingRateLimiterImpl[T]{
		routingRequestRateLimiterFn: routingRateLimiterFn,
		routingReuqestKeyFn:         routingReuqestKeyFn,

		routingRateLimiters: make(map[string]RequestRateLimiter[T]),
	}
}

// Allow attempts to allow a request to go through. The method returns
// immediately with a true or false indicating if the request can make
// progress
func (r *RoutingRateLimiterImpl[T]) Allow(
	now time.Time,
	request T,
) bool {
	rateLimiter := r.getOrInitRateLimiter(request)
	return rateLimiter.Allow(now, request)
}

// Reserve returns a Reservation that indicates how long the caller
// must wait before event happen.
func (r *RoutingRateLimiterImpl[T]) Reserve(
	now time.Time,
	request T,
) Reservation {
	rateLimiter := r.getOrInitRateLimiter(request)
	return rateLimiter.Reserve(now, request)
}

// Wait waits till the deadline for a rate limit token to allow the request
// to go through.
func (r *RoutingRateLimiterImpl[T]) Wait(
	ctx context.Context,
	request T,
) error {
	rateLimiter := r.getOrInitRateLimiter(request)
	return rateLimiter.Wait(ctx, request)
}

func (r *RoutingRateLimiterImpl[T]) getOrInitRateLimiter(
	req T,
) RequestRateLimiter[T] {
	routingKey := r.routingReuqestKeyFn(req)

	r.RLock()
	rateLimiter, ok := r.routingRateLimiters[routingKey]
	r.RUnlock()
	if ok {
		return rateLimiter
	}

	newRateLimiter := r.routingRequestRateLimiterFn(req)
	r.Lock()
	defer r.Unlock()

	rateLimiter, ok = r.routingRateLimiters[routingKey]
	if ok {
		return rateLimiter
	}
	r.routingRateLimiters[routingKey] = newRateLimiter
	return newRateLimiter
}
