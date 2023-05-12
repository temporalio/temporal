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
	// RequestRateLimiterKeyFn extracts the map key from the request
	RequestRateLimiterKeyFn[K comparable] func(req Request) K

	// MapRequestRateLimiterImpl is a generic wrapper rate limiter for a set of rate limiters
	// identified by a key
	MapRequestRateLimiterImpl[K comparable] struct {
		rateLimiterGenFn RequestRateLimiterFn
		rateLimiterKeyFn RequestRateLimiterKeyFn[K]

		sync.RWMutex
		rateLimiters map[K]RequestRateLimiter
	}
)

func NewMapRequestRateLimiter[K comparable](
	rateLimiterGenFn RequestRateLimiterFn,
	rateLimiterKeyFn RequestRateLimiterKeyFn[K],
) *MapRequestRateLimiterImpl[K] {
	return &MapRequestRateLimiterImpl[K]{
		rateLimiterGenFn: rateLimiterGenFn,
		rateLimiterKeyFn: rateLimiterKeyFn,
		rateLimiters:     make(map[K]RequestRateLimiter),
	}
}

func namespaceRequestRateLimiterKeyFn(req Request) string {
	return req.Caller
}

func NewNamespaceRequestRateLimiter(
	rateLimiterGenFn RequestRateLimiterFn,
) *MapRequestRateLimiterImpl[string] {
	return NewMapRequestRateLimiter[string](rateLimiterGenFn, namespaceRequestRateLimiterKeyFn)
}

// Allow attempts to allow a request to go through. The method returns
// immediately with a true or false indicating if the request can make
// progress
func (r *MapRequestRateLimiterImpl[_]) Allow(
	now time.Time,
	request Request,
) bool {
	rateLimiter := r.getOrInitRateLimiter(request)
	return rateLimiter.Allow(now, request)
}

// Reserve returns a Reservation that indicates how long the caller
// must wait before event happen.
func (r *MapRequestRateLimiterImpl[_]) Reserve(
	now time.Time,
	request Request,
) Reservation {
	rateLimiter := r.getOrInitRateLimiter(request)
	return rateLimiter.Reserve(now, request)
}

// Wait waits till the deadline for a rate limit token to allow the request
// to go through.
func (r *MapRequestRateLimiterImpl[_]) Wait(
	ctx context.Context,
	request Request,
) error {
	rateLimiter := r.getOrInitRateLimiter(request)
	return rateLimiter.Wait(ctx, request)
}

func (r *MapRequestRateLimiterImpl[_]) getOrInitRateLimiter(
	req Request,
) RequestRateLimiter {
	key := r.rateLimiterKeyFn(req)

	r.RLock()
	rateLimiter, ok := r.rateLimiters[key]
	r.RUnlock()
	if ok {
		return rateLimiter
	}

	newRateLimiter := r.rateLimiterGenFn(req)
	r.Lock()
	defer r.Unlock()

	rateLimiter, ok = r.rateLimiters[key]
	if ok {
		return rateLimiter
	}

	r.rateLimiters[key] = newRateLimiter
	return newRateLimiter
}
