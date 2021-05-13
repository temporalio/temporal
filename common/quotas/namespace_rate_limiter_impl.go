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
	// NamespaceRateLimiterImpl is a rate limiter special built for multi-tenancy
	NamespaceRateLimiterImpl struct {
		namespaceRateLimiterFn RequestRateLimiterFn

		sync.RWMutex
		namespaceRateLimiters map[string]RequestRateLimiter
	}
)

var _ RequestRateLimiter = (*NamespaceRateLimiterImpl)(nil)

func NewNamespaceRateLimiter(
	namespaceRateLimiterFn RequestRateLimiterFn,
) *NamespaceRateLimiterImpl {
	return &NamespaceRateLimiterImpl{
		namespaceRateLimiterFn: namespaceRateLimiterFn,

		namespaceRateLimiters: make(map[string]RequestRateLimiter),
	}
}

// Allow attempts to allow a request to go through. The method returns
// immediately with a true or false indicating if the request can make
// progress
func (r *NamespaceRateLimiterImpl) Allow(
	now time.Time,
	request Request,
) bool {
	rateLimiter := r.getOrInitRateLimiter(request)
	return rateLimiter.Allow(now, request)
}

// Reserve returns a Reservation that indicates how long the caller
// must wait before event happen.
func (r *NamespaceRateLimiterImpl) Reserve(
	now time.Time,
	request Request,
) Reservation {
	rateLimiter := r.getOrInitRateLimiter(request)
	return rateLimiter.Reserve(now, request)
}

// Wait waits till the deadline for a rate limit token to allow the request
// to go through.
func (r *NamespaceRateLimiterImpl) Wait(
	ctx context.Context,
	request Request,
) error {
	rateLimiter := r.getOrInitRateLimiter(request)
	return rateLimiter.Wait(ctx, request)
}

func (r *NamespaceRateLimiterImpl) getOrInitRateLimiter(
	req Request,
) RequestRateLimiter {
	r.RLock()
	rateLimiter, ok := r.namespaceRateLimiters[req.Caller]
	r.RUnlock()
	if ok {
		return rateLimiter
	}

	newRateLimiter := r.namespaceRateLimiterFn(req)
	r.Lock()
	defer r.Unlock()

	rateLimiter, ok = r.namespaceRateLimiters[req.Caller]
	if ok {
		return rateLimiter
	}
	r.namespaceRateLimiters[req.Caller] = newRateLimiter
	return newRateLimiter
}
