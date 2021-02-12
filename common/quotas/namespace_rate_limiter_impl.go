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
		namespaceRateLimiterFn NamespaceRateLimiterFn

		sync.RWMutex
		namespaceRateLimiters map[string]RateLimiter
	}
)

var _ NamespaceRateLimiter = (*NamespaceRateLimiterImpl)(nil)

func NewNamespaceRateLimiter(
	namespaceRateLimiterFn NamespaceRateLimiterFn,
) *NamespaceRateLimiterImpl {
	return &NamespaceRateLimiterImpl{
		namespaceRateLimiterFn: namespaceRateLimiterFn,

		namespaceRateLimiters: make(map[string]RateLimiter),
	}
}

// Allow attempts to allow a request to go through. The method returns
// immediately with a true or false indicating if the request can make
// progress
func (r *NamespaceRateLimiterImpl) Allow(
	namespaceID string,
) bool {

	return r.AllowN(namespaceID, time.Now().UTC(), 1)
}

// AllowN attempts to allow a request to go through. The method returns
// immediately with a true or false indicating if the request can make
// progress
func (r *NamespaceRateLimiterImpl) AllowN(
	namespaceID string,
	now time.Time,
	numToken int,
) bool {

	rateLimiter := r.getOrInitRateLimiter(namespaceID)
	return rateLimiter.AllowN(now, numToken)
}

// Reserve returns a Reservation that indicates how long the caller
// must wait before event happen.
func (r *NamespaceRateLimiterImpl) Reserve(
	namespaceID string,
) Reservation {

	return r.ReserveN(namespaceID, time.Now().UTC(), 1)
}

// ReserveN returns a Reservation that indicates how long the caller
// must wait before event happen.
func (r *NamespaceRateLimiterImpl) ReserveN(
	namespaceID string,
	now time.Time,
	numToken int,
) Reservation {

	rateLimiter := r.getOrInitRateLimiter(namespaceID)
	return rateLimiter.ReserveN(now, numToken)
}

// Wait waits till the deadline for a rate limit token to allow the request
// to go through.
func (r *NamespaceRateLimiterImpl) Wait(
	ctx context.Context,
	namespaceID string,
) error {

	return r.WaitN(ctx, namespaceID, 1)
}

// WaitN waits till the deadline for a rate limit token to allow the request
// to go through.
func (r *NamespaceRateLimiterImpl) WaitN(
	ctx context.Context,
	namespaceID string,
	numToken int,
) error {

	rateLimiter := r.getOrInitRateLimiter(namespaceID)
	return rateLimiter.WaitN(ctx, numToken)
}

func (r *NamespaceRateLimiterImpl) getOrInitRateLimiter(
	namespaceID string,
) RateLimiter {
	r.RLock()
	rateLimiter, ok := r.namespaceRateLimiters[namespaceID]
	r.RUnlock()
	if ok {
		return rateLimiter
	}

	newRateLimiter := r.namespaceRateLimiterFn(namespaceID)
	r.Lock()
	defer r.Unlock()

	rateLimiter, ok = r.namespaceRateLimiters[namespaceID]
	if ok {
		return rateLimiter
	}
	r.namespaceRateLimiters[namespaceID] = newRateLimiter
	return newRateLimiter
}
