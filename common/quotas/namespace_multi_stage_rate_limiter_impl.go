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
)

type (
	NamespaceRateLimiterFn func(namespaceID string) RateLimiter

	// NamespaceMultiStageRateLimiterImpl is a multi stage rate limiter
	// special built for multi-tenancy
	NamespaceMultiStageRateLimiterImpl struct {
		namespaceRateLimiterFn NamespaceRateLimiterFn
		sharedRateLimiter      []RateLimiter

		sync.RWMutex
		namespaceRateLimiters map[string]RateLimiter
	}
)

func NewNamespaceMultiStageRateLimiter(
	namespaceRateLimiterFn NamespaceRateLimiterFn,
	sharedRateLimiter []RateLimiter,
) *NamespaceMultiStageRateLimiterImpl {
	return &NamespaceMultiStageRateLimiterImpl{
		namespaceRateLimiterFn: namespaceRateLimiterFn,
		sharedRateLimiter:      sharedRateLimiter,

		namespaceRateLimiters: make(map[string]RateLimiter),
	}
}

// Allow attempts to allow a request to go through. The method returns
// immediately with a true or false indicating if the request can make
// progress
func (r *NamespaceMultiStageRateLimiterImpl) Allow(
	namespaceID string,
) bool {

	rateLimiter := r.getOrInitRateLimiter(namespaceID)
	return rateLimiter.Allow()
}

// Reserve returns a Reservation that indicates how long the caller
// must wait before event happen.
func (r *NamespaceMultiStageRateLimiterImpl) Reserve(
	namespaceID string,
) Reservation {

	rateLimiter := r.getOrInitRateLimiter(namespaceID)
	return rateLimiter.Reserve()
}

// Wait waits till the deadline for a rate limit token to allow the request
// to go through.
func (r *NamespaceMultiStageRateLimiterImpl) Wait(
	ctx context.Context,
	namespaceID string,
) error {

	rateLimiter := r.getOrInitRateLimiter(namespaceID)
	return rateLimiter.Wait(ctx)
}

func (r *NamespaceMultiStageRateLimiterImpl) getOrInitRateLimiter(
	namespaceID string,
) RateLimiter {
	r.RLock()
	rateLimiter, ok := r.namespaceRateLimiters[namespaceID]
	r.RUnlock()
	if ok {
		return rateLimiter
	}

	length := len(r.sharedRateLimiter)
	rateLimiters := make([]RateLimiter, length+1)
	rateLimiters[0] = r.namespaceRateLimiterFn(namespaceID)
	for i := 0; i < length; i++ {
		rateLimiters[i+1] = r.sharedRateLimiter[i]
	}
	namespaceRateLimiter := NewMultiStageRateLimiter(rateLimiters)

	r.Lock()
	defer r.Unlock()

	rateLimiter, ok = r.namespaceRateLimiters[namespaceID]
	if ok {
		return rateLimiter
	}
	r.namespaceRateLimiters[namespaceID] = namespaceRateLimiter
	return namespaceRateLimiter
}
