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
	RateLimiterKey struct {
		namespaceID string
		shardID     int32
	}

	KeyField func(k *RateLimiterKey)

	// RequestRateLimiterKeyFn extracts the map key from the request
	RequestRateLimiterKeyFn func(req Request) *RateLimiterKey

	// MapRequestRateLimiterImpl is a generic wrapper rate limiter for a set of rate limiters
	// identified by a key
	MapRequestRateLimiterImpl struct {
		rateLimiterGenFn RequestRateLimiterFn
		rateLimiterKeyFn RequestRateLimiterKeyFn

		sync.RWMutex
		rateLimiters map[*RateLimiterKey]RequestRateLimiter
	}
)

func NewRateLimiterKey(fields ...KeyField) *RateLimiterKey {
	key := &RateLimiterKey{
		namespaceID: "",
		shardID:     -1,
	}

	for _, field := range fields {
		field(key)
	}

	return key
}

func WithNamespaceID(namespaceID string) KeyField {
	return func(k *RateLimiterKey) {
		k.namespaceID = namespaceID
	}
}

func WithShardID(shardID int32) KeyField {
	return func(k *RateLimiterKey) {
		k.shardID = shardID
	}
}

var _ RequestRateLimiter = (*MapRequestRateLimiterImpl)(nil)

func NewMapRequestRateLimiter(
	rateLimiterGenFn RequestRateLimiterFn,
	rateLimiterKeyFn RequestRateLimiterKeyFn,
) *MapRequestRateLimiterImpl {
	return &MapRequestRateLimiterImpl{
		rateLimiterGenFn: rateLimiterGenFn,
		rateLimiterKeyFn: rateLimiterKeyFn,
		rateLimiters:     make(map[*RateLimiterKey]RequestRateLimiter),
	}
}

func namespaceRequestRateLimiterKeyFn(req Request) *RateLimiterKey {
	return NewRateLimiterKey(WithNamespaceID(req.Caller))
}

func NewNamespaceRequestRateLimiter(
	rateLimiterGenFn RequestRateLimiterFn,
) *MapRequestRateLimiterImpl {
	return NewMapRequestRateLimiter(rateLimiterGenFn, namespaceRequestRateLimiterKeyFn)
}

// Allow attempts to allow a request to go through. The method returns
// immediately with a true or false indicating if the request can make
// progress
func (r *MapRequestRateLimiterImpl) Allow(
	now time.Time,
	request Request,
) bool {
	rateLimiter := r.getOrInitRateLimiter(request)
	return rateLimiter.Allow(now, request)
}

// Reserve returns a Reservation that indicates how long the caller
// must wait before event happen.
func (r *MapRequestRateLimiterImpl) Reserve(
	now time.Time,
	request Request,
) Reservation {
	rateLimiter := r.getOrInitRateLimiter(request)
	return rateLimiter.Reserve(now, request)
}

// Wait waits till the deadline for a rate limit token to allow the request
// to go through.
func (r *MapRequestRateLimiterImpl) Wait(
	ctx context.Context,
	request Request,
) error {
	rateLimiter := r.getOrInitRateLimiter(request)
	return rateLimiter.Wait(ctx, request)
}

func (r *MapRequestRateLimiterImpl) getOrInitRateLimiter(
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
