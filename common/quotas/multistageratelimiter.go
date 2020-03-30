// Copyright (c) 2019 Uber Technologies, Inc.
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
	"sync"
)

// MultiStageRateLimiter indicates a namespace specific rate limit policy
type MultiStageRateLimiter struct {
	sync.RWMutex
	rps               RPSFunc
	namespaceRPS      RPSKeyFunc
	namespaceLimiters map[string]*DynamicRateLimiter
	globalLimiter     *DynamicRateLimiter
}

// NewMultiStageRateLimiter returns a new namespace quota rate limiter. This is about
// an order of magnitude slower than
func NewMultiStageRateLimiter(rps RPSFunc, namespaceRps RPSKeyFunc) *MultiStageRateLimiter {
	rl := &MultiStageRateLimiter{
		rps:               rps,
		namespaceRPS:      namespaceRps,
		namespaceLimiters: map[string]*DynamicRateLimiter{},
		globalLimiter:     NewDynamicRateLimiter(rps),
	}
	return rl
}

// Allow attempts to allow a request to go through. The method returns
// immediately with a true or false indicating if the request can make
// progress
func (d *MultiStageRateLimiter) Allow(info Info) bool {
	namespace := info.Namespace
	if len(namespace) == 0 {
		return d.globalLimiter.Allow()
	}

	// check if we have a per-namespace limiter - if not create a default one for
	// the namespace.
	d.RLock()
	limiter, ok := d.namespaceLimiters[namespace]
	d.RUnlock()

	if !ok {
		// create a new limiter
		namespaceLimiter := NewDynamicRateLimiter(
			func() float64 {
				return d.namespaceRPS(namespace)
			},
		)

		// verify that it is needed and add to map
		d.Lock()
		limiter, ok = d.namespaceLimiters[namespace]
		if !ok {
			d.namespaceLimiters[namespace] = namespaceLimiter
			limiter = namespaceLimiter
		}
		d.Unlock()
	}

	// take a reservation with the namespace limiter first
	rsv := limiter.Reserve()
	if !rsv.OK() {
		return false
	}

	// check whether the reservation is valid now, otherwise
	// cancel and return right away so we can drop the request
	if rsv.Delay() != 0 {
		rsv.Cancel()
		return false
	}

	// ensure that the reservation does not break the global rate limit, if it
	// does, cancel the reservation and do not allow to proceed.
	if !d.globalLimiter.Allow() {
		rsv.Cancel()
		return false
	}
	return true
}
