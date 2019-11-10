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

// MultiStageRateLimiter indicates a domain specific rate limit policy
type MultiStageRateLimiter struct {
	sync.RWMutex
	rps            RPSFunc
	domainRPS      RPSKeyFunc
	domainLimiters map[string]*DynamicRateLimiter
	globalLimiter  *DynamicRateLimiter
}

// NewMultiStageRateLimiter returns a new domain quota rate limiter. This is about
// an order of magnitude slower than
func NewMultiStageRateLimiter(rps RPSFunc, domainRps RPSKeyFunc) *MultiStageRateLimiter {
	rl := &MultiStageRateLimiter{
		rps:            rps,
		domainRPS:      domainRps,
		domainLimiters: map[string]*DynamicRateLimiter{},
		globalLimiter:  NewDynamicRateLimiter(rps),
	}
	return rl
}

// Allow attempts to allow a request to go through. The method returns
// immediately with a true or false indicating if the request can make
// progress
func (d *MultiStageRateLimiter) Allow(info Info) bool {
	domain := info.Domain
	if len(domain) == 0 {
		return d.globalLimiter.Allow()
	}

	// check if we have a per-domain limiter - if not create a default one for
	// the domain.
	d.RLock()
	limiter, ok := d.domainLimiters[domain]
	d.RUnlock()

	if !ok {
		// create a new limiter
		domainLimiter := NewDynamicRateLimiter(
			func() float64 {
				return d.domainRPS(domain)
			},
		)

		// verify that it is needed and add to map
		d.Lock()
		limiter, ok = d.domainLimiters[domain]
		if !ok {
			d.domainLimiters[domain] = domainLimiter
			limiter = domainLimiter
		}
		d.Unlock()
	}

	// take a reservation with the domain limiter first
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
