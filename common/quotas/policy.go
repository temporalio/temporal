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

	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/common/tokenbucket"
	"golang.org/x/time/rate"
)

const domainRps = 400

type simpleRateLimitPolicy struct {
	tb tokenbucket.TokenBucket
}

// NewSimpleRateLimiter returns a new simple rate limiter
func NewSimpleRateLimiter(tb tokenbucket.TokenBucket) Policy {
	return &simpleRateLimitPolicy{tb}
}

func (s *simpleRateLimitPolicy) Allow() bool {
	ok, _ := s.tb.TryConsume(1)
	return ok
}

// DomainRateLimitPolicy indicates a domain specific rate limit policy
type DomainRateLimitPolicy struct {
	sync.RWMutex
	rps            dynamicconfig.IntPropertyFn
	domainLimiters map[string]*rate.Limiter
	globalLimiter  Policy
}

// NewDomainRateLimiter returns a new domain quota rate limiter. This is about
// an order of magnitude slower than
func NewDomainRateLimiter(rps RPSFunc) *DomainRateLimitPolicy {
	rl := &DomainRateLimitPolicy{
		domainLimiters: map[string]*rate.Limiter{},
		globalLimiter:  NewDynamicRateLimiter(rps),
	}
	return rl
}

func newDomainRateLimiter(rps int) *DomainRateLimitPolicy {
	initialRps := float64(rps)
	rl := &DomainRateLimitPolicy{
		domainLimiters: map[string]*rate.Limiter{},
		globalLimiter:  NewRateLimiter(&initialRps, _defaultRPSTTL, rps),
	}
	return rl
}

// Allow attempts to allow a request to go through. The method returns
// immediately with a true or false indicating if the request can make
// progress
func (d *DomainRateLimitPolicy) Allow(domain string) bool {
	// check if we have a per-domain limiter - if not create a default one for
	// the domain.
	d.RLock()
	limiter, ok := d.domainLimiters[domain]
	d.RUnlock()

	if !ok {
		// create a new limiter
		domainLimiter := rate.NewLimiter(rate.Limit(domainRps), domainRps)

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

	// ensure that the reservation does not break the global rate limit, if it
	// does, cancel the reservation and do not allow to proceed.
	if !d.globalLimiter.Allow() {
		rsv.Cancel()
		return false
	}
	return true
}
