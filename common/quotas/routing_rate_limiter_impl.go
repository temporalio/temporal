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
	"time"
)

type (
	// RoutingRateLimiterImpl is a rate limiter special built for multi-tenancy
	RoutingRateLimiterImpl struct {
		apiToRateLimiter map[string]RequestRateLimiter
	}
)

var _ RequestRateLimiter = (*RoutingRateLimiterImpl)(nil)

func NewRoutingRateLimiter(
	apiToRateLimiter map[string]RequestRateLimiter,
) *RoutingRateLimiterImpl {
	return &RoutingRateLimiterImpl{
		apiToRateLimiter: apiToRateLimiter,
	}
}

// Allow attempts to allow a request to go through. The method returns
// immediately with a true or false indicating if the request can make
// progress
func (r *RoutingRateLimiterImpl) Allow(
	now time.Time,
	request Request,
) bool {
	rateLimiter, ok := r.apiToRateLimiter[request.API]
	if !ok {
		return true
	}
	return rateLimiter.Allow(now, request)
}

// Reserve returns a Reservation that indicates how long the caller
// must wait before event happen.
func (r *RoutingRateLimiterImpl) Reserve(
	now time.Time,
	request Request,
) Reservation {
	rateLimiter, ok := r.apiToRateLimiter[request.API]
	if !ok {
		return NewNoopReservation()
	}
	return rateLimiter.Reserve(now, request)
}

// Wait waits till the deadline for a rate limit token to allow the request
// to go through.
func (r *RoutingRateLimiterImpl) Wait(
	ctx context.Context,
	request Request,
) error {
	rateLimiter, ok := r.apiToRateLimiter[request.API]
	if !ok {
		return nil
	}
	return rateLimiter.Wait(ctx, request)
}
