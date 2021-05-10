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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination request_rate_limiter_mock.go

type (
	// RequestRateLimiterFn returns generate a namespace specific rate limiter
	RequestRateLimiterFn func(req Request) RequestRateLimiter

	// RequestRateLimiter corresponds to basic rate limiting functionality.
	RequestRateLimiter interface {
		// Allow attempts to allow a request to go through. The method returns
		// immediately with a true or false indicating if the request can make
		// progress
		Allow(now time.Time, request Request) bool

		// Reserve returns a Reservation that indicates how long the caller
		// must wait before event happen.
		Reserve(now time.Time, request Request) Reservation

		// Wait waits till the deadline for a rate limit token to allow the request
		// to go through.
		Wait(ctx context.Context, request Request) error
	}
)
