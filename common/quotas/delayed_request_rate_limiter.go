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
	"errors"
	"fmt"
	"time"

	"go.temporal.io/server/common/clock"
)

// DelayedRequestRateLimiter is a rate limiter that allows all requests without any delay for a given duration. After
// the delay expires, it delegates to another rate limiter. This rate limiter is useful for cases where you want to
// allow all requests for a given duration, e.g. during something volatile like a deployment, and then switch to another
// rate limiter after the duration expires.
type DelayedRequestRateLimiter struct {
	// RequestRateLimiter is the delegate that we switch to after the delay expires.
	RequestRateLimiter
	// timer triggers the rate limiter to delegate to the underlying rate limiter. We hold a reference to it in order to
	// cancel it prematurely if needed.
	timer clock.Timer
}

var ErrNegativeDelay = errors.New("delay cannot be negative")

// NewDelayedRequestRateLimiter returns a DelayedRequestRateLimiter that delegates to the given rate limiter after a
// delay. The timeSource is used to create the timer that triggers the switch. It returns an error if the given delay
// is negative.
func NewDelayedRequestRateLimiter(
	rl RequestRateLimiter,
	delay time.Duration,
	timeSource clock.TimeSource,
) (*DelayedRequestRateLimiter, error) {
	if delay < 0 {
		return nil, fmt.Errorf("%w: %v", ErrNegativeDelay, delay)
	}

	delegator := RequestRateLimiterDelegator{}
	delegator.SetRateLimiter(NoopRequestRateLimiter)

	timer := timeSource.AfterFunc(delay, func() {
		delegator.SetRateLimiter(rl)
	})

	return &DelayedRequestRateLimiter{
		RequestRateLimiter: &delegator,
		timer:              timer,
	}, nil
}

// Cancel stops the timer that triggers the rate limiter to delegate to the underlying rate limiter. It returns true if
// the timer was stopped before it expired.
func (rl *DelayedRequestRateLimiter) Cancel() bool {
	return rl.timer.Stop()
}
