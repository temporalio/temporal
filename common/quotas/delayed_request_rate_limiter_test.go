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

package quotas_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/quotas"
)

// disallowingRateLimiter is a rate limiter whose Allow method always returns false.
type disallowingRateLimiter struct {
	// RequestRateLimiter is an embedded field so that disallowingRateLimiter implements that interface. It doesn't
	// actually delegate to this rate limiter, and this field should be left nil.
	quotas.RequestRateLimiter
}

func (rl disallowingRateLimiter) Allow(time.Time, quotas.Request) bool {
	return false
}

func TestNewDelayedRequestRateLimiter_NegativeDelay(t *testing.T) {
	t.Parallel()

	_, err := quotas.NewDelayedRequestRateLimiter(
		quotas.NoopRequestRateLimiter,
		-time.Nanosecond,
		clock.NewRealTimeSource(),
	)
	assert.ErrorIs(t, err, quotas.ErrNegativeDelay)
}

func TestNewDelayedRequestRateLimiter_ZeroDelay(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	drl, err := quotas.NewDelayedRequestRateLimiter(disallowingRateLimiter{}, 0, timeSource)
	require.NoError(t, err)
	assert.False(t, drl.Allow(time.Time{}, quotas.Request{}), "expected Allow to return false because we "+
		"immediately switched to the disallowing rate limiter due to the zero delay")
}

func TestDelayedRequestRateLimiter_Allow(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	drl, err := quotas.NewDelayedRequestRateLimiter(disallowingRateLimiter{}, time.Second, timeSource)
	require.NoError(t, err)
	timeSource.Advance(time.Second - time.Nanosecond)
	assert.True(t, drl.Allow(time.Time{}, quotas.Request{}), "expected Allow to return true because the "+
		"timer hasn't expired yet")
	timeSource.Advance(time.Nanosecond)
	assert.False(t, drl.Allow(time.Time{}, quotas.Request{}), "expected Allow to return false because the "+
		"timer expired, and we switched to the disallowing rate limiter")
}

func TestDelayedRequestRateLimiter_Cancel(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	drl, err := quotas.NewDelayedRequestRateLimiter(disallowingRateLimiter{}, time.Second, timeSource)
	require.NoError(t, err)
	timeSource.Advance(time.Second - time.Nanosecond)
	assert.True(t, drl.Cancel(), "expected Cancel to return true because the timer was stopped before it "+
		"expired")
	timeSource.Advance(time.Nanosecond)
	assert.True(t, drl.Allow(time.Time{}, quotas.Request{}), "expected Allow to return true because the "+
		"timer was stopped before it could expire")
	assert.False(t, drl.Cancel(), "expected Cancel to return false because the timer was already stopped")
}
