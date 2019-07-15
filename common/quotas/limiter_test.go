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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/tokenbucket"
	"golang.org/x/time/rate"
)

const (
	defaultDomain = "test"
	defaultRps    = 1200
	_minBurst     = 10000
)

func TestNewRateLimiter(t *testing.T) {
	maxDispatch := float64(0.01)
	rl := NewRateLimiter(&maxDispatch, time.Second, _minBurst)
	limiter := rl.globalLimiter.Load().(*rate.Limiter)
	assert.Equal(t, _minBurst, limiter.Burst())
}

func BenchmarkSimpleRateLimiter(b *testing.B) {
	policy := NewSimpleRateLimiter(tokenbucket.New(defaultRps, clock.NewRealTimeSource()))
	for n := 0; n < b.N; n++ {
		policy.Allow()
	}
}

func BenchmarkRateLimiter(b *testing.B) {
	rps := float64(defaultRps)
	policy := NewRateLimiter(&rps, 2*time.Minute, defaultRps)
	for n := 0; n < b.N; n++ {
		policy.Allow()
	}
}
