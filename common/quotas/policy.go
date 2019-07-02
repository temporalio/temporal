// Copyright (c) 2017 Uber Technologies, Inc.
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
	"time"

	"github.com/uber/cadence/common/tokenbucket"
)

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

func (s *simpleRateLimitPolicy) Wait(d time.Duration) bool {
	return s.tb.Consume(1, d)
}
