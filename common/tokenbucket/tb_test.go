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

package tokenbucket

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	TokenBucketSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}
)

func TestTokenBucketSuite(t *testing.T) {
	suite.Run(t, new(TokenBucketSuite))
}

func (s *TokenBucketSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

type mockTimeSource struct {
	currTime time.Time
}

func (ts *mockTimeSource) Now() time.Time {
	return ts.currTime
}

func (ts *mockTimeSource) advance(d time.Duration) {
	ts.currTime = ts.currTime.Add(d)
}

func (s *TokenBucketSuite) TestRpsEnforced() {
	ts := &mockTimeSource{currTime: time.Now()}
	tb := New(99, ts)
	for i := 0; i < 2; i++ {
		total, attempts := s.testRpsEnforcedHelper(tb, ts, 11, 90, 10)
		s.Equal(90, total, "Token bucket failed to enforce limit")
		s.Equal(9, attempts, "Token bucket gave out tokens too quickly")
		ts.advance(time.Millisecond * 101)
		ok, _ := tb.TryConsume(9)
		s.True(ok, "Token bucket failed to enforce limit")
		ok, _ = tb.TryConsume(1)
		s.False(ok, "Token bucket failed to enforce limit")
		ts.advance(time.Second)
	}
}

func (s *TokenBucketSuite) TestLowRpsEnforced() {
	ts := &mockTimeSource{currTime: time.Now()}
	tb := New(3, ts)

	total, attempts := s.testRpsEnforcedHelper(tb, ts, 10, 3, 1)
	s.Equal(3, total, "Token bucket failed to enforce limit")
	s.Equal(3, attempts, "Token bucket gave out tokens too quickly")
}

func (s *TokenBucketSuite) TestDynamicRpsEnforced() {
	rpsConfigFn, rpsPtr := s.getTestRPSConfigFn(99)
	ts := &mockTimeSource{currTime: time.Now()}
	dtb := NewDynamicTokenBucket(rpsConfigFn, ts)
	total, attempts := s.testRpsEnforcedHelper(dtb, ts, 11, 90, 10)
	s.Equal(90, total, "Token bucket failed to enforce limit")
	s.Equal(9, attempts, "Token bucket gave out tokens too quickly")
	ts.advance(time.Second)

	*rpsPtr = 3
	total, attempts = s.testRpsEnforcedHelper(dtb, ts, 10, 3, 1)
	s.Equal(3, total, "Token bucket failed to enforce limit")
	s.Equal(3, attempts, "Token bucket gave out tokens too quickly")
}

func (s *TokenBucketSuite) testRpsEnforcedHelper(tb TokenBucket, ts *mockTimeSource, maxAttempts, tokenNeeded, consumeRate int) (total, attempts int) {
	total = 0
	attempts = 1
	for ; attempts < maxAttempts+1; attempts++ {
		for c := 0; c < 2; c++ {
			if ok, _ := tb.TryConsume(consumeRate); ok {
				total += consumeRate
			}
		}
		if total >= tokenNeeded {
			break
		}
		ts.advance(time.Millisecond * 101)
	}
	return
}

func (s *TokenBucketSuite) getTestRPSConfigFn(defaultValue int) (dynamicconfig.IntPropertyFn, *int) {
	rps := defaultValue
	return func(_ ...dynamicconfig.FilterOption) int {
		return rps
	}, &rps
}

func (s *TokenBucketSuite) TestPriorityRpsEnforced() {
	ts := &mockTimeSource{currTime: time.Now()}
	tb := NewPriorityTokenBucket(1, 99, ts) // behavior same to tokenBucketImpl

	for i := 0; i < 2; i++ {
		total := 0
		attempts := 1
		for ; attempts < 11; attempts++ {
			for c := 0; c < 2; c++ {
				if ok, _ := tb.GetToken(0, 10); ok {
					total += 10
				}
			}

			if total >= 90 {
				break
			}
			ts.advance(time.Millisecond * 101)
		}
		s.Equal(90, total, "Token bucket failed to enforce limit")
		s.Equal(9, attempts, "Token bucket gave out tokens too quickly")

		ts.advance(time.Millisecond * 101)
		ok, _ := tb.GetToken(0, 9)
		s.True(ok, "Token bucket failed to enforce limit")
		ok, _ = tb.GetToken(0, 1)
		s.False(ok, "Token bucket failed to enforce limit")
		ts.advance(time.Second)
	}
}

func (s *TokenBucketSuite) TestPriorityLowRpsEnforced() {
	ts := &mockTimeSource{currTime: time.Now()}
	tb := NewPriorityTokenBucket(1, 3, ts) // behavior same to tokenBucketImpl

	total := 0
	attempts := 1
	for ; attempts < 10; attempts++ {
		for c := 0; c < 2; c++ {
			if ok, _ := tb.GetToken(0, 1); ok {
				total++
			}
		}
		if total >= 3 {
			break
		}
		ts.advance(time.Millisecond * 101)
	}
	s.Equal(3, total, "Token bucket failed to enforce limit")
	s.Equal(3, attempts, "Token bucket gave out tokens too quickly")
}

func (s *TokenBucketSuite) TestPriorityTokenBucket() {
	ts := &mockTimeSource{currTime: time.Now()}
	tb := NewPriorityTokenBucket(2, 100, ts)

	for i := 0; i < 2; i++ {
		ok2, _ := tb.GetToken(1, 1)
		s.False(ok2)
		ok, _ := tb.GetToken(0, 10)
		s.True(ok)
		ts.advance(time.Millisecond * 101)
	}

	for i := 0; i < 2; i++ {
		ok, _ := tb.GetToken(0, 9)
		s.True(ok) // 1 token remaining in 1st bucket, 0 in 2nd
		ok2, _ := tb.GetToken(1, 1)
		s.False(ok2)
		ts.advance(time.Millisecond * 101)
		ok2, _ = tb.GetToken(1, 2)
		s.False(ok2)
		ok2, _ = tb.GetToken(1, 1)
		s.True(ok2)
	}
}

func (s *TokenBucketSuite) TestFullPriorityTokenBucket() {
	ts := &mockTimeSource{currTime: time.Now()}
	tb := NewFullPriorityTokenBucket(2, 100, ts)

	ok2, _ := tb.GetToken(1, 10)
	s.True(ok2)

	for i := 0; i < 2; i++ {
		ok2, _ := tb.GetToken(1, 1)
		s.False(ok2)
		ok, _ := tb.GetToken(0, 10)
		s.True(ok)
		ts.advance(time.Millisecond * 101)
	}

	ok2, _ = tb.GetToken(1, 1)
	s.False(ok2)
	ts.advance(time.Millisecond * 101)
	ok2, _ = tb.GetToken(1, 5)
	s.True(ok2)
	ts.advance(time.Millisecond * 101)
	ok2, _ = tb.GetToken(1, 15)
	s.False(ok2)
	ok2, _ = tb.GetToken(1, 10)
	s.True(ok2)
	ok, _ := tb.GetToken(0, 10)
	s.True(ok)
}
