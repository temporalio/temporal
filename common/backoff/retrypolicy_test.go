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

package backoff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	RetryPolicySuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}

	TestClock struct {
		currentTime time.Time
	}
)

func TestRetryPolicySuite(t *testing.T) {
	suite.Run(t, new(RetryPolicySuite))
}

func (s *RetryPolicySuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *RetryPolicySuite) TestExponentialBackoff() {
	policy := createPolicy(time.Second)
	policy.SetMaximumInterval(10 * time.Second)

	expectedResult := []time.Duration{1, 2, 4, 8, 10}
	for i, d := range expectedResult {
		expectedResult[i] = d * time.Second
	}

	r, _ := createRetrier(policy)
	for _, expected := range expectedResult {
		min, max := getNextBackoffRange(expected)
		next := r.NextBackOff()
		s.True(next >= min, "NextBackoff too low")
		s.True(next < max, "NextBackoff too high")
	}
}

func (s *RetryPolicySuite) TestNumberOfAttempts() {
	policy := createPolicy(time.Second)
	policy.SetMaximumAttempts(5)

	r, _ := createRetrier(policy)
	var next time.Duration
	for i := 0; i < 6; i++ {
		next = r.NextBackOff()
	}

	s.Equal(done, next)
}

// Test to make sure relative maximum interval for each retry is honoured
func (s *RetryPolicySuite) TestMaximumInterval() {
	policy := createPolicy(time.Second)
	policy.SetMaximumInterval(10 * time.Second)

	expectedResult := []time.Duration{1, 2, 4, 8, 10, 10, 10, 10, 10, 10}
	for i, d := range expectedResult {
		expectedResult[i] = d * time.Second
	}

	r, _ := createRetrier(policy)
	for _, expected := range expectedResult {
		min, max := getNextBackoffRange(expected)
		next := r.NextBackOff()
		s.True(next >= min, "NextBackoff too low")
		s.True(next < max, "NextBackoff too high")
	}
}

func (s *RetryPolicySuite) TestBackoffCoefficient() {
	policy := createPolicy(2 * time.Second)
	policy.SetBackoffCoefficient(1.0)

	r, _ := createRetrier(policy)
	min, max := getNextBackoffRange(2 * time.Second)
	for i := 0; i < 10; i++ {
		next := r.NextBackOff()
		s.True(next >= min, "NextBackoff too low")
		s.True(next < max, "NextBackoff too high")
	}
}

func (s *RetryPolicySuite) TestExpirationInterval() {
	policy := createPolicy(2 * time.Second)
	policy.SetExpirationInterval(5 * time.Minute)

	r, clock := createRetrier(policy)
	clock.moveClock(6 * time.Minute)
	next := r.NextBackOff()

	s.Equal(done, next)
}

func (s *RetryPolicySuite) TestExpirationOverflow() {
	policy := createPolicy(2 * time.Second)
	policy.SetExpirationInterval(5 * time.Second)

	r, clock := createRetrier(policy)
	next := r.NextBackOff()
	min, max := getNextBackoffRange(2 * time.Second)
	s.True(next >= min, "NextBackoff too low")
	s.True(next < max, "NextBackoff too high")

	clock.moveClock(2 * time.Second)

	next = r.NextBackOff()
	min, max = getNextBackoffRange(3 * time.Second)
	s.True(next >= min, "NextBackoff too low")
	s.True(next < max, "NextBackoff too high")
}

func (s *RetryPolicySuite) TestDefaultPublishRetryPolicy() {
	policy := NewExponentialRetryPolicy(50 * time.Millisecond)
	policy.SetExpirationInterval(time.Minute)
	policy.SetMaximumInterval(10 * time.Second)

	r, clock := createRetrier(policy)
	expectedResult := []time.Duration{
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		3200 * time.Millisecond,
		6400 * time.Millisecond,
		10000 * time.Millisecond,
		10000 * time.Millisecond,
		10000 * time.Millisecond,
		10000 * time.Millisecond,
		6000 * time.Millisecond,
		1300 * time.Millisecond,
		done,
	}

	for _, expected := range expectedResult {
		next := r.NextBackOff()
		if expected == done {
			s.Equal(done, next, "backoff not done yet!!!")
		} else {
			min, _ := getNextBackoffRange(expected)
			s.True(next >= min, "NextBackoff too low: actual: %v, expected: %v", next, expected)
			// s.True(next < max, "NextBackoff too high: actual: %v, expected: %v", next, expected)
			clock.moveClock(expected)
		}
	}
}

func (s *RetryPolicySuite) TestNoMaxAttempts() {
	policy := createPolicy(50 * time.Millisecond)
	policy.SetExpirationInterval(time.Minute)
	policy.SetMaximumInterval(10 * time.Second)

	r, clock := createRetrier(policy)
	for i := 0; i < 100; i++ {
		next := r.NextBackOff()
		//print("Iter: ", i, ", Next Backoff: ", next.String(), "\n")
		s.True(next > 0 || next == done, "Unexpected value for next retry duration: %v", next)
		clock.moveClock(next)
	}
}

func (s *RetryPolicySuite) TestUnbounded() {
	policy := createPolicy(50 * time.Millisecond)

	r, clock := createRetrier(policy)
	for i := 0; i < 100; i++ {
		next := r.NextBackOff()
		//print("Iter: ", i, ", Next Backoff: ", next.String(), "\n")
		s.True(next > 0 || next == done, "Unexpected value for next retry duration: %v", next)
		clock.moveClock(next)
	}
}

func (c *TestClock) Now() time.Time {
	return c.currentTime
}

func (c *TestClock) moveClock(duration time.Duration) {
	c.currentTime = c.currentTime.Add(duration)
}

func createPolicy(initialInterval time.Duration) *ExponentialRetryPolicy {
	policy := NewExponentialRetryPolicy(initialInterval)
	policy.SetBackoffCoefficient(2)
	policy.SetMaximumInterval(NoInterval)
	policy.SetExpirationInterval(NoInterval)
	policy.SetMaximumAttempts(noMaximumAttempts)

	return policy
}

func createRetrier(policy RetryPolicy) (Retrier, *TestClock) {
	clock := &TestClock{currentTime: time.Time{}}
	return NewRetrier(policy, clock), clock
}

func getNextBackoffRange(duration time.Duration) (time.Duration, time.Duration) {
	rangeMin := time.Duration(0.8 * float64(duration))
	return rangeMin, duration
}
