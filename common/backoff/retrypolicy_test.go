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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/clock"
)

type (
	RetryPolicySuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}
)

// ExampleExponentialRetryPolicy_WithMaximumInterval demonstrates example delays with a backoff coefficient of 2 and a
// maximum interval of 10 seconds. Keep in mind that there is a random jitter in these times, so they are not exactly
// what you'd expect.
func ExampleExponentialRetryPolicy_WithMaximumInterval() {
	rand.Seed(42)
	p1 := NewExponentialRetryPolicy(time.Second).
		WithBackoffCoefficient(2.0).
		WithMaximumInterval(0).
		WithMaximumAttempts(0).
		WithExpirationInterval(0)
	p1copy := *p1
	p2 := &p1copy
	p2 = p2.WithMaximumInterval(time.Second * 10)
	var e1, e2 time.Duration
	fmt.Printf("%-10s| %15s| %15s\n", "Attempt", "Delay", "Capped Delay")
	for attempts := 0; attempts < 10; attempts++ {
		d1 := p1.ComputeNextDelay(e1, attempts)
		d2 := p2.ComputeNextDelay(e2, attempts)
		e1 += d1
		e2 += d2
		_, _ = fmt.Printf(
			"%-10d| %14.1fs| %14.1fs\n",
			attempts,
			d1.Round(100*time.Millisecond).Seconds(),
			d2.Round(100*time.Millisecond).Seconds(),
		)
	}
	// Output:
	// Attempt   |           Delay|    Capped Delay
	// 0         |            0.0s|            0.0s
	// 1         |            0.8s|            0.9s
	// 2         |            1.7s|            1.6s
	// 3         |            3.3s|            3.2s
	// 4         |            7.2s|            7.2s
	// 5         |           15.1s|            9.6s
	// 6         |           26.2s|            8.8s
	// 7         |           62.8s|            9.4s
	// 8         |          112.8s|            9.5s
	// 9         |          219.7s|            8.3s
}

func TestRetryPolicySuite(t *testing.T) {
	suite.Run(t, new(RetryPolicySuite))
}

func (s *RetryPolicySuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *RetryPolicySuite) TestExponentialBackoff() {
	policy := createPolicy(time.Second).
		WithMaximumInterval(10 * time.Second)

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
	maxAttempts := 5
	policy := createPolicy(time.Second).
		WithMaximumAttempts(maxAttempts)

	r, _ := createRetrier(policy)
	var next time.Duration
	for i := 0; i < maxAttempts-1; i++ {
		next = r.NextBackOff()
		s.NotEqual(done, next)
	}

	s.Equal(done, r.NextBackOff())
}

// Test to make sure relative maximum interval for each retry is honoured
func (s *RetryPolicySuite) TestMaximumInterval() {
	policy := createPolicy(time.Second).
		WithMaximumInterval(10 * time.Second)

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
	policy := createPolicy(2 * time.Second).
		WithBackoffCoefficient(1.0)

	r, _ := createRetrier(policy)
	min, max := getNextBackoffRange(2 * time.Second)
	for i := 0; i < 10; i++ {
		next := r.NextBackOff()
		s.True(next >= min, "NextBackoff too low")
		s.True(next < max, "NextBackoff too high")
	}
}

func (s *RetryPolicySuite) TestExpirationInterval() {
	policy := createPolicy(2 * time.Second).
		WithExpirationInterval(5 * time.Minute)

	r, ts := createRetrier(policy)
	ts.Advance(6 * time.Minute)
	next := r.NextBackOff()

	s.Equal(done, next)
}

func (s *RetryPolicySuite) TestExpirationOverflow() {
	policy := createPolicy(2 * time.Second).
		WithExpirationInterval(5 * time.Second)

	r, ts := createRetrier(policy)
	next := r.NextBackOff()
	min, max := getNextBackoffRange(2 * time.Second)
	s.True(next >= min, "NextBackoff too low")
	s.True(next < max, "NextBackoff too high")

	ts.Advance(2 * time.Second)

	next = r.NextBackOff()
	min, max = getNextBackoffRange(3 * time.Second)
	s.True(next >= min, "NextBackoff too low")
	s.True(next < max, "NextBackoff too high")
}

func (s *RetryPolicySuite) TestDefaultPublishRetryPolicy() {
	policy := NewExponentialRetryPolicy(50 * time.Millisecond).
		WithExpirationInterval(time.Minute).
		WithMaximumInterval(10 * time.Second)

	r, ts := createRetrier(policy)
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
		7250 * time.Millisecond,
		done,
	}

	for _, expected := range expectedResult {
		next := r.NextBackOff()
		if expected == done {
			s.Equal(done, next, "backoff not done yet!!!")
		} else {
			min, max := getNextBackoffRange(expected)
			s.True(next >= min, "NextBackoff too low: actual: %v, min: %v", next, min)
			s.True(next < max, "NextBackoff too high: actual: %v, max: %v", next, max)
			ts.Advance(expected)
		}
	}
}

func (s *RetryPolicySuite) TestNoMaxAttempts() {
	policy := createPolicy(50 * time.Millisecond).
		WithExpirationInterval(time.Minute).
		WithMaximumInterval(10 * time.Second)

	r, ts := createRetrier(policy)
	for i := 0; i < 100; i++ {
		next := r.NextBackOff()
		s.True(next > 0 || next == done, "Unexpected value for next retry duration: %v", next)
		ts.Advance(next)
	}
}

func (s *RetryPolicySuite) TestUnbounded() {
	policy := createPolicy(50 * time.Millisecond)

	r, ts := createRetrier(policy)
	for i := 0; i < 100; i++ {
		next := r.NextBackOff()
		s.True(next > 0 || next == done, "Unexpected value for next retry duration: %v", next)
		ts.Advance(next)
	}
}

func createPolicy(initialInterval time.Duration) *ExponentialRetryPolicy {
	policy := NewExponentialRetryPolicy(initialInterval).
		WithBackoffCoefficient(2).
		WithMaximumInterval(NoInterval).
		WithExpirationInterval(NoInterval).
		WithMaximumAttempts(noMaximumAttempts)

	return policy
}

func createRetrier(policy RetryPolicy) (Retrier, *clock.EventTimeSource) {
	ts := clock.NewEventTimeSource()
	ts.Update(time.Time{})
	return NewRetrier(policy, ts), ts
}

func getNextBackoffRange(duration time.Duration) (time.Duration, time.Duration) {
	rangeMin := time.Duration(0.8 * float64(duration))
	return rangeMin, duration
}
