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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	RetrySuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}

	someError struct{}
)

func TestRetrySuite(t *testing.T) {
	suite.Run(t, new(RetrySuite))
}

func (s *RetrySuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *RetrySuite) TestRetrySuccess() {
	i := 0
	op := func() error {
		i++

		if i == 5 {
			return nil
		}

		return &someError{}
	}

	policy := NewExponentialRetryPolicy(1 * time.Millisecond)
	policy.SetMaximumInterval(5 * time.Millisecond)
	policy.SetMaximumAttempts(10)

	err := Retry(op, policy, nil)
	s.NoError(err)
	s.Equal(5, i)
}

func (s *RetrySuite) TestRetryFailed() {
	i := 0
	op := func() error {
		i++

		if i == 7 {
			return nil
		}

		return &someError{}
	}

	policy := NewExponentialRetryPolicy(1 * time.Millisecond)
	policy.SetMaximumInterval(5 * time.Millisecond)
	policy.SetMaximumAttempts(5)

	err := Retry(op, policy, nil)
	s.Error(err)
}

func (s *RetrySuite) TestIsRetryableSuccess() {
	i := 0
	op := func() error {
		i++

		if i == 5 {
			return nil
		}

		return &someError{}
	}

	isRetryable := func(err error) bool {
		if _, ok := err.(*someError); ok {
			return true
		}

		return false
	}

	policy := NewExponentialRetryPolicy(1 * time.Millisecond)
	policy.SetMaximumInterval(5 * time.Millisecond)
	policy.SetMaximumAttempts(10)

	err := Retry(op, policy, isRetryable)
	s.NoError(err, "Retry count: %v", i)
	s.Equal(5, i)
}

func (s *RetrySuite) TestIsRetryableFailure() {
	i := 0
	op := func() error {
		i++

		if i == 5 {
			return nil
		}

		return &someError{}
	}

	policy := NewExponentialRetryPolicy(1 * time.Millisecond)
	policy.SetMaximumInterval(5 * time.Millisecond)
	policy.SetMaximumAttempts(10)

	err := Retry(op, policy, IgnoreErrors([]error{&someError{}}))
	s.Error(err)
	s.Equal(1, i)
}

func (s *RetrySuite) TestConcurrentRetrier() {
	policy := NewExponentialRetryPolicy(1 * time.Millisecond)
	policy.SetMaximumInterval(10 * time.Millisecond)
	policy.SetMaximumAttempts(4)

	// Basic checks
	retrier := NewConcurrentRetrier(policy)
	retrier.Failed()
	s.Equal(int64(1), retrier.failureCount)
	retrier.Succeeded()
	s.Equal(int64(0), retrier.failureCount)
	sleepDuration := retrier.throttleInternal()
	s.Equal(done, sleepDuration)

	// Multiple count check.
	retrier.Failed()
	retrier.Failed()
	s.Equal(int64(2), retrier.failureCount)
	// Verify valid sleep times.
	ch := make(chan time.Duration, 3)
	go func() {
		for i := 0; i < 3; i++ {
			ch <- retrier.throttleInternal()
		}
	}()
	for i := 0; i < 3; i++ {
		val := <-ch
		fmt.Printf("Duration: %d\n", val)
		s.True(val > 0)
	}
	retrier.Succeeded()
	s.Equal(int64(0), retrier.failureCount)
	// Verify we don't have any sleep times.
	go func() {
		for i := 0; i < 3; i++ {
			ch <- retrier.throttleInternal()
		}
	}()
	for i := 0; i < 3; i++ {
		val := <-ch
		fmt.Printf("Duration: %d\n", val)
		s.Equal(done, val)
	}
}

func (s *RetrySuite) TestRetryContextCancel() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := RetryContext(ctx, func(ctx context.Context) error { return ctx.Err() },
		NewExponentialRetryPolicy(1*time.Millisecond), retryEverything)
	s.ErrorIs(err, context.Canceled)
}

func (s *RetrySuite) TestRetryContextTimeout() {
	timeout := 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	err := RetryContext(ctx, func(ctx context.Context) error { return &someError{} },
		NewExponentialRetryPolicy(1*time.Second), retryEverything)
	elapsed := time.Now().Sub(start)
	s.ErrorIs(err, context.DeadlineExceeded)
	s.GreaterOrEqual(elapsed, timeout,
		"Call to retry should take at least as long as the context timeout")
}

func (s *RetrySuite) TestContextErrorFromSomeOtherContext() {
	// These errors are returned by the function being retried by they are not
	// actually from the context.Context passed to RetryContext
	script := []error{context.Canceled, context.DeadlineExceeded, nil}
	invocation := -1
	err := RetryContext(context.Background(),
		func(ctx context.Context) error {
			invocation++
			return script[invocation]
		},
		NewExponentialRetryPolicy(1*time.Millisecond),
		retryEverything)
	s.NoError(err)
}

var retryEverything IsRetryable = nil

func (e *someError) Error() string {
	return "Some Error"
}
