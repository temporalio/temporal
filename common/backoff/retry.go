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
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/util"
)

const (
	throttleRetryInitialInterval    = time.Second
	throttleRetryMaxInterval        = 10 * time.Second
	throttleRetryExpirationInterval = NoInterval
)

var (
	throttleRetryPolicy = createThrottleRetryPolicy()
)

type (
	// Operation to retry
	Operation func() error

	// OperationCtx plays the same role as Operation but for context-aware
	// retryable functions.
	OperationCtx func(context.Context) error

	// IsRetryable handler can be used to exclude certain errors during retry
	IsRetryable func(error) bool

	// ConcurrentRetrier is used for client-side throttling. It determines whether to
	// throttle outgoing traffic in case downstream backend server rejects
	// requests due to out-of-quota or server busy errors.
	ConcurrentRetrier struct {
		sync.Mutex
		retrier      Retrier // Backoff retrier
		failureCount int64   // Number of consecutive failures seen
	}
)

// Throttle Sleep if there were failures since the last success call.
func (c *ConcurrentRetrier) Throttle() {
	c.throttleInternal()
}

func (c *ConcurrentRetrier) throttleInternal() time.Duration {
	next := done

	// Check if we have failure count.
	failureCount := c.failureCount
	if failureCount > 0 {
		defer c.Unlock()
		c.Lock()
		if c.failureCount > 0 {
			next = c.retrier.NextBackOff()
		}
	}

	if next != done {
		time.Sleep(next)
	}

	return next
}

// Succeeded marks client request succeeded.
func (c *ConcurrentRetrier) Succeeded() {
	defer c.Unlock()
	c.Lock()
	c.failureCount = 0
	c.retrier.Reset()
}

// Failed marks client request failed because backend is busy.
func (c *ConcurrentRetrier) Failed() {
	defer c.Unlock()
	c.Lock()
	c.failureCount++
}

// NewConcurrentRetrier returns an instance of concurrent backoff retrier.
func NewConcurrentRetrier(retryPolicy RetryPolicy) *ConcurrentRetrier {
	retrier := NewRetrier(retryPolicy, SystemClock)
	return &ConcurrentRetrier{retrier: retrier}
}

// Retry function can be used to wrap any call with retry logic using the passed
// in policy. A `nil` IsRetryable predicate will retry all errors. There is a
// context-aware version of this function: RetryContext.
// Deprecated: Use ThrottleRetry
func Retry(operation Operation, policy RetryPolicy, isRetryable IsRetryable) error {
	ctxOp := func(context.Context) error { return operation() }
	return RetryContext(context.Background(), ctxOp, policy, isRetryable)
}

// RetryContext is a context-aware version of Retry. Context
// timeout/cancellation errors are never retried, regardless of IsRetryable.
// Deprecated: use ThrottleRetryContext,
func RetryContext(
	ctx context.Context,
	operation OperationCtx,
	policy RetryPolicy,
	isRetryable IsRetryable,
) error {
	var err error
	var next time.Duration

	if isRetryable == nil {
		isRetryable = func(error) bool { return true }
	}

	r := NewRetrier(policy, SystemClock)
	for ctx.Err() == nil {
		if err = operation(ctx); err == nil {
			return nil
		}

		if next = r.NextBackOff(); next == done {
			return err
		}

		// stop retrying if context has expired or the error
		// is not retryable (err is known to be non-nil here)
		if err == ctx.Err() || !isRetryable(err) {
			return err
		}

		t := time.NewTimer(next)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
		}
	}
	return ctx.Err()
}

// ThrottleRetry is a resource aware version of Retry.
// Resource exhausted error will be retried using a different throttle retry policy, instead of the specified one.
func ThrottleRetry(operation Operation, policy RetryPolicy, isRetryable IsRetryable) error {
	ctxOp := func(context.Context) error { return operation() }
	return ThrottleRetryContext(context.Background(), ctxOp, policy, isRetryable)
}

// ThrottleRetryContext is a context and resource aware version of Retry.
// Context timeout/cancellation errors are never retried, regardless of IsRetryable.
// Resource exhausted error will be retried using a different throttle retry policy, instead of the specified one.
// TODO: allow customizing throttle retry policy and what kind of error are categorized as throttle error.
func ThrottleRetryContext(
	ctx context.Context,
	operation OperationCtx,
	policy RetryPolicy,
	isRetryable IsRetryable,
) error {
	var err error
	var next time.Duration

	if isRetryable == nil {
		isRetryable = func(error) bool { return true }
	}

	r := NewRetrier(policy, SystemClock)
	t := NewRetrier(throttleRetryPolicy, SystemClock)
	for ctx.Err() == nil {
		if err = operation(ctx); err == nil {
			return nil
		}

		if next = r.NextBackOff(); next == done {
			return err
		}

		if err == ctx.Err() || !isRetryable(err) {
			return err
		}

		if _, ok := err.(*serviceerror.ResourceExhausted); ok {
			next = util.Max(next, t.NextBackOff())
		}

		timer := time.NewTimer(next)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
		}
	}

	return ctx.Err()
}

// IgnoreErrors can be used as IsRetryable handler for Retry function to exclude certain errors from the retry list
func IgnoreErrors(errorsToExclude []error) func(error) bool {
	return func(err error) bool {
		for _, errorToExclude := range errorsToExclude {
			if err == errorToExclude {
				return false
			}
		}

		return true
	}
}

func createThrottleRetryPolicy() RetryPolicy {
	policy := NewExponentialRetryPolicy(throttleRetryInitialInterval)
	policy.SetMaximumInterval(throttleRetryMaxInterval)
	policy.SetExpirationInterval(throttleRetryExpirationInterval)

	return policy
}
