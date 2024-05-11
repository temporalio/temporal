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
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/clock"
)

const (
	throttleRetryInitialInterval    = time.Second
	throttleRetryMaxInterval        = 10 * time.Second
	throttleRetryExpirationInterval = NoInterval
)

var (
	throttleRetryPolicy = NewExponentialRetryPolicy(throttleRetryInitialInterval).
		WithMaximumInterval(throttleRetryMaxInterval).
		WithExpirationInterval(throttleRetryExpirationInterval)
)

type (
	// Operation to retry
	Operation func() error

	// OperationCtx plays the same role as Operation but for context-aware
	// retryable functions.
	OperationCtx func(context.Context) error

	// IsRetryable handler can be used to exclude certain errors during retry
	IsRetryable func(error) bool
)

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

	deadline, hasDeadline := ctx.Deadline()

	timeSrc := clock.NewRealTimeSource()
	r := NewRetrier(policy, timeSrc)
	t := NewRetrier(throttleRetryPolicy, timeSrc)
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
			next = max(next, t.NextBackOff())
		}

		if hasDeadline && timeSrc.Now().Add(next).After(deadline) {
			break
		}

		timer := time.NewTimer(next)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
		}
	}
	// always return the last error we got from operation, even if it is not useful
	// this retry utility does not have enough information to do any filtering/mapping
	if err != nil {
		return err
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
