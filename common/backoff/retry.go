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

		if next = r.NextBackOff(err); next == done {
			return err
		}

		if err == ctx.Err() || !isRetryable(err) {
			return err
		}

		if _, ok := err.(*serviceerror.ResourceExhausted); ok {
			next = max(next, t.NextBackOff(err))
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

// ThrottleRetryContextWithReturn is a context and resource aware version of Retry.
// Context timeout/cancellation errors are never retried, regardless of IsRetryable.
// Resource exhausted error will be retried using a different throttle retry policy, instead of the specified one.
// TODO: allow customizing throttle retry policy and what kind of error are categorized as throttle error.
func ThrottleRetryContextWithReturn[T any](
	ctx context.Context,
	fn func(context.Context) (T, error),
	policy RetryPolicy,
	isRetryable IsRetryable,
) (T, error) {
	var zero T
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
		result, err := fn(ctx)
		if err == nil {
			return result, nil
		}

		if next = r.NextBackOff(err); next == done {
			return zero, err
		}

		if err == ctx.Err() || !isRetryable(err) {
			return zero, err
		}

		if _, ok := err.(*serviceerror.ResourceExhausted); ok {
			next = max(next, t.NextBackOff(err))
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
		return zero, err
	}
	return zero, ctx.Err()
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
