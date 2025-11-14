package backoff

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
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

	policy := NewExponentialRetryPolicy(1 * time.Millisecond).
		WithMaximumInterval(5 * time.Millisecond).
		WithMaximumAttempts(10)

	err := ThrottleRetry(op, policy, nil)
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

	policy := NewExponentialRetryPolicy(1 * time.Millisecond).
		WithMaximumInterval(5 * time.Millisecond).
		WithMaximumAttempts(5)

	err := ThrottleRetry(op, policy, nil)
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

	policy := NewExponentialRetryPolicy(1 * time.Millisecond).
		WithMaximumInterval(5 * time.Millisecond).
		WithMaximumAttempts(10)

	err := ThrottleRetry(op, policy, isRetryable)
	s.NoError(err, "Retry count: %v", i)
	s.Equal(5, i)
}

func (s *RetrySuite) TestIsRetryableFailure() {
	i := 0
	theErr := someError{}
	op := func() error {
		i++

		if i == 5 {
			return nil
		}

		return &theErr
	}

	policy := NewExponentialRetryPolicy(1 * time.Millisecond).
		WithMaximumInterval(5 * time.Millisecond).
		WithMaximumAttempts(10)

	err := ThrottleRetry(op, policy, IgnoreErrors([]error{&theErr}))
	s.Error(err)
	s.Equal(1, i)
}

func (s *RetrySuite) TestRetryContextCancel() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := ThrottleRetryContext(ctx, func(ctx context.Context) error { return ctx.Err() },
		NewExponentialRetryPolicy(1*time.Millisecond), retryEverything)
	s.ErrorIs(err, context.Canceled)
}

func (s *RetrySuite) TestRetryContextTimeout() {
	timeout := 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	err := ThrottleRetryContext(ctx, func(ctx context.Context) error { return &someError{} },
		NewExponentialRetryPolicy(1*time.Second), retryEverything)
	elapsed := time.Since(start)
	s.ErrorAs(err, new(*someError))
	s.Less(elapsed, timeout,
		"Call to retry should return early if backoff exceeds context timeout")
}

func (s *RetrySuite) TestContextErrorFromSomeOtherContext() {
	// These errors are returned by the function being retried by they are not
	// actually from the context.Context passed to RetryContext
	script := []error{context.Canceled, context.DeadlineExceeded, nil}
	invocation := -1
	err := ThrottleRetryContext(context.Background(),
		func(ctx context.Context) error {
			invocation++
			return script[invocation]
		},
		NewExponentialRetryPolicy(1*time.Millisecond),
		retryEverything)
	s.NoError(err)
}

func (s *RetrySuite) TestThrottleRetryContext() {
	throttleInitialInterval := 100 * time.Millisecond
	testThrottleRetryPolicy := NewExponentialRetryPolicy(throttleInitialInterval).
		WithMaximumInterval(throttleRetryMaxInterval).
		WithExpirationInterval(throttleRetryExpirationInterval)
	originalThrottleRetryPolicy := throttleRetryPolicy
	throttleRetryPolicy = testThrottleRetryPolicy

	policy := NewExponentialRetryPolicy(10 * time.Millisecond).
		WithMaximumAttempts(2)

	// test if throttle retry policy is used on resource exhausted error
	attempt := 1
	op := func(_ context.Context) error {
		if attempt == 1 {
			attempt++
			return &serviceerror.ResourceExhausted{}
		}

		return &someError{}
	}

	start := time.Now()
	err := ThrottleRetryContext(context.Background(), op, policy, retryEverything)
	s.Equal(&someError{}, err)
	s.GreaterOrEqual(
		time.Since(start),
		throttleInitialInterval/2, // due to jitter
		"Resource exhausted error should use throttle retry policy",
	)

	// test if context timeout is respected
	start = time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err = ThrottleRetryContext(ctx, func(_ context.Context) error { return &serviceerror.ResourceExhausted{} }, policy, retryEverything)
	s.Equal(&serviceerror.ResourceExhausted{}, err)
	s.LessOrEqual(
		time.Since(start),
		throttleInitialInterval,
		"Context timeout should be respected",
	)
	cancel()

	// test if retry will stop if there's no more retry indicated by the retry policy
	attempt = 0
	op = func(_ context.Context) error {
		attempt++
		return &serviceerror.ResourceExhausted{}
	}
	err = ThrottleRetryContext(context.Background(), op, policy, retryEverything)
	s.Equal(&serviceerror.ResourceExhausted{}, err)
	s.Equal(2, attempt)

	// set the default global throttle retry policy back to its original value
	throttleRetryPolicy = originalThrottleRetryPolicy
}

var retryEverything IsRetryable = nil

func (e *someError) Error() string {
	return "Some Error"
}
