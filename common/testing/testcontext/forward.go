// TODO: Remove this forwarding file once all callers use go.temporal.io/testx/testcontext.
package testcontext

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/testx/testcontext"
)

type Option = testcontext.Option

func GoTestDeadline(tb testing.TB) (time.Time, bool) {
	tb.Helper()
	return testcontext.GoTestDeadline(tb)
}

func DefaultTimeout() time.Duration {
	return testcontext.DefaultTimeout()
}

func For(tb testing.TB, opts ...Option) context.Context {
	tb.Helper()
	return testcontext.For(tb, opts...)
}

func WithTimeout(timeout time.Duration) Option {
	return testcontext.WithTimeout(timeout)
}

func AttachDecorator[K comparable](tb testing.TB, key K, decorator func(context.Context) context.Context) {
	tb.Helper()
	testcontext.AttachDecorator(tb, key, decorator)
}

func EnsureRemaining(ctx context.Context, tb testing.TB, minRemaining time.Duration) context.Context {
	tb.Helper()
	return testcontext.EnsureRemaining(ctx, tb, minRemaining)
}
