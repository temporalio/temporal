// TODO: Remove this forwarding file once all callers use go.temporal.io/testx/testcontext.
package testcontext

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/testx/testcontext"
	"google.golang.org/grpc/metadata"
)

type Option = testcontext.Option

type testNameDecoratorKey struct{}

func GoTestDeadline(tb testing.TB) (time.Time, bool) {
	tb.Helper()
	return testcontext.GoTestDeadline(tb)
}

func DefaultTimeout() time.Duration {
	return testcontext.DefaultTimeout()
}

func For(tb testing.TB, opts ...Option) context.Context {
	tb.Helper()
	testcontext.For(tb, opts...)
	// Annotate gRPC requests with the test name for OTEL tracing.
	testcontext.AttachDecorator(tb, testNameDecoratorKey{}, func(ctx context.Context) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "temporal-test-name", tb.Name())
	})
	return testcontext.For(tb)
}

func WithTimeout(timeout time.Duration) Option {
	return testcontext.WithTimeout(timeout)
}

func AttachDecorator[K comparable](tb testing.TB, key K, decorator func(context.Context) context.Context) {
	tb.Helper()
	For(tb)
	testcontext.AttachDecorator(tb, key, decorator)
}

func EnsureRemaining(ctx context.Context, tb testing.TB, minRemaining time.Duration) context.Context {
	tb.Helper()
	return testcontext.EnsureRemaining(ctx, tb, minRemaining)
}
