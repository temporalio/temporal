package testtelemetry

import (
	"context"
	"testing"

	"go.temporal.io/testx/testcontext"
	"google.golang.org/grpc/metadata"
)

type testNameDecoratorKey struct{}

// AttachTestName annotates gRPC requests with the test name for OTEL tracing.
func AttachTestName(tb testing.TB) {
	tb.Helper()
	testcontext.AttachDecorator(tb, testNameDecoratorKey{}, func(ctx context.Context) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "temporal-test-name", tb.Name())
	})
}
