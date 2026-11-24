package failpoint_test

import (
	"context"
	"testing"

	"go.temporal.io/testx/failpoint"
)

// requestContext returns a context with a few values, like the context of a request.
func requestContext() context.Context {
	type ctxKey int
	ctx := context.Background()
	for i := range 8 {
		ctx = context.WithValue(ctx, ctxKey(i), i)
	}
	return ctx
}

func BenchmarkExecuteNilRegistry(b *testing.B) {
	var src failpoint.Source = registrySource{}
	for b.Loop() {
		_ = failpoint.Checkpoint(src, key1)
	}
}

func BenchmarkExecuteEmptyRegistry(b *testing.B) {
	var src failpoint.Source = registrySource{registry: failpoint.NewRegistry()}
	key := key1.ForScope("ns")
	for b.Loop() {
		_ = failpoint.Checkpoint(src, key)
	}
}

func BenchmarkExecuteCtxNoRegistry(b *testing.B) {
	ctx := requestContext()
	for b.Loop() {
		_, _ = failpoint.ExecuteCtx[error](ctx, key1)
	}
}

func BenchmarkExecuteCtxEmptyRegistry(b *testing.B) {
	ctx := failpoint.InjectRegistry(requestContext(), failpoint.NewRegistry())
	for b.Loop() {
		_, _ = failpoint.ExecuteCtx[error](ctx, key1)
	}
}
