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

func BenchmarkExecuteWithEmptyRegistry(b *testing.B) {
	var src failpoint.Source = registrySource{registry: failpoint.NewRegistry()}
	key := key1.ForScope("ns")
	arg := any(&struct{}{})
	for b.Loop() {
		_, _ = failpoint.ExecuteWith[error](src, key, arg)
	}
}

func BenchmarkBuggifyEmptyRegistry(b *testing.B) {
	var src failpoint.Source = registrySource{registry: failpoint.NewRegistry()}
	key := key1.ForScope("ns")
	for b.Loop() {
		_ = failpoint.Buggify(src, key)
	}
}

// BenchmarkCheckpointBuggify measures an execution of a key that has nothing registered while
// BUGGIFY is enabled, for a key that is not activated and one that is but doesn't fire.
func BenchmarkCheckpointBuggify(b *testing.B) {
	for _, bc := range []struct {
		name       string
		activation float64
	}{
		{name: "not activated", activation: 0},
		{name: "activated", activation: 1},
	} {
		b.Run(bc.name, func(b *testing.B) {
			registry := failpoint.NewRegistry()
			if err := registry.SetBuggify(failpoint.BuggifyConfig{Seed: 1, Activation: bc.activation}); err != nil {
				b.Fatal(err)
			}
			var src failpoint.Source = registrySource{registry: registry}
			key := key1.ForScope("ns")
			b.ReportAllocs()
			for b.Loop() {
				_ = failpoint.Checkpoint(src, key)
			}
		})
	}
}
