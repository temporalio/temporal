package buggify_test

import (
	"testing"

	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
)

func BenchmarkFiresEmptyRegistry(b *testing.B) {
	var src failpoint.Source = registrySource{registry: failpoint.NewRegistry()}
	key := key1.ForScope("ns")
	for b.Loop() {
		_ = buggify.Fires(src, key)
	}
}

// BenchmarkCheckpoint measures an execution of a key that has nothing registered while
// BUGGIFY is enabled, for a key that is not activated and one that is but doesn't fire.
func BenchmarkCheckpoint(b *testing.B) {
	for _, bc := range []struct {
		name       string
		activation float64
	}{
		{name: "not activated", activation: 0},
		{name: "activated", activation: 1},
	} {
		b.Run(bc.name, func(b *testing.B) {
			registry := failpoint.NewRegistry()
			if err := buggify.For(registry).Set(buggify.Config{Seed: 1, Activation: bc.activation}); err != nil {
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
