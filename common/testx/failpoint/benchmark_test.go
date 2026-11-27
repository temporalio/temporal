package failpoint_test

import (
	"testing"

	"go.temporal.io/testx/failpoint"
)

// These benchmarks measure the cost of the failpoint call-site helpers when no failpoint fires.
// Run them with:
//
//	cd common/testx && go test -run='^$' -bench=. -benchmem -count=10 -cpu=18 ./failpoint
//
// The serial rows call the helper and discard its result; b.Loop keeps the call and its result
// alive, so no sink variable is needed and every row handles its result the same way. The
// "Parallel" rows use b.RunParallel, which runs GOMAXPROCS goroutines (18 with -cpu=18).
// BenchmarkTestHooksGet lives in benchmark_nodep_test.go because it only measures the production
// stub when built without test_dep.

var (
	benchmarkErrorKey  = failpoint.NewKey[error]("benchmark.error")
	benchmarkSwitchKey = failpoint.NewKey[struct{}]("benchmark.switch")
	benchmarkOtherKey  = failpoint.NewKey[struct{}]("benchmark.other")

	// The nil registry and scope are loaded at runtime, as at a server call site.
	benchmarkNilRegistry *failpoint.Registry
	benchmarkScope       = "benchmark-scope"
)

func BenchmarkNilRegistry(b *testing.B) {
	reg := benchmarkNilRegistry
	scope := benchmarkScope
	b.Run("CheckError", func(b *testing.B) {
		for b.Loop() {
			_ = failpoint.CheckError(reg, benchmarkErrorKey)
		}
	})
	b.Run("ExecuteSwitch", func(b *testing.B) {
		for b.Loop() {
			_ = failpoint.ExecuteSwitch(reg, benchmarkSwitchKey.ForScope(scope))
		}
	})
	b.Run("GuardedNilCheck", func(b *testing.B) {
		for b.Loop() {
			if reg != nil {
				_ = failpoint.CheckError(reg, benchmarkErrorKey)
			}
		}
	})
}

func BenchmarkEmptyRegistry(b *testing.B) {
	reg := failpoint.NewRegistry()
	scope := benchmarkScope
	b.Run("CheckError", func(b *testing.B) {
		for b.Loop() {
			_ = failpoint.CheckError(reg, benchmarkErrorKey)
		}
	})
	b.Run("ExecuteSwitch", func(b *testing.B) {
		for b.Loop() {
			_ = failpoint.ExecuteSwitch(reg, benchmarkSwitchKey.ForScope(scope))
		}
	})
}

func BenchmarkActiveOtherKey(b *testing.B) {
	reg := failpoint.NewRegistry()
	other := failpoint.NewFailpoint(benchmarkOtherKey)
	other.On()
	reg.Set(other)
	scope := benchmarkScope
	b.Run("CheckError", func(b *testing.B) {
		for b.Loop() {
			_ = failpoint.CheckError(reg, benchmarkErrorKey)
		}
	})
	b.Run("ExecuteSwitch", func(b *testing.B) {
		for b.Loop() {
			_ = failpoint.ExecuteSwitch(reg, benchmarkSwitchKey.ForScope(scope))
		}
	})

	// Parallel means GOMAXPROCS goroutines (18 with -cpu=18) calling the helper concurrently.
	b.Run("CheckErrorParallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = failpoint.CheckError(reg, benchmarkErrorKey)
			}
		})
	})
	b.Run("ExecuteSwitchParallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = failpoint.ExecuteSwitch(reg, benchmarkSwitchKey.ForScope(scope))
			}
		})
	})
}
