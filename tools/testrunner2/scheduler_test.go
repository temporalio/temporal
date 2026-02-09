package testrunner2

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScheduler_Parallelism(t *testing.T) {
	t.Parallel()

	s := newScheduler(2)

	var running atomic.Int32
	var maxConcurrent atomic.Int32
	var processed atomic.Int32

	items := make([]*queueItem, 4)
	for i := range items {
		items[i] = &queueItem{
			run: func(ctx context.Context, emit func(...*queueItem)) {
				cur := running.Add(1)
				for {
					old := maxConcurrent.Load()
					if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
						break
					}
				}
				running.Add(-1)
				processed.Add(1)
			},
		}
	}

	s.run(context.Background(), items)

	require.Equal(t, int32(4), processed.Load(), "all items should be processed")
	require.LessOrEqual(t, maxConcurrent.Load(), int32(2), "should not exceed parallelism")
}

func TestScheduler_Empty(t *testing.T) {
	t.Parallel()

	s := newScheduler(2)
	s.run(context.Background(), nil)
	// Should not panic or hang
}

func TestScheduler_ContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := newScheduler(2)

	var processed atomic.Int32
	items := []*queueItem{
		{run: func(ctx context.Context, emit func(...*queueItem)) { processed.Add(1) }},
		{run: func(ctx context.Context, emit func(...*queueItem)) { processed.Add(1) }},
	}

	s.run(ctx, items)
	// With cancelled context, should exit quickly (may process 0-2 items)
}

func TestRunnerResultCollection(t *testing.T) {
	t.Parallel()

	r := &runner{}

	// Test thread-safe operations
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Go(func() {
			r.addReport(&junitReport{})
			r.addAlerts([]alert{{Kind: failureKindCrash}})
			r.addError(os.ErrNotExist)
		})
	}
	wg.Wait()

	require.Len(t, r.junitReports, 10)
	require.Len(t, r.alerts, 10)
	require.Len(t, r.errors, 10)
}

func TestQuarantinedTestNames(t *testing.T) {
	t.Parallel()

	as := alerts{
		{Kind: failureKindTimeout, Tests: []string{"TestTimeout"}},
		{Kind: failureKindPanic, Tests: []string{"pkg.TestPanic.func1.3"}},
		{Kind: failureKindFatal, Tests: []string{"pkg.TestFatal"}},
		{Kind: failureKindDataRace, Tests: []string{"pkg.TestRace.func2"}},
		{Kind: failureKindCrash, Tests: []string{"TestCrash"}}, // crash kind is NOT quarantined
	}
	names := quarantinedTestNames(as)
	require.ElementsMatch(t, []string{"TestTimeout", "TestPanic", "TestFatal", "TestRace"}, names)
}

func TestFilterEmitted_ParentFiltering(t *testing.T) {
	t.Parallel()

	t.Run("filters exact match", func(t *testing.T) {
		failures := []testFailure{
			{Name: "TestA"},
			{Name: "TestB"},
		}
		emitted := map[string]bool{"TestA": true}
		result := filterEmitted(failures, emitted)
		require.Len(t, result, 1)
		require.Equal(t, "TestB", result[0].Name)
	})

	t.Run("filters parent when child was emitted", func(t *testing.T) {
		failures := []testFailure{
			{Name: "TestSuite"},
			{Name: "TestOther"},
		}
		emitted := map[string]bool{
			"TestSuite/Sub1": true,
			"TestSuite/Sub2": true,
		}
		result := filterEmitted(failures, emitted)
		require.Len(t, result, 1)
		require.Equal(t, "TestOther", result[0].Name)
	})

	t.Run("does not filter unrelated tests", func(t *testing.T) {
		failures := []testFailure{
			{Name: "TestA"},
			{Name: "TestB"},
		}
		emitted := map[string]bool{"TestC/Sub1": true}
		result := filterEmitted(failures, emitted)
		require.Len(t, result, 2)
	})

	t.Run("empty emitted keeps all", func(t *testing.T) {
		failures := []testFailure{
			{Name: "TestA"},
			{Name: "TestSuite"},
		}
		result := filterEmitted(failures, map[string]bool{})
		require.Len(t, result, 2)
	})
}

func TestBuildRetryPlans_MixedDepthSkip(t *testing.T) {
	t.Parallel()

	t.Run("mixed-depth passed tests are preserved", func(t *testing.T) {
		passed := []string{"TestFoo", "TestBar", "TestSuite/Sub1", "TestSuite/Sub2"}
		plans := buildRetryPlans(passed, nil)
		require.Len(t, plans, 1)
		// Skip list preserves original names; buildTestFilterPattern handles per-level
		// matching. Shallower names may be dropped from the pattern (causing re-runs)
		// but never over-skipped.
		require.Equal(t, passed, plans[0].skipTests)
	})

	t.Run("consistent-depth passed tests are not collapsed", func(t *testing.T) {
		passed := []string{"TestA", "TestB", "TestC"}
		plans := buildRetryPlans(passed, nil)
		require.Len(t, plans, 1)
		require.Equal(t, passed, plans[0].skipTests)
	})

	t.Run("quarantine regular skip includes quarantined tests at same depth", func(t *testing.T) {
		// Simulate: TestSuite has Sub1 (passed), Sub2 (passed), Slow (timed out).
		// Other tests TestA/X and TestA/Y also passed.
		// The regular plan should skip all passed + quarantined tests at their
		// original depth, not collapse to parent names (which could over-skip).
		passed := []string{"TestSuite/Sub1", "TestSuite/Sub2", "TestA/X", "TestA/Y"}
		quarantined := []string{"TestSuite/Slow"}
		plans := buildRetryPlans(passed, quarantined)

		// Should have 2 plans: quarantine + regular
		require.Len(t, plans, 2)

		// Quarantine plan: run TestSuite, skip passed Sub1 and Sub2
		require.Equal(t, []string{"TestSuite"}, plans[0].tests)
		require.ElementsMatch(t, []string{"TestSuite/Sub1", "TestSuite/Sub2"}, plans[0].skipTests)

		// Regular plan: skip passed tests + quarantined tests (all at same depth)
		require.ElementsMatch(t, []string{
			"TestA/X", "TestA/Y", // passed non-quarantined
			"TestSuite/Sub1", "TestSuite/Sub2", // passed under quarantined parent
			"TestSuite/Slow", // quarantined test itself
		}, plans[1].skipTests)
	})
}

func TestMergeUnique(t *testing.T) {
	t.Parallel()

	t.Run("both nil", func(t *testing.T) {
		result := mergeUnique(nil, nil)
		require.Nil(t, result)
	})

	t.Run("first nil", func(t *testing.T) {
		result := mergeUnique(nil, []string{"a", "b"})
		require.Equal(t, []string{"a", "b"}, result)
	})

	t.Run("second nil", func(t *testing.T) {
		result := mergeUnique([]string{"a", "b"}, nil)
		require.Equal(t, []string{"a", "b"}, result)
	})

	t.Run("no overlap", func(t *testing.T) {
		result := mergeUnique([]string{"a", "b"}, []string{"c", "d"})
		require.Equal(t, []string{"a", "b", "c", "d"}, result)
	})

	t.Run("with overlap", func(t *testing.T) {
		result := mergeUnique([]string{"a", "b", "c"}, []string{"b", "c", "d"})
		require.Equal(t, []string{"a", "b", "c", "d"}, result)
	})

	t.Run("identical", func(t *testing.T) {
		result := mergeUnique([]string{"a", "b"}, []string{"a", "b"})
		require.Equal(t, []string{"a", "b"}, result)
	})
}

func TestFilterParentFailures(t *testing.T) {
	t.Parallel()

	t.Run("no subtests - keeps all", func(t *testing.T) {
		failures := []testFailure{
			{Name: "TestA"},
			{Name: "TestB"},
		}
		filtered := filterParentFailures(failures)
		require.Len(t, filtered, 2)
	})

	t.Run("removes parent when child present", func(t *testing.T) {
		failures := []testFailure{
			{Name: "TestSuite"},
			{Name: "TestSuite/Sub1"},
			{Name: "TestSuite/Sub2"},
		}
		filtered := filterParentFailures(failures)
		require.Len(t, filtered, 2)
		require.Equal(t, "TestSuite/Sub1", filtered[0].Name)
		require.Equal(t, "TestSuite/Sub2", filtered[1].Name)
	})

	t.Run("removes all ancestor levels", func(t *testing.T) {
		failures := []testFailure{
			{Name: "TestSuite"},
			{Name: "TestSuite/Group"},
			{Name: "TestSuite/Group/Sub1"},
		}
		filtered := filterParentFailures(failures)
		require.Len(t, filtered, 1)
		require.Equal(t, "TestSuite/Group/Sub1", filtered[0].Name)
	})

	t.Run("independent tests kept alongside subtests", func(t *testing.T) {
		failures := []testFailure{
			{Name: "TestSuite"},
			{Name: "TestSuite/Sub1"},
			{Name: "TestOther"},
		}
		filtered := filterParentFailures(failures)
		require.Len(t, filtered, 2)
		require.Equal(t, "TestSuite/Sub1", filtered[0].Name)
		require.Equal(t, "TestOther", filtered[1].Name)
	})
}
