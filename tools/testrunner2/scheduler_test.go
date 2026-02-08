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

func TestBuildRetryFiles_Subtests(t *testing.T) {
	t.Parallel()

	testFiles := []testFile{
		{path: "a_test.go", pkg: "./pkg", tests: []testCase{{name: "TestSuite"}}},
	}

	// Subtests failed
	retryFiles := buildRetryFiles(testFiles, []testCase{{name: "TestSuite/SubTest1"}, {name: "TestSuite/SubTest2"}})
	require.Len(t, retryFiles, 1)
	require.Len(t, retryFiles[0].tests, 2)
	require.Equal(t, "TestSuite/SubTest1", retryFiles[0].tests[0].name)
	require.Equal(t, "TestSuite/SubTest2", retryFiles[0].tests[1].name)
}

func TestBuildRetryFiles_NoMatch(t *testing.T) {
	t.Parallel()

	testFiles := []testFile{
		{path: "a_test.go", pkg: "./pkg", tests: []testCase{{name: "TestA"}}},
	}

	// Different test failed
	retryFiles := buildRetryFiles(testFiles, []testCase{{name: "TestB"}})
	require.Empty(t, retryFiles)
}

func TestResultCollector(t *testing.T) {
	t.Parallel()

	c := &resultCollector{}

	// Test thread-safe operations
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Go(func() {
			c.addReport(&junitReport{})
			c.addAlerts([]alert{{Kind: failureKindCrash}})
			c.addError(os.ErrNotExist)
		})
	}
	wg.Wait()

	require.Len(t, c.junitReports, 10)
	require.Len(t, c.alerts, 10)
	require.Len(t, c.errors, 10)
}

func TestBuildRetryUnit(t *testing.T) {
	t.Parallel()

	t.Run("sets tests from failed tests", func(t *testing.T) {
		unit := workUnit{
			pkg: "./pkg",
			files: []testFile{
				{path: "a_test.go", pkg: "./pkg", tests: []testCase{{name: "TestA"}, {name: "TestB"}}},
			},
			tests: []testCase{{name: "TestA"}, {name: "TestB"}},
			label: "a_test.go",
		}
		retryUnit := buildRetryUnit(unit, []testCase{{name: "TestA", attempts: 1}})
		require.NotNil(t, retryUnit)
		require.Equal(t, "./pkg", retryUnit.pkg)
		require.Len(t, retryUnit.files, 1)
		// unit.tests is always set to the failed tests
		require.Len(t, retryUnit.tests, 1)
		require.Equal(t, "TestA", retryUnit.tests[0].name)
		require.Equal(t, 1, retryUnit.tests[0].attempts)
	})

	t.Run("filters files to those with failed tests", func(t *testing.T) {
		unit := workUnit{
			pkg: "./pkg",
			files: []testFile{
				{path: "a_test.go", pkg: "./pkg", tests: []testCase{{name: "TestA"}}},
				{path: "b_test.go", pkg: "./pkg", tests: []testCase{{name: "TestB"}}},
			},
			tests: []testCase{{name: "TestA"}, {name: "TestB"}},
			label: "./pkg",
		}
		retryUnit := buildRetryUnit(unit, []testCase{{name: "TestB", attempts: 1}})
		require.NotNil(t, retryUnit)
		require.Len(t, retryUnit.files, 1)
		require.Equal(t, "b_test.go", retryUnit.files[0].path)
	})

	t.Run("no matching tests", func(t *testing.T) {
		unit := workUnit{
			pkg: "./pkg",
			files: []testFile{
				{path: "a_test.go", pkg: "./pkg", tests: []testCase{{name: "TestA"}}},
			},
			tests: []testCase{{name: "TestA"}},
		}
		retryUnit := buildRetryUnit(unit, []testCase{{name: "TestB"}})
		require.Nil(t, retryUnit)
	})
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
