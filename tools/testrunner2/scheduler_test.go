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

func TestDescribeUnit(t *testing.T) {
	t.Parallel()

	t.Run("test mode", func(t *testing.T) {
		unit := workUnit{pkg: "./pkg", label: "TestFoo", tests: []testCase{{name: "TestFoo"}}}
		desc := describeUnit(unit, GroupByTest)
		require.Equal(t, "TestFoo", desc)
	})

	t.Run("default mode returns label", func(t *testing.T) {
		unit := workUnit{
			pkg:   "./pkg",
			label: "my-label",
			tests: []testCase{{name: "TestA"}, {name: "TestB"}},
		}
		desc := describeUnit(unit, "unknown")
		require.Equal(t, "my-label", desc)
	})
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

func TestBuildRetryUnitExcluding(t *testing.T) {
	t.Parallel()

	t.Run("sets skipTests for passed tests", func(t *testing.T) {
		unit := workUnit{
			pkg: "./pkg",
			files: []testFile{
				{path: "a_test.go", pkg: "./pkg", tests: []testCase{{name: "TestA"}, {name: "TestB"}, {name: "TestC"}}},
			},
			tests: []testCase{{name: "TestA"}, {name: "TestB"}, {name: "TestC"}},
			label: "a_test.go",
		}
		// TestA and TestB passed - they should be in skipTests, all tests should still run
		retryUnits := buildRetryUnitExcluding(unit, []string{"TestA", "TestB"}, nil, 1)
		require.Len(t, retryUnits, 1)
		retryUnit := retryUnits[0]
		require.Equal(t, "./pkg", retryUnit.pkg)
		require.Len(t, retryUnit.files, 1)
		require.Len(t, retryUnit.tests, 3) // all tests present
		require.Equal(t, 1, retryUnit.tests[0].attempts)
		// skipTests should contain the passed tests
		require.ElementsMatch(t, []string{"TestA", "TestB"}, retryUnit.skipTests)
	})

	t.Run("all tests passed sets skipTests for all", func(t *testing.T) {
		unit := workUnit{
			pkg: "./pkg",
			files: []testFile{
				{path: "a_test.go", pkg: "./pkg", tests: []testCase{{name: "TestA"}, {name: "TestB"}}},
			},
			tests: []testCase{{name: "TestA"}, {name: "TestB"}},
		}
		// All tests passed - skipTests should contain all of them
		retryUnits := buildRetryUnitExcluding(unit, []string{"TestA", "TestB"}, nil, 1)
		require.Len(t, retryUnits, 1)
		require.ElementsMatch(t, []string{"TestA", "TestB"}, retryUnits[0].skipTests)
	})

	t.Run("no passed tests means no skipTests", func(t *testing.T) {
		unit := workUnit{
			pkg: "./pkg",
			files: []testFile{
				{path: "a_test.go", pkg: "./pkg", tests: []testCase{{name: "TestA"}}},
				{path: "b_test.go", pkg: "./pkg", tests: []testCase{{name: "TestB"}}},
			},
			tests: []testCase{{name: "TestA"}, {name: "TestB"}},
		}
		// No tests passed - skipTests should be empty
		retryUnits := buildRetryUnitExcluding(unit, []string{}, nil, 1)
		require.Len(t, retryUnits, 1)
		require.Len(t, retryUnits[0].files, 2) // all files present
		require.Empty(t, retryUnits[0].skipTests)
	})

	t.Run("quarantines timed-out subtest", func(t *testing.T) {
		unit := workUnit{
			pkg: "./pkg",
			files: []testFile{
				{path: "a_test.go", pkg: "./pkg", tests: []testCase{{name: "TestMixed"}}},
			},
			tests: []testCase{{name: "TestMixed"}},
			label: "TestMixed",
		}
		// Mixed-depth passed tests: TestMixed/SimpleA (depth 2) and TestMixed/Param/Var1 (depth 3).
		// Quarantined: TestMixed/Param/Var2 (timed out).
		passedTests := []string{"TestMixed/SimpleA", "TestMixed/SimpleB", "TestMixed/Param/Var1"}
		quarantined := []string{"TestMixed/Param/Var2"}
		retryUnits := buildRetryUnitExcluding(unit, passedTests, quarantined, 1)
		require.Len(t, retryUnits, 2, "expected quarantine unit + regular unit")

		// First unit: quarantine for TestMixed/Param, skipping passed sibling Var1
		qUnit := retryUnits[0]
		require.Len(t, qUnit.tests, 1)
		require.Equal(t, "TestMixed/Param", qUnit.tests[0].name)
		require.Equal(t, 1, qUnit.tests[0].attempts)
		require.ElementsMatch(t, []string{"TestMixed/Param/Var1"}, qUnit.skipTests)

		// Second unit: regular retry skipping SimpleA, SimpleB, and the quarantined parent Param
		rUnit := retryUnits[1]
		require.Len(t, rUnit.tests, 1)
		require.Equal(t, "TestMixed", rUnit.tests[0].name)
		require.Equal(t, 1, rUnit.tests[0].attempts)
		require.ElementsMatch(t, []string{"TestMixed/SimpleA", "TestMixed/SimpleB", "TestMixed/Param"}, rUnit.skipTests)
	})

	t.Run("no quarantine when no stuck test", func(t *testing.T) {
		unit := workUnit{
			pkg: "./pkg",
			files: []testFile{
				{path: "a_test.go", pkg: "./pkg", tests: []testCase{{name: "TestA"}}},
			},
			tests: []testCase{{name: "TestA"}},
		}
		retryUnits := buildRetryUnitExcluding(unit, []string{"TestA/Sub1"}, nil, 1)
		require.Len(t, retryUnits, 1)
		require.ElementsMatch(t, []string{"TestA/Sub1"}, retryUnits[0].skipTests)
	})

	t.Run("no quarantine when stuck test has no parent", func(t *testing.T) {
		unit := workUnit{
			pkg: "./pkg",
			files: []testFile{
				{path: "a_test.go", pkg: "./pkg", tests: []testCase{{name: "TestA"}, {name: "TestB"}}},
			},
			tests: []testCase{{name: "TestA"}, {name: "TestB"}},
		}
		// Quarantined test is top-level, no parent to isolate
		retryUnits := buildRetryUnitExcluding(unit, []string{"TestA"}, []string{"TestB"}, 1)
		require.Len(t, retryUnits, 1)
		require.ElementsMatch(t, []string{"TestA"}, retryUnits[0].skipTests)
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
