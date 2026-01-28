package testrunner2

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// nopLog is a no-op logger for tests.
func nopLog(string, ...any) {}

// createTestJunit creates a temp junit file for testing and returns its path.
func createTestJunit(t *testing.T, tests, failures int) string {
	t.Helper()
	f, err := os.CreateTemp("", "junit-*.xml")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(f.Name()) })

	jr := &junitReport{}
	jr.Tests = tests
	jr.Failures = failures
	jr.path = f.Name()
	require.NoError(t, jr.write())
	return f.Name()
}

func TestScheduler_Parallelism(t *testing.T) {
	s := newScheduler(2)

	var running atomic.Int32
	var maxConcurrent atomic.Int32
	var processed atomic.Int32

	items := make([]*queueItem, 4)
	for i := range items {
		items[i] = &queueItem{
			run: func(ctx context.Context) {
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
	s := newScheduler(2)
	s.run(context.Background(), nil)
	// Should not panic or hang
}

func TestScheduler_NextEnqueue(t *testing.T) {
	s := newScheduler(1)

	var order []int
	var mu sync.Mutex

	addOrder := func(n int) {
		mu.Lock()
		order = append(order, n)
		mu.Unlock()
	}

	secondItem := &queueItem{
		run: func(ctx context.Context) { addOrder(2) },
	}

	firstItem := &queueItem{
		run: func(ctx context.Context) { addOrder(1) },
		next: func() []*queueItem {
			return []*queueItem{secondItem}
		},
	}

	s.run(context.Background(), []*queueItem{firstItem})

	require.Equal(t, []int{1, 2}, order, "second item should be processed after first")
}

func TestScheduler_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := newScheduler(2)

	var processed atomic.Int32
	items := []*queueItem{
		{run: func(ctx context.Context) { processed.Add(1) }},
		{run: func(ctx context.Context) { processed.Add(1) }},
	}

	s.run(ctx, items)
	// With cancelled context, should exit quickly (may process 0-2 items)
}

func TestNewTestItem_CreatesCallbacks(t *testing.T) {
	cfg := config{
		log:            nopLog,
		filesPerWorker: 10,
		maxAttempts:    2,
		groupBy:        GroupByFile,
	}
	testFiles := []testFile{
		{path: "a_test.go", pkg: "./pkg", tests: []testCase{{name: "TestA"}}},
		{path: "b_test.go", pkg: "./pkg", tests: []testCase{{name: "TestB"}}},
	}
	unit := workUnit{
		pkg:   "./pkg",
		files: testFiles,
		tests: []testCase{{name: "TestA"}, {name: "TestB"}},
		label: "a_test.go, b_test.go",
	}
	r := &runner{
		config:    cfg,
		console:   &consoleWriter{mu: &sync.Mutex{}},
		collector: &resultCollector{},
		progress:  &progressTracker{total: 1},
	}

	item := r.newTestItem(unit, "/fake/binary", 1)
	require.NotNil(t, item.run)
	require.NotNil(t, item.next)
}

func TestNewTestItem_CapturesAttempt(t *testing.T) {
	cfg := config{
		log:            nopLog,
		filesPerWorker: 10,
		maxAttempts:    2,
		groupBy:        GroupByFile,
	}
	testFiles := []testFile{
		{path: "a_test.go", pkg: "./pkg", tests: []testCase{{name: "TestA"}}},
	}
	unit := workUnit{
		pkg:   "./pkg",
		files: testFiles,
		tests: []testCase{{name: "TestA"}},
		label: "a_test.go",
	}
	r := &runner{
		config:    cfg,
		console:   &consoleWriter{mu: &sync.Mutex{}},
		collector: &resultCollector{},
		progress:  &progressTracker{total: 1},
	}

	// Create items with different attempts
	item1 := r.newTestItem(unit, "/fake/binary", 1)
	item2 := r.newTestItem(unit, "/fake/binary", 2)

	// Both should have callbacks
	require.NotNil(t, item1.run)
	require.NotNil(t, item2.run)
}

func TestNewCompileItem(t *testing.T) {
	cfg := config{
		log:            nopLog,
		filesPerWorker: 2,
		groupBy:        GroupByFile,
	}
	units := []workUnit{
		{pkg: "./pkg", files: []testFile{{path: "a_test.go", pkg: "./pkg", tests: []testCase{{name: "TestA"}}}}, tests: []testCase{{name: "TestA"}}, label: "a_test.go"},
		{pkg: "./pkg", files: []testFile{{path: "b_test.go", pkg: "./pkg", tests: []testCase{{name: "TestB"}}}}, tests: []testCase{{name: "TestB"}}, label: "b_test.go"},
		{pkg: "./pkg", files: []testFile{{path: "c_test.go", pkg: "./pkg", tests: []testCase{{name: "TestC"}}}}, tests: []testCase{{name: "TestC"}}, label: "c_test.go"},
	}
	r := &runner{
		config:    cfg,
		console:   &consoleWriter{mu: &sync.Mutex{}},
		collector: &resultCollector{},
		progress:  &progressTracker{total: int64(len(units))},
	}

	item := r.newCompileItem("./pkg", units, "/fake/binary", []string{"test", "-c"})
	require.NotNil(t, item.run)
	require.NotNil(t, item.next)
}

func TestBuildRetryFiles(t *testing.T) {
	testFiles := []testFile{
		{path: "a_test.go", pkg: "./pkg", tests: []testCase{{name: "TestA"}}},
		{path: "b_test.go", pkg: "./pkg", tests: []testCase{{name: "TestB"}}},
		{path: "c_test.go", pkg: "./pkg", tests: []testCase{{name: "TestC"}}},
	}

	// Only TestB failed
	retryFiles := buildRetryFiles(testFiles, []testCase{{name: "TestB", attempts: 1}})
	require.Len(t, retryFiles, 1)
	require.Equal(t, "b_test.go", retryFiles[0].path)
	require.Len(t, retryFiles[0].tests, 1)
	require.Equal(t, "TestB", retryFiles[0].tests[0].name)
	require.Equal(t, 1, retryFiles[0].tests[0].attempts)
}

func TestBuildRetryFiles_Subtests(t *testing.T) {
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
	testFiles := []testFile{
		{path: "a_test.go", pkg: "./pkg", tests: []testCase{{name: "TestA"}}},
	}

	// Different test failed
	retryFiles := buildRetryFiles(testFiles, []testCase{{name: "TestB"}})
	require.Empty(t, retryFiles)
}

func TestResultCollector(t *testing.T) {
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
	t.Run("package mode", func(t *testing.T) {
		unit := workUnit{
			pkg:   "./common/persistence",
			tests: []testCase{{name: "TestA"}},
			label: "./common/persistence",
		}
		desc := describeUnit(unit, GroupByPackage)
		require.Equal(t, "./common/persistence", desc)
	})

	t.Run("suite mode", func(t *testing.T) {
		unit := workUnit{pkg: "./pkg", label: "TestFoo", tests: []testCase{{name: "TestFoo"}}}
		desc := describeUnit(unit, GroupBySuite)
		require.Equal(t, "TestFoo", desc)
	})

	t.Run("file mode single file few tests", func(t *testing.T) {
		unit := workUnit{
			pkg: "./pkg",
			files: []testFile{
				{path: "foo_test.go", tests: []testCase{{name: "TestA"}, {name: "TestB"}}},
			},
			tests: []testCase{{name: "TestA"}, {name: "TestB"}},
		}
		desc := describeUnit(unit, GroupByFile)
		require.Equal(t, "foo_test.go (TestA, TestB)", desc)
	})

	t.Run("file mode single file many tests", func(t *testing.T) {
		unit := workUnit{
			pkg: "./pkg",
			files: []testFile{
				{path: "foo_test.go", tests: []testCase{{name: "TestA"}, {name: "TestB"}, {name: "TestC"}, {name: "TestD"}, {name: "TestE"}, {name: "TestF"}}},
			},
			tests: []testCase{{name: "TestA"}, {name: "TestB"}, {name: "TestC"}, {name: "TestD"}, {name: "TestE"}, {name: "TestF"}},
		}
		desc := describeUnit(unit, GroupByFile)
		require.Equal(t, "foo_test.go (6 tests)", desc)
	})

	t.Run("file mode multiple files", func(t *testing.T) {
		unit := workUnit{
			pkg: "./pkg",
			files: []testFile{
				{path: "a_test.go", tests: []testCase{{name: "TestA"}}},
				{path: "b_test.go", tests: []testCase{{name: "TestB"}}},
			},
			tests: []testCase{{name: "TestA"}, {name: "TestB"}},
		}
		desc := describeUnit(unit, GroupByFile)
		require.Equal(t, "a_test.go, b_test.go", desc)
	})
}

func TestBuildRetryUnit(t *testing.T) {
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
		retryUnit := buildRetryUnitExcluding(unit, []string{"TestA", "TestB"}, 1)
		require.NotNil(t, retryUnit)
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
		retryUnit := buildRetryUnitExcluding(unit, []string{"TestA", "TestB"}, 1)
		require.NotNil(t, retryUnit)
		require.ElementsMatch(t, []string{"TestA", "TestB"}, retryUnit.skipTests)
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
		retryUnit := buildRetryUnitExcluding(unit, []string{}, 1)
		require.NotNil(t, retryUnit)
		require.Len(t, retryUnit.files, 2) // all files present
		require.Empty(t, retryUnit.skipTests)
	})
}
