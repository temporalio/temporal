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
