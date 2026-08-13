package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/tasks"
)

var readerStuckFireTime = time.Unix(1000, 0).UTC()

func readerStuckKey(offset time.Duration, taskID int64) tasks.Key {
	return tasks.NewKey(readerStuckFireTime.Add(offset), taskID)
}

func newReaderStuckTestMonitor() *monitorImpl {
	return newMonitor(tasks.CategoryTypeScheduled, clock.NewEventTimeSource(), &MonitorOptions{
		PendingTasksCriticalCount:   dynamicconfig.GetIntPropertyFn(1000),
		ReaderStuckCriticalAttempts: dynamicconfig.GetIntPropertyFn(5),
		SliceCountCriticalThreshold: dynamicconfig.GetIntPropertyFn(50),
	})
}

func newReaderStuckTestSlice(monitor Monitor, r Range) Slice {
	slice := NewSlice(
		nil,
		nil,
		monitor,
		NewScope(r, predicates.Universal[tasks.Task]()),
		GrouperNamespaceID{},
		noPredicateSizeLimit,
		defaultMaxPendingKeys,
		metrics.NoopMetricsHandler,
	)
	// paginationFnProvider is nil, so these slices must never be read. Clearing the
	// iterators keeps MoreTasks false so no reader ever selects them.
	slice.iterators = nil
	return slice
}

func newReaderStuckTestReaderGroup(monitor Monitor) *ReaderGroup {
	return NewReaderGroup(func(readerID int64, slices []Slice) Reader {
		return NewReader(
			readerID,
			slices,
			&ReaderOptions{
				BatchSize:            dynamicconfig.GetIntPropertyFn(10),
				MaxPendingTasksCount: dynamicconfig.GetIntPropertyFn(100),
				PollBackoffInterval:  dynamicconfig.GetDurationPropertyFn(200 * time.Millisecond),
				MaxPredicateSize:     dynamicconfig.GetIntPropertyFn(10),
			},
			nil,
			nil,
			clock.NewEventTimeSource(),
			NewReaderPriorityRateLimiter(func() float64 { return 20 }, 2),
			monitor,
			NoopReaderCompletionFn,
			log.NewTestLogger(),
			metrics.NoopMetricsHandler,
		)
	})
}

func newReaderStuckTestAction(readerID int64, maxReaderCount int) *actionReaderStuck {
	return newReaderStuckAction(
		&AlertAttributesReaderStuck{
			ReaderID:         readerID,
			CurrentWatermark: readerStuckKey(0, 0),
		},
		maxReaderCount,
		log.NewTestLogger(),
	)
}

func readerSliceRanges(reader Reader) []Range {
	var ranges []Range
	reader.WalkSlices(func(slice Slice) {
		ranges = append(ranges, slice.Scope().Range)
	})
	return ranges
}

func TestReaderStuckActionRun(t *testing.T) {
	testCases := []struct {
		name           string
		readerID       int64
		maxReaderCount int
		sliceRanges    []Range
		wantMitigated  bool
		wantKept       []Range
		wantMoved      []Range
	}{
		{
			name:           "slice spans both sides of the stuck range",
			readerID:       DefaultReaderId,
			maxReaderCount: 2,
			sliceRanges:    []Range{NewRange(readerStuckKey(-10*time.Second, 0), readerStuckKey(10*time.Second, 0))},
			wantMitigated:  true,
			wantKept: []Range{
				NewRange(readerStuckKey(-10*time.Second, 0), readerStuckKey(0, 0)),
				NewRange(readerStuckKey(time.Second, 0), readerStuckKey(10*time.Second, 0)),
			},
			wantMoved: []Range{NewRange(readerStuckKey(0, 0), readerStuckKey(time.Second, 0))},
		},
		{
			name:           "slice contained in the stuck range",
			readerID:       DefaultReaderId,
			maxReaderCount: 2,
			sliceRanges:    []Range{NewRange(readerStuckKey(0, 5), readerStuckKey(500*time.Millisecond, 0))},
			wantMitigated:  true,
			wantKept:       nil,
			wantMoved:      []Range{NewRange(readerStuckKey(0, 5), readerStuckKey(500*time.Millisecond, 0))},
		},
		{
			name:           "slice starts before the stuck range and ends inside it",
			readerID:       DefaultReaderId,
			maxReaderCount: 2,
			sliceRanges:    []Range{NewRange(readerStuckKey(-time.Second, 0), readerStuckKey(500*time.Millisecond, 0))},
			wantMitigated:  true,
			wantKept:       []Range{NewRange(readerStuckKey(-time.Second, 0), readerStuckKey(0, 0))},
			wantMoved:      []Range{NewRange(readerStuckKey(0, 0), readerStuckKey(500*time.Millisecond, 0))},
		},
		{
			name:           "slice starts inside the stuck range and ends after it",
			readerID:       DefaultReaderId,
			maxReaderCount: 2,
			sliceRanges:    []Range{NewRange(readerStuckKey(500*time.Millisecond, 0), readerStuckKey(2*time.Second, 0))},
			wantMitigated:  true,
			wantKept:       []Range{NewRange(readerStuckKey(time.Second, 0), readerStuckKey(2*time.Second, 0))},
			wantMoved:      []Range{NewRange(readerStuckKey(500*time.Millisecond, 0), readerStuckKey(time.Second, 0))},
		},
		{
			name:           "several slices overlap the stuck range",
			readerID:       DefaultReaderId,
			maxReaderCount: 2,
			sliceRanges: []Range{
				NewRange(readerStuckKey(0, 0), readerStuckKey(300*time.Millisecond, 0)),
				NewRange(readerStuckKey(300*time.Millisecond, 0), readerStuckKey(2*time.Second, 0)),
			},
			wantMitigated: true,
			wantKept:      []Range{NewRange(readerStuckKey(time.Second, 0), readerStuckKey(2*time.Second, 0))},
			wantMoved:     []Range{NewRange(readerStuckKey(0, 0), readerStuckKey(time.Second, 0))},
		},
		{
			name:           "slices only abut the stuck range",
			readerID:       DefaultReaderId,
			maxReaderCount: 2,
			sliceRanges: []Range{
				NewRange(readerStuckKey(-5*time.Second, 0), readerStuckKey(0, 0)),
				NewRange(readerStuckKey(time.Second, 0), readerStuckKey(5*time.Second, 0)),
			},
			wantMitigated: false,
			wantKept: []Range{
				NewRange(readerStuckKey(-5*time.Second, 0), readerStuckKey(0, 0)),
				NewRange(readerStuckKey(time.Second, 0), readerStuckKey(5*time.Second, 0)),
			},
			wantMoved: nil,
		},
		{
			name:           "max reader count reached",
			readerID:       DefaultReaderId,
			maxReaderCount: 1,
			sliceRanges:    []Range{NewRange(readerStuckKey(0, 0), readerStuckKey(500*time.Millisecond, 0))},
			wantMitigated:  false,
			wantKept:       []Range{NewRange(readerStuckKey(0, 0), readerStuckKey(500*time.Millisecond, 0))},
			wantMoved:      nil,
		},
		{
			name:           "already on the last reader",
			readerID:       DefaultReaderId + 1,
			maxReaderCount: 2,
			sliceRanges:    []Range{NewRange(readerStuckKey(0, 0), readerStuckKey(500*time.Millisecond, 0))},
			wantMitigated:  false,
			wantKept:       []Range{NewRange(readerStuckKey(0, 0), readerStuckKey(500*time.Millisecond, 0))},
			wantMoved:      nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			monitor := newReaderStuckTestMonitor()
			t.Cleanup(monitor.Close)

			slices := make([]Slice, 0, len(tc.sliceRanges))
			for _, r := range tc.sliceRanges {
				slices = append(slices, newReaderStuckTestSlice(monitor, r))
			}
			readerGroup := newReaderStuckTestReaderGroup(monitor)
			readerGroup.NewReader(tc.readerID, slices...)

			action := newReaderStuckTestAction(tc.readerID, tc.maxReaderCount)
			require.Equal(t, tc.wantMitigated, action.Run(readerGroup))

			reader, ok := readerGroup.ReaderByID(tc.readerID)
			require.True(t, ok)
			require.Equal(t, tc.wantKept, readerSliceRanges(reader))

			nextReader, ok := readerGroup.ReaderByID(tc.readerID + 1)
			if tc.wantMoved == nil {
				// Also guards against the next reader being created for an empty slice;
				// SplitSlices drops those, so the kept ranges alone would not notice.
				require.False(t, ok, "next reader should not be created when nothing moved")
				return
			}
			require.True(t, ok)
			require.Equal(t, tc.wantMoved, readerSliceRanges(nextReader))
		})
	}
}

func TestReaderStuckActionMergesIntoExistingNextReader(t *testing.T) {
	monitor := newReaderStuckTestMonitor()
	t.Cleanup(monitor.Close)

	readerGroup := newReaderStuckTestReaderGroup(monitor)
	readerGroup.NewReader(
		DefaultReaderId,
		newReaderStuckTestSlice(monitor, NewRange(readerStuckKey(0, 0), readerStuckKey(2*time.Second, 0))),
	)
	existing := NewRange(readerStuckKey(10*time.Second, 0), readerStuckKey(20*time.Second, 0))
	readerGroup.NewReader(DefaultReaderId+1, newReaderStuckTestSlice(monitor, existing))

	require.True(t, newReaderStuckTestAction(DefaultReaderId, 2).Run(readerGroup))

	nextReader, ok := readerGroup.ReaderByID(DefaultReaderId + 1)
	require.True(t, ok)
	require.Equal(t, []Range{
		NewRange(readerStuckKey(0, 0), readerStuckKey(time.Second, 0)),
		existing,
	}, readerSliceRanges(nextReader))
}

func TestReaderStuckActionSkipsWhenReaderMissing(t *testing.T) {
	monitor := newReaderStuckTestMonitor()
	t.Cleanup(monitor.Close)

	readerGroup := newReaderStuckTestReaderGroup(monitor)

	require.False(t, newReaderStuckTestAction(DefaultReaderId, 2).Run(readerGroup))
	require.Empty(t, readerGroup.Readers())
}

func TestReaderStuckActionDoesNotCascadeAfterMovingSlice(t *testing.T) {
	// The moved slice keeps its monitor entry, so the receiving reader has to build up
	// its own attempts before the slice is demoted again.
	monitor := newReaderStuckTestMonitor()
	t.Cleanup(monitor.Close)

	sliceRange := NewRange(readerStuckKey(0, 0), readerStuckKey(500*time.Millisecond, 0))
	slice := newReaderStuckTestSlice(monitor, sliceRange)
	readerGroup := newReaderStuckTestReaderGroup(monitor)
	readerGroup.NewReader(DefaultReaderId, slice)

	criticalAttempts := monitor.options.ReaderStuckCriticalAttempts()
	for i := 0; i != criticalAttempts; i++ {
		monitor.SetSliceReadWatermark(slice, DefaultReaderId, readerStuckKey(0, int64(i)))
	}
	var alert *Alert
	select {
	case alert = <-monitor.AlertCh():
	default:
		require.FailNow(t, "expected the original reader to alert")
	}
	require.Equal(t, DefaultReaderId, alert.AlertAttributesReaderStuck.ReaderID)
	monitor.ResolveAlert(alert.AlertType)

	require.True(t, newReaderStuckTestAction(DefaultReaderId, 3).Run(readerGroup))

	monitor.SetSliceReadWatermark(slice, DefaultReaderId+1, readerStuckKey(0, 0))
	select {
	case <-monitor.AlertCh():
		require.FailNow(t, "a slice must not be demoted again on the new reader's first read")
	default:
	}

	for i := 1; i != criticalAttempts; i++ {
		monitor.SetSliceReadWatermark(slice, DefaultReaderId+1, readerStuckKey(0, int64(i)))
	}
	select {
	case alert := <-monitor.AlertCh():
		require.Equal(t, DefaultReaderId+1, alert.AlertAttributesReaderStuck.ReaderID)
	default:
		require.FailNow(t, "expected the new reader to alert after its own attempts")
	}
}
