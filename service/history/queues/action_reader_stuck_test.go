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

// readerStuckTestTime is the fire time the reader is assumed to be stuck on. The
// stuck range the action builds from it is [readerStuckTestTime, +1s).
var readerStuckTestTime = time.Unix(1000, 0).UTC()

func newReaderStuckTestMonitor() *monitorImpl {
	return newMonitor(tasks.CategoryTypeScheduled, clock.NewRealTimeSource(), &MonitorOptions{
		PendingTasksCriticalCount:   dynamicconfig.GetIntPropertyFn(1000),
		ReaderStuckCriticalAttempts: dynamicconfig.GetIntPropertyFn(5),
		SliceCountCriticalThreshold: dynamicconfig.GetIntPropertyFn(50),
	})
}

func newReaderStuckTestSlice(monitor Monitor, r Range) *SliceImpl {
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
	// Drop the iterators so the slices never attempt to load from persistence.
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
			clock.NewRealTimeSource(),
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
			CurrentWatermark: tasks.NewKey(readerStuckTestTime, 0),
		},
		maxReaderCount,
		log.NewTestLogger(),
	)
}

func sliceRanges(reader Reader) []Range {
	var ranges []Range
	reader.WalkSlices(func(slice Slice) {
		ranges = append(ranges, slice.Scope().Range)
	})
	return ranges
}

func TestReaderStuckActionMovesStuckWindowToNextReader(t *testing.T) {
	monitor := newReaderStuckTestMonitor()
	defer monitor.Close()

	// The slice extends on both sides of the stuck window, so it has to be split
	// into three and only the middle piece handed to the next reader.
	slice := newReaderStuckTestSlice(monitor, NewRange(
		tasks.NewKey(readerStuckTestTime.Add(-10*time.Second), 0),
		tasks.NewKey(readerStuckTestTime.Add(10*time.Second), 0),
	))

	readerGroup := newReaderStuckTestReaderGroup(monitor)
	readerGroup.NewReader(DefaultReaderId, slice)

	require.True(t, newReaderStuckTestAction(DefaultReaderId, 2).Run(readerGroup))

	reader, ok := readerGroup.ReaderByID(DefaultReaderId)
	require.True(t, ok)
	require.Equal(t, []Range{
		NewRange(
			tasks.NewKey(readerStuckTestTime.Add(-10*time.Second), 0),
			tasks.NewKey(readerStuckTestTime, 0),
		),
		NewRange(
			tasks.NewKey(readerStuckTestTime.Add(time.Second), 0),
			tasks.NewKey(readerStuckTestTime.Add(10*time.Second), 0),
		),
	}, sliceRanges(reader))

	nextReader, ok := readerGroup.ReaderByID(DefaultReaderId + 1)
	require.True(t, ok)
	require.Equal(t, []Range{
		NewRange(
			tasks.NewKey(readerStuckTestTime, 0),
			tasks.NewKey(readerStuckTestTime.Add(time.Second), 0),
		),
	}, sliceRanges(nextReader))
}

func TestReaderStuckActionMovesWholeSliceInsideStuckWindow(t *testing.T) {
	monitor := newReaderStuckTestMonitor()
	defer monitor.Close()

	sliceRange := NewRange(
		tasks.NewKey(readerStuckTestTime, 5),
		tasks.NewKey(readerStuckTestTime.Add(500*time.Millisecond), 0),
	)
	readerGroup := newReaderStuckTestReaderGroup(monitor)
	readerGroup.NewReader(DefaultReaderId, newReaderStuckTestSlice(monitor, sliceRange))

	require.True(t, newReaderStuckTestAction(DefaultReaderId, 2).Run(readerGroup))

	reader, ok := readerGroup.ReaderByID(DefaultReaderId)
	require.True(t, ok)
	require.Empty(t, sliceRanges(reader))

	nextReader, ok := readerGroup.ReaderByID(DefaultReaderId + 1)
	require.True(t, ok)
	require.Equal(t, []Range{sliceRange}, sliceRanges(nextReader))
}

func TestReaderStuckActionLeavesNonOverlappingSlicesAlone(t *testing.T) {
	monitor := newReaderStuckTestMonitor()
	defer monitor.Close()

	// These two slices only touch the stuck range at its bounds. Splitting either of
	// them can only produce empty slices, so both must be left where they are.
	before := NewRange(
		tasks.NewKey(readerStuckTestTime.Add(-5*time.Second), 0),
		tasks.NewKey(readerStuckTestTime, 0),
	)
	after := NewRange(
		tasks.NewKey(readerStuckTestTime.Add(time.Second), 0),
		tasks.NewKey(readerStuckTestTime.Add(5*time.Second), 0),
	)

	readerGroup := newReaderStuckTestReaderGroup(monitor)
	readerGroup.NewReader(
		DefaultReaderId,
		newReaderStuckTestSlice(monitor, before),
		newReaderStuckTestSlice(monitor, after),
	)

	require.False(t, newReaderStuckTestAction(DefaultReaderId, 2).Run(readerGroup))

	reader, ok := readerGroup.ReaderByID(DefaultReaderId)
	require.True(t, ok)
	require.Equal(t, []Range{before, after}, sliceRanges(reader))

	_, ok = readerGroup.ReaderByID(DefaultReaderId + 1)
	require.False(t, ok, "next reader should not be created when nothing was moved")
}

func TestReaderStuckActionStopsAtMaxReaderCount(t *testing.T) {
	testCases := []struct {
		name           string
		readerID       int64
		maxReaderCount int
	}{
		{name: "only default reader allowed", readerID: DefaultReaderId, maxReaderCount: 1},
		{name: "already on the last reader", readerID: DefaultReaderId + 1, maxReaderCount: 2},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			monitor := newReaderStuckTestMonitor()
			defer monitor.Close()

			sliceRange := NewRange(
				tasks.NewKey(readerStuckTestTime, 0),
				tasks.NewKey(readerStuckTestTime.Add(500*time.Millisecond), 0),
			)
			readerGroup := newReaderStuckTestReaderGroup(monitor)
			readerGroup.NewReader(tc.readerID, newReaderStuckTestSlice(monitor, sliceRange))

			require.False(t, newReaderStuckTestAction(tc.readerID, tc.maxReaderCount).Run(readerGroup))

			reader, ok := readerGroup.ReaderByID(tc.readerID)
			require.True(t, ok)
			require.Equal(t, []Range{sliceRange}, sliceRanges(reader))

			_, ok = readerGroup.ReaderByID(tc.readerID + 1)
			require.False(t, ok)
		})
	}
}

func TestReaderStuckActionMissingReader(t *testing.T) {
	monitor := newReaderStuckTestMonitor()
	defer monitor.Close()

	readerGroup := newReaderStuckTestReaderGroup(monitor)

	require.False(t, newReaderStuckTestAction(DefaultReaderId, 2).Run(readerGroup))
	require.Empty(t, readerGroup.Readers())
}
