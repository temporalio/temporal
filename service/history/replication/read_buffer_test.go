package replication

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
)

// fakeStore is a fetch function that serves tasks at the given ids and counts calls.
type fakeStore struct {
	ids     []int64
	fetches int
}

func (f *fakeStore) fetch(minInclusive, maxExclusive int64) ([]tasks.Task, int64, error) {
	f.fetches++
	var page []tasks.Task
	for _, id := range f.ids {
		if id >= minInclusive && id < maxExclusive {
			page = append(page, bufferedTask(id))
		}
	}
	return page, maxExclusive, nil
}

func bufferedTask(id int64) tasks.Task {
	return &tasks.HistoryReplicationTask{
		WorkflowKey: definition.NewWorkflowKey("ns", "wf", "run"),
		TaskID:      id,
	}
}

func taskIDs(page []tasks.Task) []int64 {
	out := make([]int64, 0, len(page))
	for _, t := range page {
		out = append(out, t.GetTaskID())
	}
	return out
}

func newTestBuffer(capacity int) *readBuffer {
	return &readBuffer{
		capacityFn: func() int { return capacity },
		serializer: serialization.NewSerializer(),
	}
}

func TestReadBuffer_Disabled_PassesThrough(t *testing.T) {
	store := &fakeStore{ids: []int64{1, 2, 3}}
	b := newTestBuffer(0)

	page, next, err := b.readPage(1, 4, store.fetch)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3}, taskIDs(page))
	require.Zero(t, next)

	_, _, err = b.readPage(1, 4, store.fetch)
	require.NoError(t, err)
	require.Equal(t, 2, store.fetches) // nothing cached
}

func TestReadBuffer_SecondReaderServedFromMemory(t *testing.T) {
	store := &fakeStore{ids: []int64{10, 11, 12}}
	b := newTestBuffer(100)

	page, next, err := b.readPage(10, 13, store.fetch)
	require.NoError(t, err)
	require.Equal(t, []int64{10, 11, 12}, taskIDs(page))
	require.Zero(t, next)
	require.Equal(t, 1, store.fetches)

	// A second lane scanning the same range hits memory, not persistence.
	page, next, err = b.readPage(10, 13, store.fetch)
	require.NoError(t, err)
	require.Equal(t, []int64{10, 11, 12}, taskIDs(page))
	require.Zero(t, next)
	require.Equal(t, 1, store.fetches)

	// A partially-overlapping scan is served from memory for the covered part and
	// continues from the covered bound.
	page, next, err = b.readPage(11, 20, store.fetch)
	require.NoError(t, err)
	require.Equal(t, []int64{11, 12}, taskIDs(page))
	require.Equal(t, int64(13), next) // resume at the uncovered remainder
	require.Equal(t, 1, store.fetches)
}

func TestReadBuffer_ContiguousExtension(t *testing.T) {
	store := &fakeStore{ids: []int64{1, 2, 3, 4}}
	b := newTestBuffer(100)

	_, _, err := b.readPage(1, 3, store.fetch)
	require.NoError(t, err)
	_, _, err = b.readPage(3, 5, store.fetch) // extends coverage to [1, 5)
	require.NoError(t, err)
	require.Equal(t, 2, store.fetches)

	page, next, err := b.readPage(1, 5, store.fetch)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4}, taskIDs(page))
	require.Zero(t, next)
	require.Equal(t, 2, store.fetches) // fully from memory
}

func TestReadBuffer_BelowCoverageNotCached(t *testing.T) {
	store := &fakeStore{ids: []int64{1, 2, 50, 51}}
	b := newTestBuffer(100)

	_, _, err := b.readPage(50, 52, store.fetch) // coverage [50, 52)
	require.NoError(t, err)

	// A lagging reader below coverage reads persistence, and does not disturb the
	// tip coverage.
	page, _, err := b.readPage(1, 3, store.fetch)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, taskIDs(page))
	require.Equal(t, 2, store.fetches)

	_, _, err = b.readPage(50, 52, store.fetch)
	require.NoError(t, err)
	require.Equal(t, 2, store.fetches) // tip still served from memory
}

func TestReadBuffer_GapRestartsCoverageAtNewerTip(t *testing.T) {
	store := &fakeStore{ids: []int64{1, 2, 100, 101}}
	b := newTestBuffer(100)

	_, _, err := b.readPage(1, 3, store.fetch) // coverage [1, 3)
	require.NoError(t, err)
	_, _, err = b.readPage(100, 102, store.fetch) // jump: coverage restarts [100, 102)
	require.NoError(t, err)

	_, _, err = b.readPage(100, 102, store.fetch)
	require.NoError(t, err)
	require.Equal(t, 2, store.fetches) // new tip served from memory

	_, _, err = b.readPage(1, 3, store.fetch)
	require.NoError(t, err)
	require.Equal(t, 3, store.fetches) // old range fell out of coverage
}

func TestReadBuffer_EvictionShrinksCoverageFromFront(t *testing.T) {
	store := &fakeStore{ids: []int64{1, 2, 3, 4, 5, 6}}
	b := newTestBuffer(3)

	_, _, err := b.readPage(1, 4, store.fetch) // [1, 4): 3 tasks, at capacity
	require.NoError(t, err)
	_, _, err = b.readPage(4, 7, store.fetch) // extends, then evicts 1-3
	require.NoError(t, err)

	_, _, err = b.readPage(4, 7, store.fetch)
	require.NoError(t, err)
	require.Equal(t, 2, store.fetches) // tip retained

	_, _, err = b.readPage(1, 4, store.fetch)
	require.NoError(t, err)
	require.Equal(t, 3, store.fetches) // evicted range reads persistence
}

// TestReadBuffer_ServesFreshCopies pins the ownership contract: every reader gets its
// own task structs, never pointers shared with the buffer or other readers —
// downstream converters mutate tasks in place, which must stay reader-local.
func TestReadBuffer_ServesFreshCopies(t *testing.T) {
	store := &fakeStore{ids: []int64{10, 11}}
	b := newTestBuffer(100)

	first, _, err := b.readPage(10, 12, store.fetch)
	require.NoError(t, err)
	second, _, err := b.readPage(10, 12, store.fetch)
	require.NoError(t, err)
	require.Equal(t, 1, store.fetches)
	require.Equal(t, taskIDs(first), taskIDs(second))

	// Mutating one reader's copy must not leak into another reader's serve.
	second[0].SetTaskID(999999)
	third, _, err := b.readPage(10, 12, store.fetch)
	require.NoError(t, err)
	require.Equal(t, []int64{10, 11}, taskIDs(third))
	for i := range second {
		require.NotSame(t, second[i], third[i])
	}
}

// TestReadBuffer_DisableClearsState verifies that flipping capacity to 0 at runtime
// releases the buffered rows instead of stranding them until shard restart.
func TestReadBuffer_DisableClearsState(t *testing.T) {
	store := &fakeStore{ids: []int64{1, 2, 3}}
	capacity := 100
	b := &readBuffer{
		capacityFn: func() int { return capacity },
		serializer: serialization.NewSerializer(),
	}

	_, _, err := b.readPage(1, 4, store.fetch)
	require.NoError(t, err)
	require.NotEmpty(t, b.buffered)

	capacity = 0
	_, _, err = b.readPage(1, 4, store.fetch)
	require.NoError(t, err)
	require.Empty(t, b.buffered)
	require.Zero(t, b.maxCovered)

	// Re-enabling starts from a clean slate: the next read is a fresh fill.
	capacity = 100
	_, _, err = b.readPage(1, 4, store.fetch)
	require.NoError(t, err)
	require.Equal(t, 3, store.fetches)
	_, _, err = b.readPage(1, 4, store.fetch)
	require.NoError(t, err)
	require.Equal(t, 3, store.fetches) // served from memory again
}

// TestReadBuffer_ConcurrentReaders is a -race stress test: many readers page through
// a moving tip concurrently; each must observe exactly the store's tasks for its
// range, with no torn or shared state.
func TestReadBuffer_ConcurrentReaders(t *testing.T) {
	const maxID = 400
	ids := make([]int64, 0, maxID)
	for id := int64(1); id <= maxID; id++ {
		ids = append(ids, id)
	}
	var fetchMu sync.Mutex
	fetches := 0
	fetch := func(minInclusive, maxExclusive int64) ([]tasks.Task, int64, error) {
		fetchMu.Lock()
		fetches++
		fetchMu.Unlock()
		// Truncate to a small page size to exercise partial-coverage paths.
		var page []tasks.Task
		coveredTo := minInclusive
		for _, id := range ids {
			if id >= minInclusive && id < maxExclusive {
				page = append(page, bufferedTask(id))
				coveredTo = id + 1
				if len(page) == 16 {
					return page, coveredTo, nil
				}
			}
		}
		return page, maxExclusive, nil
	}
	b := newTestBuffer(64)

	var wg sync.WaitGroup
	for reader := range 8 {
		offset := int64(reader) * 3
		wg.Go(func() {
			// Each reader scans the whole id space in overlapping windows,
			// starting at a slightly different position like staggered lanes.
			next := 1 + offset
			for next != 0 && next < maxID {
				end := min(next+50, maxID+1)
				got := map[int64]struct{}{}
				cursor := next
				for cursor != 0 {
					page, n, err := b.readPage(cursor, end, fetch)
					if err != nil {
						t.Errorf("readPage(%d, %d) failed: %v", cursor, end, err)
						return
					}
					for _, task := range page {
						got[task.GetTaskID()] = struct{}{}
					}
					cursor = n
				}
				for id := next; id < end; id++ {
					if _, ok := got[id]; !ok {
						t.Errorf("reader missed task %d in [%d, %d)", id, next, end)
					}
				}
				if len(got) != int(end-next) {
					t.Errorf("got %d tasks in [%d, %d), want %d", len(got), next, end, end-next)
				}
				next = end
			}
		})
	}
	wg.Wait()
}

func TestReadBuffer_Metrics(t *testing.T) {
	store := &fakeStore{ids: []int64{1, 2, 50, 51}}
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	b := &readBuffer{
		capacityFn:     func() int { return 100 },
		metricsHandler: handler,
		serializer:     serialization.NewSerializer(),
	}

	counterTotal := func(name string) (total int64) {
		for _, rec := range capture.Snapshot()[name] {
			if v, ok := rec.Value.(int64); ok {
				total += v
			}
		}
		return total
	}

	_, _, err := b.readPage(50, 52, store.fetch) // first fill: a miss with no lag
	require.NoError(t, err)
	_, _, err = b.readPage(50, 52, store.fetch) // served from memory
	require.NoError(t, err)
	_, _, err = b.readPage(1, 3, store.fetch) // below coverage: miss with lag 49
	require.NoError(t, err)

	require.Equal(t, int64(1), counterTotal("replication_stream_read_buffer_hits"))
	require.Equal(t, int64(2), counterTotal("replication_stream_read_buffer_misses"))
	lags := capture.Snapshot()["replication_stream_read_buffer_miss_lag"]
	require.Len(t, lags, 1) // only the below-coverage miss records a lag
	require.Equal(t, int64(49), lags[0].Value)
}

func TestReadBuffer_TruncatedFetchLimitsCoverage(t *testing.T) {
	// A fetch that reports authority only through its last task (page-size limit).
	store := &fakeStore{ids: []int64{1, 2, 3, 4}}
	truncated := func(minInclusive, maxExclusive int64) ([]tasks.Task, int64, error) {
		page, _, err := store.fetch(minInclusive, min(maxExclusive, minInclusive+2))
		return page, min(maxExclusive, minInclusive+2), err
	}
	b := newTestBuffer(100)

	page, next, err := b.readPage(1, 5, truncated)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, taskIDs(page))
	require.Equal(t, int64(3), next) // continue from the authoritative bound

	page, next, err = b.readPage(next, 5, truncated)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4}, taskIDs(page))
	require.Zero(t, next)

	// Whole range now covered contiguously.
	page, next, err = b.readPage(1, 5, store.fetch)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4}, taskIDs(page))
	require.Zero(t, next)
	require.Equal(t, 2, store.fetches)
}
