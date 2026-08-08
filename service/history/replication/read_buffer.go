package replication

import (
	"sort"
	"sync"
	"sync/atomic"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
)

// readBuffer is a shard-scoped read-through buffer over the tip of the replication
// task queue. Every replication stream lane — and every remote cluster's stream —
// scans the same queue with its own iterator, so without sharing, each new task page
// is read from persistence once per scanner. The buffer makes overlapping tip scans
// share one persistence read: readers at or inside the buffered range are served
// from memory, and only readers below it (deep catch-up, lanes that have lagged out
// of coverage) fall through to persistence.
//
// The buffer holds SERIALIZED queue rows and deserializes per serve, so every reader
// gets fresh task structs it exclusively owns — exactly what a persistence read
// would have given it. Never hand out shared task objects: downstream converters
// mutate tasks in place (e.g. task-equivalent ID assignment via AddTasks), which is
// safe on owned structs and a cross-stream data race on shared ones. Rows are slim
// for every priority (task metadata; event payloads only enter the pipeline at
// send-time conversion), so the memory cost is a few hundred bytes per row.
//
// Coverage is a contiguous task-id interval [minCovered, maxCovered) established by
// persistence pages: within it, the buffer is authoritative — absence of a task
// means the range holds none. Rows are immutable and the queue is append-only, so
// there is no invalidation; eviction from the front just shrinks coverage. Reads are
// only ever requested below the shard's exclusive-high read watermark, so covered
// ranges are stable once established.
type readBuffer struct {
	capacityFn     func() int               // max buffered tasks; <= 0 disables the buffer
	metricsHandler metrics.Handler          // optional; nil means no metrics
	logger         log.Logger               // optional; nil means no logging
	serializer     serialization.Serializer // required when capacityFn can return > 0

	// hasRows mirrors len(buffered) > 0; see passThrough.
	hasRows atomic.Bool

	mu         sync.Mutex
	buffered   []bufferedRow // sorted by task id, contiguous coverage
	minCovered int64         // inclusive
	maxCovered int64         // exclusive; == minCovered means empty
}

type bufferedRow struct {
	id   int64
	blob *commonpb.DataBlob
}

func newReadBuffer(
	capacityFn func() int,
	metricsHandler metrics.Handler,
	logger log.Logger,
) *readBuffer {
	return &readBuffer{
		capacityFn:     capacityFn,
		metricsHandler: metricsHandler,
		logger:         logger,
		serializer:     serialization.NewSerializer(),
	}
}

func (b *readBuffer) handler() metrics.Handler {
	if b.metricsHandler == nil {
		return metrics.NoopMetricsHandler
	}
	return b.metricsHandler
}

func (b *readBuffer) log() log.Logger {
	if b.logger == nil {
		return log.NewNoopLogger()
	}
	return b.logger
}

// readPage reads one page of tasks in [minInclusive, maxExclusive), fetching from
// persistence via fetch when the range is not covered. It returns the tasks and the
// next inclusive task id to continue from (or 0 when the requested range is done).
//
// fetch(min, max) must return tasks in [min, max) in id order along with the
// exclusive id bound the read is authoritative for (max when the read was not
// truncated by a page-size limit, lastTask+1 otherwise).
func (b *readBuffer) readPage(
	minInclusive int64,
	maxExclusive int64,
	fetch func(minInclusive int64, maxExclusive int64) ([]tasks.Task, int64, error),
) ([]tasks.Task, int64, error) {
	if minInclusive >= maxExclusive {
		return nil, 0, nil
	}
	capacity := 0
	if b.capacityFn != nil {
		capacity = b.capacityFn()
	}
	if capacity <= 0 {
		return b.passThrough(minInclusive, maxExclusive, fetch)
	}

	b.mu.Lock()
	if minInclusive >= b.minCovered && minInclusive < b.maxCovered {
		// Serve from memory up to the covered bound. Blob pointers are collected
		// under the lock (blobs are immutable once buffered); deserialization — the
		// per-reader copy — happens outside it.
		servedTo := min(maxExclusive, b.maxCovered)
		start := sort.Search(len(b.buffered), func(i int) bool {
			return b.buffered[i].id >= minInclusive
		})
		var blobs []*commonpb.DataBlob
		for _, row := range b.buffered[start:] {
			if row.id >= servedTo {
				break
			}
			blobs = append(blobs, row.blob)
		}
		b.mu.Unlock()
		page, err := b.deserialize(blobs)
		if err == nil {
			metrics.ReplicationStreamReadBufferHits.With(b.handler()).Record(1)
			return page, nextPageStart(servedTo, maxExclusive), nil
		}
		// A row that cannot deserialize means the buffer's contents cannot be
		// trusted: drop coverage entirely and fall through to persistence. This
		// should never happen — the row round-tripped this serializer on the way in
		// — so it indicates a bug (or corrupted memory) and must be visible.
		b.log().Error("replication read buffer failed to deserialize a buffered row; dropping buffer coverage", tag.Error(err))
		b.clear()
		b.mu.Lock()
	}
	// missLag: how far below coverage this read starts, or -1 when it is not below
	// coverage (tip extension / restart misses have no meaningful lag).
	missLag := int64(-1)
	if b.maxCovered > b.minCovered && minInclusive < b.minCovered {
		missLag = b.minCovered - minInclusive
	}
	b.mu.Unlock()

	// Not covered: fetch from persistence (outside the lock), then cache the result
	// when it extends coverage contiguously or restarts it at a newer tip.
	page, coveredTo, err := fetch(minInclusive, maxExclusive)
	if err != nil {
		return nil, 0, err
	}
	metrics.ReplicationStreamReadBufferMisses.With(b.handler()).Record(1)
	if missLag >= 0 {
		metrics.ReplicationStreamReadBufferMissLag.With(b.handler()).Record(missLag)
	}
	rows, serializeErr := b.serialize(page)
	if serializeErr != nil {
		// Cannot cache what we cannot round-trip; serve the fetched page uncached.
		// A row persistence returned but the serializer cannot encode indicates a
		// bug (e.g. a new task type missing serializer support) or broken
		// persistence data — it silently degrades the buffer, so make it visible.
		b.log().Error("replication read buffer failed to serialize a fetched task; serving page uncached", tag.Error(serializeErr))
		return page, nextPageStart(coveredTo, maxExclusive), nil
	}
	b.mu.Lock()
	switch {
	case minInclusive == b.maxCovered || b.minCovered == b.maxCovered:
		// Contiguous extension, or first fill.
		if b.minCovered == b.maxCovered {
			b.buffered = b.buffered[:0]
			b.minCovered = minInclusive
		}
		b.buffered = append(b.buffered, rows...)
		b.maxCovered = coveredTo
	case minInclusive > b.maxCovered:
		// The tip moved past coverage (e.g. first reader after a jump): restart
		// coverage at the newer range; laggards below it read persistence anyway.
		b.buffered = append(b.buffered[:0], rows...)
		b.minCovered = minInclusive
		b.maxCovered = coveredTo
	default:
		// Below coverage: serve without caching (no re-buffering of old ranges).
	}
	if len(b.buffered) > 0 {
		b.hasRows.Store(true)
	}
	if len(b.buffered) > capacity {
		evicted := b.buffered[:len(b.buffered)-capacity]
		b.minCovered = evicted[len(evicted)-1].id + 1
		b.buffered = append([]bufferedRow(nil), b.buffered[len(evicted):]...)
	}
	b.mu.Unlock()
	return page, nextPageStart(coveredTo, maxExclusive), nil
}

func (b *readBuffer) serialize(page []tasks.Task) ([]bufferedRow, error) {
	rows := make([]bufferedRow, 0, len(page))
	for _, task := range page {
		blob, err := b.serializer.SerializeTask(task)
		if err != nil {
			return nil, err
		}
		rows = append(rows, bufferedRow{id: task.GetTaskID(), blob: blob})
	}
	return rows, nil
}

func (b *readBuffer) deserialize(blobs []*commonpb.DataBlob) ([]tasks.Task, error) {
	if len(blobs) == 0 {
		return nil, nil
	}
	page := make([]tasks.Task, 0, len(blobs))
	for _, blob := range blobs {
		task, err := b.serializer.DeserializeTask(tasks.CategoryReplication, blob)
		if err != nil {
			return nil, err
		}
		page = append(page, task)
	}
	return page, nil
}

// passThrough serves a read straight from persistence with the buffer disabled,
// first releasing any rows buffered while it was enabled (capacity is dynamic).
// The hasRows gate keeps the disabled path off the shard-wide lock per read.
func (b *readBuffer) passThrough(
	minInclusive int64,
	maxExclusive int64,
	fetch func(minInclusive int64, maxExclusive int64) ([]tasks.Task, int64, error),
) ([]tasks.Task, int64, error) {
	if b.hasRows.Load() {
		b.clear()
	}
	page, coveredTo, err := fetch(minInclusive, maxExclusive)
	return page, nextPageStart(coveredTo, maxExclusive), err
}

// clear drops all buffered rows and coverage (buffer disabled at runtime, or
// contents no longer trustworthy).
func (b *readBuffer) clear() {
	b.mu.Lock()
	b.buffered = nil
	b.minCovered = 0
	b.maxCovered = 0
	b.hasRows.Store(false)
	b.mu.Unlock()
}

// nextPageStart returns the next inclusive task id to continue a ranged read from,
// or 0 when the read reached maxExclusive.
func nextPageStart(coveredTo, maxExclusive int64) int64 {
	if coveredTo >= maxExclusive {
		return 0
	}
	return coveredTo
}
