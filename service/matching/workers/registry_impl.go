package workers

import (
	"container/list"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/fx"
)

const (
	defaultBuckets          = 10
	defaultEntryTTL         = 24 * time.Hour
	defaultMinEvictAge      = 10 * time.Minute
	defaultMaxEntries       = 1_000_000
	defaultEvictionInterval = 1 * time.Hour
)

type (
	// entry wraps a WorkerHeartbeat along with its namespace and eviction metadata.
	entry struct {
		nsID     namespace.ID
		hb       *workerpb.WorkerHeartbeat
		lastSeen time.Time
		elem     *list.Element
	}
	// bucket holds part of the keyspace: a map from namespace → (map of instanceKey → entry),
	// plus a recency list for eviction.
	bucket struct {
		mu         sync.Mutex
		namespaces map[namespace.ID]map[string]*entry
		order      *list.List // front = oldest, back = newest
	}

	// registryImpl implements Registry interface. It contains all worker heartbeats.
	// It partitions the keyspace into buckets and enforces TTL and capacity.
	// Eviction runs in the background.
	registryImpl struct {
		buckets          []*bucket     // buckets for partitioning the keyspace
		maxItems         int64         // maximum number of entries allowed across all buckets
		ttl              time.Duration // time after which entries are considered expired
		minEvictAge      time.Duration // minimum age of entries to consider for eviction
		evictionInterval time.Duration // interval for periodic eviction checks
		total            atomic.Int64  // atomic counter of total entries
		quit             chan struct{} // channel to signal shutdown of the eviction loop
		seed             maphash.Seed  // seed for the hasher, used to ensure consistent hashing
	}
)

func newBucket() *bucket {
	return &bucket{
		namespaces: make(map[namespace.ID]map[string]*entry),
		order:      list.New(),
	}
}

// upsertHeartbeats inserts or refreshes a WorkerHeartbeat under the given namespace.
// Returns the number of new entries.
func (b *bucket) upsertHeartbeats(nsID namespace.ID, heartbeats []*workerpb.WorkerHeartbeat) int64 {
	now := time.Now()

	b.mu.Lock()
	defer b.mu.Unlock()
	var newEntries int64

	mp, ok := b.namespaces[nsID]
	if !ok {
		mp = make(map[string]*entry)
		b.namespaces[nsID] = mp
	}

	for _, hb := range heartbeats {
		key := hb.WorkerInstanceKey
		if e, exists := mp[key]; exists {
			e.hb = hb
			e.lastSeen = now
			b.order.MoveToBack(e.elem)
		} else {
			e = &entry{
				nsID:     nsID,
				hb:       hb,
				lastSeen: now,
			}
			e.elem = b.order.PushBack(e)
			mp[key] = e
			newEntries += 1
		}
	}

	return newEntries
}

// filterWorkers returns all WorkerHeartbeats in a namespace
// for which predicate(hb) returns true.
func (b *bucket) filterWorkers(
	nsID namespace.ID,
	predicate func(*workerpb.WorkerHeartbeat) bool,
) []*workerpb.WorkerHeartbeat {
	b.mu.Lock()
	defer b.mu.Unlock()

	mp := b.namespaces[nsID]
	if mp == nil {
		return nil
	}
	out := make([]*workerpb.WorkerHeartbeat, 0, len(mp))
	for _, e := range mp {
		if predicate(e.hb) {
			out = append(out, e.hb)
		}
	}
	return out
}

func (b *bucket) getWorkerHeartbeat(nsID namespace.ID, workerInstanceKey string) (*workerpb.WorkerHeartbeat, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	mp, ok := b.namespaces[nsID]
	if !ok {
		return nil, serviceerror.NewNamespaceNotFound(nsID.String())
	}

	e, exists := mp[workerInstanceKey]
	if !exists {
		return nil, serviceerror.NewNotFoundf("Worker %s not found", workerInstanceKey)
	}

	return e.hb, nil
}

// evictByTTL removes entries older than expireBefore from this bucket.
// Returns the number of entries removed.
func (b *bucket) evictByTTL(expireBefore time.Time) int {
	removed := 0
	b.mu.Lock()
	defer b.mu.Unlock()

	for {
		front := b.order.Front()
		if front == nil {
			break
		}
		e := front.Value.(*entry) //nolint:revive
		if !e.lastSeen.Before(expireBefore) {
			break
		}
		b.order.Remove(front)
		delete(b.namespaces[e.nsID], e.hb.WorkerInstanceKey)
		removed++
	}
	return removed
}

func (b *bucket) evictByCapacity(threshold time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	front := b.order.Front()
	if front == nil {
		return false
	}

	e := front.Value.(*entry) //nolint:revive
	if !e.lastSeen.Before(threshold) {
		return false
	}
	b.order.Remove(front)
	delete(b.namespaces[e.nsID], e.hb.WorkerInstanceKey)
	return true
}

// NewRegistry creates a workers heartbeat registry with the given parameters.
func NewRegistry(lc fx.Lifecycle) Registry {
	m := newRegistryImpl(
		defaultBuckets,
		defaultEntryTTL,
		defaultMinEvictAge,
		defaultMaxEntries,
		defaultEvictionInterval,
	)

	lc.Append(fx.StartStopHook(m.Start, m.Stop))

	return m
}

func newRegistryImpl(numBuckets int,
	ttl time.Duration,
	minEvictAge time.Duration,
	maxItems int64,
	evictionInterval time.Duration,
) *registryImpl {
	m := &registryImpl{
		buckets:          make([]*bucket, numBuckets),
		maxItems:         maxItems,
		ttl:              ttl,
		minEvictAge:      minEvictAge,
		evictionInterval: evictionInterval,
		seed:             maphash.MakeSeed(),
		quit:             make(chan struct{}),
	}

	for i := range m.buckets {
		m.buckets[i] = newBucket()
	}
	return m
}

// bucketFor hashes the namespace to select a bucket.
func (m *registryImpl) getBucket(nsID namespace.ID) *bucket {
	var h maphash.Hash
	h.SetSeed(m.seed)
	h.WriteString(nsID.String()) //nolint:revive
	hs := h.Sum64()
	idx := int(hs % uint64(len(m.buckets)))

	return m.buckets[idx]
}

// upsertHeartbeat records or refreshes a WorkerHeartbeat under the given namespace.
// New entries increment the global counter.
func (m *registryImpl) upsertHeartbeats(nsID namespace.ID, heartbeats []*workerpb.WorkerHeartbeat) {
	b := m.getBucket(nsID)
	newEntries := b.upsertHeartbeats(nsID, heartbeats)
	m.total.Add(newEntries)
}

// filterWorkers returns all WorkerHeartbeats in a namespace
// for which predicate(hb) returns true.
func (m *registryImpl) filterWorkers(
	nsID namespace.ID,
	predicate func(*workerpb.WorkerHeartbeat) bool,
) []*workerpb.WorkerHeartbeat {
	b := m.getBucket(nsID)

	if b == nil {
		return nil
	}
	return b.filterWorkers(nsID, predicate)
}

// evictLoop periodically triggers TTL and capacity-based eviction.
func (m *registryImpl) evictLoop() {
	ticker := time.NewTicker(m.evictionInterval)
	for {
		select {
		case <-ticker.C:
			m.evictByTTL()
			m.evictByCapacity()
		case <-m.quit:
			ticker.Stop()
			return
		}
	}
}

// evictByTTL removes expired entries across all buckets.
func (m *registryImpl) evictByTTL() {
	expireBefore := time.Now().Add(-m.ttl)
	var removed int64
	for _, b := range m.buckets {
		removed += int64(b.evictByTTL(expireBefore))
	}
	if removed > 0 {
		m.total.Add(-removed)
	}
}

// evictByCapacity removes entries older than MinEvictAge until under capacity.
func (m *registryImpl) evictByCapacity() {
	for m.total.Load() > m.maxItems {
		removedAny := false
		threshold := time.Now().Add(-m.minEvictAge)
		for _, b := range m.buckets {
			if m.total.Load() <= m.maxItems {
				return
			}
			if b.evictByCapacity(threshold) {
				removedAny = true
				m.total.Add(-1)
			}
		}
		if !removedAny {
			break
		}
	}
}

// Start begins the background eviction process.
func (m *registryImpl) Start() {
	go m.evictLoop()
}

// Stop halts background eviction.
func (m *registryImpl) Stop() {
	close(m.quit)
}

func (m *registryImpl) RecordWorkerHeartbeats(nsID namespace.ID, workerHeartbeat []*workerpb.WorkerHeartbeat) {
	m.upsertHeartbeats(nsID, workerHeartbeat)
}

func (m *registryImpl) ListWorkers(nsID namespace.ID, query string, _ []byte) ([]*workerpb.WorkerHeartbeat, error) {
	if query == "" {
		return m.filterWorkers(nsID, func(_ *workerpb.WorkerHeartbeat) bool { return true }), nil
	}

	queryEngine, err := newWorkerQueryEngine(nsID.String(), query)
	if err != nil {
		return nil, err
	}

	predicate := func(heartbeat *workerpb.WorkerHeartbeat) bool {
		result, err := queryEngine.EvaluateWorker(heartbeat)
		return err == nil && result
	}
	return m.filterWorkers(nsID, predicate), nil
}

func (m *registryImpl) DescribeWorker(nsID namespace.ID, workerInstanceKey string) (*workerpb.WorkerHeartbeat, error) {
	b := m.getBucket(nsID)
	if b == nil {
		return nil, serviceerror.NewNotFoundf("namespace not found: %s", nsID.String())
	}
	return b.getWorkerHeartbeat(nsID, workerInstanceKey)
}
