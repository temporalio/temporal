package heartbeats

import (
	"container/list"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	workerpb "go.temporal.io/api/worker/v1"
)

// entry wraps a WorkerHeartbeat along with its namespace and eviction metadata.
type entry struct {
	namespace string
	hb        *workerpb.WorkerHeartbeat
	lastSeen  time.Time
	elem      *list.Element
}

// bucket holds part of the keyspace: a map from namespace → (map of instanceKey → entry),
// plus a recency list for eviction.
type bucket struct {
	mu         sync.Mutex
	namespaces map[string]map[string]*entry
	order      *list.List // front = oldest, back = newest
}

func newBucket() *bucket {
	return &bucket{
		namespaces: make(map[string]map[string]*entry),
		order:      list.New(),
	}
}

// update inserts or refreshes a WorkerHeartbeat under the given namespace.
// Returns true if a brand-new entry was created.
func (b *bucket) update(namespace string, hb *workerpb.WorkerHeartbeat) bool {
	now := time.Now()
	key := hb.WorkerInstanceKey

	b.mu.Lock()
	defer b.mu.Unlock()

	mp, ok := b.namespaces[namespace]
	if !ok {
		mp = make(map[string]*entry)
		b.namespaces[namespace] = mp
	}

	if e, exists := mp[key]; exists {
		e.hb = hb
		e.lastSeen = now
		b.order.MoveToBack(e.elem)
		return false
	}

	e := &entry{
		namespace: namespace,
		hb:        hb,
		lastSeen:  now,
	}
	e.elem = b.order.PushBack(e)
	mp[key] = e
	return true
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
		delete(b.namespaces[e.namespace], e.hb.WorkerInstanceKey)
		removed++
	}
	return removed
}

// Registry contains all worker heartbeats.
// It partitions the keyspace into buckets and enforces TTL and capacity.
// Eviction runs in the background.
type Registry struct {
	buckets     []*bucket
	maxItems    int64
	TTL         time.Duration
	MinEvictAge time.Duration
	ticker      *time.Ticker
	hashFn      func(string) uint32
	total       atomic.Int64 // atomic counter of total entries
	quit        chan struct{}
}

// NewRegistry creates a workers heartbeat registry with the given parameters.
func NewRegistry(
	numBuckets int,
	ttl time.Duration,
	minEvictAge time.Duration,
	maxItems int64,
	evictionInterval time.Duration,
) *Registry {
	m := &Registry{
		buckets:     make([]*bucket, numBuckets),
		maxItems:    maxItems,
		TTL:         ttl,
		MinEvictAge: minEvictAge,
		ticker:      time.NewTicker(evictionInterval),
		hashFn: func(s string) uint32 {
			h := fnv.New32a()
			h.Write([]byte(s)) //nolint:revive
			return h.Sum32()
		},
		quit: make(chan struct{}),
	}
	for i := range m.buckets {
		m.buckets[i] = newBucket()
	}
	go m.evictLoop()
	return m
}

// bucketFor hashes the namespace to select a bucket.
func (m *Registry) bucketFor(namespace string) *bucket {
	idx := int(m.hashFn(namespace)) % len(m.buckets)
	return m.buckets[idx]
}

// Update records or refreshes a WorkerHeartbeat under the given namespace.
// New entries increment the global counter.
func (m *Registry) Update(namespace string, hb *workerpb.WorkerHeartbeat) {
	b := m.bucketFor(namespace)
	if newEntry := b.update(namespace, hb); newEntry {
		m.total.Add(1)
	}
}

// ListNamespace returns all WorkerHeartbeats in a namespace
// for which predicate(hb) returns true.
func (m *Registry) ListNamespace(
	namespace string,
	predicate func(*workerpb.WorkerHeartbeat) bool,
) []*workerpb.WorkerHeartbeat {
	b := m.bucketFor(namespace)
	b.mu.Lock()
	defer b.mu.Unlock()

	mp := b.namespaces[namespace]
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

// evictLoop periodically triggers TTL and capacity-based eviction.
func (m *Registry) evictLoop() {
	for {
		select {
		case <-m.ticker.C:
			m.evictByTTL()
			m.evictByCapacity()
		case <-m.quit:
			m.ticker.Stop()
			return
		}
	}
}

// evictByTTL removes expired entries across all buckets.
func (m *Registry) evictByTTL() {
	expireBefore := time.Now().Add(-m.TTL)
	var removed int64
	for _, b := range m.buckets {
		removed += int64(b.evictByTTL(expireBefore))
	}
	if removed > 0 {
		m.total.Add(-removed)
	}
}

// evictByCapacity removes entries older than MinEvictAge until under capacity.
func (m *Registry) evictByCapacity() {
	for m.total.Load() > m.maxItems {
		removedAny := false
		threshold := time.Now().Add(-m.MinEvictAge)
		for _, b := range m.buckets {
			if m.total.Load() <= m.maxItems {
				return
			}
			b.mu.Lock()
			front := b.order.Front()
			if front != nil {
				e := front.Value.(*entry) //nolint:revive
				if e.lastSeen.Before(threshold) {
					b.order.Remove(front)
					delete(b.namespaces[e.namespace], e.hb.WorkerInstanceKey)
					removedAny = true
					m.total.Add(-1)
				}
			}
			b.mu.Unlock()
		}
		if !removedAny {
			break
		}
	}
}

// Stop halts background eviction.
func (m *Registry) Stop() {
	close(m.quit)
}
