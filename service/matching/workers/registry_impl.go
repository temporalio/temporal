package workers

import (
	"container/list"
	"encoding/json"
	"hash/maphash"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/fx"
)

// listWorkersPageToken is the cursor for paginating ListWorkers results.
type listWorkersPageToken struct {
	// LastWorkerInstanceKey is the WorkerInstanceKey of the last worker returned in the previous page.
	// The next page will return workers with keys > this value.
	LastWorkerInstanceKey string `json:"l"`
}

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
		buckets                   []*bucket                        // buckets for partitioning the keyspace
		maxItemsFn                dynamicconfig.IntPropertyFn      // dynamic config for maximum entries
		ttlFn                     dynamicconfig.DurationPropertyFn // dynamic config for entry TTL
		minEvictAgeFn             dynamicconfig.DurationPropertyFn // dynamic config for minimum evict age
		evictionIntervalFn        dynamicconfig.DurationPropertyFn // dynamic config for eviction interval
		total                     atomic.Int64                     // atomic counter of total entries
		quit                      chan struct{}                    // channel to signal shutdown of the eviction loop
		seed                      maphash.Seed                     // seed for the hasher, used to ensure consistent hashing
		metricsHandler            metrics.Handler                  // metrics handler for recording registry metrics
		enableWorkerPluginMetrics dynamicconfig.BoolPropertyFn     // dynamic config function to control plugin metrics export
	}

	// RegistryParams contains all parameters for creating a worker registry.
	RegistryParams struct {
		NumBuckets          dynamicconfig.IntPropertyFn
		TTL                 dynamicconfig.DurationPropertyFn
		MinEvictAge         dynamicconfig.DurationPropertyFn
		MaxItems            dynamicconfig.IntPropertyFn
		EvictionInterval    dynamicconfig.DurationPropertyFn
		MetricsHandler      metrics.Handler
		EnablePluginMetrics dynamicconfig.BoolPropertyFn
	}
)

func newBucket() *bucket {
	return &bucket{
		namespaces: make(map[namespace.ID]map[string]*entry),
		order:      list.New(),
	}
}

// upsertHeartbeats inserts or refreshes a WorkerHeartbeat under the given namespace.
// Returns the net change in entry count (positive for new entries, negative for removals).
// Workers with WORKER_STATUS_SHUTDOWN are immediately removed from the registry.
func (b *bucket) upsertHeartbeats(nsID namespace.ID, heartbeats []*workerpb.WorkerHeartbeat) int64 {
	now := time.Now()

	b.mu.Lock()
	defer b.mu.Unlock()
	var delta int64

	mp, ok := b.namespaces[nsID]
	if !ok {
		mp = make(map[string]*entry)
		b.namespaces[nsID] = mp
	}

	for _, hb := range heartbeats {
		key := hb.WorkerInstanceKey

		// If worker is shutting down, remove it immediately
		if hb.Status == enumspb.WORKER_STATUS_SHUTDOWN {
			if e, exists := mp[key]; exists {
				b.order.Remove(e.elem)
				delete(mp, key)
				delta--
			}
			continue
		}

		// Normal upsert
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
			delta++
		}
	}

	return delta
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
func NewRegistry(lc fx.Lifecycle, params RegistryParams) Registry {
	m := newRegistryImpl(params)
	lc.Append(fx.StartStopHook(m.Start, m.Stop))
	return m
}

func newRegistryImpl(params RegistryParams) *registryImpl {
	m := &registryImpl{
		buckets:                   make([]*bucket, params.NumBuckets()),
		maxItemsFn:                params.MaxItems,
		ttlFn:                     params.TTL,
		minEvictAgeFn:             params.MinEvictAge,
		evictionIntervalFn:        params.EvictionInterval,
		seed:                      maphash.MakeSeed(),
		quit:                      make(chan struct{}),
		metricsHandler:            params.MetricsHandler,
		enableWorkerPluginMetrics: params.EnablePluginMetrics,
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
	delta := b.upsertHeartbeats(nsID, heartbeats)
	m.total.Add(delta)
	m.recordUtilizationMetric()
}

// recordUtilizationMetric records the overall capacity utilization ratio.
func (m *registryImpl) recordUtilizationMetric() {
	maxItems := int64(m.maxItemsFn())
	utilization := float64(m.total.Load()) / float64(maxItems)
	metrics.WorkerRegistryCapacityUtilizationMetric.With(m.metricsHandler).Record(utilization)
}

// recordEvictionMetric sets the eviction metric based on current capacity state.
// Assumes EvictByCapacity has already been called.
func (m *registryImpl) recordEvictionMetric() {
	maxItems := int64(m.maxItemsFn())
	if m.total.Load() > maxItems {
		// Still over capacity - eviction failed
		metrics.WorkerRegistryEvictionBlockedByAgeMetric.With(m.metricsHandler).Record(1)
	} else {
		// Back under capacity - clear the issue
		metrics.WorkerRegistryEvictionBlockedByAgeMetric.With(m.metricsHandler).Record(0)
	}
}

// recordPluginMetric sets a value of 1 for each unique plugin name present in the heartbeats.
func (m *registryImpl) recordPluginMetric(nsName namespace.Name, heartbeats []*workerpb.WorkerHeartbeat) {
	// Check if plugin metrics are enabled via dynamic config
	if !m.enableWorkerPluginMetrics() {
		return
	}

	// Track which plugins we've already recorded
	recordedPlugins := make(map[string]bool)

	for _, hb := range heartbeats {
		for _, pluginInfo := range hb.Plugins {
			pluginName := pluginInfo.Name
			if !recordedPlugins[pluginName] {
				metrics.WorkerPluginNameMetric.
					With(m.metricsHandler).
					Record(
						1,
						metrics.NamespaceIDTag(nsName.String()),
						metrics.WorkerPluginNameTag(pluginName),
					)
				recordedPlugins[pluginName] = true
			}
		}
	}
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
	for {
		select {
		case <-time.After(m.evictionIntervalFn()):
			m.evictByTTL()
			m.evictByCapacity()
			m.recordUtilizationMetric()
		case <-m.quit:
			return
		}
	}
}

// evictByTTL removes expired entries across all buckets.
func (m *registryImpl) evictByTTL() {
	ttl := m.ttlFn()
	expireBefore := time.Now().Add(-ttl)
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
	defer m.recordEvictionMetric()

	maxItems := int64(m.maxItemsFn())
	minEvictAge := m.minEvictAgeFn()

	// Keep evicting until we are under capacity. In each iteration, we remove one entry from each
	// bucket for fairness.
	for m.total.Load() > maxItems {
		removedAny := false
		threshold := time.Now().Add(-minEvictAge)

		for _, b := range m.buckets {
			if m.total.Load() <= maxItems {
				return
			}
			if b.evictByCapacity(threshold) {
				removedAny = true
				m.total.Add(-1)
			}
		}

		// To avoid infinite loops, we break if we didn't remove any entries in this iteration.
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

func (m *registryImpl) RecordWorkerHeartbeats(nsID namespace.ID, nsName namespace.Name, workerHeartbeat []*workerpb.WorkerHeartbeat) {
	m.upsertHeartbeats(nsID, workerHeartbeat)
	m.recordPluginMetric(nsName, workerHeartbeat)
}

func (m *registryImpl) ListWorkers(nsID namespace.ID, params ListWorkersParams) (ListWorkersResponse, error) {
	// Build the predicate for filtering
	var predicate func(*workerpb.WorkerHeartbeat) bool
	if params.Query == "" {
		predicate = func(_ *workerpb.WorkerHeartbeat) bool { return true }
	} else {
		queryEngine, err := newWorkerQueryEngine(nsID.String(), params.Query)
		if err != nil {
			return ListWorkersResponse{}, err
		}
		predicate = func(heartbeat *workerpb.WorkerHeartbeat) bool {
			result, err := queryEngine.EvaluateWorker(heartbeat)
			return err == nil && result
		}
	}

	// Get all matching workers and paginate
	workers := m.filterWorkers(nsID, predicate)
	return paginateWorkers(workers, params.PageSize, params.NextPageToken)
}

// paginateWorkers applies cursor-based pagination to a list of workers.
// Workers are sorted by WorkerInstanceKey for deterministic ordering.
// Returns the paginated slice and a token for the next page (nil if no more pages).
func paginateWorkers(workers []*workerpb.WorkerHeartbeat, pageSize int, nextPageToken []byte) (ListWorkersResponse, error) {
	if len(workers) == 0 {
		return ListWorkersResponse{Workers: workers}, nil
	}

	// If pagination is not requested, return all workers without sorting.
	if pageSize == 0 && len(nextPageToken) == 0 {
		return ListWorkersResponse{Workers: workers}, nil
	}

	// Sort by WorkerInstanceKey for deterministic pagination
	slices.SortFunc(workers, func(a, b *workerpb.WorkerHeartbeat) int {
		return strings.Compare(a.WorkerInstanceKey, b.WorkerInstanceKey)
	})

	// Decode page token to find the cursor
	var cursor string
	if len(nextPageToken) > 0 {
		var token listWorkersPageToken
		if err := json.Unmarshal(nextPageToken, &token); err != nil {
			return ListWorkersResponse{}, serviceerror.NewInvalidArgument("invalid next_page_token")
		}
		cursor = token.LastWorkerInstanceKey
	}

	// Find the starting index using binary search (O(log n))
	startIdx := 0
	if cursor != "" {
		// BinarySearchFunc returns the index where cursor would be inserted.
		// We want the first worker with key > cursor.
		startIdx, _ = slices.BinarySearchFunc(workers, cursor, func(worker *workerpb.WorkerHeartbeat, target string) int {
			return strings.Compare(worker.WorkerInstanceKey, target)
		})
		// If exact match found, move past it to get first key > cursor
		if startIdx < len(workers) && workers[startIdx].WorkerInstanceKey == cursor {
			startIdx++
		}
		// If we've gone past the end, return empty
		if startIdx >= len(workers) {
			return ListWorkersResponse{}, nil
		}
	}

	// Apply page size (0 means no limit)
	endIdx := len(workers)
	if pageSize > 0 {
		endIdx = min(startIdx+pageSize, len(workers))
	}

	result := workers[startIdx:endIdx]

	// Generate next page token if there are more results
	var newNextPageToken []byte
	if endIdx < len(workers) {
		token := listWorkersPageToken{
			LastWorkerInstanceKey: result[len(result)-1].WorkerInstanceKey,
		}
		newNextPageToken, _ = json.Marshal(token)
	}

	return ListWorkersResponse{
		Workers:       result,
		NextPageToken: newNextPageToken,
	}, nil
}

func (m *registryImpl) DescribeWorker(nsID namespace.ID, workerInstanceKey string) (*workerpb.WorkerHeartbeat, error) {
	b := m.getBucket(nsID)
	if b == nil {
		return nil, serviceerror.NewNotFoundf("namespace not found: %s", nsID.String())
	}
	return b.getWorkerHeartbeat(nsID, workerInstanceKey)
}
