package workers

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/namespace"
)

// alwaysTrue predicate for convenience
func alwaysTrue(_ *workerpb.WorkerHeartbeat) bool { return true }

func TestUpdateAndListNamespace(t *testing.T) {
	m := newRegistryImpl(
		2,         // numBuckets
		time.Hour, // TTL
		0,         // MinEvictAge
		10,        // maxItems
		time.Hour, // evictionInterval
	)
	defer m.Stop()

	// No entries initially
	list := m.filterWorkers("ns1", alwaysTrue)
	assert.Len(t, list, 0, "expected empty list before updates")

	// Add some heartbeats
	hb1 := &workerpb.WorkerHeartbeat{WorkerInstanceKey: "workerA", Status: enumspb.WORKER_STATUS_RUNNING}
	hb2 := &workerpb.WorkerHeartbeat{WorkerInstanceKey: "workerB", Status: enumspb.WORKER_STATUS_SHUTDOWN}
	m.upsertHeartbeats("ns1", []*workerpb.WorkerHeartbeat{hb1, hb2})

	list = m.filterWorkers("ns1", alwaysTrue)
	// Order is not guaranteed; check contents by keys
	keys := []string{list[0].WorkerInstanceKey, list[1].WorkerInstanceKey}
	assert.Contains(t, keys, "workerA")
	assert.Contains(t, keys, "workerB")
}

func TestListNamespacePredicate(t *testing.T) {
	m := newRegistryImpl(1, time.Hour, 0, 10, time.Hour)
	defer m.Stop()

	// Set up multiple entries
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("key%d", i)
		hb := &workerpb.WorkerHeartbeat{WorkerInstanceKey: key, CurrentStickyCacheSize: int32(i)}
		m.upsertHeartbeats("ns", []*workerpb.WorkerHeartbeat{hb})
	}

	// Table-driven tests for predicates
	tests := []struct {
		name      string
		pred      func(*workerpb.WorkerHeartbeat) bool
		wantCount int
	}{
		{"metric>3", func(h *workerpb.WorkerHeartbeat) bool { return h.CurrentStickyCacheSize > 3 }, 2},
		{"metric even", func(h *workerpb.WorkerHeartbeat) bool { return h.CurrentStickyCacheSize%2 == 0 }, 2},
		{"All", alwaysTrue, 5},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			list := m.filterWorkers("ns", tc.pred)
			assert.Len(t, list, tc.wantCount)
		})
	}
}

func TestEvictByTTL(t *testing.T) {
	m := newRegistryImpl(1, 1*time.Second, 0, 10, time.Hour)
	defer m.Stop()

	hb := &workerpb.WorkerHeartbeat{WorkerInstanceKey: "oldWorker"}
	m.upsertHeartbeats("ns", []*workerpb.WorkerHeartbeat{hb})

	// Manually move beyond TTL
	b := m.getBucket("ns")
	e := b.namespaces["ns"][hb.WorkerInstanceKey]
	e.lastSeen = time.Now().Add(-2 * time.Second)

	// Perform eviction
	m.evictByTTL()

	list := m.filterWorkers("ns", alwaysTrue)
	assert.Len(t, list, 0, "entry should be evicted by TTL")
	assert.Equal(t, int64(0), m.total.Load(), "total counter should be decremented")
}

func TestEvictByCapacity(t *testing.T) {
	maxItems := int64(3)
	m := newRegistryImpl(1, time.Hour, 0, maxItems, time.Hour)
	defer m.Stop()

	// Insert more entries than maxItems
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("cap%d", i)
		hb := &workerpb.WorkerHeartbeat{WorkerInstanceKey: key}
		m.upsertHeartbeats("ns", []*workerpb.WorkerHeartbeat{hb})
	}

	// All entries have lastSeen.Before(now) when MinEvictAge=0, so eligible
	m.evictByCapacity()

	// Ensure we evicted down to maxItems
	remaining := m.filterWorkers("ns", alwaysTrue)
	assert.Len(t, remaining, int(maxItems), "should evict down to maxItems")
	assert.LessOrEqual(t, m.total.Load(), maxItems, "total counter should not exceed maxItems")
}

func BenchmarkUpdate(b *testing.B) {
	m := newRegistryImpl(16, time.Hour, time.Minute, int64(b.N), time.Hour)
	defer m.Stop()
	hb := &workerpb.WorkerHeartbeat{WorkerInstanceKey: "benchWorker"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.upsertHeartbeats("benchNs", []*workerpb.WorkerHeartbeat{hb})
	}
}

func BenchmarkListNamespace(b *testing.B) {
	m := newRegistryImpl(16, time.Hour, time.Minute, 1000, time.Hour)
	defer m.Stop()
	// Pre-populate with entries
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("worker%d", i)
		hb := &workerpb.WorkerHeartbeat{WorkerInstanceKey: key}
		m.upsertHeartbeats("benchNs", []*workerpb.WorkerHeartbeat{hb})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.filterWorkers("benchNs", alwaysTrue)
	}
}

// BenchmarkRandomUpdate simulates random updates across multiple namespaces.
func BenchmarkRandomUpdate(b *testing.B) {
	namespaces := []namespace.ID{"ns1", "ns2", "ns3"}
	totalHeartbeats := 30 // Total heartbeats per namespace
	m := newRegistryImpl(len(namespaces), time.Hour, time.Minute, int64(b.N), time.Hour)
	defer m.Stop()

	// Pre-populate heartbeats per namespace
	type pair struct {
		ns namespace.ID
		hb *workerpb.WorkerHeartbeat
	}
	var pairs []pair
	for _, ns := range namespaces {
		for i := 0; i < totalHeartbeats; i++ {
			key := fmt.Sprintf("%s-worker%d", ns, i)
			hb := &workerpb.WorkerHeartbeat{WorkerInstanceKey: key, CurrentStickyCacheSize: int32(i)}
			m.upsertHeartbeats(ns, []*workerpb.WorkerHeartbeat{hb})
			pairs = append(pairs, pair{ns: ns, hb: hb})
		}
	}

	r := rand.New(rand.NewSource(42))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := pairs[r.Intn(len(pairs))]
		m.upsertHeartbeats(p.ns, []*workerpb.WorkerHeartbeat{p.hb})
	}
}
