package workers

import (
	"fmt"
	"math/rand"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testvars"
)

// alwaysTrue predicate for convenience
func alwaysTrue(_ *workerpb.WorkerHeartbeat) bool { return true }

func TestUpdateAndListNamespace(t *testing.T) {
	// Use capture handler to verify metrics
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	m := newRegistryImpl(RegistryParams{
		NumBuckets:          dynamicconfig.GetIntPropertyFn(2),
		TTL:                 dynamicconfig.GetDurationPropertyFn(time.Hour),
		MinEvictAge:         dynamicconfig.GetDurationPropertyFn(0),
		MaxItems:            dynamicconfig.GetIntPropertyFn(10),
		EvictionInterval:    dynamicconfig.GetDurationPropertyFn(time.Hour),
		MetricsHandler:      captureHandler,
		EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
	})
	defer m.Stop()

	// No entries initially
	list := m.filterWorkers("ns1", alwaysTrue)
	assert.Empty(t, list, "expected empty list before updates")

	// Add some heartbeats
	hb1 := &workerpb.WorkerHeartbeat{WorkerInstanceKey: "workerA", Status: enumspb.WORKER_STATUS_RUNNING}
	hb2 := &workerpb.WorkerHeartbeat{WorkerInstanceKey: "workerB", Status: enumspb.WORKER_STATUS_RUNNING}
	m.upsertHeartbeats("ns1", []*workerpb.WorkerHeartbeat{hb1, hb2})

	list = m.filterWorkers("ns1", alwaysTrue)
	// Order is not guaranteed; check contents by keys
	keys := []string{list[0].WorkerInstanceKey, list[1].WorkerInstanceKey}
	assert.Contains(t, keys, "workerA")
	assert.Contains(t, keys, "workerB")

	// Verify metrics
	snapshot := capture.Snapshot()

	// Check capacity utilization metric
	utilizationMetrics := snapshot["worker_registry_capacity_utilization"]
	assert.Equal(t, len(utilizationMetrics), 1, "should have capacity utilization metric")
	lastUtilization := utilizationMetrics[0]
	assert.Equal(t, float64(2)/float64(10), lastUtilization.Value, "should record correct capacity utilization")
}

func TestShutdownStatusRemovesWorker(t *testing.T) {
	m := newRegistryImpl(RegistryParams{
		NumBuckets:          dynamicconfig.GetIntPropertyFn(1),
		TTL:                 dynamicconfig.GetDurationPropertyFn(time.Hour),
		MinEvictAge:         dynamicconfig.GetDurationPropertyFn(0),
		MaxItems:            dynamicconfig.GetIntPropertyFn(10),
		EvictionInterval:    dynamicconfig.GetDurationPropertyFn(time.Hour),
		MetricsHandler:      metrics.NoopMetricsHandler,
		EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
	})
	defer m.Stop()

	// Add two running workers
	hb1 := &workerpb.WorkerHeartbeat{WorkerInstanceKey: "worker1", Status: enumspb.WORKER_STATUS_RUNNING}
	hb2 := &workerpb.WorkerHeartbeat{WorkerInstanceKey: "worker2", Status: enumspb.WORKER_STATUS_RUNNING}
	m.upsertHeartbeats("ns1", []*workerpb.WorkerHeartbeat{hb1, hb2})

	// Verify both workers are registered
	list := m.filterWorkers("ns1", alwaysTrue)
	assert.Len(t, list, 2, "both workers should be registered")
	assert.Equal(t, int64(2), m.total.Load(), "total should be 2")

	// Worker1 sends shutdown status
	hbShutdown := &workerpb.WorkerHeartbeat{WorkerInstanceKey: "worker1", Status: enumspb.WORKER_STATUS_SHUTDOWN}
	m.upsertHeartbeats("ns1", []*workerpb.WorkerHeartbeat{hbShutdown})

	// Verify only worker1 is removed, worker2 remains
	list = m.filterWorkers("ns1", alwaysTrue)
	assert.Len(t, list, 1, "only one worker should remain")
	assert.Equal(t, "worker2", list[0].WorkerInstanceKey, "worker2 should remain")
	assert.Equal(t, int64(1), m.total.Load(), "total should be 1 after shutdown")
}

func TestShutdownStatusForNonExistentWorker(t *testing.T) {
	m := newRegistryImpl(RegistryParams{
		NumBuckets:          dynamicconfig.GetIntPropertyFn(1),
		TTL:                 dynamicconfig.GetDurationPropertyFn(time.Hour),
		MinEvictAge:         dynamicconfig.GetDurationPropertyFn(0),
		MaxItems:            dynamicconfig.GetIntPropertyFn(10),
		EvictionInterval:    dynamicconfig.GetDurationPropertyFn(time.Hour),
		MetricsHandler:      metrics.NoopMetricsHandler,
		EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
	})
	defer m.Stop()

	// Send shutdown for non-existent worker - should be a no-op
	hb := &workerpb.WorkerHeartbeat{WorkerInstanceKey: "unknown", Status: enumspb.WORKER_STATUS_SHUTDOWN}
	m.upsertHeartbeats("ns1", []*workerpb.WorkerHeartbeat{hb})

	// Verify nothing happened
	list := m.filterWorkers("ns1", alwaysTrue)
	assert.Empty(t, list, "no workers should exist")
	assert.Zero(t, m.total.Load(), "total should remain 0")
}

func TestListNamespacePredicate(t *testing.T) {
	m := newRegistryImpl(RegistryParams{
		NumBuckets:          dynamicconfig.GetIntPropertyFn(1),
		TTL:                 dynamicconfig.GetDurationPropertyFn(time.Hour),
		MinEvictAge:         dynamicconfig.GetDurationPropertyFn(0),
		MaxItems:            dynamicconfig.GetIntPropertyFn(10),
		EvictionInterval:    dynamicconfig.GetDurationPropertyFn(time.Hour),
		MetricsHandler:      metrics.NoopMetricsHandler,
		EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
	})
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
	m := newRegistryImpl(RegistryParams{
		NumBuckets:          dynamicconfig.GetIntPropertyFn(1),
		TTL:                 dynamicconfig.GetDurationPropertyFn(1 * time.Second),
		MinEvictAge:         dynamicconfig.GetDurationPropertyFn(0),
		MaxItems:            dynamicconfig.GetIntPropertyFn(10),
		EvictionInterval:    dynamicconfig.GetDurationPropertyFn(time.Hour),
		MetricsHandler:      metrics.NoopMetricsHandler,
		EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
	})
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
	assert.Empty(t, list, "entry should be evicted by TTL")
	assert.Zero(t, m.total.Load(), "total counter should be decremented")
}

func TestEvictByCapacity(t *testing.T) {
	maxItems := 3

	// Use capture handler to verify metrics
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	m := newRegistryImpl(RegistryParams{
		NumBuckets:          dynamicconfig.GetIntPropertyFn(1),
		TTL:                 dynamicconfig.GetDurationPropertyFn(time.Hour),
		MinEvictAge:         dynamicconfig.GetDurationPropertyFn(0),
		MaxItems:            dynamicconfig.GetIntPropertyFn(maxItems),
		EvictionInterval:    dynamicconfig.GetDurationPropertyFn(time.Hour),
		MetricsHandler:      captureHandler,
		EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
	})
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
	assert.LessOrEqual(t, m.total.Load(), int64(maxItems), "total counter should not exceed maxItems")

	// Verify metrics: eviction should succeed (no age protection issues)
	snapshot := capture.Snapshot()
	evictionMetrics := snapshot["worker_registry_eviction_blocked_by_age"]
	assert.Len(t, evictionMetrics, 1, "should have eviction metric")

	metric := evictionMetrics[0]
	assert.Equal(t, float64(0), metric.Value, "should clear age protection issue when eviction succeeds")
}

// Tests the critical edge case where evictByCapacity() cannot evict any entries because they're all
// protected by minEvictAge. This verifies that we do not keep checking the same entries repeatedly
// when there is no space.
func TestEvictByCapacityWithMinAgeProtection(t *testing.T) {
	maxItems := 2
	minEvictAge := 5 * time.Second

	// Use capture handler to verify metrics
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	m := newRegistryImpl(RegistryParams{
		NumBuckets:          dynamicconfig.GetIntPropertyFn(1),
		TTL:                 dynamicconfig.GetDurationPropertyFn(time.Hour),
		MinEvictAge:         dynamicconfig.GetDurationPropertyFn(minEvictAge),
		MaxItems:            dynamicconfig.GetIntPropertyFn(maxItems),
		EvictionInterval:    dynamicconfig.GetDurationPropertyFn(time.Hour),
		MetricsHandler:      captureHandler,
		EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
	})
	defer m.Stop()

	// Add 3 entries (over capacity) - all will be "new" (< minEvictAge)
	for i := 1; i <= 3; i++ {
		key := fmt.Sprintf("worker%d", i)
		hb := &workerpb.WorkerHeartbeat{WorkerInstanceKey: key}
		m.upsertHeartbeats("ns", []*workerpb.WorkerHeartbeat{hb})
	}

	// Verify we're over capacity
	assert.Equal(t, int64(3), m.total.Load(), "should have 3 entries initially")
	assert.Greater(t, m.total.Load(), int64(maxItems), "should be over capacity")

	// Attempt eviction - should not happen because all entries are too new
	m.evictByCapacity()

	// All entries should still be there (protected by minEvictAge)
	workers := m.filterWorkers("ns", alwaysTrue)
	assert.Len(t, workers, 3, "all entries should be protected by minEvictAge")
	assert.Equal(t, int64(3), m.total.Load(), "should still exceed maxItems due to protection")

	// Verify metrics: should record eviction blocked by age
	snapshot := capture.Snapshot()
	evictionMetrics := snapshot["worker_registry_eviction_blocked_by_age"]
	assert.Len(t, evictionMetrics, 1, "should have eviction blocked by age metric")

	metric := evictionMetrics[0]
	assert.Equal(t, float64(1), metric.Value, "should record eviction blocked by age")
}

// Tests that entries can be evicted once they exceed minEvictAge.
func TestEvictByCapacityAfterMinAge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		maxItems := 2
		minEvictAge := 100 * time.Millisecond // Short age for test

		captureHandler := metricstest.NewCaptureHandler()
		capture := captureHandler.StartCapture()
		defer captureHandler.StopCapture(capture)

		// Uses real time.NewTicker - synctest provides virtual time control
		m := newRegistryImpl(RegistryParams{
			NumBuckets:          dynamicconfig.GetIntPropertyFn(1),
			TTL:                 dynamicconfig.GetDurationPropertyFn(time.Hour),
			MinEvictAge:         dynamicconfig.GetDurationPropertyFn(minEvictAge),
			MaxItems:            dynamicconfig.GetIntPropertyFn(maxItems),
			EvictionInterval:    dynamicconfig.GetDurationPropertyFn(time.Hour),
			MetricsHandler:      captureHandler,
			EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
		})
		defer m.Stop()

		// Add 3 entries (over capacity)
		for i := 1; i <= 3; i++ {
			key := fmt.Sprintf("worker%d", i)
			hb := &workerpb.WorkerHeartbeat{WorkerInstanceKey: key}
			m.upsertHeartbeats("ns", []*workerpb.WorkerHeartbeat{hb})
		}

		// Virtual time advance - instant with synctest!
		time.Sleep(300 * time.Millisecond) // nolint:forbidigo

		// Now eviction should work
		m.evictByCapacity()

		// Should have evicted down to maxItems
		workers := m.filterWorkers("ns", alwaysTrue)
		assert.LessOrEqual(t, len(workers), int(maxItems), "should evict down to maxItems")
		assert.LessOrEqual(t, m.total.Load(), int64(maxItems), "total should be within limits")

		// Verify metrics: eviction should succeed (entries were old enough)
		snapshot := capture.Snapshot()
		evictionMetrics := snapshot["worker_registry_eviction_blocked_by_age"]
		assert.Len(t, evictionMetrics, 1, "should have eviction metric")

		metric := evictionMetrics[0]
		assert.Equal(t, float64(0), metric.Value, "should clear age protection issue when eviction succeeds")
	})
}

// TestMultipleNamespaces tests with registry with multiple namespaces.
func TestMultipleNamespaces(t *testing.T) {
	maxItems := 10
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	m := newRegistryImpl(RegistryParams{
		NumBuckets:          dynamicconfig.GetIntPropertyFn(2),
		TTL:                 dynamicconfig.GetDurationPropertyFn(time.Hour),
		MinEvictAge:         dynamicconfig.GetDurationPropertyFn(0),
		MaxItems:            dynamicconfig.GetIntPropertyFn(maxItems),
		EvictionInterval:    dynamicconfig.GetDurationPropertyFn(time.Hour),
		MetricsHandler:      captureHandler,
		EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
	})
	defer m.Stop()

	// Add 3 workers to namespace1
	ns1Workers := []*workerpb.WorkerHeartbeat{
		{WorkerInstanceKey: "ns1-worker1", TaskQueue: "queue1"},
		{WorkerInstanceKey: "ns1-worker2", TaskQueue: "queue1"},
		{WorkerInstanceKey: "ns1-worker3", TaskQueue: "queue2"},
	}
	m.upsertHeartbeats("namespace1", ns1Workers)

	// Add 2 workers to namespace2
	ns2Workers := []*workerpb.WorkerHeartbeat{
		{WorkerInstanceKey: "ns2-worker1", TaskQueue: "queue3"},
		{WorkerInstanceKey: "ns2-worker2", TaskQueue: "queue3"},
	}
	m.upsertHeartbeats("namespace2", ns2Workers)

	// Verify functional behavior first
	ns1List := m.filterWorkers("namespace1", alwaysTrue)
	assert.Len(t, ns1List, 3, "namespace1 should have 3 workers")

	ns2List := m.filterWorkers("namespace2", alwaysTrue)
	assert.Len(t, ns2List, 2, "namespace2 should have 2 workers")

	assert.Equal(t, int64(5), m.total.Load(), "total should be 5 workers across namespaces")

	// Verify metrics
	snapshot := capture.Snapshot()

	// Check capacity utilization reflects total across all namespaces
	utilizationMetrics := snapshot["worker_registry_capacity_utilization"]
	assert.GreaterOrEqual(t, len(utilizationMetrics), 1, "should have capacity utilization metric")

	lastUtilization := utilizationMetrics[len(utilizationMetrics)-1]
	expectedUtilization := float64(5) / float64(10) // 5 total workers / 10 max capacity
	assert.Equal(t, expectedUtilization, lastUtilization.Value, "capacity utilization should reflect total across all namespaces")
}

func TestEvictLoopRecordsUtilizationMetric(t *testing.T) {
	// Using synctest as it provides virtual time control.
	synctest.Test(t, func(t *testing.T) {
		maxItems := 5
		evictionInterval := 100 * time.Millisecond

		captureHandler := metricstest.NewCaptureHandler()
		capture := captureHandler.StartCapture()
		defer captureHandler.StopCapture(capture)

		m := newRegistryImpl(RegistryParams{
			NumBuckets:          dynamicconfig.GetIntPropertyFn(1),
			TTL:                 dynamicconfig.GetDurationPropertyFn(time.Hour),
			MinEvictAge:         dynamicconfig.GetDurationPropertyFn(0),
			MaxItems:            dynamicconfig.GetIntPropertyFn(maxItems),
			EvictionInterval:    dynamicconfig.GetDurationPropertyFn(evictionInterval),
			MetricsHandler:      captureHandler,
			EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
		})

		// Add some entries to create utilization
		for i := 1; i <= 3; i++ {
			key := fmt.Sprintf("worker%d", i)
			hb := &workerpb.WorkerHeartbeat{WorkerInstanceKey: key}
			m.upsertHeartbeats("ns", []*workerpb.WorkerHeartbeat{hb})
		}

		// Verify initial state
		assert.Equal(t, int64(3), m.total.Load(), "should have 3 workers")

		// Start the evictLoop
		m.Start()
		defer m.Stop()

		// Advance virtual time to trigger one eviction cycle - instant with synctest!
		time.Sleep(evictionInterval) // nolint:forbidigo

		// Wait until all goroutines are blocked (evictLoop processes timer and blocks on next timer)
		synctest.Wait()

		// Verify evictLoop recorded utilization metric
		snapshot := capture.Snapshot()
		utilizationMetrics := snapshot["worker_registry_capacity_utilization"]
		assert.NotEmpty(t, utilizationMetrics, "evictLoop should record utilization metrics")

		// Verify the utilization value is correct
		lastUtilization := utilizationMetrics[len(utilizationMetrics)-1]
		expectedUtilization := float64(3) / float64(5) // 3 entries / 5 max capacity = 0.6
		assert.Equal(t, expectedUtilization, lastUtilization.Value, "evictLoop should record correct utilization")
	})
}

func BenchmarkUpdate(b *testing.B) {
	m := newRegistryImpl(RegistryParams{
		NumBuckets:          dynamicconfig.GetIntPropertyFn(16),
		TTL:                 dynamicconfig.GetDurationPropertyFn(time.Hour),
		MinEvictAge:         dynamicconfig.GetDurationPropertyFn(time.Minute),
		MaxItems:            dynamicconfig.GetIntPropertyFn(b.N),
		EvictionInterval:    dynamicconfig.GetDurationPropertyFn(time.Hour),
		MetricsHandler:      metrics.NoopMetricsHandler,
		EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
	})
	defer m.Stop()
	hb := &workerpb.WorkerHeartbeat{WorkerInstanceKey: "benchWorker"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.upsertHeartbeats("benchNs", []*workerpb.WorkerHeartbeat{hb})
	}
}

func BenchmarkListNamespace(b *testing.B) {
	m := newRegistryImpl(RegistryParams{
		NumBuckets:          dynamicconfig.GetIntPropertyFn(16),
		TTL:                 dynamicconfig.GetDurationPropertyFn(time.Hour),
		MinEvictAge:         dynamicconfig.GetDurationPropertyFn(time.Minute),
		MaxItems:            dynamicconfig.GetIntPropertyFn(1000),
		EvictionInterval:    dynamicconfig.GetDurationPropertyFn(time.Hour),
		MetricsHandler:      metrics.NoopMetricsHandler,
		EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
	})
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
	m := newRegistryImpl(RegistryParams{
		NumBuckets:          dynamicconfig.GetIntPropertyFn(len(namespaces)),
		TTL:                 dynamicconfig.GetDurationPropertyFn(time.Hour),
		MinEvictAge:         dynamicconfig.GetDurationPropertyFn(time.Minute),
		MaxItems:            dynamicconfig.GetIntPropertyFn(b.N),
		EvictionInterval:    dynamicconfig.GetDurationPropertyFn(time.Hour),
		MetricsHandler:      metrics.NoopMetricsHandler,
		EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
	})
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

// TestPluginMetricsExported verifies that plugin metrics are correctly recorded
// with the expected dimensions (namespace_name and plugin_name).
func TestPluginMetricsExported(t *testing.T) {
	tv := testvars.New(t)

	// Generate test values using testvars for consistency and uniqueness
	worker1Key := tv.WorkerIdentity() + "_1"
	worker2Key := tv.WorkerIdentity() + "_2"
	worker3Key := tv.WorkerIdentity() + "_3"

	pluginA := "plugin-a"
	pluginB := "plugin-b"
	pluginC := "plugin-c"
	pluginAVersion := "1.0.0"
	pluginBVersion := "2.1.0"
	pluginCVersion := "3.0.0"
	pluginAVersion2 := "1.5.0" // Different version of plugin-a for worker2

	// Use capture handler to verify metrics
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	m := newRegistryImpl(RegistryParams{
		NumBuckets:          dynamicconfig.GetIntPropertyFn(2),
		TTL:                 dynamicconfig.GetDurationPropertyFn(time.Hour),
		MinEvictAge:         dynamicconfig.GetDurationPropertyFn(0),
		MaxItems:            dynamicconfig.GetIntPropertyFn(10),
		EvictionInterval:    dynamicconfig.GetDurationPropertyFn(time.Hour),
		MetricsHandler:      captureHandler,
		EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(true),
	})
	defer m.Stop()

	// Test data with 3 workers (tests uniqueness logic):
	// Worker 1: 2 plugins (plugin-a, plugin-b)
	// Worker 2: 2 plugins (plugin-a, plugin-c) - plugin-a is shared with worker1
	// Worker 3: 0 plugins

	worker1 := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: worker1Key,
		Status:            enumspb.WORKER_STATUS_RUNNING,
		Plugins: []*workerpb.PluginInfo{
			{Name: pluginA, Version: pluginAVersion},
			{Name: pluginB, Version: pluginBVersion},
		},
	}

	worker2 := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: worker2Key,
		Status:            enumspb.WORKER_STATUS_RUNNING,
		Plugins: []*workerpb.PluginInfo{
			{Name: pluginA, Version: pluginAVersion2}, // Same plugin name as worker1, different version
			{Name: pluginC, Version: pluginCVersion},
		},
	}

	worker3 := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: worker3Key,
		Status:            enumspb.WORKER_STATUS_RUNNING,
		// No plugins - empty array
	}

	testNamespace := tv.NamespaceID()
	testNamespaceName := namespace.Name(testNamespace + "_name")
	m.RecordWorkerHeartbeats(testNamespace, testNamespaceName, []*workerpb.WorkerHeartbeat{worker1, worker2, worker3})

	// Verify plugin metrics - should have exactly 3 recordings despite plugin-a being in both workers
	snapshot := capture.Snapshot()
	pluginMetrics := snapshot[metrics.WorkerPluginNameMetric.Name()]
	assert.Len(t, pluginMetrics, 3, "plugin-a from both workers should be deduplicated")

	// Helper function to find metric by namespace and plugin name
	findMetric := func(namespaceName namespace.Name, pluginName string) *metricstest.CapturedRecording {
		for _, metric := range pluginMetrics {
			if metric.Tags["namespace_id"] == namespaceName.String() && metric.Tags[metrics.WorkerPluginNameTagName] == pluginName {
				return metric
			}
		}
		return nil
	}

	// Verify each expected plugin has a metric with correct dimensions and value
	expectedPlugins := []string{pluginA, pluginB, pluginC}

	for _, expectedPlugin := range expectedPlugins {
		metric := findMetric(testNamespaceName, expectedPlugin)
		assert.NotNil(t, metric, "should have metric for namespace '%s' and plugin: %s", testNamespaceName.String(), expectedPlugin)
		assert.InEpsilon(t, float64(1), metric.Value, 0.000001, "plugin metric value should be 1 for %s", expectedPlugin)
	}
}

// TestPluginMetricsDisabled verifies that plugin metrics are not exported when disabled via dynamic config.
func TestPluginMetricsDisabled(t *testing.T) {
	tv := testvars.New(t)

	// Use capture handler to verify metrics
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	m := newRegistryImpl(RegistryParams{
		NumBuckets:          dynamicconfig.GetIntPropertyFn(2),
		TTL:                 dynamicconfig.GetDurationPropertyFn(time.Hour),
		MinEvictAge:         dynamicconfig.GetDurationPropertyFn(0),
		MaxItems:            dynamicconfig.GetIntPropertyFn(10),
		EvictionInterval:    dynamicconfig.GetDurationPropertyFn(time.Hour),
		MetricsHandler:      captureHandler,
		EnablePluginMetrics: dynamicconfig.GetBoolPropertyFn(false),
	})
	defer m.Stop()

	// Test data with workers having plugins
	worker1 := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: tv.WorkerIdentity() + "_1",
		Status:            enumspb.WORKER_STATUS_RUNNING,
		Plugins: []*workerpb.PluginInfo{
			{Name: "plugin-a", Version: "1.0.0"},
			{Name: "plugin-b", Version: "2.1.0"},
		},
	}

	// Upsert heartbeats for test namespace
	testNamespace := tv.NamespaceID()
	testNamespaceName := namespace.Name(testNamespace + "_name")
	m.RecordWorkerHeartbeats(testNamespace, testNamespaceName, []*workerpb.WorkerHeartbeat{worker1})

	// Verify no plugin metrics are recorded when disabled
	snapshot := capture.Snapshot()
	pluginMetrics := snapshot[metrics.WorkerPluginNameMetric.Name()]
	assert.Empty(t, pluginMetrics, "should not record any plugin metrics when disabled")
}
