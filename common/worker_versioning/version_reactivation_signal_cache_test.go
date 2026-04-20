//nolint:staticcheck
package worker_versioning

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/metrics"
)

func newReactivationSignalCacheForTest(ttl time.Duration) ReactivationSignalCache {
	c := cache.New(100, &cache.Options{TTL: ttl})
	return NewReactivationSignalCache(c, metrics.NoopMetricsHandler)
}

func TestReactivationSignalCache_ShouldSendSignal_DedupsWithinSameRevision(t *testing.T) {
	c := newReactivationSignalCacheForTest(time.Minute)

	// First call for (ns, dep, build, rev=5) is a miss → should fire.
	require.True(t, c.ShouldSendSignal("ns", "dep", "build", 5))
	// Second call for the exact same tuple within TTL → dedup'd.
	require.False(t, c.ShouldSendSignal("ns", "dep", "build", 5))
}

func TestReactivationSignalCache_ShouldSendSignal_DifferentRevisionsDoNotDedup(t *testing.T) {
	c := newReactivationSignalCacheForTest(time.Minute)

	// A drain → reactivate → drain cycle within TTL: same (ns, dep, build), different revisions.
	// Each revision is a distinct reactivation cycle and must fire its own signal.
	require.True(t, c.ShouldSendSignal("ns", "dep", "build", 5))
	require.True(t, c.ShouldSendSignal("ns", "dep", "build", 7))
	// But repeated within a given revision still dedups.
	require.False(t, c.ShouldSendSignal("ns", "dep", "build", 7))
	require.False(t, c.ShouldSendSignal("ns", "dep", "build", 5))
}

func TestReactivationSignalCache_ShouldSendSignal_KeyComponentsIsolated(t *testing.T) {
	// Entries that differ in exactly one key component must not dedup against each other.
	tests := []struct {
		name              string
		ns1, dep1, build1 string
		rev1              int64
		ns2, dep2, build2 string
		rev2              int64
	}{
		{"different namespaceID", "ns1", "dep", "build", 5, "ns2", "dep", "build", 5},
		{"different deploymentName", "ns", "dep1", "build", 5, "ns", "dep2", "build", 5},
		{"different buildID", "ns", "dep", "build1", 5, "ns", "dep", "build2", 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newReactivationSignalCacheForTest(time.Minute)
			require.True(t, c.ShouldSendSignal(tt.ns1, tt.dep1, tt.build1, tt.rev1))
			require.True(t, c.ShouldSendSignal(tt.ns2, tt.dep2, tt.build2, tt.rev2))
		})
	}
}

func TestReactivationSignalCache_ShouldSendSignal_FiresAgainAfterTTLExpiry(t *testing.T) {
	c := newReactivationSignalCacheForTest(50 * time.Millisecond)

	require.True(t, c.ShouldSendSignal("ns", "dep", "build", 5))
	require.False(t, c.ShouldSendSignal("ns", "dep", "build", 5))

	time.Sleep(100 * time.Millisecond)

	// After TTL, the entry is gone — same tuple fires again.
	require.True(t, c.ShouldSendSignal("ns", "dep", "build", 5))
}
