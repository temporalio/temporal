package worker_versioning

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/metrics"
)

func newReactivationSignalCacheForTest() ReactivationSignalCache {
	c := cache.New(100, &cache.Options{TTL: time.Minute})
	return NewReactivationSignalCache(c, metrics.NoopMetricsHandler)
}

func TestReactivationSignalCache_ShouldSendSignal_DedupsWithinSameRevision(t *testing.T) {
	c := newReactivationSignalCacheForTest()

	// First call for (ns, dep, build, rev=5) is a miss → should fire.
	assert.True(t, c.ShouldSendSignal("ns", "dep", "build", 5))
	// Second call for the exact same tuple within TTL → dedup'd.
	assert.False(t, c.ShouldSendSignal("ns", "dep", "build", 5))
}

func TestReactivationSignalCache_ShouldSendSignal_DifferentRevisionsDoNotDedup(t *testing.T) {
	c := newReactivationSignalCacheForTest()

	// A drain → reactivate → drain cycle within TTL: same (ns, dep, build), different revisions.
	// Each revision is a distinct reactivation cycle and must fire its own signal.
	assert.True(t, c.ShouldSendSignal("ns", "dep", "build", 5))
	assert.True(t, c.ShouldSendSignal("ns", "dep", "build", 7))
	// But repeated within a given revision still dedups.
	assert.False(t, c.ShouldSendSignal("ns", "dep", "build", 7))
	assert.False(t, c.ShouldSendSignal("ns", "dep", "build", 5))
}
