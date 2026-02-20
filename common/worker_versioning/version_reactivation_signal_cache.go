//nolint:staticcheck
package worker_versioning

import (
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/metrics"
)

// ReactivationSignalCache deduplicates reactivation signals to version workflows.
//
// Implementations are expected to be safe for concurrent use.
type (
	ReactivationSignalCache interface {
		// ShouldSendSignal returns true if signal should be sent (not recently sent)
		// and atomically marks it as sent. Returns false if recently sent.
		ShouldSendSignal(namespaceID, deploymentName, buildID string) bool
	}

	reactivationSignalCacheKey struct {
		namespaceID    string
		deploymentName string
		buildID        string
	}

	ReactivationSignalCacheImpl struct {
		cache.Cache
		metricsHandler metrics.Handler
	}
)

// NewReactivationSignalCache wraps the provided cache with a typed API and metrics.
func NewReactivationSignalCache(c cache.Cache, metricsHandler metrics.Handler) ReactivationSignalCache {
	h := metricsHandler.WithTags(metrics.CacheTypeTag(metrics.VersionReactivationSignalCacheTypeTagValue))
	return &ReactivationSignalCacheImpl{
		Cache:          c,
		metricsHandler: h,
	}
}

func (c *ReactivationSignalCacheImpl) ShouldSendSignal(
	namespaceID, deploymentName, buildID string,
) bool {
	handler := c.metricsHandler.WithTags(
		metrics.OperationTag(metrics.VersionReactivationSignalCacheShouldSendScope),
		metrics.NamespaceIDTag(namespaceID),
	)
	metrics.CacheRequests.With(handler).Record(1)

	key := reactivationSignalCacheKey{
		namespaceID:    namespaceID,
		deploymentName: deploymentName,
		buildID:        buildID,
	}

	// Check if we recently sent a signal for this version
	if c.Cache.Get(key) != nil {
		// Entry exists, signal was recently sent - deduplicate
		return false
	}

	// No recent signal, mark as sent and return true
	metrics.CacheMissCounter.With(handler).Record(1)
	c.Cache.Put(key, true)
	return true
}
