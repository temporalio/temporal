//nolint:staticcheck
package worker_versioning

import (
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/metrics"
)

// ReactivationSignalCache deduplicates reactivation signals to version workflows.
//
// The key includes revisionNumber so that a rapid drain → reactivate → drain cycle within
// the cache TTL does not have its second signal suppressed by a stale entry from the first
// cycle. Each drainage cycle increments the version's revisionNumber in matching's deployment
// data, so distinct cycles occupy distinct cache entries and signal independently.
//
// NOTE: EFFECTIVE DEDUP WINDOW TIME:
// This cache's configured TTL (VersionReactivationSignalCacheTTL, default 10s) is dependent
// on how frequently the version workflow changes its status. Every change to the version
// workflow's status — current/ramping updates, drainage transitions, etc. — bumps the
// revisionNumber in matching's deployment data. The pod reads the current revision from
// VersionMembershipAndReactivationStatusCache on each validation; when that cache's entry
// expires (VersionMembershipCacheTTL, default 1s) and the version's status has changed
// since, the pod re-fetches from matching and may see a fresh revision. Lookups here then
// use a new key, so the effective dedup window collapses to the membership cache's TTL under
// revision churn — which is intentional, since a new revision represents a new reactivation
// scenario that warrants a new signal.
//
// In practice this rarely fires at sub-second cadence because version_workflow.go enforces
// graceful intervals for drainage-related checks (VersionDrainageStatusRefreshInterval and
// VersionDrainageStatusVisibilityGracePeriod), so status changes do not cascade every
// second. Cross-pod duplicates within the same revision are caught by receiver-side dedup
// via pendingSignalRequestedIDs regardless of this cache's state.
//
// Implementations are expected to be safe for concurrent use.
type (
	ReactivationSignalCache interface {
		// ShouldSendSignal returns true if signal should be sent (not recently sent)
		// and atomically marks it as sent. Returns false if recently sent for the same
		// (namespaceID, deploymentName, buildID, revisionNumber) tuple.
		ShouldSendSignal(namespaceID, deploymentName, buildID string, revisionNumber int64) bool
	}

	reactivationSignalCacheKey struct {
		namespaceID    string
		deploymentName string
		buildID        string
		revisionNumber int64
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
	revisionNumber int64,
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
		revisionNumber: revisionNumber,
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
