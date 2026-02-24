//nolint:staticcheck
package worker_versioning

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/metrics"
)

// VersionMembershipCache is used to cache results of Matching's CheckTaskQueueVersionMembership
// calls (used internally by the worker versioning pinned override validation).
//
// Implementations are expected to be safe for concurrent use.
type (
	VersionMembershipCache interface {
		// Get returns (isMember, ok). ok=false means there was no cached value.
		Get(
			namespaceID string,
			taskQueue string,
			taskQueueType enumspb.TaskQueueType,
			deploymentName string,
			buildID string,
		) (isMember bool, ok bool)

		Put(
			namespaceID string,
			taskQueue string,
			taskQueueType enumspb.TaskQueueType,
			deploymentName string,
			buildID string,
			isMember bool,
		)
	}

	versionMembershipCacheKey struct {
		namespaceID    string
		taskQueue      string
		taskQueueType  enumspb.TaskQueueType
		deploymentName string
		buildID        string
	}

	VersionMembershipCacheImpl struct {
		cache.Cache
		metricsHandler metrics.Handler
	}
)

// NewVersionMembershipCache wraps the provided cache with a typed API and metrics.
func NewVersionMembershipCache(c cache.Cache, metricsHandler metrics.Handler) VersionMembershipCache {
	h := metricsHandler.WithTags(metrics.CacheTypeTag(metrics.VersionMembershipCacheTypeTagValue))
	return &VersionMembershipCacheImpl{
		Cache:          c,
		metricsHandler: h,
	}
}

func (c *VersionMembershipCacheImpl) Get(
	namespaceID string,
	taskQueue string,
	taskQueueType enumspb.TaskQueueType,
	deploymentName string,
	buildID string,
) (isMember bool, ok bool) {
	handler := c.metricsHandler.WithTags(metrics.OperationTag(metrics.VersionMembershipCacheGetScope), metrics.NamespaceIDTag(namespaceID))
	metrics.CacheRequests.With(handler).Record(1)

	key := versionMembershipCacheKey{
		namespaceID:    namespaceID,
		taskQueue:      taskQueue,
		taskQueueType:  taskQueueType,
		deploymentName: deploymentName,
		buildID:        buildID,
	}
	v := c.Cache.Get(key)
	if v == nil {
		metrics.CacheMissCounter.With(handler).Record(1)
		return false, false
	}
	isMember, ok = v.(bool)
	if !ok {
		// Unexpected type: treat as miss to avoid false positives.
		metrics.CacheMissCounter.With(handler).Record(1)
		return false, false
	}
	return isMember, true
}

func (c *VersionMembershipCacheImpl) Put(
	namespaceID string,
	taskQueue string,
	taskQueueType enumspb.TaskQueueType,
	deploymentName string,
	buildID string,
	isMember bool,
) {
	handler := c.metricsHandler.WithTags(metrics.OperationTag(metrics.VersionMembershipCachePutScope), metrics.NamespaceIDTag(namespaceID))
	metrics.CacheRequests.With(handler).Record(1)

	key := versionMembershipCacheKey{
		namespaceID:    namespaceID,
		taskQueue:      taskQueue,
		taskQueueType:  taskQueueType,
		deploymentName: deploymentName,
		buildID:        buildID,
	}
	c.Cache.Put(key, isMember)
}
