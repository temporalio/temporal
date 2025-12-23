package history

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/worker_versioning"
)

// versionMembershipCache is a typed wrapper around cache.Cache.
// As of this implementation, this cache is present on every history host and stores results of the matching service's CheckTaskQueueVersionMembership RPC.
// This cache emits cache hit/miss metrics.
type versionMembershipCache struct {
	cache   cache.Cache
	metrics metrics.Handler
}

type versionMembershipCacheKey struct {
	namespaceID    string
	taskQueue      string
	taskQueueType  enumspb.TaskQueueType
	deploymentName string
	buildID        string
}

func newVersionMembershipCache(c cache.Cache, metricsHandler metrics.Handler) worker_versioning.VersionMembershipCache {
	h := metricsHandler.WithTags(metrics.CacheTypeTag("version_membership"))
	return &versionMembershipCache{
		cache:   c,
		metrics: h,
	}
}

func (c *versionMembershipCache) Get(
	namespaceID string,
	taskQueue string,
	taskQueueType enumspb.TaskQueueType,
	deploymentName string,
	buildID string,
) (isMember bool, ok bool) {
	metrics.CacheRequests.With(c.metrics).Record(1, metrics.OperationTag("VersionMembershipCacheGet"))
	key := versionMembershipCacheKey{
		namespaceID:    namespaceID,
		taskQueue:      taskQueue,
		taskQueueType:  taskQueueType,
		deploymentName: deploymentName,
		buildID:        buildID,
	}
	v := c.cache.Get(key)
	if v == nil {
		metrics.CacheMissCounter.With(c.metrics).Record(1, metrics.OperationTag("VersionMembershipCacheGet"))
		return false, false
	}
	isMember, ok = v.(bool)
	if !ok {
		// Unexpected type: treat as miss to avoid false positives.
		metrics.CacheMissCounter.With(c.metrics).Record(1, metrics.OperationTag("VersionMembershipCacheGet"))
		return false, false
	}
	return isMember, true
}

func (c *versionMembershipCache) Put(
	namespaceID string,
	taskQueue string,
	taskQueueType enumspb.TaskQueueType,
	deploymentName string,
	buildID string,
	isMember bool,
) {
	metrics.CacheRequests.With(c.metrics).Record(1, metrics.OperationTag("VersionMembershipCachePut"))
	key := versionMembershipCacheKey{
		namespaceID:    namespaceID,
		taskQueue:      taskQueue,
		taskQueueType:  taskQueueType,
		deploymentName: deploymentName,
		buildID:        buildID,
	}
	c.cache.Put(key, isMember)
}
