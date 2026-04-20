//nolint:staticcheck
package worker_versioning

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/metrics"
)

// VersionMembershipAndReactivationStatusCache caches results of Matching's
// CheckTaskQueueVersionMembership calls. It stores three pieces of information per version:
//   - isMember: whether the task queue exists in the version (used for pinned override validation).
//   - isDrainedOrInactive: whether the version's status is DRAINED or INACTIVE, used to decide
//     if a reactivation signal should be sent. nil means unknown (e.g. old matching server).
//   - revisionNumber: the version's current revision per matching's view. Used as part of the
//     reactivation signal's RequestId so that all history pods targeting the same version at
//     the same revision compose the same dedup key. Zero means unknown (old matching server or
//     legacy DeploymentVersionData format with no revision_number field).
//
// Implementations are expected to be safe for concurrent use.
type (
	VersionMembershipAndReactivationStatusCache interface {
		Get(
			namespaceID string,
			taskQueue string,
			taskQueueType enumspb.TaskQueueType,
			deploymentName string,
			buildID string,
		) (isMember bool, isDrainedOrInactive *bool, revisionNumber int64, ok bool)

		Put(
			namespaceID string,
			taskQueue string,
			taskQueueType enumspb.TaskQueueType,
			deploymentName string,
			buildID string,
			isMember bool,
			isDrainedOrInactive *bool,
			revisionNumber int64,
		)
	}

	versionMembershipCacheKey struct {
		namespaceID    string
		taskQueue      string
		taskQueueType  enumspb.TaskQueueType
		deploymentName string
		buildID        string
	}

	versionTaskQueueInfoCacheValue struct {
		isMember            bool
		isDrainedOrInactive *bool // nil = unknown (old matching server)
		revisionNumber      int64 // 0 = unknown (old matching server or legacy format)
	}

	VersionMembershipAndReactivationStatusCacheImpl struct {
		cache.Cache
		metricsHandler metrics.Handler
	}
)

// NewVersionMembershipAndReactivationStatusCache wraps the provided cache with a typed API and metrics.
func NewVersionMembershipAndReactivationStatusCache(c cache.Cache, metricsHandler metrics.Handler) VersionMembershipAndReactivationStatusCache {
	h := metricsHandler.WithTags(metrics.CacheTypeTag(metrics.VersionMembershipCacheTypeTagValue))
	return &VersionMembershipAndReactivationStatusCacheImpl{
		Cache:          c,
		metricsHandler: h,
	}
}

func (c *VersionMembershipAndReactivationStatusCacheImpl) Get(
	namespaceID string,
	taskQueue string,
	taskQueueType enumspb.TaskQueueType,
	deploymentName string,
	buildID string,
) (isMember bool, isDrainedOrInactive *bool, revisionNumber int64, ok bool) {
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
		return false, nil, 0, false
	}
	value, ok := v.(versionTaskQueueInfoCacheValue)
	if !ok {
		// Unexpected type: treat as miss to avoid false positives.
		metrics.CacheMissCounter.With(handler).Record(1)
		return false, nil, 0, false
	}
	return value.isMember, value.isDrainedOrInactive, value.revisionNumber, true
}

func (c *VersionMembershipAndReactivationStatusCacheImpl) Put(
	namespaceID string,
	taskQueue string,
	taskQueueType enumspb.TaskQueueType,
	deploymentName string,
	buildID string,
	isMember bool,
	isDrainedOrInactive *bool,
	revisionNumber int64,
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
	c.Cache.Put(key, versionTaskQueueInfoCacheValue{
		isMember:            isMember,
		isDrainedOrInactive: isDrainedOrInactive,
		revisionNumber:      revisionNumber,
	})
}
