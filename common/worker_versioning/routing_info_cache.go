//nolint:staticcheck
package worker_versioning

import (
	enumspb "go.temporal.io/api/enums/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/metrics"
)

// RoutingInfoCache is used to cache results of GetTaskQueueUserData
// calls followed by CalculateTaskQueueVersioningInfo computation.
//
// Implementations are expected to be safe for concurrent use.
type (
	RoutingInfoCache interface {
		// Get returns the cached routing info. ok=false means there was no cached value.
		Get(
			namespaceID string,
			taskQueue string,
			taskQueueType enumspb.TaskQueueType,
		) (
			current *deploymentspb.WorkerDeploymentVersion,
			currentRevisionNumber int64,
			ramping *deploymentspb.WorkerDeploymentVersion,
			rampPercentage float32,
			rampingRevisionNumber int64,
			ok bool,
		)

		Put(
			namespaceID string,
			taskQueue string,
			taskQueueType enumspb.TaskQueueType,
			current *deploymentspb.WorkerDeploymentVersion,
			currentRevisionNumber int64,
			ramping *deploymentspb.WorkerDeploymentVersion,
			rampPercentage float32,
			rampingRevisionNumber int64,
		)
	}

	routingInfoCacheKey struct {
		namespaceID   string
		taskQueue     string
		taskQueueType enumspb.TaskQueueType
	}

	routingInfoCacheValue struct {
		current               *deploymentspb.WorkerDeploymentVersion
		currentRevisionNumber int64
		ramping               *deploymentspb.WorkerDeploymentVersion
		rampPercentage        float32
		rampingRevisionNumber int64
	}

	RoutingInfoCacheImpl struct {
		cache.Cache
		metricsHandler metrics.Handler
	}
)

// NewRoutingInfoCache wraps the provided cache with a typed API and metrics.
func NewRoutingInfoCache(c cache.Cache, metricsHandler metrics.Handler) RoutingInfoCache {
	h := metricsHandler.WithTags(metrics.CacheTypeTag(metrics.RoutingInfoCacheTypeTagValue))
	return &RoutingInfoCacheImpl{
		Cache:          c,
		metricsHandler: h,
	}
}

func (c *RoutingInfoCacheImpl) Get(
	namespaceID string,
	taskQueue string,
	taskQueueType enumspb.TaskQueueType,
) (
	current *deploymentspb.WorkerDeploymentVersion,
	currentRevisionNumber int64,
	ramping *deploymentspb.WorkerDeploymentVersion,
	rampPercentage float32,
	rampingRevisionNumber int64,
	ok bool,
) {
	handler := c.metricsHandler.WithTags(metrics.OperationTag(metrics.RoutingInfoCacheGetScope), metrics.NamespaceIDTag(namespaceID))
	metrics.CacheRequests.With(handler).Record(1)

	key := routingInfoCacheKey{
		namespaceID:   namespaceID,
		taskQueue:     taskQueue,
		taskQueueType: taskQueueType,
	}
	v := c.Cache.Get(key)
	if v == nil {
		metrics.CacheMissCounter.With(handler).Record(1)
		return nil, 0, nil, 0, 0, false
	}
	value, ok := v.(routingInfoCacheValue)
	if !ok {
		// Unexpected type: treat as miss to avoid false positives.
		metrics.CacheMissCounter.With(handler).Record(1)
		return nil, 0, nil, 0, 0, false
	}
	return value.current, value.currentRevisionNumber,
		value.ramping, value.rampPercentage,
		value.rampingRevisionNumber, true
}

func (c *RoutingInfoCacheImpl) Put(
	namespaceID string,
	taskQueue string,
	taskQueueType enumspb.TaskQueueType,
	current *deploymentspb.WorkerDeploymentVersion,
	currentRevisionNumber int64,
	ramping *deploymentspb.WorkerDeploymentVersion,
	rampPercentage float32,
	rampingRevisionNumber int64,
) {
	handler := c.metricsHandler.WithTags(metrics.OperationTag(metrics.RoutingInfoCachePutScope), metrics.NamespaceIDTag(namespaceID))
	metrics.CacheRequests.With(handler).Record(1)

	key := routingInfoCacheKey{
		namespaceID:   namespaceID,
		taskQueue:     taskQueue,
		taskQueueType: taskQueueType,
	}
	value := routingInfoCacheValue{
		current:               current,
		currentRevisionNumber: currentRevisionNumber,
		ramping:               ramping,
		rampPercentage:        rampPercentage,
		rampingRevisionNumber: rampingRevisionNumber,
	}
	c.Cache.Put(key, value)
}
