//nolint:staticcheck
package worker_versioning

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
)

func TestRoutingInfoCache_GetPut(t *testing.T) {
	t.Parallel()

	metricsHandler := metricstest.NewCaptureHandler()
	c := cache.NewLRU(10, metrics.NoopMetricsHandler)
	routingCache := NewRoutingInfoCache(c, metricsHandler)

	namespaceID := "test-namespace"
	taskQueue := "test-task-queue"
	taskQueueType := enumspb.TASK_QUEUE_TYPE_WORKFLOW

	currentVersion := &deploymentspb.WorkerDeploymentVersion{
		DeploymentName: "deployment-1",
		BuildId:        "build-1",
	}
	currentRevNum := int64(100)

	rampingVersion := &deploymentspb.WorkerDeploymentVersion{
		DeploymentName: "deployment-2",
		BuildId:        "build-2",
	}
	rampPercentage := float32(0.3)
	rampingRevNum := int64(200)

	// Test cache miss
	_, ok := routingCache.Get(namespaceID, taskQueue, taskQueueType)
	assert.False(t, ok, "Cache should be empty initially")

	// Test Put
	routingCache.Put(
		namespaceID,
		taskQueue,
		taskQueueType,
		currentVersion,
		currentRevNum,
		rampingVersion,
		rampPercentage,
		rampingRevNum,
	)

	// Test cache hit
	result, ok := routingCache.Get(
		namespaceID,
		taskQueue,
		taskQueueType,
	)
	require.True(t, ok, "Cache should contain the entry")
	assert.Equal(t, currentVersion, result.Current)
	assert.Equal(t, currentRevNum, result.CurrentRevisionNumber)
	assert.Equal(t, rampingVersion, result.Ramping)
	assert.InDelta(t, rampPercentage, result.RampPercentage, 0.0001)
	assert.Equal(t, rampingRevNum, result.RampingRevisionNumber)
}

func TestRoutingInfoCache_DifferentKeys(t *testing.T) {
	t.Parallel()

	metricsHandler := metricstest.NewCaptureHandler()
	c := cache.NewLRU(10, metrics.NoopMetricsHandler)
	routingCache := NewRoutingInfoCache(c, metricsHandler)

	// Setup two different cache entries
	namespace1 := "namespace-1"
	namespace2 := "namespace-2"
	taskQueue := "test-queue"

	version1 := &deploymentspb.WorkerDeploymentVersion{
		DeploymentName: "deployment-1",
		BuildId:        "build-1",
	}
	version2 := &deploymentspb.WorkerDeploymentVersion{
		DeploymentName: "deployment-2",
		BuildId:        "build-2",
	}

	routingCache.Put(namespace1, taskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, version1, 100, nil, 0, 0)
	routingCache.Put(namespace2, taskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, version2, 200, nil, 0, 0)

	// Verify both entries exist and are independent
	result1, ok1 := routingCache.Get(namespace1, taskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	assert.True(t, ok1)
	assert.Equal(t, version1, result1.Current)
	assert.Equal(t, int64(100), result1.CurrentRevisionNumber)

	result2, ok2 := routingCache.Get(namespace2, taskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	assert.True(t, ok2)
	assert.Equal(t, version2, result2.Current)
	assert.Equal(t, int64(200), result2.CurrentRevisionNumber)
}

func TestRoutingInfoCache_TaskQueueTypes(t *testing.T) {
	t.Parallel()

	metricsHandler := metricstest.NewCaptureHandler()
	c := cache.NewLRU(10, metrics.NoopMetricsHandler)
	routingCache := NewRoutingInfoCache(c, metricsHandler)

	namespace := "namespace-1"
	taskQueue := "test-queue"

	workflowVersion := &deploymentspb.WorkerDeploymentVersion{
		DeploymentName: "workflow-deployment",
		BuildId:        "workflow-build",
	}
	activityVersion := &deploymentspb.WorkerDeploymentVersion{
		DeploymentName: "activity-deployment",
		BuildId:        "activity-build",
	}

	// Put different versions for different task queue types
	routingCache.Put(namespace, taskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, workflowVersion, 100, nil, 0, 0)
	routingCache.Put(namespace, taskQueue, enumspb.TASK_QUEUE_TYPE_ACTIVITY, activityVersion, 200, nil, 0, 0)

	// Verify workflow task queue type
	result, ok := routingCache.Get(namespace, taskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	assert.True(t, ok)
	assert.Equal(t, workflowVersion, result.Current)
	assert.Equal(t, int64(100), result.CurrentRevisionNumber)

	// Verify activity task queue type
	result, ok = routingCache.Get(namespace, taskQueue, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	assert.True(t, ok)
	assert.Equal(t, activityVersion, result.Current)
	assert.Equal(t, int64(200), result.CurrentRevisionNumber)
}

func TestRoutingInfoCache_NilValues(t *testing.T) {
	t.Parallel()

	metricsHandler := metricstest.NewCaptureHandler()
	c := cache.NewLRU(10, metrics.NoopMetricsHandler)
	routingCache := NewRoutingInfoCache(c, metricsHandler)

	namespace := "namespace-1"
	taskQueue := "test-queue"
	taskQueueType := enumspb.TASK_QUEUE_TYPE_WORKFLOW

	// Put with nil current and ramping (unversioned task queue)
	routingCache.Put(namespace, taskQueue, taskQueueType, nil, 0, nil, 0, 0)

	// Verify we can retrieve nil values
	result, ok := routingCache.Get(namespace, taskQueue, taskQueueType)
	assert.True(t, ok)
	assert.Nil(t, result.Current)
	assert.Equal(t, int64(0), result.CurrentRevisionNumber)
	assert.Nil(t, result.Ramping)
	assert.InDelta(t, float32(0), result.RampPercentage, 0.0001)
	assert.Equal(t, int64(0), result.RampingRevisionNumber)
}

func TestRoutingInfoCache_UpdateExisting(t *testing.T) {
	t.Parallel()

	metricsHandler := metricstest.NewCaptureHandler()
	c := cache.NewLRU(10, metrics.NoopMetricsHandler)
	routingCache := NewRoutingInfoCache(c, metricsHandler)

	namespace := "namespace-1"
	taskQueue := "test-queue"
	taskQueueType := enumspb.TASK_QUEUE_TYPE_WORKFLOW

	version1 := &deploymentspb.WorkerDeploymentVersion{
		DeploymentName: "deployment-1",
		BuildId:        "build-1",
	}
	version2 := &deploymentspb.WorkerDeploymentVersion{
		DeploymentName: "deployment-2",
		BuildId:        "build-2",
	}

	// Put initial value
	routingCache.Put(namespace, taskQueue, taskQueueType, version1, 100, nil, 0, 0)

	// Update with new value
	routingCache.Put(namespace, taskQueue, taskQueueType, version2, 200, nil, 0, 0)

	// Verify updated value
	result, ok := routingCache.Get(namespace, taskQueue, taskQueueType)
	assert.True(t, ok)
	assert.Equal(t, version2, result.Current)
	assert.Equal(t, int64(200), result.CurrentRevisionNumber)
}

func TestRoutingInfoCache_WithRamping(t *testing.T) {
	t.Parallel()

	metricsHandler := metricstest.NewCaptureHandler()
	c := cache.NewLRU(10, metrics.NoopMetricsHandler)
	routingCache := NewRoutingInfoCache(c, metricsHandler)

	namespace := "namespace-1"
	taskQueue := "test-queue"
	taskQueueType := enumspb.TASK_QUEUE_TYPE_WORKFLOW

	currentVersion := &deploymentspb.WorkerDeploymentVersion{
		DeploymentName: "current-deployment",
		BuildId:        "current-build",
	}
	rampingVersion := &deploymentspb.WorkerDeploymentVersion{
		DeploymentName: "ramping-deployment",
		BuildId:        "ramping-build",
	}

	// Put with ramping info
	routingCache.Put(
		namespace,
		taskQueue,
		taskQueueType,
		currentVersion,
		150,
		rampingVersion,
		0.25,
		250,
	)

	// Verify all values including ramping
	result, ok := routingCache.Get(namespace, taskQueue, taskQueueType)
	assert.True(t, ok)
	assert.Equal(t, currentVersion, result.Current)
	assert.Equal(t, int64(150), result.CurrentRevisionNumber)
	assert.Equal(t, rampingVersion, result.Ramping)
	assert.InDelta(t, float32(0.25), result.RampPercentage, 0.0001)
	assert.Equal(t, int64(250), result.RampingRevisionNumber)
}

func TestRoutingInfoCache_Concurrent(t *testing.T) {
	t.Parallel()

	metricsHandler := metricstest.NewCaptureHandler()
	c := cache.NewLRU(100, metrics.NoopMetricsHandler)
	routingCache := NewRoutingInfoCache(c, metricsHandler)

	namespace := "namespace-1"
	taskQueue := "test-queue"
	taskQueueType := enumspb.TASK_QUEUE_TYPE_WORKFLOW

	var wg sync.WaitGroup
	numGoroutines := 10
	numOperations := 100

	// Concurrent writes
	for i := range numGoroutines {
		idx := i
		wg.Add(1) //nolint:revive // use-waitgroup-go: standard sync.WaitGroup doesn't have Go() method
		go func() {
			defer wg.Done()
			for j := range numOperations {
				version := &deploymentspb.WorkerDeploymentVersion{
					DeploymentName: "deployment",
					BuildId:        "build",
				}
				routingCache.Put(
					namespace,
					taskQueue,
					taskQueueType,
					version,
					int64(idx*numOperations+j),
					nil,
					0,
					0,
				)
			}
		}()
	}

	// Concurrent reads
	for range numGoroutines {
		wg.Add(1) //nolint:revive // use-waitgroup-go: standard sync.WaitGroup doesn't have Go() method
		go func() {
			defer wg.Done()
			for range numOperations {
				routingCache.Get(namespace, taskQueue, taskQueueType)
			}
		}()
	}

	wg.Wait()

	// Verify cache is in a consistent state
	_, ok := routingCache.Get(namespace, taskQueue, taskQueueType)
	assert.True(t, ok, "Cache should contain an entry after concurrent operations")
}

func TestRoutingInfoCache_Metrics(t *testing.T) {
	t.Parallel()

	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()

	c := cache.NewLRU(10, metrics.NoopMetricsHandler)
	routingCache := NewRoutingInfoCache(c, metricsHandler)

	namespace := "namespace-1"
	taskQueue := "test-queue"
	taskQueueType := enumspb.TASK_QUEUE_TYPE_WORKFLOW

	version := &deploymentspb.WorkerDeploymentVersion{
		DeploymentName: "deployment",
		BuildId:        "build",
	}

	// Test cache miss - should record cache request and cache miss
	routingCache.Get(namespace, taskQueue, taskQueueType)

	// Test Put - should record cache request
	routingCache.Put(namespace, taskQueue, taskQueueType, version, 100, nil, 0, 0)

	// Test cache hit - should record cache request only (no miss)
	routingCache.Get(namespace, taskQueue, taskQueueType)

	snapshot := capture.Snapshot()

	// Verify cache requests were recorded
	cacheRequests, ok := snapshot[metrics.CacheRequests.Name()]
	assert.True(t, ok, "CacheRequests metric should be present")
	assert.NotEmpty(t, cacheRequests, "CacheRequests should have entries")

	// Verify cache misses were recorded
	cacheMisses, ok := snapshot[metrics.CacheMissCounter.Name()]
	assert.True(t, ok, "CacheMissCounter metric should be present")
	assert.NotEmpty(t, cacheMisses, "CacheMissCounter should have entries")
}
