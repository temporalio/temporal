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
	_, _, _, _, _, ok := routingCache.Get(namespaceID, taskQueue, taskQueueType)
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
	gotCurrent, gotCurrentRev, gotRamping, gotRampPct, gotRampingRev, ok := routingCache.Get(
		namespaceID,
		taskQueue,
		taskQueueType,
	)
	require.True(t, ok, "Cache should contain the entry")
	assert.Equal(t, currentVersion, gotCurrent)
	assert.Equal(t, currentRevNum, gotCurrentRev)
	assert.Equal(t, rampingVersion, gotRamping)
	assert.Equal(t, rampPercentage, gotRampPct)
	assert.Equal(t, rampingRevNum, gotRampingRev)
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
	current1, rev1, _, _, _, ok1 := routingCache.Get(namespace1, taskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	assert.True(t, ok1)
	assert.Equal(t, version1, current1)
	assert.Equal(t, int64(100), rev1)

	current2, rev2, _, _, _, ok2 := routingCache.Get(namespace2, taskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	assert.True(t, ok2)
	assert.Equal(t, version2, current2)
	assert.Equal(t, int64(200), rev2)
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
	current, rev, _, _, _, ok := routingCache.Get(namespace, taskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	assert.True(t, ok)
	assert.Equal(t, workflowVersion, current)
	assert.Equal(t, int64(100), rev)

	// Verify activity task queue type
	current, rev, _, _, _, ok = routingCache.Get(namespace, taskQueue, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	assert.True(t, ok)
	assert.Equal(t, activityVersion, current)
	assert.Equal(t, int64(200), rev)
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
	current, rev, ramping, rampPct, rampingRev, ok := routingCache.Get(namespace, taskQueue, taskQueueType)
	assert.True(t, ok)
	assert.Nil(t, current)
	assert.Equal(t, int64(0), rev)
	assert.Nil(t, ramping)
	assert.Equal(t, float32(0), rampPct)
	assert.Equal(t, int64(0), rampingRev)
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
	current, rev, _, _, _, ok := routingCache.Get(namespace, taskQueue, taskQueueType)
	assert.True(t, ok)
	assert.Equal(t, version2, current)
	assert.Equal(t, int64(200), rev)
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
	current, currentRev, ramping, rampPct, rampingRev, ok := routingCache.Get(namespace, taskQueue, taskQueueType)
	assert.True(t, ok)
	assert.Equal(t, currentVersion, current)
	assert.Equal(t, int64(150), currentRev)
	assert.Equal(t, rampingVersion, ramping)
	assert.Equal(t, float32(0.25), rampPct)
	assert.Equal(t, int64(250), rampingRev)
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
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
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
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				routingCache.Get(namespace, taskQueue, taskQueueType)
			}
		}()
	}

	wg.Wait()

	// Verify cache is in a consistent state
	_, _, _, _, _, ok := routingCache.Get(namespace, taskQueue, taskQueueType)
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
