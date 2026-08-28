package workerdeployment

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
)

func TestRegisterTaskQueueWorkerErrorCacheScopes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		errorType string
		hits      []registerTaskQueueWorkerErrorCacheKey
		misses    []registerTaskQueueWorkerErrorCacheKey
	}{
		{
			name:      "too many deployments",
			errorType: errTooManyDeployments,
			hits: []registerTaskQueueWorkerErrorCacheKey{
				{namespaceID: "namespace-a", deploymentName: "deployment-b", buildID: "build-b"},
			},
			misses: []registerTaskQueueWorkerErrorCacheKey{
				{namespaceID: "namespace-b", deploymentName: "deployment-a", buildID: "build-a"},
			},
		},
		{
			name:      "too many versions",
			errorType: errTooManyVersions,
			hits: []registerTaskQueueWorkerErrorCacheKey{
				{namespaceID: "namespace-a", deploymentName: "deployment-a", buildID: "build-b"},
			},
			misses: []registerTaskQueueWorkerErrorCacheKey{
				{namespaceID: "namespace-a", deploymentName: "deployment-b", buildID: "build-a"},
				{namespaceID: "namespace-b", deploymentName: "deployment-a", buildID: "build-a"},
			},
		},
		{
			name:      "too many task queues",
			errorType: errMaxTaskQueuesInVersionType,
			hits: []registerTaskQueueWorkerErrorCacheKey{
				{namespaceID: "namespace-a", deploymentName: "deployment-a", buildID: "build-a"},
			},
			misses: []registerTaskQueueWorkerErrorCacheKey{
				{namespaceID: "namespace-a", deploymentName: "deployment-a", buildID: "build-b"},
				{namespaceID: "namespace-a", deploymentName: "deployment-b", buildID: "build-a"},
				{namespaceID: "namespace-b", deploymentName: "deployment-a", buildID: "build-a"},
			},
		},
		{
			name:      "other error",
			errorType: "other",
			hits: []registerTaskQueueWorkerErrorCacheKey{
				{namespaceID: "namespace-a", deploymentName: "deployment-a", buildID: "build-b"},
			},
			misses: []registerTaskQueueWorkerErrorCacheKey{
				{namespaceID: "namespace-a", deploymentName: "deployment-b", buildID: "build-a"},
				{namespaceID: "namespace-b", deploymentName: "deployment-a", buildID: "build-a"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			errorCache := cache.New(100, &cache.Options{TTL: time.Minute})
			t.Cleanup(errorCache.Stop)
			client := &ClientImpl{registerTaskQueueWorkerErrorCache: errorCache}
			cachedErr := errors.New(test.name)

			client.cacheRegisterTaskQueueWorkerError("namespace-a", "deployment-a", "build-a", test.errorType, cachedErr)

			for _, key := range test.hits {
				actualErrorType, actualErr := client.getRegisterTaskQueueWorkerError(key.namespaceID, key.deploymentName, key.buildID)
				require.ErrorIs(t, actualErr, cachedErr)
				require.Equal(t, test.errorType, actualErrorType)
			}
			for _, key := range test.misses {
				actualErrorType, actualErr := client.getRegisterTaskQueueWorkerError(key.namespaceID, key.deploymentName, key.buildID)
				require.NoError(t, actualErr)
				require.Empty(t, actualErrorType)
			}
		})
	}
}

func TestRegisterTaskQueueWorkerErrorCacheOnlyCachesErrorsUntilTTL(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	ttl := 10 * time.Second
	errorCache := cache.New(100, &cache.Options{
		TTL:        ttl,
		TimeSource: timeSource,
	})
	t.Cleanup(errorCache.Stop)
	client := &ClientImpl{registerTaskQueueWorkerErrorCache: errorCache}

	client.cacheRegisterTaskQueueWorkerError("namespace-a", "deployment-a", "build-a", "", nil)
	require.Zero(t, errorCache.Size())

	cachedErr := errors.New("cached error")
	client.cacheRegisterTaskQueueWorkerError("namespace-a", "deployment-a", "build-a", "", cachedErr)
	_, actualErr := client.getRegisterTaskQueueWorkerError("namespace-a", "deployment-a", "build-b")
	require.ErrorIs(t, actualErr, cachedErr)

	timeSource.Advance(ttl + time.Nanosecond)
	_, actualErr = client.getRegisterTaskQueueWorkerError("namespace-a", "deployment-a", "build-b")
	require.NoError(t, actualErr)
}

func TestRecordRegisterTaskQueueWorkerError(t *testing.T) {
	t.Parallel()

	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	t.Cleanup(func() { metricsHandler.StopCapture(capture) })
	client := &ClientImpl{metricsHandler: metricsHandler}
	err := errors.New("registration failed")

	client.recordRegisterTaskQueueWorkerError("namespace-a", errTooManyVersions, err, false)
	client.recordRegisterTaskQueueWorkerError("namespace-a", errTooManyVersions, err, true)

	recordings := capture.Snapshot()[metrics.WorkerDeploymentRegisterTaskQueueErrors.Name()]
	require.Len(t, recordings, 2)
	require.Equal(t, int64(1), recordings[0].Value)
	require.Equal(t, map[string]string{
		"namespace":              "namespace-a",
		metrics.ErrorTypeTagName: errTooManyVersions,
		metrics.CacheHitTagName:  "false",
	}, recordings[0].Tags)
	require.Equal(t, int64(1), recordings[1].Value)
	require.Equal(t, map[string]string{
		"namespace":              "namespace-a",
		metrics.ErrorTypeTagName: errTooManyVersions,
		metrics.CacheHitTagName:  "true",
	}, recordings[1].Tags)
}
