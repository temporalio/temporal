package recordactivitytaskstarted

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/worker_versioning"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// mockRoutingInfoCache is a simple mock implementation of RoutingInfoCache for testing
type mockRoutingInfoCache struct {
	getCalled   bool
	putCalled   bool
	cacheHit    bool
	cachedData  *worker_versioning.RoutingInfo
	lastPutData *worker_versioning.RoutingInfo
}

func newMockRoutingInfoCache() *mockRoutingInfoCache {
	return &mockRoutingInfoCache{}
}

func (m *mockRoutingInfoCache) Get(
	namespaceID string,
	taskQueue string,
	taskQueueType enumspb.TaskQueueType,
) (worker_versioning.RoutingInfo, bool) {
	m.getCalled = true
	if !m.cacheHit || m.cachedData == nil {
		return worker_versioning.RoutingInfo{}, false
	}
	return *m.cachedData, true
}

func (m *mockRoutingInfoCache) Put(
	namespaceID string,
	taskQueue string,
	taskQueueType enumspb.TaskQueueType,
	current *deploymentspb.WorkerDeploymentVersion,
	currentRevisionNumber int64,
	ramping *deploymentspb.WorkerDeploymentVersion,
	rampPercentage float32,
	rampingRevisionNumber int64,
) {
	m.putCalled = true
	m.lastPutData = &worker_versioning.RoutingInfo{
		Current:               current,
		CurrentRevisionNumber: currentRevisionNumber,
		Ramping:               ramping,
		RampPercentage:        rampPercentage,
		RampingRevisionNumber: rampingRevisionNumber,
	}
}

func TestGetDeploymentVersionForWorkflowID_CacheHit(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()

	mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
	mockCache := newMockRoutingInfoCache()

	// Setup cache to return a hit
	mockCache.cacheHit = true
	mockCache.cachedData = &worker_versioning.RoutingInfo{
		Current: &deploymentspb.WorkerDeploymentVersion{
			DeploymentName: "deployment-1",
			BuildId:        "build-1",
		},
		CurrentRevisionNumber: 100,
		Ramping: &deploymentspb.WorkerDeploymentVersion{
			DeploymentName: "deployment-2",
			BuildId:        "build-2",
		},
		RampPercentage:        0.3,
		RampingRevisionNumber: 200,
	}

	// Matching client should NOT be called when cache hits
	mockMatchingClient.EXPECT().GetTaskQueueUserData(gomock.Any(), gomock.Any()).Times(0)

	ctx := context.Background()
	namespaceID := "test-namespace"
	taskQueue := "test-queue"
	workflowID := "test-workflow-1"

	// Call the function
	targetVersion, targetRevNum, err := getDeploymentVersionAndRevisionNumberForWorkflowID(
		ctx,
		namespaceID,
		taskQueue,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		mockMatchingClient,
		mockCache,
		workflowID,
	)

	// Verify cache was queried
	assert.True(t, mockCache.getCalled, "Cache Get should have been called")
	assert.False(t, mockCache.putCalled, "Cache Put should NOT have been called on cache hit")

	// Verify result is based on cached data
	require.NoError(t, err)
	assert.NotNil(t, targetVersion)

	// The result should be computed using FindTargetDeploymentVersionAndRevisionNumberForWorkflowID
	// with the cached routing info
	expectedVersion, expectedRevNum := worker_versioning.FindTargetDeploymentVersionAndRevisionNumberForWorkflowID(
		mockCache.cachedData.Current,
		mockCache.cachedData.CurrentRevisionNumber,
		mockCache.cachedData.Ramping,
		mockCache.cachedData.RampPercentage,
		mockCache.cachedData.RampingRevisionNumber,
		workflowID,
	)
	assert.Equal(t, expectedVersion, targetVersion)
	assert.Equal(t, expectedRevNum, targetRevNum)
}

func TestGetDeploymentVersionForWorkflowID_CacheMiss(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()

	mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
	mockCache := newMockRoutingInfoCache()

	// Setup cache to return a miss
	mockCache.cacheHit = false

	currentTime := timestamppb.Now()

	currentVersion := &deploymentspb.WorkerDeploymentVersion{
		DeploymentName: "current-deployment",
		BuildId:        "current-build",
	}

	// Mock matching client response with VersionedTaskQueueUserData
	mockMatchingClient.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:   "test-namespace",
			TaskQueue:     "test-queue",
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		},
	).Return(&matchingservice.GetTaskQueueUserDataResponse{
		UserData: &persistencespb.VersionedTaskQueueUserData{
			Version: 1,
			Data: &persistencespb.TaskQueueUserData{
				PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
					int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {
						DeploymentData: &persistencespb.DeploymentData{
							Versions: []*deploymentspb.DeploymentVersionData{
								{
									Version:           currentVersion,
									CurrentSinceTime:  currentTime,
									RoutingUpdateTime: currentTime,
								},
							},
						},
					},
				},
			},
		},
	}, nil)

	ctx := context.Background()
	namespaceID := "test-namespace"
	taskQueue := "test-queue"
	workflowID := "test-workflow-1"

	// Call the function
	targetVersion, _, err := getDeploymentVersionAndRevisionNumberForWorkflowID(
		ctx,
		namespaceID,
		taskQueue,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		mockMatchingClient,
		mockCache,
		workflowID,
	)

	// Verify cache was queried and populated
	assert.True(t, mockCache.getCalled, "Cache Get should have been called")
	assert.True(t, mockCache.putCalled, "Cache Put should have been called on cache miss")

	// Verify the cached data was populated correctly
	require.NotNil(t, mockCache.lastPutData)
	assert.Equal(t, currentVersion, mockCache.lastPutData.Current)

	// Verify result
	require.NoError(t, err)
	assert.NotNil(t, targetVersion)
}

func TestGetDeploymentVersionForWorkflowID_UnversionedTaskQueue(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()

	mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
	mockCache := newMockRoutingInfoCache()

	// Setup cache miss
	mockCache.cacheHit = false

	// Mock matching client response with no versioning data
	mockMatchingClient.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		gomock.Any(),
	).Return(&matchingservice.GetTaskQueueUserDataResponse{
		UserData: &persistencespb.VersionedTaskQueueUserData{
			Version: 1,
			Data: &persistencespb.TaskQueueUserData{
				PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
					// No entry for TASK_QUEUE_TYPE_WORKFLOW - unversioned
				},
			},
		},
	}, nil)

	ctx := context.Background()
	namespaceID := "test-namespace"
	taskQueue := "unversioned-queue"
	workflowID := "test-workflow-1"

	// Call the function
	targetVersion, targetRevNum, err := getDeploymentVersionAndRevisionNumberForWorkflowID(
		ctx,
		namespaceID,
		taskQueue,
		enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		mockMatchingClient,
		mockCache,
		workflowID,
	)

	// Verify cache was queried and populated even for unversioned task queue
	assert.True(t, mockCache.getCalled, "Cache Get should have been called")
	assert.True(t, mockCache.putCalled, "Cache Put should have been called even for unversioned task queue")

	// Verify the cached data for unversioned task queue (all nil/zero)
	require.NotNil(t, mockCache.lastPutData)
	assert.Nil(t, mockCache.lastPutData.Current, "Cached current should be nil for unversioned")
	assert.Equal(t, int64(0), mockCache.lastPutData.CurrentRevisionNumber, "Cached current rev should be 0")
	assert.Nil(t, mockCache.lastPutData.Ramping, "Cached ramping should be nil for unversioned")
	assert.InDelta(t, float32(0), mockCache.lastPutData.RampPercentage, 0.0001, "Cached ramp percentage should be 0")
	assert.Equal(t, int64(0), mockCache.lastPutData.RampingRevisionNumber, "Cached ramping rev should be 0")

	// Verify result for unversioned task queue
	require.NoError(t, err)
	assert.Nil(t, targetVersion, "Unversioned task queue should return nil version")
	assert.Equal(t, int64(0), targetRevNum, "Unversioned task queue should return 0 revision number")
}
