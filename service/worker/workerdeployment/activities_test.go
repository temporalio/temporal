package workerdeployment

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/testsuite"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

func TestDeleteWorkerDeploymentVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		historyErr error
		wantErr    bool
	}{
		{
			name:       "version workflow not found",
			historyErr: serviceerror.NewNotFound("version workflow not found"),
		},
		{
			name:       "other history error",
			historyErr: serviceerror.NewUnavailable("history unavailable"),
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			controller := gomock.NewController(t)
			historyClient := historyservicemock.NewMockHistoryServiceClient(controller)
			historyClient.EXPECT().
				UpdateWorkflowExecution(gomock.Any(), gomock.Any()).
				Return(nil, tt.historyErr)

			tv := testvars.New(t)
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)
			activity := &Activities{
				activityDeps: activityDeps{
					HistoryClient:  historyClient,
					MetricsHandler: metricsHandler,
				},
				namespace: namespace.NewLocalNamespaceForTest(
					&persistencespb.NamespaceInfo{Id: tv.NamespaceID().String(), Name: tv.NamespaceName().String()},
					nil,
					"",
				),
			}
			env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
			env.RegisterActivity(activity)

			_, err := env.ExecuteActivity(activity.DeleteWorkerDeploymentVersion, &deploymentspb.DeleteVersionActivityArgs{
				DeploymentName: tv.DeploymentSeries(),
				Version:        tv.DeploymentVersionString(),
				RequestId:      tv.RequestID(),
			})
			if tt.wantErr {
				require.Error(t, err)
				require.Empty(t, capture.Snapshot()[metrics.WorkerDeploymentVersionNotFoundDuringDelete.Name()])
			} else {
				require.NoError(t, err)
				recordings := capture.Snapshot()[metrics.WorkerDeploymentVersionNotFoundDuringDelete.Name()]
				require.Len(t, recordings, 1)
				require.Equal(t, int64(1), recordings[0].Value)
				namespaceTag := metrics.NamespaceTag(tv.NamespaceName().String())
				require.Equal(t, namespaceTag.Value, recordings[0].Tags[namespaceTag.Key])
				deploymentTag := metrics.WorkerDeploymentNameTag(tv.DeploymentSeries(), true)
				require.Equal(t, deploymentTag.Value, recordings[0].Tags[deploymentTag.Key])
				buildIDTag := metrics.WorkerDeploymentBuildIDTag(tv.BuildID(), true)
				require.Equal(t, buildIDTag.Value, recordings[0].Tags[buildIDTag.Key])
			}
		})
	}
}

func TestSyncDeploymentVersionUserDataRoutingConfigTarget(t *testing.T) {
	t.Parallel()
	controller := gomock.NewController(t)
	client := matchingservicemock.NewMockMatchingServiceClient(controller)
	tv := testvars.New(t)
	types := []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_ACTIVITY}
	routingConfig := &deploymentpb.RoutingConfig{RevisionNumber: 42}
	expected := &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId: tv.NamespaceID().String(), DeploymentName: "deployment", TaskQueue: "queue",
		TaskQueueTypes: types, UpdateRoutingConfig: routingConfig,
	}
	client.EXPECT().SyncDeploymentUserData(gomock.Any(), gomock.Cond(func(req *matchingservice.SyncDeploymentUserDataRequest) bool { return proto.Equal(req, expected) })).Return(&matchingservice.SyncDeploymentUserDataResponse{Version: 100}, nil)
	a := &VersionActivities{
		activityDeps: activityDeps{MatchingClient: client, MetricsHandler: metrics.NoopMetricsHandler},
		namespace:    namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: tv.NamespaceID().String()}, nil, ""),
	}
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(a)
	result, err := env.ExecuteActivity(a.SyncDeploymentVersionUserData, &deploymentspb.SyncDeploymentVersionUserDataRequest{
		Version:             &deploymentspb.WorkerDeploymentVersion{DeploymentName: "deployment"},
		UpdateRoutingConfig: routingConfig,
		Sync:                []*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData{{Name: "queue", Types: types}},
	})
	require.NoError(t, err)
	var response deploymentspb.SyncDeploymentVersionUserDataResponse
	require.NoError(t, result.Get(&response))
	require.Equal(t, map[string]int64{"queue": 100}, response.GetTaskQueueMaxVersions())
	require.Equal(t, "deployment", response.GetDeploymentName())
	require.Equal(t, int64(42), response.GetRevisionNumber())
	require.Len(t, response.GetTaskQueues(), 1)
	require.True(t, proto.Equal(&deploymentspb.TaskQueuePropagationTarget{
		Name: "queue", TaskQueueTypes: types,
	}, response.GetTaskQueues()[0]))
}

func TestWorkerDeploymentPropagationActivitiesRoutingConfigTarget(t *testing.T) {
	t.Parallel()
	for _, versionActivity := range []bool{false, true} {
		for _, legacyVersion := range []int64{0, 100} {
			t.Run(fmt.Sprintf("version-activity=%t/legacy-version=%d", versionActivity, legacyVersion), func(t *testing.T) {
				t.Parallel()
				controller := gomock.NewController(t)
				client := matchingservicemock.NewMockMatchingServiceClient(controller)
				tv := testvars.New(t)
				target := &deploymentspb.RoutingConfigPropagationTarget{
					DeploymentName: "deployment", RevisionNumber: 42,
					TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_ACTIVITY},
				}
				expected := &matchingservice.CheckTaskQueueUserDataPropagationRequest{
					NamespaceId: tv.NamespaceID().String(), TaskQueue: "queue", Version: legacyVersion, RoutingConfigTarget: target,
				}
				client.EXPECT().CheckTaskQueueUserDataPropagation(gomock.Any(), gomock.Cond(func(req *matchingservice.CheckTaskQueueUserDataPropagationRequest) bool {
					return proto.Equal(req, expected)
				})).Return(&matchingservice.CheckTaskQueueUserDataPropagationResponse{}, nil)
				deps := activityDeps{MatchingClient: client, MetricsHandler: metrics.NoopMetricsHandler}
				ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: tv.NamespaceID().String()}, nil, "")
				env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
				env.SetTestTimeout(5 * time.Second)
				input := &deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest{
					DeploymentName: "deployment", RevisionNumber: 42,
					TaskQueues: []*deploymentspb.TaskQueuePropagationTarget{{Name: "queue", TaskQueueTypes: target.TaskQueueTypes}},
				}
				if legacyVersion > 0 {
					input.TaskQueueMaxVersions = map[string]int64{"queue": legacyVersion}
				}
				var err error
				if versionActivity {
					a := &VersionActivities{activityDeps: deps, namespace: ns}
					env.RegisterActivity(a)
					_, err = env.ExecuteActivity(a.CheckWorkerDeploymentUserDataPropagation, input)
				} else {
					a := &Activities{activityDeps: deps, namespace: ns}
					env.RegisterActivity(a)
					_, err = env.ExecuteActivity(a.CheckUnversionedRampUserDataPropagation, input)
				}
				require.NoError(t, err)
			})
		}
	}
}
