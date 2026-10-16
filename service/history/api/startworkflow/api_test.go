package startworkflow

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

const (
	testDeriveNamespaceID = "my-namespace-1234-5678-9abc-def012345678"
	testDeriveWorkflowID  = "my-workflow-id"
	testDeriveRequestID   = "my-request-id"
	testDeriveArchetypeID = 1
	testDeriveClusterID   = 1
)

func TestDeriveRunID_Deterministic(t *testing.T) {
	got := DeriveRunID(testDeriveNamespaceID, testDeriveWorkflowID, testDeriveArchetypeID, testDeriveRequestID, testDeriveClusterID)
	require.Equal(t, "291b333c-b896-5d8c-a196-d791dfe6ca28", got)
	second := DeriveRunID(testDeriveNamespaceID, testDeriveWorkflowID, testDeriveArchetypeID, testDeriveRequestID, testDeriveClusterID)
	require.Equal(t, got, second)

	parsed, err := uuid.Parse(got)
	require.NoError(t, err)
	require.Equal(t, uuid.Version(5), parsed.Version())
}

func TestDeriveRunID_EveryFieldMatters(t *testing.T) {
	base := DeriveRunID(testDeriveNamespaceID, testDeriveWorkflowID, testDeriveArchetypeID, testDeriveRequestID, testDeriveClusterID)

	for name, got := range map[string]string{
		"different namespaceID": DeriveRunID("11111111-2222-3333-4444-555555555555", testDeriveWorkflowID, testDeriveArchetypeID, testDeriveRequestID, testDeriveClusterID),
		"different workflowID":  DeriveRunID(testDeriveNamespaceID, "other-workflow-id", testDeriveArchetypeID, testDeriveRequestID, testDeriveClusterID),
		"different archetypeID": DeriveRunID(testDeriveNamespaceID, testDeriveWorkflowID, testDeriveArchetypeID+1, testDeriveRequestID, testDeriveClusterID),
		"different requestID":   DeriveRunID(testDeriveNamespaceID, testDeriveWorkflowID, testDeriveArchetypeID, "other-request-id", testDeriveClusterID),
		"empty requestID":       DeriveRunID(testDeriveNamespaceID, testDeriveWorkflowID, testDeriveArchetypeID, "", testDeriveClusterID),
		"different clusterID":   DeriveRunID(testDeriveNamespaceID, testDeriveWorkflowID, testDeriveArchetypeID, testDeriveRequestID, testDeriveClusterID+1),
	} {
		require.NotEqual(t, base, got, "%s must derive a different run ID", name)
	}
}

// TestDeriveRunID_FieldsAreUnambiguouslyFramed guards the length-prefixing.
func TestDeriveRunID_FieldsAreUnambiguouslyFramed(t *testing.T) {
	left := DeriveRunID(testDeriveNamespaceID, "a", testDeriveArchetypeID, "bc", testDeriveClusterID)
	right := DeriveRunID(testDeriveNamespaceID, "ab", testDeriveArchetypeID, "c", testDeriveClusterID)
	require.NotEqual(t, left, right)
}

func TestPrepareEagerDispatchCheck(t *testing.T) {
	t.Parallel()

	priority := &commonpb.Priority{PriorityKey: 2}
	testCases := []struct {
		name               string
		checkEnabled       bool
		response           *matchingservice.GrantEagerDispatchResponse
		matchingErr        error
		blockUntilDeadline bool
		wantEager          bool
	}{
		{
			name:      "check disabled",
			wantEager: true,
		},
		{
			name:         "granted",
			checkEnabled: true,
			response: &matchingservice.GrantEagerDispatchResponse{Items: []*matchingservice.GrantEagerDispatchResponse_Item{
				{GrantedCount: 1},
			}},
			wantEager: true,
		},
		{
			name:         "denied",
			checkEnabled: true,
			response: &matchingservice.GrantEagerDispatchResponse{Items: []*matchingservice.GrantEagerDispatchResponse_Item{
				{},
			}},
		},
		{
			name:         "matching error",
			checkEnabled: true,
			matchingErr:  errors.New("matching unavailable"),
		},
		{
			name:               "matching timeout",
			checkEnabled:       true,
			blockUntilDeadline: true,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			configClient := dynamicconfig.NewMemoryClient()
			configClient.OverrideSetting(dynamicconfig.EnableWorkflowEagerDispatchCheck, test.checkEnabled)
			config := configs.NewConfig(dynamicconfig.NewCollection(configClient, log.NewNoopLogger()), 1)
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			shardContext := historyi.NewMockShardContext(ctrl)
			shardContext.EXPECT().GetConfig().Return(config).AnyTimes()
			shardContext.EXPECT().GetMetricsHandler().Return(metricsHandler).AnyTimes()
			shardContext.EXPECT().GetLogger().Return(log.NewNoopLogger()).AnyTimes()
			shardContext.EXPECT().GetThrottledLogger().Return(log.NewNoopLogger()).AnyTimes()

			matchingClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
			if test.checkEnabled {
				matchingClient.EXPECT().GrantEagerDispatch(gomock.Any(), protomock.Eq(&matchingservice.GrantEagerDispatchRequest{
					NamespaceId: tests.NamespaceID.String(),
					TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
						TaskQueue:     "workflow-task-queue",
						TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
					},
					Items: []*matchingservice.GrantEagerDispatchRequest_Item{{Count: 1, Priority: priority}},
				})).DoAndReturn(func(ctx context.Context, _ *matchingservice.GrantEagerDispatchRequest, _ ...grpc.CallOption) (*matchingservice.GrantEagerDispatchResponse, error) {
					if test.blockUntilDeadline {
						<-ctx.Done()
						return nil, ctx.Err()
					}
					return test.response, test.matchingErr
				})
			}

			starter := &Starter{
				shardContext:   shardContext,
				matchingClient: matchingClient,
				namespace:      tests.LocalNamespaceEntry,
				request: &historyservice.StartWorkflowExecutionRequest{
					NamespaceId: tests.NamespaceID.String(),
					StartRequest: &workflowservice.StartWorkflowExecutionRequest{
						Namespace:             tests.Namespace.String(),
						RequestId:             "request-id",
						WorkflowId:            "workflow-id",
						WorkflowType:          &commonpb.WorkflowType{Name: "workflow-type"},
						TaskQueue:             &taskqueuepb.TaskQueue{Name: "workflow-task-queue"},
						RequestEagerExecution: true,
						Priority:              priority,
					},
				},
			}
			require.NoError(t, starter.prepare(t.Context()))
			require.Equal(t, test.wantEager, starter.requestEagerStart())
			require.Equal(t, !test.wantEager, starter.eagerDispatchNotGranted)

			var sentTaskQueueTypes []string
			for _, recording := range capture.Snapshot()[metrics.EagerDispatchRequestsSent.Name()] {
				sentTaskQueueTypes = append(sentTaskQueueTypes, recording.Tags[metrics.TaskTypeTagName])
			}
			if test.checkEnabled {
				require.Equal(t, []string{enumspb.TASK_QUEUE_TYPE_WORKFLOW.String()}, sentTaskQueueTypes)
			} else {
				require.Empty(t, sentTaskQueueTypes)
			}

			reasonTag := metrics.ReasonTag(eagerStartDeniedReasonDispatchNotGranted)
			var deniedReasons []string
			for _, recording := range capture.Snapshot()[metrics.WorkflowEagerExecutionDeniedCounter.Name()] {
				deniedReasons = append(deniedReasons, recording.Tags[reasonTag.Key])
			}
			if test.wantEager {
				require.Empty(t, deniedReasons)
			} else {
				require.Equal(t, []string{reasonTag.Value}, deniedReasons)
			}
		})
	}
}

func TestGrantEagerWorkflowDispatchVersion(t *testing.T) {
	t.Parallel()

	version := &deploymentpb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "build-id"}
	testCases := []struct {
		name        string
		override    *workflowpb.VersioningOverride
		wantVersion *deploymentspb.WorkerDeploymentVersion
	}{
		{
			name: "no override",
		},
		{
			name:     "auto-upgrade override",
			override: &workflowpb.VersioningOverride{Override: &workflowpb.VersioningOverride_AutoUpgrade{AutoUpgrade: true}},
		},
		{
			name: "pinned override",
			override: &workflowpb.VersioningOverride{Override: &workflowpb.VersioningOverride_Pinned{
				Pinned: &workflowpb.VersioningOverride_PinnedOverride{
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
					Version:  version,
				},
			}},
			wantVersion: &deploymentspb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "build-id"},
		},
		{
			name: "one-time override",
			override: &workflowpb.VersioningOverride{Override: &workflowpb.VersioningOverride_OneTime{
				OneTime: &workflowpb.VersioningOverride_OneTimeOverride{TargetDeploymentVersion: version},
			}},
			wantVersion: &deploymentspb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "build-id"},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			matchingClient := matchingservicemock.NewMockMatchingServiceClient(ctrl)
			matchingClient.EXPECT().GrantEagerDispatch(gomock.Any(), protomock.Eq(&matchingservice.GrantEagerDispatchRequest{
				NamespaceId: tests.NamespaceID.String(),
				TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
					TaskQueue:     "workflow-task-queue",
					TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				},
				Items: []*matchingservice.GrantEagerDispatchRequest_Item{{Count: 1, Version: test.wantVersion}},
			})).Return(&matchingservice.GrantEagerDispatchResponse{Items: []*matchingservice.GrantEagerDispatchResponse_Item{
				{GrantedCount: 1},
			}}, nil)

			starter := &Starter{
				metricsHandler: metrics.NoopMetricsHandler,
				matchingClient: matchingClient,
				namespace:      tests.LocalNamespaceEntry,
				request: &historyservice.StartWorkflowExecutionRequest{
					StartRequest: &workflowservice.StartWorkflowExecutionRequest{
						TaskQueue:          &taskqueuepb.TaskQueue{Name: "workflow-task-queue"},
						VersioningOverride: test.override,
					},
				},
			}
			require.True(t, starter.grantEagerWorkflowDispatch(context.Background()))
		})
	}
}
