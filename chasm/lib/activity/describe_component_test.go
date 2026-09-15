package activity

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// decodeRegistry builds the registry a reader gets: libraries with nil handlers and no config.
func decodeRegistry(t *testing.T) *chasm.Registry {
	t.Helper()
	registry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewNilLibrary()))
	require.NoError(t, registry.Register(callback.NewNilLibrary()))
	return registry
}

func describeTestBackend() *chasm.MockNodeBackend {
	return &chasm.MockNodeBackend{
		HandleGetWorkflowKey: func() definition.WorkflowKey {
			return definition.NewWorkflowKey("ns-id", "my-activity-id", "run-id-1")
		},
		HandleGetExecutionInfo: func() *persistencespb.WorkflowExecutionInfo {
			return &persistencespb.WorkflowExecutionInfo{
				StateTransitionCount: 7,
				CloseTime:            timestamppb.New(time.Unix(900, 0).UTC()),
			}
		},
		HandleGetApproximatePersistedSize: func() int { return 4242 },
		HandleNextTransitionCount:         func() int64 { return 1 },
		HandleGetCurrentVersion:           func() int64 { return 1 },
	}
}

// newClosedTestActivity exercises every field the projection reads, so a field lost in
// persistence shows up as a diff rather than an unnoticed zero value.
func newClosedTestActivity(ctx chasm.MutableContext) *Activity {
	return &Activity{
		ActivityState: &activitypb.ActivityState{
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
			ActivityType:           &commonpb.ActivityType{Name: "MyActivity"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "my-tq"},
			ScheduleTime:           timestamppb.New(time.Unix(100, 0).UTC()),
			ScheduleToCloseTimeout: durationpb.New(time.Hour),
			ScheduleToStartTimeout: durationpb.New(10 * time.Minute),
			StartToCloseTimeout:    durationpb.New(30 * time.Minute),
			HeartbeatTimeout:       durationpb.New(time.Minute),
			StartDelay:             durationpb.New(5 * time.Second),
			RetryPolicy:            &commonpb.RetryPolicy{MaximumAttempts: 5},
			CancelState:            &activitypb.ActivityCancelState{Reason: "no-longer-needed"},
		},
		Visibility: chasm.NewComponentField(ctx, chasm.NewVisibility(ctx)),
		LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
			Count:              3,
			StartedTime:        timestamppb.New(time.Unix(500, 0).UTC()),
			CompleteTime:       timestamppb.New(time.Unix(900, 0).UTC()),
			LastWorkerIdentity: "worker-1",
			SdkName:            "go-sdk",
			SdkVersion:         "1.2.3",
			LastDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: "test-deployment",
				BuildId:        "test-build-1",
			},
			CurrentRetryInterval: durationpb.New(15 * time.Second),
			LastFailureDetails: &activitypb.ActivityAttemptState_LastFailureDetails{
				Failure: &failurepb.Failure{Message: "boom"},
			},
		}),
		LastHeartbeat: chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{
			RecordedTime:        timestamppb.New(time.Unix(800, 0).UTC()),
			TotalHeartbeatCount: 12,
			Details:             &commonpb.Payloads{},
		}),
		RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
			Input:  &commonpb.Payloads{},
			Header: &commonpb.Header{Fields: map[string]*commonpb.Payload{"k": {}}},
		}),
		Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{
			Variant: &activitypb.ActivityOutcome_Failed_{
				Failed: &activitypb.ActivityOutcome_Failed{
					Failure: &failurepb.Failure{Message: "boom"},
				},
			},
		}),
	}
}

// TestDescribeComponent_MatchesLiveDescribe asserts that describing from persisted nodes, with
// nil handlers and no live execution, yields exactly what a live DescribeActivityExecution
// would. A field rename or projection change here cannot then quietly degrade a detached read.
func TestDescribeComponent_MatchesLiveDescribe(t *testing.T) {
	logger := log.NewTestLogger()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Unix(1000, 0).UTC())

	registry := decodeRegistry(t)
	backend := describeTestBackend()

	// Build the activity the way the server does, then persist it.
	liveRoot := chasm.NewEmptyTree(registry, timeSource, backend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	liveCtx := chasm.NewMutableContext(context.Background(), liveRoot)
	require.NoError(t, liveRoot.SetRootComponent(newClosedTestActivity(liveCtx)))
	_, err := liveRoot.CloseTransaction()
	require.NoError(t, err)

	// What a live DescribeActivityExecution with all details requested would return.
	liveReadCtx := chasm.NewContext(context.Background(), liveRoot)
	liveComponent, err := liveRoot.Component(liveReadCtx, chasm.ComponentRef{})
	require.NoError(t, err)
	liveDescribe, err := liveComponent.(*Activity).buildDescribeActivityExecutionResponse(
		liveReadCtx,
		&activitypb.DescribeActivityExecutionRequest{
			FrontendRequest: &workflowservice.DescribeActivityExecutionRequest{
				IncludeInput:            true,
				IncludeOutcome:          true,
				IncludeHeartbeatDetails: true,
				IncludeLastFailure:      true,
			},
		},
	)
	require.NoError(t, err)

	expected := liveDescribe.GetFrontendResponse()
	require.NotEmpty(t, expected.GetLongPollToken(), "describe must issue a token, so clearing it below is meaningful")
	expected.LongPollToken = nil // absent from a detached read by design

	// Same route a reader takes: persisted nodes in, described state out.
	persistedNodes := liveRoot.Snapshot(nil).Nodes
	detachedRoot, err := chasm.NewTreeFromDB(
		persistedNodes, registry, timeSource, backend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	require.NoError(t, err)

	detachedCtx := chasm.NewContext(context.Background(), detachedRoot)
	detachedComponent, err := detachedRoot.Component(detachedCtx, chasm.ComponentRef{})
	require.NoError(t, err)

	describable, ok := detachedComponent.(chasm.DescribableComponent)
	require.True(t, ok, "rehydrated root %T must implement chasm.DescribableComponent", detachedComponent)

	described, err := describable.DescribeComponent(detachedCtx)
	require.NoError(t, err)

	actual, ok := described.(*workflowservice.DescribeActivityExecutionResponse)
	require.True(t, ok, "activity must describe as a DescribeActivityExecutionResponse, got %T", described)

	protorequire.ProtoEqual(t, expected, actual)

	// Fields a detached reader cannot reconstruct on its own.
	require.Equal(t, "my-activity-id", actual.GetInfo().GetActivityId())
	require.Equal(t, "run-id-1", actual.GetRunId())
	require.Equal(t, "run-id-1", actual.GetInfo().GetRunId())
	require.NotNil(t, actual.GetInfo().GetCloseTime())
	require.NotNil(t, actual.GetOutcome().GetFailure())
	require.Empty(t, actual.GetLongPollToken())
}
