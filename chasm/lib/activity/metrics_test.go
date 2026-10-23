package activity

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCompletionMetricsWorkerDeploymentLabelKeyParity(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleMetricsHandler: func() metrics.Handler { return metricsHandler },
			HandleNamespaceEntry: testNamespaceEntry,
			GoCtx: context.WithValue(context.Background(), ctxKeyActivityContext, &activityContext{
				config: &Config{
					BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
				},
			}),
		},
	}
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType: &commonpb.ActivityType{Name: "test-activity-type"},
			ScheduleTime: timestamppb.New(defaultTime),
			TaskQueue:    &taskqueuepb.TaskQueue{Name: "test-task-queue"},
		},
		LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
			StartedTime: timestamppb.New(defaultTime),
		}),
	}

	baseHandler := activity.baseMetricsHandler(ctx, metrics.HistoryRespondActivityTaskCompletedScope)
	completionHandler := activity.completionMetricsHandler(ctx, metrics.HistoryRespondActivityTaskCompletedScope)
	activity.emitOnCompletedMetrics(ctx, baseHandler, completionHandler, nil, true)
	activity.emitOnFailedMetrics(ctx, baseHandler, completionHandler, &failurepb.Failure{})
	activity.emitOnCanceledMetrics(ctx, completionHandler, activitypb.ACTIVITY_EXECUTION_STATUS_STARTED)
	activity.emitOnTimedOutMetrics(
		completionHandler,
		enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
	)

	expectedTags := []metrics.Tag{
		metrics.WorkerDeploymentNameTag("", false),
		metrics.WorkerDeploymentBuildIDTag("", false),
	}
	for _, metricName := range []string{
		metrics.ActivitySuccess.Name(),
		metrics.ActivityFail.Name(),
		metrics.ActivityTaskFail.Name(),
		metrics.ActivityCancel.Name(),
		metrics.ActivityTaskTimeout.Name(),
		metrics.ActivityTimeout.Name(),
		metrics.ActivityStartToCloseLatency.Name(),
		metrics.ActivityScheduleToCloseLatency.Name(),
	} {
		recordings := capture.Snapshot()[metricName]
		require.NotEmpty(t, recordings, "expected %s to be emitted", metricName)
		for _, recording := range recordings {
			for _, expectedTag := range expectedTags {
				require.Contains(t, recording.Tags, expectedTag.Key, "%s is missing label %s", metricName, expectedTag.Key)
				require.Equal(t, expectedTag.Value, recording.Tags[expectedTag.Key])
			}
		}
	}

	activity.emitOnTerminatedMetrics(activity.enrichedMetricsHandler(ctx, metrics.ActivityTerminatedScope))
	terminated := capture.Snapshot()[metrics.ActivityTerminate.Name()]
	require.Len(t, terminated, 1)
	for _, expectedTag := range expectedTags {
		require.NotContains(t, terminated[0].Tags, expectedTag.Key)
	}
}

func TestHandoffMetrics(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name                string
		alreadyRecorded     bool
		updateError         error
		expectedMetricCount int
	}{
		{
			name:                "EmitsMetricForNewHandoff",
			expectedMetricCount: 1,
		},
		{
			name:            "NoMetricForDuplicateHandoff",
			alreadyRecorded: true,
		},
		{
			name:        "NoMetricWhenUpdateFails",
			updateError: serviceerror.NewUnavailable("persistence unavailable"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)

			ctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{
				HandleNow:            func(chasm.Component) time.Time { return defaultTime },
				HandleMetricsHandler: func() metrics.Handler { return metricsHandler },
			}}
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					Status:             activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
					AwaitingCompletion: test.alreadyRecorded,
				},
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{Count: 1, StartedStamp: 3}),
			}

			if test.alreadyRecorded {
				activity.AsyncHandoffTime = timestamppb.New(defaultTime)
			}

			controller := gomock.NewController(t)
			ns := testNamespaceEntry()
			registry := namespace.NewMockRegistry(controller)
			registry.EXPECT().GetNamespaceByID(ns.ID()).Return(ns, nil)
			h := &handler{namespaceRegistry: registry, config: &Config{
				Enabled:                       dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
				StandaloneAsyncHandoffEnabled: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
			}}

			ref, err := (&persistencespb.ChasmComponentRef{
				NamespaceId: ns.ID().String(),
				BusinessId:  "activity-id",
				ArchetypeId: ArchetypeID,
			}).Marshal()
			require.NoError(t, err)
			token, err := (&tokenspb.Task{
				NamespaceId:          ns.ID().String(),
				Attempt:              1,
				ActivityAttemptStamp: 3,
				ComponentRef:         ref,
			}).Marshal()
			require.NoError(t, err)

			engine := chasm.NewMockEngine(controller)
			engine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, _ chasm.ComponentRef, update func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) ([]byte, error) {
					// Evaluate the mutation before returning the simulated write result.
					require.NoError(t, update(ctx, activity))
					return ref, test.updateError
				},
			)

			_, err = h.RespondActivityTaskHandoff(chasm.NewEngineContext(t.Context(), engine), &activitypb.RespondActivityTaskHandoffRequest{
				NamespaceId: ns.ID().String(),
				BusinessId:  "activity-id",
				FrontendRequest: &workflowservice.RespondActivityTaskHandoffRequest{
					TaskToken: token,
				},
			})
			if test.updateError != nil {
				require.ErrorIs(t, err, test.updateError)
			} else {
				require.NoError(t, err)
			}

			recordings := capture.SnapshotMetric(metrics.ActivityAsyncHandoff.Name())
			require.Len(t, recordings, test.expectedMetricCount)
			if test.expectedMetricCount > 0 {
				require.Equal(t, int64(1), recordings[0].Value)
			}
		})
	}
}
