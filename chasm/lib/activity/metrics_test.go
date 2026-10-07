package activity

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStartActivityExecutionUseExistingNexusContextMetric(t *testing.T) {
	serializationContext := &nexuspb.PropagatedSerializationContext{
		Endpoint:  "endpoint",
		Service:   "service",
		Operation: "operation",
	}
	updateErr := errors.New("update failed")
	for _, tc := range []struct {
		name            string
		existingContext *nexuspb.PropagatedSerializationContext
		incomingContext *nexuspb.PropagatedSerializationContext
		attachCallback  bool
		updateErr       error
		expectedOutcome string
	}{
		{
			name:            "same context",
			existingContext: serializationContext,
			incomingContext: serializationContext,
			attachCallback:  true,
			expectedOutcome: "same_nexus_context",
		},
		{
			name:            "different endpoint",
			existingContext: serializationContext,
			incomingContext: &nexuspb.PropagatedSerializationContext{Endpoint: "other-endpoint", Service: "service", Operation: "operation"},
			attachCallback:  true,
			expectedOutcome: "different_nexus_context",
		},
		{
			name:            "different service",
			existingContext: serializationContext,
			incomingContext: &nexuspb.PropagatedSerializationContext{Endpoint: "endpoint", Service: "other-service", Operation: "operation"},
			attachCallback:  true,
			expectedOutcome: "different_nexus_context",
		},
		{
			name:            "different operation",
			existingContext: serializationContext,
			incomingContext: &nexuspb.PropagatedSerializationContext{Endpoint: "endpoint", Service: "service", Operation: "other-operation"},
			attachCallback:  true,
			expectedOutcome: "different_nexus_context",
		},
		{
			name:            "existing context missing",
			incomingContext: serializationContext,
			attachCallback:  true,
			expectedOutcome: "existing_nexus_context_missing",
		},
		{
			name:            "incoming context missing",
			existingContext: serializationContext,
			attachCallback:  true,
			expectedOutcome: "incoming_nexus_context_missing",
		},
		{
			name:           "both contexts missing",
			attachCallback: true,
		},
		{
			name:            "callback not attached",
			existingContext: serializationContext,
			incomingContext: serializationContext,
		},
		{
			name:            "attachment failed",
			existingContext: serializationContext,
			incomingContext: serializationContext,
			attachCallback:  true,
			updateErr:       updateErr,
		},
		{
			name:            "request ID already used",
			existingContext: serializationContext,
			incomingContext: serializationContext,
			attachCallback:  true,
			updateErr:       chasm.ErrRequestIDAlreadyUsed,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)

			engine := chasm.NewMockEngine(gomock.NewController(t))
			executionKey := chasm.ExecutionKey{NamespaceID: "namespace-id", BusinessID: "activity-id", RunID: "run-id"}
			engine.EXPECT().StartExecution(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(chasm.StartExecutionResult{ExecutionKey: executionKey}, nil)
			if tc.attachCallback {
				engine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, _ chasm.ComponentRef, updateFn func(chasm.MutableContext, chasm.Component) error, _ ...chasm.TransitionOption) ([]byte, error) {
						if errors.Is(tc.updateErr, chasm.ErrRequestIDAlreadyUsed) {
							return nil, tc.updateErr
						}
						mutableContext := &chasm.MockMutableContext{}
						activity := &Activity{
							ActivityState: &activitypb.ActivityState{},
							RequestData: chasm.NewDataField(mutableContext, &activitypb.ActivityRequestData{
								PropagatedNexusSerializationContext: tc.existingContext,
							}),
						}
						require.NoError(t, updateFn(mutableContext, activity))
						return nil, tc.updateErr
					},
				)
			}

			frontendRequest := &workflowservice.StartActivityExecutionRequest{
				Namespace:                           "namespace",
				ActivityId:                          "activity-id",
				RequestId:                           "request-id",
				IdReusePolicy:                       enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE,
				IdConflictPolicy:                    enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING,
				PropagatedNexusSerializationContext: tc.incomingContext,
				CompletionCallbacks: []*commonpb.Callback{{
					Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://destination/path"}},
				}},
			}
			if tc.attachCallback {
				frontendRequest.OnConflictOptions = &commonpb.OnConflictOptions{AttachCompletionCallbacks: true}
			}
			h := &handler{
				config:         &Config{MaxCallbacksPerExecution: func(string) int { return 10 }},
				metricsHandler: metricsHandler,
			}
			response, err := h.StartActivityExecution(chasm.NewEngineContext(t.Context(), engine), &activitypb.StartActivityExecutionRequest{
				NamespaceId:     executionKey.NamespaceID,
				FrontendRequest: frontendRequest,
			})
			if tc.updateErr != nil && !errors.Is(tc.updateErr, chasm.ErrRequestIDAlreadyUsed) {
				require.ErrorIs(t, err, tc.updateErr)
			} else {
				require.NoError(t, err)
				require.False(t, response.GetFrontendResponse().GetStarted())
			}

			recordings := capture.SnapshotMetric(metrics.NexusActivityUseExisting.Name())
			if tc.expectedOutcome == "" {
				require.Empty(t, recordings)
			} else {
				require.Len(t, recordings, 1)
				require.Equal(t, int64(1), recordings[0].Value)
				require.Equal(t, tc.expectedOutcome, recordings[0].Tags["context_match"])
			}
		})
	}
}

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
