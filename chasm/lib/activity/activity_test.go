package activity

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apiactivitypb "go.temporal.io/api/activity/v1" //nolint:importas
	commonpb "go.temporal.io/api/common/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute/sadefs"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSearchAttributesIncludesExecutionTime(t *testing.T) {
	testTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	testCases := []struct {
		name       string
		startDelay time.Duration
	}{
		{name: "without start delay"},
		{name: "with start delay", startDelay: 5 * time.Minute},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return testTime },
				},
			}

			activity, err := NewStandaloneActivity(ctx, &workflowservice.StartActivityExecutionRequest{
				Namespace:           "ns",
				ActivityId:          "act",
				ActivityType:        &commonpb.ActivityType{Name: "T"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: "tq"},
				StartToCloseTimeout: durationpb.New(10 * time.Second),
				StartDelay:          durationpb.New(tc.startDelay),
			})
			require.NoError(t, err)

			var executionTime time.Time
			for _, sa := range activity.SearchAttributes(ctx) {
				if sa.Field == sadefs.ExecutionTime {
					var ok bool
					executionTime, ok = sa.Value.Value().(time.Time)
					require.True(t, ok)
					break
				}
			}
			require.False(t, executionTime.IsZero(), "SearchAttributes must include ExecutionTime")
			require.Equal(t, testTime.Add(tc.startDelay), executionTime)
		})
	}
}

func TestHandleStarted(t *testing.T) {
	testTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	testRequestID := "test-request-id"
	testStamp := int32(1)

	testCases := []struct {
		name           string
		activityStatus activitypb.ActivityExecutionStatus
		attemptStamp   int32
		requestStamp   int32
		startRequestID string
		requestID      string
		checkOutcome   func(t *testing.T, response *historyservice.RecordActivityTaskStartedResponse, err error)
	}{
		{
			name:           "successful transition from scheduled",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			attemptStamp:   testStamp,
			requestStamp:   testStamp,
			requestID:      testRequestID,
			checkOutcome: func(t *testing.T, response *historyservice.RecordActivityTaskStartedResponse, err error) {
				require.Equal(t, int32(1), response.Attempt)
				require.NoError(t, err)
			},
		},
		{
			name:           "idempotent retry - same request ID",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			attemptStamp:   testStamp,
			requestStamp:   testStamp,
			startRequestID: testRequestID,
			requestID:      testRequestID,
			checkOutcome: func(t *testing.T, response *historyservice.RecordActivityTaskStartedResponse, err error) {
				require.Equal(t, int32(1), response.Attempt)
				require.NoError(t, err)
			},
		},
		{
			name:           "error - already started with different request ID",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			attemptStamp:   testStamp,
			requestStamp:   testStamp,
			startRequestID: "different-request-id",
			requestID:      testRequestID,
			checkOutcome: func(t *testing.T, response *historyservice.RecordActivityTaskStartedResponse, err error) {
				require.ErrorAs(t, err, new(*serviceerrors.ObsoleteMatchingTask))
			},
		},
		{
			name:           "error - stamp mismatch",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			attemptStamp:   testStamp,
			requestStamp:   testStamp + 1,
			requestID:      testRequestID,
			checkOutcome: func(t *testing.T, response *historyservice.RecordActivityTaskStartedResponse, err error) {
				require.ErrorAs(t, err, new(*serviceerrors.ObsoleteMatchingTask))
			},
		},
		{
			name:           "error - invalid transition from completed",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
			attemptStamp:   testStamp,
			requestStamp:   testStamp,
			requestID:      testRequestID,
			checkOutcome: func(t *testing.T, response *historyservice.RecordActivityTaskStartedResponse, err error) {
				require.ErrorAs(t, err, new(*serviceerrors.ObsoleteMatchingTask))
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup mock context
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return testTime },
					HandleExecutionKey: func() chasm.ExecutionKey {
						return chasm.ExecutionKey{
							BusinessID: "test-activity-id",
							RunID:      "test-run-id",
						}
					},
				},
			}

			// Setup activity state
			attemptState := &activitypb.ActivityAttemptState{
				Count:          1,
				Stamp:          tc.attemptStamp,
				StartRequestId: tc.startRequestID,
			}
			if tc.activityStatus == activitypb.ACTIVITY_EXECUTION_STATUS_STARTED {
				attemptState.StartedTime = timestamppb.New(testTime.Add(-1 * time.Minute))
			}

			// Determine heartbeat timeout based on test case
			heartbeatTimeout := 1 * time.Minute
			if tc.name == "successful transition without heartbeat timeout" {
				heartbeatTimeout = 0
			}

			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					Status:                 tc.activityStatus,
					TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
					ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
					ScheduleToStartTimeout: durationpb.New(2 * time.Minute),
					StartToCloseTimeout:    durationpb.New(3 * time.Minute),
					HeartbeatTimeout:       durationpb.New(heartbeatTimeout),
					ScheduleTime:           timestamppb.New(testTime.Add(-30 * time.Second)),
				},
				LastAttempt: chasm.NewDataField(ctx, attemptState),
				RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
					Input: &commonpb.Payloads{
						Payloads: []*commonpb.Payload{{Data: []byte("test-input")}},
					},
					Header: &commonpb.Header{
						Fields: map[string]*commonpb.Payload{
							"test-header": {Data: []byte("test-value")},
						},
					},
				}),
				Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
			}

			// Create request
			request := &historyservice.RecordActivityTaskStartedRequest{
				Stamp:     tc.requestStamp,
				RequestId: tc.requestID,
			}

			// Execute HandleStarted
			response, err := activity.HandleStarted(ctx, request)

			tc.checkOutcome(t, response, err)
		})
	}
}

func TestActivityTerminate(t *testing.T) {
	testCases := []struct {
		name           string
		activityStatus activitypb.ActivityExecutionStatus
		expectErr      string
	}{
		{
			name:           "terminate scheduled activity",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		},
		{
			name:           "terminate started activity",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		},
		{
			name:           "terminate cancel-requested activity",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		},
		{
			name:           "error on completed activity",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
			expectErr:      "invalid transition from Completed",
		},
		{
			name:           "no-op on already terminated activity",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		},
		{
			name:           "error on failed activity",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
			expectErr:      "invalid transition from Failed",
		},
		{
			name:           "error on timed out activity",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
			expectErr:      "invalid transition from TimedOut",
		},
		{
			name:           "error on canceled activity",
			activityStatus: activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
			expectErr:      "invalid transition from Canceled",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			nsRegistry := namespace.NewMockRegistry(ctrl)
			nsRegistry.EXPECT().GetNamespaceName(gomock.Any()).Return(namespace.Name("test-namespace"), nil).AnyTimes()

			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return defaultTime },
					GoCtx: context.WithValue(context.Background(), ctxKeyActivityContext, &activityContext{
						config: &Config{
							BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
						},
						namespaceRegistry: nsRegistry,
					}),
				},
			}

			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					Status:                 tc.activityStatus,
					TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
					ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
					ScheduleToStartTimeout: durationpb.New(2 * time.Minute),
					StartToCloseTimeout:    durationpb.New(3 * time.Minute),
				},
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{Count: 1}),
				Outcome:     chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
			}

			_, err := activity.Terminate(ctx, chasm.TerminateComponentRequest{
				Reason: "Delete activity execution",
			})

			if tc.expectErr != "" {
				require.EqualError(t, err, tc.expectErr)
				require.Equal(t, tc.activityStatus, activity.Status, "expected no state change on error")
			} else {
				require.NoError(t, err)
				require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED, activity.Status)
			}
		})
	}
}

// Check that we do not emit a StartToCloseLatency metric when cancelling an activity that has no
// attempt in progress. Cancelling a SCHEDULED or PAUSED activity transitions straight to Canceled,
// and the status captured before the CancelRequested transition must be forwarded so that the
// metric is not emitted.
func TestHandleCancellationRequestedDirectCancelMetrics(t *testing.T) {
	testCases := []struct {
		name   string
		status activitypb.ActivityExecutionStatus
	}{
		{
			name:   "scheduled during retry backoff, stale StartedTime",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		},
		{
			name:   "paused during retry backoff, stale StartedTime",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			nsRegistry := namespace.NewMockRegistry(ctrl)
			nsRegistry.EXPECT().GetNamespaceName(gomock.Any()).Return(namespace.Name("test-namespace"), nil).AnyTimes()

			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()

			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow:            func(chasm.Component) time.Time { return defaultTime },
					HandleMetricsHandler: func() metrics.Handler { return metricsHandler },
					GoCtx: context.WithValue(context.Background(), ctxKeyActivityContext, &activityContext{
						config: &Config{
							BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
						},
						namespaceRegistry: nsRegistry,
					}),
				},
			}

			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					RetryPolicy:            defaultRetryPolicy,
					Status:                 tc.status,
					ScheduleTime:           timestamppb.New(defaultTime),
					TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
					ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
					ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
					StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
				},
				// A stale StartedTime from a prior attempt: no attempt is currently running, so
				// StartToCloseLatency must not be recorded on cancellation.
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
					Count:       1,
					StartedTime: timestamppb.New(defaultTime),
				}),
				Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
			}

			_, err := activity.handleCancellationRequested(ctx, &activitypb.RequestCancelActivityExecutionRequest{
				FrontendRequest: &workflowservice.RequestCancelActivityExecutionRequest{Reason: "test"},
			})
			require.NoError(t, err)
			require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED, activity.Status)

			snapshot := capture.Snapshot()
			require.NotEmpty(t, snapshot[metrics.ActivityCancel.Name()])
			require.NotEmpty(t, snapshot[metrics.ActivityScheduleToCloseLatency.Name()])
			require.Empty(t, snapshot[metrics.ActivityStartToCloseLatency.Name()],
				"no attempt was running; StartToCloseLatency must not be recorded")
		})
	}
}

func TestRecordHeartbeatPauseResetCancelFlags(t *testing.T) {
	testTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	const (
		namespaceID = "test-namespace-id"
		activityID  = "test-activity-id"
		runID       = "test-run-id"
		attempt     = int32(1)
	)

	componentRef, err := (&persistencespb.ChasmComponentRef{
		NamespaceId: namespaceID,
		BusinessId:  activityID,
		RunId:       runID,
	}).Marshal()
	require.NoError(t, err)

	testCases := []struct {
		name            string
		status          activitypb.ActivityExecutionStatus
		resetKeepPaused bool
		wantPaused      bool
		wantReset       bool
		wantCancel      bool
	}{
		{
			name:   "no pause or reset returns zero flags",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		},
		{
			// Regression guard: reset must propagate to the next heartbeat response
			// immediately so the worker can abort the in-flight attempt; previously
			// reset was withheld until the next retry.
			name:      "RESET_REQUESTED status propagates ActivityReset on next heartbeat",
			status:    activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
			wantReset: true,
		},
		{
			name:       "PAUSE_REQUESTED status propagates ActivityPaused",
			status:     activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED,
			wantPaused: true,
		},
		{
			name:            "reset with keep-paused propagates only reset",
			status:          activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
			resetKeepPaused: true,
			wantReset:       true,
		},
		{
			name:       "cancel requested status propagates CancelRequested",
			status:     activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			wantCancel: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return testTime },
					HandleExecutionKey: func() chasm.ExecutionKey {
						return chasm.ExecutionKey{
							NamespaceID: namespaceID,
							BusinessID:  activityID,
							RunID:       runID,
						}
					},
				},
			}

			act := &Activity{
				ActivityState: &activitypb.ActivityState{
					Status:           tc.status,
					HeartbeatTimeout: durationpb.New(0),
					ResetKeepPaused:  tc.resetKeepPaused,
				},
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{Count: attempt}),
			}

			token := &tokenspb.Task{
				NamespaceId:  namespaceID,
				Attempt:      attempt,
				ComponentRef: componentRef,
			}
			req := &historyservice.RecordActivityTaskHeartbeatRequest{
				NamespaceId:      namespaceID,
				HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{},
			}

			resp, err := act.RecordHeartbeat(ctx, WithToken[*historyservice.RecordActivityTaskHeartbeatRequest]{
				Token:   token,
				Request: req,
			})

			require.NoError(t, err)
			require.Equal(t, tc.wantPaused, resp.ActivityPaused, "ActivityPaused")
			require.Equal(t, tc.wantReset, resp.ActivityReset, "ActivityReset")
			require.Equal(t, tc.wantCancel, resp.CancelRequested, "CancelRequested")
		})
	}
}

func TestActivityTaskTokenAttemptStampRejectsTokenFromBeforeAttemptReset(t *testing.T) {
	testTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	const (
		namespaceID = "test-namespace-id"
		activityID  = "test-activity-id"
		runID       = "test-run-id"
		attempt     = int32(1)
	)

	componentRef, err := (&persistencespb.ChasmComponentRef{
		NamespaceId: namespaceID,
		BusinessId:  activityID,
		RunId:       runID,
	}).Marshal()
	require.NoError(t, err)

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return testTime },
			HandleExecutionKey: func() chasm.ExecutionKey {
				return chasm.ExecutionKey{
					NamespaceID: namespaceID,
					BusinessID:  activityID,
					RunID:       runID,
				}
			},
		},
	}

	act := &Activity{
		ActivityState: &activitypb.ActivityState{
			Status:           activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			HeartbeatTimeout: durationpb.New(0),
		},
		LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
			Count:        attempt,
			Stamp:        7,
			StartedStamp: 7,
		}),
	}
	originalToken := &tokenspb.Task{
		NamespaceId:          namespaceID,
		Attempt:              attempt,
		ComponentRef:         componentRef,
		ActivityAttemptStamp: act.LastAttempt.Get(ctx).GetStamp(),
	}
	req := &historyservice.RecordActivityTaskHeartbeatRequest{
		NamespaceId:      namespaceID,
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{},
	}

	_, err = act.RecordHeartbeat(ctx, WithToken[*historyservice.RecordActivityTaskHeartbeatRequest]{
		Token:   originalToken,
		Request: req,
	})
	require.NoError(t, err)

	resetAttempt := act.LastAttempt.Get(ctx)
	resetAttempt.Count = attempt
	resetAttempt.Stamp++
	resetAttempt.StartedStamp = resetAttempt.GetStamp()

	resetToken := &tokenspb.Task{
		NamespaceId:          namespaceID,
		Attempt:              attempt,
		ComponentRef:         componentRef,
		ActivityAttemptStamp: resetAttempt.GetStamp(),
	}
	require.Equal(t, originalToken.GetAttempt(), resetToken.GetAttempt())
	require.NotEqual(t, originalToken.GetActivityAttemptStamp(), resetToken.GetActivityAttemptStamp())

	_, err = act.RecordHeartbeat(ctx, WithToken[*historyservice.RecordActivityTaskHeartbeatRequest]{
		Token:   originalToken,
		Request: req,
	})
	var notFoundErr *serviceerror.NotFound
	require.ErrorAs(t, err, &notFoundErr)

	_, err = act.HandleCompleted(ctx, RespondCompletedEvent{
		Token: originalToken,
		Request: &historyservice.RespondActivityTaskCompletedRequest{
			NamespaceId: namespaceID,
		},
	})
	require.ErrorAs(t, err, &notFoundErr)

	_, err = act.RecordHeartbeat(ctx, WithToken[*historyservice.RecordActivityTaskHeartbeatRequest]{
		Token:   resetToken,
		Request: req,
	})
	require.NoError(t, err)
}

func TestActivityTaskTokenWithoutAttemptStampAcceptedForCurrentRetryAttempt(t *testing.T) {
	testTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	const (
		namespaceID = "test-namespace-id"
		activityID  = "test-activity-id"
		runID       = "test-run-id"
		attempt     = int32(2)
	)

	componentRef, err := (&persistencespb.ChasmComponentRef{
		NamespaceId: namespaceID,
		BusinessId:  activityID,
		RunId:       runID,
	}).Marshal()
	require.NoError(t, err)

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return testTime },
			HandleExecutionKey: func() chasm.ExecutionKey {
				return chasm.ExecutionKey{
					NamespaceID: namespaceID,
					BusinessID:  activityID,
					RunID:       runID,
				}
			},
		},
	}

	act := &Activity{
		ActivityState: &activitypb.ActivityState{
			Status:           activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			HeartbeatTimeout: durationpb.New(0),
		},
		LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
			Count:        attempt,
			Stamp:        11,
			StartedStamp: 8,
		}),
	}
	token := &tokenspb.Task{
		NamespaceId:  namespaceID,
		Attempt:      attempt,
		ComponentRef: componentRef,
	}
	req := &historyservice.RecordActivityTaskHeartbeatRequest{
		NamespaceId:      namespaceID,
		HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{},
	}

	_, err = act.RecordHeartbeat(ctx, WithToken[*historyservice.RecordActivityTaskHeartbeatRequest]{
		Token:   token,
		Request: req,
	})
	require.NoError(t, err)
}

func TestUpdateStartedActivityExecutionOptionsDoesNotBumpStartedStamp(t *testing.T) {
	testTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().GetNamespaceName(gomock.Any()).Return(namespace.Name("test-namespace"), nil).AnyTimes()

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return testTime },
			GoCtx: context.WithValue(context.Background(), ctxKeyActivityContext, &activityContext{
				config: &Config{
					BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
				},
				namespaceRegistry: nsRegistry,
			}),
		},
	}
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
			ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
			ScheduleToStartTimeout: durationpb.New(2 * time.Minute),
			StartToCloseTimeout:    durationpb.New(3 * time.Minute),
			HeartbeatTimeout:       durationpb.New(time.Minute),
		},
		LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
			Count:        1,
			Stamp:        7,
			StartedStamp: 7,
		}),
		Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
	}
	attempt := activity.LastAttempt.Get(ctx)
	originalStamp := attempt.GetStamp()
	originalStartedStamp := attempt.GetStartedStamp()

	_, err := activity.UpdateActivityExecutionOptions(ctx, &activitypb.UpdateActivityExecutionOptionsRequest{
		FrontendRequest: &workflowservice.UpdateActivityExecutionOptionsRequest{
			ActivityId: "test-activity-id",
			ActivityOptions: &apiactivitypb.ActivityOptions{
				HeartbeatTimeout: durationpb.New(2 * time.Minute),
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
		},
	})
	require.NoError(t, err)

	require.Equal(t, originalStamp+1, attempt.GetStamp())
	require.Equal(t, originalStartedStamp, attempt.GetStartedStamp())
}

func TestContextMetadata(t *testing.T) {
	t.Run("returns activity type and task queue", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				ActivityType: &commonpb.ActivityType{Name: "my-activity"},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: "my-task-queue"},
			},
		}

		md := activity.ContextMetadata(ctx)
		require.Equal(t, map[string]string{
			"standalone-activity-type":       "my-activity",
			"standalone-activity-task-queue": "my-task-queue",
		}, md)
	})

	t.Run("returns only activity type when task queue is empty", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				ActivityType: &commonpb.ActivityType{Name: "my-activity"},
			},
		}

		md := activity.ContextMetadata(ctx)
		require.Equal(t, map[string]string{
			"standalone-activity-type": "my-activity",
		}, md)
	})

	t.Run("returns only task queue when activity type is empty", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				TaskQueue: &taskqueuepb.TaskQueue{Name: "my-task-queue"},
			},
		}

		md := activity.ContextMetadata(ctx)
		require.Equal(t, map[string]string{
			"standalone-activity-task-queue": "my-task-queue",
		}, md)
	})

	t.Run("returns nil when both are empty", func(t *testing.T) {
		ctx := &chasm.MockMutableContext{}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{},
		}

		md := activity.ContextMetadata(ctx)
		require.Nil(t, md)
	})
}

// TestNewStandaloneActivity_UserMetadataDualWrite verifies that user metadata
// supplied on a StartActivityExecution request is persisted to BOTH the
// framework-level ChasmComponentAttributes (the authoritative new location)
// and the legacy ActivityRequestData.user_metadata field. The dual-write is
// load-bearing for rollback safety: a binary rolled back to pre-migration code
// only knows how to read the legacy field, so dropping it would silently empty
// the user metadata on Describe for any activity created during the new-deploy
// window.
func TestNewStandaloneActivity_UserMetadataDualWrite(t *testing.T) {
	md := &sdkpb.UserMetadata{
		Summary: &commonpb.Payload{Data: []byte("summary-blob")},
		Details: &commonpb.Payload{Data: []byte("details-blob")},
	}

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return time.Unix(0, 0) },
			HandleExecutionKey: func() chasm.ExecutionKey {
				return chasm.ExecutionKey{NamespaceID: "ns", BusinessID: "act", RunID: "run"}
			},
		},
	}

	activity, err := NewStandaloneActivity(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:    "ns",
		ActivityId:   "act",
		ActivityType: &commonpb.ActivityType{Name: "T"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: "Q"},
		RequestId:    "req-id",
		UserMetadata: md,
	})
	require.NoError(t, err)

	// New location: SetUserMetadata recorded against the activity.
	require.Contains(t, ctx.UserMetadataByComponent, chasm.Component(activity))
	require.Same(t, md, ctx.UserMetadataByComponent[activity])

	// Legacy location: ActivityRequestData also carries it so rolled-back code
	// can still surface user metadata via the old field.
	require.Same(t, md, activity.RequestData.Get(ctx).GetUserMetadata()) //nolint:staticcheck // exercising legacy field
}

func TestNewStandaloneActivity_OriginalOptionsUnaffectedBySubfieldUpdate(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return time.Unix(0, 0) },
		},
	}

	activity, err := NewStandaloneActivity(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:              "ns",
		ActivityId:             "act",
		ActivityType:           &commonpb.ActivityType{Name: "T"},
		TaskQueue:              &taskqueuepb.TaskQueue{Name: "original-task-queue"},
		ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
		ScheduleToStartTimeout: durationpb.New(20 * time.Second),
		StartToCloseTimeout:    durationpb.New(10 * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(10 * time.Second),
			BackoffCoefficient: 2,
			MaximumInterval:    durationpb.New(100 * time.Second),
			MaximumAttempts:    5,
		},
		Priority: &commonpb.Priority{
			FairnessKey: "original-fairness-key",
		},
	})
	require.NoError(t, err)

	err = activity.mergeActivityOptions(&workflowservice.UpdateActivityExecutionOptionsRequest{
		ActivityId: "act",
		ActivityOptions: &apiactivitypb.ActivityOptions{
			TaskQueue: &taskqueuepb.TaskQueue{Name: "updated-task-queue"},
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(time.Second),
			},
			Priority: &commonpb.Priority{
				FairnessKey: "updated-fairness-key",
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{
			"task_queue.name",
			"retry_policy.initial_interval",
			"priority.fairness_key",
		}},
	})
	require.NoError(t, err)

	originalOptions := activity.GetOriginalOptions()
	require.Equal(t, "original-task-queue", originalOptions.GetTaskQueue().GetName())
	require.Equal(t, 10*time.Second, originalOptions.GetRetryPolicy().GetInitialInterval().AsDuration())
	require.Equal(t, "original-fairness-key", originalOptions.GetPriority().GetFairnessKey())
}

func TestMergeActivityOptionsRejectsInvalidMergedRetryPolicy(t *testing.T) {
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:           &commonpb.ActivityType{Name: "T"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "Q"},
			ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
			ScheduleToStartTimeout: durationpb.New(20 * time.Second),
			StartToCloseTimeout:    durationpb.New(10 * time.Second),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(10 * time.Second),
				BackoffCoefficient: 2,
				MaximumInterval:    durationpb.New(30 * time.Second),
				MaximumAttempts:    5,
			},
		},
	}

	err := activity.mergeActivityOptions(&workflowservice.UpdateActivityExecutionOptionsRequest{
		ActivityId: "act",
		ActivityOptions: &apiactivitypb.ActivityOptions{
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(60 * time.Second),
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval"}},
	})
	require.ErrorContains(t, err, "MaximumInterval cannot be less than InitialInterval")
}

// TestEffectiveUserMetadata_PrefersFrameworkLocation ensures the helper used by
// readers (Describe, etc.) returns the framework-level user metadata when both
// the new and legacy locations are populated.
func TestEffectiveUserMetadata_PrefersFrameworkLocation(t *testing.T) {
	frameworkMD := &sdkpb.UserMetadata{Summary: &commonpb.Payload{Data: []byte("new")}}
	legacyMD := &sdkpb.UserMetadata{Summary: &commonpb.Payload{Data: []byte("legacy")}}

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleUserMetadata: func(chasm.Component) *sdkpb.UserMetadata {
				return frameworkMD
			},
		},
	}
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{},
		RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
			UserMetadata: legacyMD, //nolint:staticcheck // exercising legacy field
		}),
	}

	got := activity.effectiveUserMetadata(ctx)
	require.Same(t, frameworkMD, got)
}

// TestAttachLinks_SameRequestIDIsNoOp verifies that calling attachLinks with a
// requestID that already has links recorded is a no-op — the request is
// treated as an idempotent retry regardless of payload.
func TestAttachLinks_SameRequestIDIsNoOp(t *testing.T) {
	linkA := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns", WorkflowId: "a", RunId: "run"},
	}}
	linkB := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns", WorkflowId: "b", RunId: "run"},
	}}

	stored := map[string][]*commonpb.Link{}
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleLinks: func(chasm.Component) []*commonpb.Link {
				var all []*commonpb.Link
				for _, ls := range stored {
					all = append(all, ls...)
				}
				return all
			},
			HandleRequestLinks: func(_ chasm.Component, reqID string) ([]*commonpb.Link, error) {
				return stored[reqID], nil
			},
		},
	}
	validator := newLinkValidator(
		func(string) int { return 100 },
		func(string) int { return 100 },
		func(string) int { return 4000 },
	)
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{Status: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED},
	}

	// First call records the request's links verbatim (no intra-batch dedup, matching
	// the workflow start path).
	require.NoError(t, activity.attachLinks(ctx, []*commonpb.Link{linkA, linkA}, "req-1", validator, "ns"))
	stored["req-1"] = ctx.LinksByRequest[activity]["req-1"]
	require.Equal(t, []*commonpb.Link{linkA, linkA}, stored["req-1"])

	// Retry with the same requestID (even with different links) is a no-op.
	require.NoError(t, activity.attachLinks(ctx, []*commonpb.Link{linkB}, "req-1", validator, "ns"))
	require.Equal(t, []*commonpb.Link{linkA, linkA}, ctx.LinksByRequest[activity]["req-1"])
}

// TestAttachLinks_RejectsClosedActivity verifies that attachLinks returns a
// FailedPrecondition error when the activity is already in a terminal state and
// the requestID has not been seen before.
func TestAttachLinks_RejectsClosedActivity(t *testing.T) {
	link := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns", WorkflowId: "a", RunId: "run"},
	}}

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleRequestLinks: func(chasm.Component, string) ([]*commonpb.Link, error) {
				return nil, nil
			},
		},
	}
	validator := newLinkValidator(
		func(string) int { return 100 },
		func(string) int { return 100 },
		func(string) int { return 4000 },
	)
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{Status: activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
	}

	err := activity.attachLinks(ctx, []*commonpb.Link{link}, "req-new", validator, "ns")
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
}

// TestAttachLinks_IdempotentAfterClose verifies that a retry with a previously
// seen requestID is a no-op even after the activity has closed — protecting
// against the case where the original attach succeeded but the response was
// lost before the client could observe it.
func TestAttachLinks_IdempotentAfterClose(t *testing.T) {
	link := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns", WorkflowId: "a", RunId: "run"},
	}}

	stored := map[string][]*commonpb.Link{"req-1": {link}}
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleRequestLinks: func(_ chasm.Component, reqID string) ([]*commonpb.Link, error) {
				return stored[reqID], nil
			},
		},
	}
	validator := newLinkValidator(
		func(string) int { return 100 },
		func(string) int { return 100 },
		func(string) int { return 4000 },
	)
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{Status: activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
	}

	require.NoError(t, activity.attachLinks(ctx, []*commonpb.Link{link}, "req-1", validator, "ns"))
}

// TestAttachLinks_RejectsWhenComponentCapExceeded verifies that an attach
// against a still-running activity is rejected with FailedPrecondition when the
// combined existing + incoming link count would exceed the per-component cap.
func TestAttachLinks_RejectsWhenComponentCapExceeded(t *testing.T) {
	existingLink := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns", WorkflowId: "existing", RunId: "run"},
	}}
	newLink := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{
		WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns", WorkflowId: "new", RunId: "run"},
	}}

	stored := map[string][]*commonpb.Link{"req-existing": {existingLink}}
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleLinks: func(chasm.Component) []*commonpb.Link {
				var all []*commonpb.Link
				for _, ls := range stored {
					all = append(all, ls...)
				}
				return all
			},
			HandleRequestLinks: func(_ chasm.Component, reqID string) ([]*commonpb.Link, error) {
				return stored[reqID], nil
			},
		},
	}
	// per-component cap = 1; activity already has 1 link, so any additional must fail.
	validator := newLinkValidator(
		func(string) int { return 100 },
		func(string) int { return 1 },
		func(string) int { return 4000 },
	)
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{Status: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED},
	}

	err := activity.attachLinks(ctx, []*commonpb.Link{newLink}, "req-new", validator, "ns")
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
}

// TestEffectiveUserMetadata_FallsBackToLegacy ensures that activities persisted
// before the migration (no ChasmComponentAttributes.user_metadata; only the
// legacy ActivityRequestData.user_metadata is populated) still surface their
// user metadata to readers.
func TestEffectiveUserMetadata_FallsBackToLegacy(t *testing.T) {
	legacyMD := &sdkpb.UserMetadata{Summary: &commonpb.Payload{Data: []byte("legacy")}}

	ctx := &chasm.MockMutableContext{} // HandleUserMetadata nil → returns nil, mimicking absent new field.
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{},
		RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
			UserMetadata: legacyMD, //nolint:staticcheck // exercising legacy field
		}),
	}

	got := activity.effectiveUserMetadata(ctx)
	require.Same(t, legacyMD, got)
}

func TestShouldRecalculateCurrentRetryInterval(t *testing.T) {
	retryInterval := 2 * time.Second

	testCases := []struct {
		name                 string
		status               activitypb.ActivityExecutionStatus
		restoreOriginal      bool
		updateFields         map[string]struct{}
		currentRetryInterval *durationpb.Duration
		retryIntervalSource  activitypb.ActivityRetryIntervalSource
		expectRecalculate    bool
	}{
		{
			name:                 "retry policy subfield update with policy-derived interval",
			status:               activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			updateFields:         map[string]struct{}{"retryPolicy.initialInterval": {}},
			currentRetryInterval: durationpb.New(retryInterval),
			retryIntervalSource:  activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY,
			expectRecalculate:    true,
		},
		{
			name:                 "retry policy replacement with policy-derived interval",
			status:               activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			updateFields:         map[string]struct{}{"retryPolicy": {}},
			currentRetryInterval: durationpb.New(retryInterval),
			retryIntervalSource:  activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY,
			expectRecalculate:    true,
		},
		{
			name:                 "restore original with policy-derived interval",
			status:               activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
			restoreOriginal:      true,
			currentRetryInterval: durationpb.New(retryInterval),
			retryIntervalSource:  activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY,
			expectRecalculate:    true,
		},
		{
			name:                 "unrelated update preserves retry interval",
			status:               activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			updateFields:         map[string]struct{}{"heartbeatTimeout": {}},
			currentRetryInterval: durationpb.New(retryInterval),
			retryIntervalSource:  activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY,
		},
		{
			name:                 "worker override is preserved regardless of value",
			status:               activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			updateFields:         map[string]struct{}{"retryPolicy.initialInterval": {}},
			currentRetryInterval: durationpb.New(retryInterval),
			retryIntervalSource:  activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_WORKER_OVERRIDE,
		},
		{
			name:                 "unspecified source (pre-existing attempt state) is preserved",
			status:               activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			updateFields:         map[string]struct{}{"retryPolicy.initialInterval": {}},
			currentRetryInterval: durationpb.New(retryInterval),
			retryIntervalSource:  activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_UNSPECIFIED,
		},
		{
			name:                 "started activity is not in retry backoff",
			status:               activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			updateFields:         map[string]struct{}{"retryPolicy.initialInterval": {}},
			currentRetryInterval: durationpb.New(retryInterval),
			retryIntervalSource:  activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY,
		},
		{
			name:         "missing retry interval",
			status:       activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			updateFields: map[string]struct{}{"retryPolicy.initialInterval": {}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					Status: tc.status,
				},
			}
			attempt := &activitypb.ActivityAttemptState{
				CurrentRetryInterval:       tc.currentRetryInterval,
				CurrentRetryIntervalSource: tc.retryIntervalSource,
			}

			got := activity.shouldRecalculateCurrentRetryInterval(
				attempt,
				tc.restoreOriginal,
				tc.updateFields,
			)

			require.Equal(t, tc.expectRecalculate, got)
		})
	}
}

// TestUpdateActivityExecutionOptions_RestoreOriginal_RejectsMissingOriginalOptions mimics an
// activity persisted before original_options existed (e.g. created by a binary predating this
// field's introduction). Restoring such an activity's options has nothing valid to fall back to
// and must be rejected rather than silently wiping TaskQueue and both close/start timeouts.
func TestUpdateActivityExecutionOptions_RestoreOriginal_RejectsMissingOriginalOptions(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return time.Unix(0, 0) },
		},
	}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			ActivityType:           &commonpb.ActivityType{Name: "T"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "current-task-queue"},
			ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
			StartToCloseTimeout:    durationpb.New(10 * time.Second),
			OriginalOptions:        nil,
		},
		LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{}),
	}

	_, err := activity.UpdateActivityExecutionOptions(ctx, &activitypb.UpdateActivityExecutionOptionsRequest{
		FrontendRequest: &workflowservice.UpdateActivityExecutionOptionsRequest{
			ActivityId:      "act",
			RestoreOriginal: true,
		},
	})

	require.Error(t, err)
	require.Equal(t, "current-task-queue", activity.GetTaskQueue().GetName())
	require.Equal(t, 30*time.Second, activity.GetScheduleToCloseTimeout().AsDuration())
	require.Equal(t, 10*time.Second, activity.GetStartToCloseTimeout().AsDuration())
}

// TestHandleReset_RestoreOriginalOptions_RejectsMissingOriginalOptions covers the equivalent gap
// on the Reset(RestoreOriginalOptions=true) path, for an activity with no original_options
// snapshot (see the Update test above for the scenario this mimics).
func TestHandleReset_RestoreOriginalOptions_RejectsMissingOriginalOptions(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return time.Unix(0, 0) },
		},
	}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			ActivityType:           &commonpb.ActivityType{Name: "T"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "current-task-queue"},
			ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
			StartToCloseTimeout:    durationpb.New(10 * time.Second),
			OriginalOptions:        nil,
		},
		LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{}),
	}

	_, err := activity.handleReset(ctx, &activitypb.ResetActivityExecutionRequest{
		FrontendRequest: &workflowservice.ResetActivityExecutionRequest{
			ActivityId:             "act",
			RestoreOriginalOptions: true,
		},
	})

	require.Error(t, err)
	require.Equal(t, "current-task-queue", activity.GetTaskQueue().GetName())
}
