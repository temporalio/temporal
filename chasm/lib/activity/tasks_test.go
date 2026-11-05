package activity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestActivityDispatchTaskHook(t *testing.T) {
	t.Run("wraps active dispatch", func(t *testing.T) {
		controller := gomock.NewController(t)
		engine := chasm.NewMockEngine(controller)
		matchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
		activityRef := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{NamespaceID: "namespace-id"})
		task := &activitypb.ActivityDispatchTask{DispatchReason: activitypb.DISPATCH_REASON_RETRY}
		chasmCtx := &chasm.MockMutableContext{
			MockContext: chasm.MockContext{
				HandleRef: func(chasm.Component) ([]byte, error) {
					return []byte("component-ref"), nil
				},
			},
		}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				TaskQueue: &taskqueuepb.TaskQueue{Name: "task-queue"},
			},
			LastAttempt: chasm.NewDataField(chasmCtx, &activitypb.ActivityAttemptState{Stamp: 7}),
		}

		engine.EXPECT().ReadComponent(gomock.Any(), activityRef, gomock.Any()).DoAndReturn(
			func(
				_ context.Context,
				_ chasm.ComponentRef,
				readFn func(chasm.Context, chasm.Component) error,
				_ ...chasm.TransitionOption,
			) error {
				return readFn(chasmCtx, activity)
			},
		)
		matchingClient.EXPECT().AddActivityTask(gomock.Any(), gomock.Any()).Return(nil, nil)

		hookCalls := 0
		handler := newActivityDispatchTaskHandler(activityDispatchTaskHandlerOptions{
			MatchingClient: matchingClient,
			DispatchTaskHook: func(
				ctx context.Context,
				namespaceID string,
				actualTask *activitypb.ActivityDispatchTask,
				dispatch func(context.Context) error,
			) error {
				hookCalls++
				require.Equal(t, "namespace-id", namespaceID)
				require.Same(t, task, actualTask)

				return dispatch(ctx)
			},
		})

		err := handler.Execute(chasm.NewEngineContext(context.Background(), engine), activityRef, chasm.TaskAttributes{}, task)
		require.NoError(t, err)
		require.Equal(t, 1, hookCalls)
	})

	t.Run("honors hook result", func(t *testing.T) {
		hookErr := errors.New("hook failed")
		testCases := []struct {
			name    string
			hookErr error
		}{
			{name: "suppresses dispatch"},
			{name: "propagates error", hookErr: hookErr},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				controller := gomock.NewController(t)
				engine := chasm.NewMockEngine(controller)
				activityRef := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{NamespaceID: "namespace-id"})
				chasmCtx := &chasm.MockMutableContext{
					MockContext: chasm.MockContext{
						HandleRef: func(chasm.Component) ([]byte, error) {
							return []byte("component-ref"), nil
						},
					},
				}
				activity := &Activity{
					ActivityState: &activitypb.ActivityState{
						TaskQueue: &taskqueuepb.TaskQueue{Name: "task-queue"},
					},
					LastAttempt: chasm.NewDataField(chasmCtx, &activitypb.ActivityAttemptState{Stamp: 7}),
				}

				engine.EXPECT().ReadComponent(gomock.Any(), activityRef, gomock.Any()).DoAndReturn(
					func(
						_ context.Context,
						_ chasm.ComponentRef,
						readFn func(chasm.Context, chasm.Component) error,
						_ ...chasm.TransitionOption,
					) error {
						return readFn(chasmCtx, activity)
					},
				)

				handler := newActivityDispatchTaskHandler(activityDispatchTaskHandlerOptions{
					MatchingClient: matchingservicemock.NewMockMatchingServiceClient(controller),
					DispatchTaskHook: func(
						context.Context,
						string,
						*activitypb.ActivityDispatchTask,
						func(context.Context) error,
					) error {
						return tc.hookErr
					},
				})

				err := handler.Execute(
					chasm.NewEngineContext(context.Background(), engine),
					activityRef,
					chasm.TaskAttributes{},
					&activitypb.ActivityDispatchTask{},
				)
				if tc.hookErr == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, tc.hookErr)
				}
			})
		}
	})

	t.Run("skipped when request construction fails", func(t *testing.T) {
		controller := gomock.NewController(t)
		engine := chasm.NewMockEngine(controller)
		activityRef := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{NamespaceID: "namespace-id"})
		validationErr := serviceerror.NewNotFound("task is no longer valid")

		engine.EXPECT().ReadComponent(gomock.Any(), activityRef, gomock.Any()).Return(validationErr)

		hookCalls := 0
		handler := newActivityDispatchTaskHandler(activityDispatchTaskHandlerOptions{
			MatchingClient: matchingservicemock.NewMockMatchingServiceClient(controller),
			DispatchTaskHook: func(
				context.Context,
				string,
				*activitypb.ActivityDispatchTask,
				func(context.Context) error,
			) error {
				hookCalls++
				return nil
			},
		})

		err := handler.Execute(
			chasm.NewEngineContext(context.Background(), engine),
			activityRef,
			chasm.TaskAttributes{},
			&activitypb.ActivityDispatchTask{},
		)
		require.ErrorIs(t, err, validationErr)
		require.Zero(t, hookCalls)
	})

	t.Run("bypassed on standby discard", func(t *testing.T) {
		controller := gomock.NewController(t)
		engine := chasm.NewMockEngine(controller)
		matchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
		activityRef := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{NamespaceID: "namespace-id"})
		chasmCtx := &chasm.MockMutableContext{
			MockContext: chasm.MockContext{
				HandleRef: func(chasm.Component) ([]byte, error) {
					return []byte("component-ref"), nil
				},
			},
		}
		activity := &Activity{
			ActivityState: &activitypb.ActivityState{
				TaskQueue: &taskqueuepb.TaskQueue{Name: "task-queue"},
			},
			LastAttempt: chasm.NewDataField(chasmCtx, &activitypb.ActivityAttemptState{Stamp: 7}),
		}

		engine.EXPECT().ReadComponent(gomock.Any(), activityRef, gomock.Any()).DoAndReturn(
			func(
				_ context.Context,
				_ chasm.ComponentRef,
				readFn func(chasm.Context, chasm.Component) error,
				_ ...chasm.TransitionOption,
			) error {
				return readFn(chasmCtx, activity)
			},
		)
		matchingClient.EXPECT().AddActivityTask(gomock.Any(), gomock.Any()).Return(nil, nil)

		hookCalls := 0
		handler := newActivityDispatchTaskHandler(activityDispatchTaskHandlerOptions{
			MatchingClient: matchingClient,
			DispatchTaskHook: func(
				context.Context,
				string,
				*activitypb.ActivityDispatchTask,
				func(context.Context) error,
			) error {
				hookCalls++
				return nil
			},
		})

		err := handler.Discard(
			chasm.NewEngineContext(context.Background(), engine),
			activityRef,
			chasm.TaskAttributes{},
			&activitypb.ActivityDispatchTask{DispatchReason: activitypb.DISPATCH_REASON_RETRY},
		)
		require.NoError(t, err)
		require.Zero(t, hookCalls)
	})
}

func TestActivityIsDispatchTaskValid(t *testing.T) {
	const currentStamp int32 = 42

	testCases := []struct {
		name   string
		status activitypb.ActivityExecutionStatus
		stamp  int32
		valid  bool
	}{
		{
			name:   "scheduled current attempt",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			stamp:  currentStamp,
			valid:  true,
		},
		{
			name:   "scheduled obsolete attempt",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			stamp:  currentStamp - 1,
			valid:  false,
		},
		{
			name:   "scheduled future attempt",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			stamp:  currentStamp + 1,
			valid:  false,
		},
		{
			name:   "started",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			stamp:  currentStamp,
			valid:  false,
		},
		{
			name:   "paused",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
			stamp:  currentStamp,
			valid:  false,
		},
		{
			name:   "completed",
			status: activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
			stamp:  currentStamp,
			valid:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{Status: tc.status},
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
					Stamp: currentStamp,
				}),
			}

			valid, err := activity.IsDispatchTaskValid(ctx, tc.stamp)

			require.NoError(t, err)
			require.Equal(t, tc.valid, valid)
		})
	}
}

func TestScheduleToCloseTimeoutTaskValidateStamp(t *testing.T) {
	handler := newScheduleToCloseTimeoutTaskHandler()

	newActivity := func(stamp int32) *Activity {
		return &Activity{
			ActivityState: &activitypb.ActivityState{
				Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
				ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
				ScheduleToCloseStamp:   stamp,
			},
		}
	}

	testCases := []struct {
		name          string
		activityStamp int32
		taskStamp     int32
		wantValid     bool
	}{
		// A legacy activity (created before ScheduleToCloseStamp existed) has stamp 0 on both
		// the state and the timer task. Its timer is valid only while the activity stamp is
		// still 0; once an options update bumps the stamp, the zero-stamp timer must be rejected.
		{"legacy task valid while activity stamp still zero", 0, 0, true},
		{"legacy zero-stamp task invalid after stamp bumped", 5, 0, false},
		{"matching stamp valid", 5, 5, true},
		{"stale non-zero stamp invalid", 5, 3, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			valid, err := handler.Validate(
				nil,
				newActivity(tc.activityStamp),
				chasm.TaskInvocation{},
				&activitypb.ScheduleToCloseTimeoutTask{Stamp: tc.taskStamp},
			)
			require.NoError(t, err)
			require.Equal(t, tc.wantValid, valid)
		})
	}
}

func TestTimeoutTaskTerminalFailure(t *testing.T) {
	testCases := []struct {
		name                string
		timeoutType         enumspb.TimeoutType
		startStatus         activitypb.ActivityExecutionStatus
		maximumAttempts     int32
		scheduleToClose     time.Duration
		nonRetryable        bool
		expectedTimeoutType enumspb.TimeoutType
		expectedRetryState  enumspb.RetryState
		expectedMessage     string
	}{
		{
			name:                "start to close deadline exhausted",
			timeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			scheduleToClose:     2 * time.Second,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			expectedRetryState:  enumspb.RETRY_STATE_TIMEOUT,
			expectedMessage:     common.FailureReasonActivityRetryScheduleToCloseTimeout,
		},
		{
			name:                "heartbeat deadline exhausted",
			timeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
			scheduleToClose:     2 * time.Second,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			expectedRetryState:  enumspb.RETRY_STATE_TIMEOUT,
			expectedMessage:     common.FailureReasonActivityRetryScheduleToCloseTimeout,
		},
		{
			name:                "start to close maximum attempts reached",
			timeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			maximumAttempts:     1,
			scheduleToClose:     time.Minute,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			expectedRetryState:  enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
		},
		{
			name:                "heartbeat maximum attempts reached",
			timeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
			maximumAttempts:     1,
			scheduleToClose:     time.Minute,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT,
			expectedRetryState:  enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
		},
		{
			name:                "start to close non-retryable timeout",
			timeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			scheduleToClose:     time.Minute,
			nonRetryable:        true,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			expectedRetryState:  enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
		},
		{
			name:                "start to close cancellation requested",
			timeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			startStatus:         activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			scheduleToClose:     time.Minute,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			expectedRetryState:  enumspb.RETRY_STATE_CANCEL_REQUESTED,
		},
		{
			name:                "schedule to close cancellation requested",
			timeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			startStatus:         activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			scheduleToClose:     time.Minute,
			expectedTimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			expectedRetryState:  enumspb.RETRY_STATE_CANCEL_REQUESTED,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow:            func(chasm.Component) time.Time { return defaultTime.Add(1500 * time.Millisecond) },
					HandleMetricsHandler: func() metrics.Handler { return metricsHandler },
					HandleNamespaceEntry: testNamespaceEntry,
					GoCtx: context.WithValue(context.Background(), ctxKeyActivityContext, &activityContext{
						config: &Config{
							BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(false),
						},
					}),
				},
			}
			retryPolicy := &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(time.Second),
				BackoffCoefficient: 1,
				MaximumAttempts:    tc.maximumAttempts,
			}
			if tc.nonRetryable {
				retryPolicy.NonRetryableErrorTypes = []string{
					retrypolicy.TimeoutFailureTypePrefix + tc.timeoutType.String(),
				}
			}
			startStatus := tc.startStatus
			if startStatus == activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED {
				startStatus = activitypb.ACTIVITY_EXECUTION_STATUS_STARTED
			}
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					RetryPolicy:            retryPolicy,
					ScheduleTime:           timestamppb.New(defaultTime),
					ScheduleToCloseTimeout: durationpb.New(tc.scheduleToClose),
					Status:                 startStatus,
					TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
				},
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
					Count:       1,
					StartedTime: timestamppb.New(defaultTime),
				}),
				Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
				LastHeartbeat: chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{
					Details: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte("heartbeat details")}}},
				}),
			}

			var err error
			switch tc.timeoutType {
			case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
				err = newScheduleToCloseTimeoutTaskHandler().Execute(ctx, activity, chasm.TaskAttributes{}, &activitypb.ScheduleToCloseTimeoutTask{})
			case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
				err = newStartToCloseTimeoutTaskHandler().Execute(ctx, activity, chasm.TaskAttributes{}, &activitypb.StartToCloseTimeoutTask{})
			case enumspb.TIMEOUT_TYPE_HEARTBEAT:
				err = newHeartbeatTimeoutTaskHandler().Execute(ctx, activity, chasm.TaskAttributes{}, &activitypb.HeartbeatTimeoutTask{})
			default:
				t.Fatalf("unexpected timeout type: %v", tc.timeoutType)
			}
			require.NoError(t, err)

			terminalFailure := activity.outcome(ctx).GetFailure()
			require.Equal(t, tc.expectedTimeoutType, terminalFailure.GetTimeoutFailureInfo().GetTimeoutType())
			require.Equal(t, tc.expectedRetryState, activity.outcome(ctx).GetRetryState())
			if tc.expectedMessage != "" {
				require.Equal(t, tc.expectedMessage, terminalFailure.GetMessage())
			}
			if tc.timeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE || tc.expectedMessage != "" {
				require.NotNil(t, activity.Outcome.Get(ctx).GetFailed())
			} else {
				require.Nil(t, activity.Outcome.Get(ctx).GetVariant())
			}
			if tc.timeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE {
				require.Nil(t, activity.LastAttempt.Get(ctx).GetLastFailureDetails())
			} else {
				lastFailure := activity.LastAttempt.Get(ctx).GetLastFailureDetails().GetFailure()
				require.Equal(t, tc.timeoutType, lastFailure.GetTimeoutFailureInfo().GetTimeoutType())
				require.Equal(t, lastFailure.GetTimeoutFailureInfo().GetLastHeartbeatDetails(), terminalFailure.GetTimeoutFailureInfo().GetLastHeartbeatDetails())
			}

			for _, metricName := range []string{metrics.ActivityTimeout.Name(), metrics.ActivityTaskTimeout.Name()} {
				recordings := capture.Snapshot()[metricName]
				require.Len(t, recordings, 1)
				require.Equal(t, tc.timeoutType.String(), recordings[0].Tags["timeout_type"])
			}
		})
	}
}

func TestHandoffTimeoutTasks(t *testing.T) {
	t.Parallel()

	// Start-to-close expiry during handoff backoff schedules an activity retry and invalidates superseded tasks.
	t.Run("StartToCloseDuringRedispatch", func(t *testing.T) {
		t.Parallel()

		a, ctx := newHandoffRetryTestActivity(t)
		now := ctx.Now(a)
		ctx.HandleNow = func(chasm.Component) time.Time { return now }
		attempt := a.LastAttempt.Get(ctx)
		attempt.Count = 1
		initialAttempt := attempt.GetCount()
		startToCloseDeadline := attempt.GetAsyncHandoffState().GetStartToCloseStartedTime().AsTime().Add(a.GetStartToCloseTimeout().AsDuration())
		activityRetryInterval := a.GetRetryPolicy().GetInitialInterval().AsDuration()
		scheduleToStartTimeout := a.GetScheduleToStartTimeout().AsDuration()
		failure := &failurepb.Failure{Message: "submission failed"}
		oldBudget := &activitypb.HandoffStartToCloseTimeoutTask{Stamp: attempt.GetAsyncHandoffState().GetStartToCloseStamp()}
		retryInterval := 90 * time.Second

		state, err := a.tryReschedule(ctx, retryEvent{overridingRetryInterval: retryInterval, failure: failure})
		require.NoError(t, err)
		require.Equal(t, enumspb.RETRY_STATE_IN_PROGRESS, state)

		oldQueueTask := ctx.Tasks[0]
		oldDispatch := ctx.Tasks[1]
		require.Equal(t, now.Add(retryInterval), oldDispatch.Attributes.ScheduledTime)
		// The activity attempt is still 1; handoff redispatch must use retry quota/accounting.
		require.Equal(t, activitypb.DISPATCH_REASON_RETRY, oldDispatch.Payload.(*activitypb.ActivityDispatchTask).GetDispatchReason())
		require.Equal(t, initialAttempt, attempt.GetCount())

		now = startToCloseDeadline
		ctx.Tasks = nil
		handler := newHandoffStartToCloseTimeoutTaskHandler()
		attrs := chasm.TaskAttributes{ScheduledTime: now}
		valid, err := handler.Validate(ctx, a, chasm.TaskInvocation{TaskAttributes: attrs}, oldBudget)
		require.NoError(t, err)
		require.True(t, valid)

		require.NoError(t, handler.Execute(ctx, a, attrs, oldBudget))
		require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED, a.GetStatus())
		require.Equal(t, initialAttempt+1, attempt.GetCount())
		require.EqualValues(t, 1, attempt.GetAsyncHandoffState().GetCount())
		require.Equal(t, activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY, attempt.GetPendingRetryType())
		require.Nil(t, attempt.GetAsyncHandoffState().GetStartToCloseStartedTime())
		expectedDispatchTime := now.Add(activityRetryInterval)
		require.Equal(t, expectedDispatchTime, attempt.GetDispatchTime().AsTime())
		require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, attempt.GetLastFailureDetails().GetFailure().GetTimeoutFailureInfo().GetTimeoutType())
		require.Equal(t, []chasm.MockTask{
			{Component: a, Attributes: chasm.TaskAttributes{ScheduledTime: expectedDispatchTime.Add(scheduleToStartTimeout)}, Payload: &activitypb.ScheduleToStartTimeoutTask{Stamp: attempt.GetStamp()}},
			{Component: a, Attributes: chasm.TaskAttributes{ScheduledTime: expectedDispatchTime}, Payload: &activitypb.ActivityDispatchTask{Stamp: attempt.GetStamp(), DispatchReason: activitypb.DISPATCH_REASON_RETRY, StartDelayBucket: activitypb.START_DELAY_BUCKET_NONE}},
		}, ctx.Tasks)

		valid, err = handler.Validate(ctx, a, chasm.TaskInvocation{TaskAttributes: attrs}, oldBudget)
		require.NoError(t, err)
		require.False(t, valid)

		valid, err = newActivityDispatchTaskHandler(activityDispatchTaskHandlerOptions{}).Validate(ctx, a, chasm.TaskInvocation{}, oldDispatch.Payload.(*activitypb.ActivityDispatchTask))
		require.NoError(t, err)
		require.False(t, valid)

		valid, err = newScheduleToStartTimeoutTaskHandler().Validate(ctx, a, chasm.TaskInvocation{}, oldQueueTask.Payload.(*activitypb.ScheduleToStartTimeoutTask))
		require.NoError(t, err)
		require.False(t, valid)
	})

	// Handoff replaces phase and heartbeat timers without invalidating the worker token or start-to-close timer.
	t.Run("PhaseAndHeartbeatTimers", func(t *testing.T) {
		t.Parallel()

		for _, testCase := range []struct {
			name                       string
			preHandoffHeartbeatTimeout time.Duration
		}{
			{name: "different heartbeat timeouts", preHandoffHeartbeatTimeout: 45 * time.Second},
			{name: "equal heartbeat timeouts", preHandoffHeartbeatTimeout: 30 * time.Second},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				t.Parallel()

				a, ctx := newHandoffRetryTestActivity(t)
				now := defaultTime.Add(15 * time.Second)
				ctx.HandleNow = func(chasm.Component) time.Time { return now }
				a.StartToCloseTimeout = durationpb.New(2 * time.Minute)
				a.AsyncHandoffOptions.StartToHandoffTimeout = durationpb.New(time.Minute)
				a.AsyncHandoffOptions.HandoffToCloseTimeout = durationpb.New(90 * time.Second)
				a.AsyncHandoffOptions.HeartbeatTimeout = durationpb.New(testCase.preHandoffHeartbeatTimeout)

				a.HeartbeatTimeout = durationpb.New(30 * time.Second)
				ref, err := (&persistencespb.ChasmComponentRef{NamespaceId: "test-namespace-id", BusinessId: "test-activity"}).Marshal()
				require.NoError(t, err)

				before := common.CloneProto(a.LastAttempt.Get(ctx))
				startToHandoffDeadline := before.GetStartedTime().AsTime().Add(a.GetAsyncHandoffOptions().GetStartToHandoffTimeout().AsDuration())
				handoffToCloseTimeout := a.GetAsyncHandoffOptions().GetHandoffToCloseTimeout().AsDuration()
				postHandoffHeartbeatTimeout := a.GetHeartbeatTimeout().AsDuration()
				token := &tokenspb.Task{Attempt: before.GetCount(), ActivityAttemptStamp: before.GetStartedStamp(), ComponentRef: ref}
				_, err = a.RecordHeartbeat(ctx, WithToken[*historyservice.RecordActivityTaskHeartbeatRequest]{Token: token, Request: &historyservice.RecordActivityTaskHeartbeatRequest{NamespaceId: "test-namespace-id", HeartbeatRequest: &workflowservice.RecordActivityTaskHeartbeatRequest{Details: payloads.EncodeString("external-job")}}})
				require.NoError(t, err)

				require.Len(t, ctx.Tasks, 1)
				oldHeartbeat := ctx.Tasks[0]
				require.Equal(t, now.Add(testCase.preHandoffHeartbeatTimeout), oldHeartbeat.Attributes.ScheduledTime)

				ctx.Tasks = nil
				now = defaultTime.Add(20 * time.Second)
				input := WithToken[*activitypb.RespondActivityTaskHandoffRequest]{Token: token, Request: &activitypb.RespondActivityTaskHandoffRequest{NamespaceId: "test-namespace-id"}}

				result, err := a.HandleHandoff(ctx, input)
				require.NoError(t, err)
				require.True(t, result.NewlyRecorded)
				protorequire.ProtoEqual(t, payloads.EncodeString("external-job"), a.lastHeartbeatDetails(ctx))
				require.Equal(t, before.GetAsyncHandoffState().GetStartToCloseStamp(), a.LastAttempt.Get(ctx).GetAsyncHandoffState().GetStartToCloseStamp())
				require.Equal(t, []chasm.MockTask{
					{Component: a, Attributes: chasm.TaskAttributes{ScheduledTime: now.Add(handoffToCloseTimeout)}, Payload: &activitypb.HandoffToCloseTimeoutTask{Stamp: before.GetStamp() + 1}},
					{Component: a, Attributes: chasm.TaskAttributes{ScheduledTime: now.Add(postHandoffHeartbeatTimeout)}, Payload: &activitypb.HeartbeatTimeoutTask{Stamp: before.GetStamp() + 1}},
				}, ctx.Tasks)

				valid, err := newHeartbeatTimeoutTaskHandler().Validate(ctx, a, chasm.TaskInvocation{TaskAttributes: oldHeartbeat.Attributes}, oldHeartbeat.Payload.(*activitypb.HeartbeatTimeoutTask))
				require.NoError(t, err)
				require.False(t, valid)

				valid, err = newStartToHandoffTimeoutTaskHandler().Validate(ctx, a, chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: startToHandoffDeadline}}, &activitypb.StartToHandoffTimeoutTask{Stamp: before.GetStamp()})
				require.NoError(t, err)
				require.False(t, valid)

				result, err = a.HandleHandoff(ctx, input)
				require.NoError(t, err)
				require.False(t, result.NewlyRecorded)
				require.Len(t, ctx.Tasks, 2)

				valid, err = newHeartbeatTimeoutTaskHandler().Validate(ctx, a, chasm.TaskInvocation{TaskAttributes: ctx.Tasks[1].Attributes}, ctx.Tasks[1].Payload.(*activitypb.HeartbeatTimeoutTask))
				require.NoError(t, err)
				require.True(t, valid)

				now = defaultTime.Add(40 * time.Second)
				_, err = a.RecordHeartbeat(ctx, WithToken[*historyservice.RecordActivityTaskHeartbeatRequest]{Token: token, Request: &historyservice.RecordActivityTaskHeartbeatRequest{NamespaceId: "test-namespace-id"}})
				require.NoError(t, err)
				require.Equal(t, now.Add(postHandoffHeartbeatTimeout), ctx.Tasks[2].Attributes.ScheduledTime)

				valid, err = newHeartbeatTimeoutTaskHandler().Validate(ctx, a, chasm.TaskInvocation{TaskAttributes: ctx.Tasks[1].Attributes}, ctx.Tasks[1].Payload.(*activitypb.HeartbeatTimeoutTask))
				require.NoError(t, err)
				require.False(t, valid)
			})
		}
	})

	// Shared timeout handling reports the correct timeout type and retry state, preserving failure details without advancing counters.
	t.Run("TerminalOutcomes", func(t *testing.T) {
		t.Parallel()

		for _, testCase := range []struct {
			timeoutType enumspb.TimeoutType
			reason      string
		}{
			{timeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE, reason: "maximum attempts"},
			{timeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE, reason: "retry window"},
			{timeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE, reason: "excluded"},
			{timeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE, reason: "no policy"},

			{timeoutType: enumspb.TIMEOUT_TYPE_START_TO_HANDOFF, reason: "maximum attempts"},
			{timeoutType: enumspb.TIMEOUT_TYPE_START_TO_HANDOFF, reason: "retry window"},
			{timeoutType: enumspb.TIMEOUT_TYPE_START_TO_HANDOFF, reason: "excluded"},
			{timeoutType: enumspb.TIMEOUT_TYPE_START_TO_HANDOFF, reason: "no policy"},

			{timeoutType: enumspb.TIMEOUT_TYPE_HANDOFF_TO_CLOSE, reason: "maximum attempts"},
			{timeoutType: enumspb.TIMEOUT_TYPE_HANDOFF_TO_CLOSE, reason: "retry window"},
			{timeoutType: enumspb.TIMEOUT_TYPE_HANDOFF_TO_CLOSE, reason: "excluded"},
			{timeoutType: enumspb.TIMEOUT_TYPE_HANDOFF_TO_CLOSE, reason: "no policy"},

			{timeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT, reason: "maximum attempts"},
			{timeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT, reason: "retry window"},
			{timeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT, reason: "excluded"},
			{timeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT, reason: "no policy"},
		} {
			t.Run(testCase.timeoutType.String()+"/"+testCase.reason, func(t *testing.T) {
				t.Parallel()

				timeoutType := testCase.timeoutType
				a, ctx := newHandoffRetryTestActivity(t)
				a.AwaitingCompletion = timeoutType == enumspb.TIMEOUT_TYPE_HANDOFF_TO_CLOSE
				typ := retryTypeForTimeout(true, a.GetAwaitingCompletion(), timeoutType)
				policy, count := a.retryPolicyAndCount(ctx, typ)
				expectedType := timeoutType
				expectedState := enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED
				switch testCase.reason {
				case "maximum attempts":
					policy.MaximumAttempts = count
				case "retry window":
					a.ScheduleToCloseTimeout = durationpb.New(ctx.Now(a).Sub(a.GetScheduleTime().AsTime()) + time.Second)
					expectedState = enumspb.RETRY_STATE_TIMEOUT
					expectedType = enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE
				case "excluded":
					policy.NonRetryableErrorTypes = []string{retrypolicy.TimeoutFailureTypePrefix + timeoutType.String()}
					expectedState = enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE
				case "no policy":
					if typ == activitypb.ACTIVITY_RETRY_TYPE_HANDOFF {
						a.AsyncHandoffOptions.RetryPolicy = nil
					} else {
						a.RetryPolicy = nil
					}

					expectedState = enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET
				default:
					t.Fatalf("unhandled terminal reason %q", testCase.reason)
				}

				prior := &failurepb.Failure{Message: "prior failure"}
				attempt := a.LastAttempt.Get(ctx)
				attempt.LastFailureDetails = &activitypb.ActivityAttemptState_LastFailureDetails{Failure: prior}
				beforeMain, beforeHandoff := attempt.GetCount(), attempt.GetAsyncHandoffState().GetCount()
				a.LastHeartbeat = chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{Details: payloads.EncodeString("progress")})

				require.NoError(t, a.handleTimeout(ctx, timeoutType))
				require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, a.GetStatus())
				require.Equal(t, expectedState, a.Outcome.Get(ctx).GetRetryState())

				failure := a.Outcome.Get(ctx).GetFailed().GetFailure()
				if failure == nil {
					failure = attempt.GetLastFailureDetails().GetFailure()
				}

				require.Equal(t, expectedType, failure.GetTimeoutFailureInfo().GetTimeoutType())
				protorequire.ProtoEqual(t, prior, failure.GetCause())
				protorequire.ProtoEqual(t, payloads.EncodeString("progress"), failure.GetTimeoutFailureInfo().GetLastHeartbeatDetails())
				require.Equal(t, [2]int32{beforeMain, beforeHandoff}, [2]int32{attempt.GetCount(), attempt.GetAsyncHandoffState().GetCount()})
				require.False(t, a.GetAwaitingCompletion())
				require.Nil(t, attempt.GetAsyncHandoffState().GetStartToCloseStartedTime())
				require.Empty(t, ctx.Tasks)
			})
		}
	})

	// A start-to-close timeout invalidates a competing handoff timeout at the same deadline.
	t.Run("StartToCloseInvalidatesHandoffTimeout", func(t *testing.T) {
		t.Parallel()

		a, ctx := newHandoffRetryTestActivity(t)
		now := ctx.Now(a)
		ctx.HandleNow = func(chasm.Component) time.Time { return now }
		a.StartToCloseTimeout = a.GetAsyncHandoffOptions().GetStartToHandoffTimeout()
		require.NoError(t, a.issueHandoffTimers(ctx, true, true))
		require.Len(t, ctx.Tasks, 2)

		startToClose := ctx.Tasks[0]
		startToHandoff := ctx.Tasks[1]
		require.Equal(t, startToClose.Attributes.ScheduledTime, startToHandoff.Attributes.ScheduledTime)
		now = startToClose.Attributes.ScheduledTime
		startToCloseHandler := newHandoffStartToCloseTimeoutTaskHandler()
		startToHandoffHandler := newStartToHandoffTimeoutTaskHandler()
		invocation := chasm.TaskInvocation{TaskAttributes: startToClose.Attributes}

		// Both timeouts apply before either one ends the current activity task.
		valid, err := startToCloseHandler.Validate(ctx, a, invocation, startToClose.Payload.(*activitypb.HandoffStartToCloseTimeoutTask))
		require.NoError(t, err)
		require.True(t, valid)
		valid, err = startToHandoffHandler.Validate(ctx, a, invocation, startToHandoff.Payload.(*activitypb.StartToHandoffTimeoutTask))
		require.NoError(t, err)
		require.True(t, valid)

		attempt := a.LastAttempt.Get(ctx)
		initialAttempt := attempt.GetCount()
		ctx.Tasks = nil
		require.NoError(t, startToCloseHandler.Execute(ctx, a, startToClose.Attributes, startToClose.Payload.(*activitypb.HandoffStartToCloseTimeoutTask)))

		valid, err = startToHandoffHandler.Validate(ctx, a, invocation, startToHandoff.Payload.(*activitypb.StartToHandoffTimeoutTask))
		require.NoError(t, err)
		require.False(t, valid)
		require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED, a.GetStatus())
		require.Equal(t, initialAttempt+1, attempt.GetCount())
		require.EqualValues(t, 1, attempt.GetAsyncHandoffState().GetCount())
		require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, attempt.GetLastFailureDetails().GetFailure().GetTimeoutFailureInfo().GetTimeoutType())
	})

	// Invalid timeout types or missing clock state must return errors without changing the activity or adding tasks.
	t.Run("InvalidTimeoutState", func(t *testing.T) {
		t.Parallel()

		for _, testCase := range []struct {
			name        string
			timeoutType enumspb.TimeoutType
		}{
			{name: "unspecified", timeoutType: enumspb.TIMEOUT_TYPE_UNSPECIFIED},
			{name: "unknown", timeoutType: enumspb.TimeoutType(100)},
		} {
			t.Run(testCase.name+" timeout cannot apply a pending reset", func(t *testing.T) {
				t.Parallel()

				a, ctx := newHandoffRetryTestActivity(t)
				a.Status = activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED
				beforeActivity := common.CloneProto(a.ActivityState)
				beforeAttempt := common.CloneProto(a.LastAttempt.Get(ctx))

				err := a.handleTimeout(ctx, testCase.timeoutType)
				require.ErrorAs(t, err, new(*serviceerror.Internal))
				protorequire.ProtoEqual(t, beforeActivity, a.ActivityState)
				protorequire.ProtoEqual(t, beforeAttempt, a.LastAttempt.Get(ctx))
				require.Empty(t, ctx.Tasks)
			})
		}

		t.Run("a phase timer executed without options fails atomically", func(t *testing.T) {
			t.Parallel()

			a, ctx := newHandoffRetryTestActivity(t)
			a.AsyncHandoffOptions = nil
			beforeActivity := common.CloneProto(a.ActivityState)
			beforeAttempt := common.CloneProto(a.LastAttempt.Get(ctx))

			err := newStartToHandoffTimeoutTaskHandler().Execute(ctx, a, chasm.TaskAttributes{}, &activitypb.StartToHandoffTimeoutTask{})
			require.ErrorAs(t, err, new(*serviceerror.Internal))
			protorequire.ProtoEqual(t, beforeActivity, a.ActivityState)
			protorequire.ProtoEqual(t, beforeAttempt, a.LastAttempt.Get(ctx))
			require.Empty(t, ctx.Tasks)
		})
		for _, testCase := range []struct {
			name   string
			mutate func(*Activity, chasm.Context)
		}{
			{name: "missing pickup", mutate: func(a *Activity, ctx chasm.Context) { a.LastAttempt.Get(ctx).StartedTime = nil }},
			{name: "missing handoff anchor", mutate: func(a *Activity, _ chasm.Context) { a.AwaitingCompletion = true }},
			{name: "missing budget anchor", mutate: func(a *Activity, ctx chasm.Context) {
				a.LastAttempt.Get(ctx).AsyncHandoffState.StartToCloseStartedTime = nil
			}},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				t.Parallel()

				a, ctx := newHandoffRetryTestActivity(t)
				testCase.mutate(a, ctx)
				_, err := newHandoffStartToCloseTimeoutTaskHandler().Validate(ctx, a, chasm.TaskInvocation{}, &activitypb.HandoffStartToCloseTimeoutTask{Stamp: a.LastAttempt.Get(ctx).GetAsyncHandoffState().GetStartToCloseStamp()})
				require.ErrorAs(t, err, new(*serviceerror.Internal))
				require.Empty(t, ctx.Tasks)
			})
		}
	})
}
