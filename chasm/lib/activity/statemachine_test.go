package activity

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	defaultTime        = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	defaultRetryPolicy = &commonpb.RetryPolicy{
		InitialInterval:    durationpb.New(1 * time.Second),
		BackoffCoefficient: 2.0,
		MaximumAttempts:    5,
		MaximumInterval:    durationpb.New(100 * time.Second),
	}
	defaultScheduleToCloseTimeout = 10 * time.Minute
	defaultScheduleToStartTimeout = 2 * time.Minute
	defaultStartToCloseTimeout    = 3 * time.Minute
)

func TestTransitionScheduled(t *testing.T) {
	testCases := []struct {
		name                     string
		startingAttemptCount     int32
		expectedTasks            []chasm.MockTask
		startDelay               time.Duration
		scheduleToStartTimeout   time.Duration
		scheduleToCloseTimeout   time.Duration
		expectedDispatchReason   activitypb.DispatchReason
		expectedStartDelayBucket activitypb.StartDelayBucket
	}{
		{
			name:                 "all timeouts set",
			startingAttemptCount: 0,
			expectedTasks: []chasm.MockTask{
				{Payload: &activitypb.ScheduleToStartTimeoutTask{}},
				{Payload: &activitypb.ScheduleToCloseTimeoutTask{}},
				{Payload: &activitypb.ActivityDispatchTask{}},
			},
			scheduleToStartTimeout:   defaultScheduleToStartTimeout,
			scheduleToCloseTimeout:   defaultScheduleToCloseTimeout,
			expectedDispatchReason:   activitypb.DISPATCH_REASON_IMMEDIATE,
			expectedStartDelayBucket: activitypb.START_DELAY_BUCKET_NONE,
		},
		{
			name:                 "schedule to start timeout not set",
			startingAttemptCount: 0,
			expectedTasks: []chasm.MockTask{
				{Payload: &activitypb.ScheduleToCloseTimeoutTask{}},
				{Payload: &activitypb.ActivityDispatchTask{}},
			},
			scheduleToStartTimeout:   0,
			scheduleToCloseTimeout:   defaultScheduleToCloseTimeout,
			expectedDispatchReason:   activitypb.DISPATCH_REASON_IMMEDIATE,
			expectedStartDelayBucket: activitypb.START_DELAY_BUCKET_NONE,
		},
		{
			name:                 "schedule to close timeout not set",
			startingAttemptCount: 0,
			expectedTasks: []chasm.MockTask{
				{Payload: &activitypb.ScheduleToStartTimeoutTask{}},
				{Payload: &activitypb.ActivityDispatchTask{}},
			},
			scheduleToStartTimeout:   defaultScheduleToStartTimeout,
			scheduleToCloseTimeout:   0,
			expectedDispatchReason:   activitypb.DISPATCH_REASON_IMMEDIATE,
			expectedStartDelayBucket: activitypb.START_DELAY_BUCKET_NONE,
		},
		{
			name:                 "start delay",
			startingAttemptCount: 0,
			expectedTasks: []chasm.MockTask{
				{Payload: &activitypb.ScheduleToStartTimeoutTask{}},
				{Payload: &activitypb.ScheduleToCloseTimeoutTask{}},
				{Payload: &activitypb.ActivityDispatchTask{}},
			},
			startDelay:               5 * time.Minute,
			scheduleToStartTimeout:   defaultScheduleToStartTimeout,
			scheduleToCloseTimeout:   defaultScheduleToCloseTimeout,
			expectedDispatchReason:   activitypb.DISPATCH_REASON_START_DELAY,
			expectedStartDelayBucket: activitypb.START_DELAY_BUCKET_1M_10M,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return defaultTime },
				},
			}
			attemptState := &activitypb.ActivityAttemptState{Count: tc.startingAttemptCount}
			outcome := &activitypb.ActivityOutcome{}
			input := payloads.EncodeString("test-input")

			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					RetryPolicy:            defaultRetryPolicy,
					ScheduleTime:           timestamppb.New(defaultTime),
					StartDelay:             durationpb.New(tc.startDelay),
					ScheduleToCloseTimeout: durationpb.New(tc.scheduleToCloseTimeout),
					ScheduleToStartTimeout: durationpb.New(tc.scheduleToStartTimeout),
					StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
					Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED,
					TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
				},
				LastAttempt: chasm.NewDataField(ctx, attemptState),
				Outcome:     chasm.NewDataField(ctx, outcome),
				RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
					Input: input,
				}),
			}

			err := TransitionScheduled.Apply(activity, ctx, nil)
			require.NoError(t, err)
			require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED, activity.Status)
			require.EqualValues(t, 1, attemptState.Count)

			// Verify added tasks
			require.Len(t, ctx.Tasks, len(tc.expectedTasks))
			for i, expectedTask := range tc.expectedTasks {
				actualTask := ctx.Tasks[i]

				require.IsType(t, expectedTask.Payload, actualTask.Payload, "expected %T at index %d, got %T",
					expectedTask.Payload, i, actualTask.Payload)

				switch expectedTask.Payload.(type) {
				case *activitypb.ActivityDispatchTask:
					dispatchTask := actualTask.Payload.(*activitypb.ActivityDispatchTask)
					if tc.startDelay == 0 {
						require.Empty(t, actualTask.Attributes.ScheduledTime)
					} else {
						require.Equal(t, defaultTime.Add(tc.startDelay), actualTask.Attributes.ScheduledTime)
					}
					require.Equal(t, tc.expectedDispatchReason, dispatchTask.GetDispatchReason())
					require.Equal(t, tc.expectedStartDelayBucket, dispatchTask.GetStartDelayBucket())
				case *activitypb.ScheduleToStartTimeoutTask:
					require.Equal(t, defaultTime.Add(tc.startDelay).Add(tc.scheduleToStartTimeout), actualTask.Attributes.ScheduledTime)
				case *activitypb.ScheduleToCloseTimeoutTask:
					require.Equal(t, defaultTime.Add(tc.startDelay).Add(tc.scheduleToCloseTimeout), actualTask.Attributes.ScheduledTime)
				default:
					t.Fatalf("unexpected task payload type at index %d: %T", i, actualTask.Payload)
				}

			}
		})
	}
}

func TestTransitionRescheduled(t *testing.T) {
	testCases := []struct {
		name                     string
		startingAttemptCount     int32
		expectedTasks            []chasm.MockTask
		expectedRetryInterval    time.Duration
		startDelay               time.Duration
		expectedStartDelayBucket activitypb.StartDelayBucket
		retryPolicy              *commonpb.RetryPolicy
		scheduleToStartTimeout   time.Duration
		operationTag             string
		counterMetric            string
		timeoutType              enumspb.TimeoutType
	}{
		{
			name:                 "second attempt - timeout recorded",
			startingAttemptCount: 1,
			expectedTasks: []chasm.MockTask{
				{Payload: &activitypb.ScheduleToStartTimeoutTask{}},
				{Payload: &activitypb.ActivityDispatchTask{}},
			},
			expectedRetryInterval:    2 * time.Second,
			expectedStartDelayBucket: activitypb.START_DELAY_BUCKET_NONE,
			retryPolicy:              defaultRetryPolicy,
			scheduleToStartTimeout:   defaultScheduleToStartTimeout,
			operationTag:             metrics.TimerActiveTaskActivityTimeoutScope,
			counterMetric:            metrics.ActivityTaskTimeout.Name(),
			timeoutType:              enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		},
		{
			name:                 "third attempt - timeout recorded",
			startingAttemptCount: 2,
			expectedTasks: []chasm.MockTask{
				{Payload: &activitypb.ScheduleToStartTimeoutTask{}},
				{Payload: &activitypb.ActivityDispatchTask{}},
			},
			expectedRetryInterval:    4 * time.Second,
			expectedStartDelayBucket: activitypb.START_DELAY_BUCKET_NONE,
			retryPolicy:              defaultRetryPolicy,
			scheduleToStartTimeout:   defaultScheduleToStartTimeout,
			operationTag:             metrics.TimerActiveTaskActivityTimeoutScope,
			counterMetric:            metrics.ActivityTaskTimeout.Name(),
			timeoutType:              enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		},
		{
			name:                 "no schedule to start timeout",
			startingAttemptCount: 1,
			expectedTasks: []chasm.MockTask{
				{Payload: &activitypb.ActivityDispatchTask{}},
			},
			expectedRetryInterval:    2 * time.Second,
			expectedStartDelayBucket: activitypb.START_DELAY_BUCKET_NONE,
			retryPolicy:              defaultRetryPolicy,
			scheduleToStartTimeout:   0,
			operationTag:             metrics.TimerActiveTaskActivityTimeoutScope,
			counterMetric:            metrics.ActivityTaskTimeout.Name(),
			timeoutType:              enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		},
		{
			name:                 "heartbeat timeout - timeout recorded",
			startingAttemptCount: 1,
			expectedTasks: []chasm.MockTask{
				{Payload: &activitypb.ScheduleToStartTimeoutTask{}},
				{Payload: &activitypb.ActivityDispatchTask{}},
			},
			expectedRetryInterval:    2 * time.Second,
			expectedStartDelayBucket: activitypb.START_DELAY_BUCKET_NONE,
			retryPolicy:              defaultRetryPolicy,
			scheduleToStartTimeout:   defaultScheduleToStartTimeout,
			operationTag:             metrics.TimerActiveTaskActivityTimeoutScope,
			counterMetric:            metrics.ActivityTaskTimeout.Name(),
			timeoutType:              enumspb.TIMEOUT_TYPE_HEARTBEAT,
		},

		{
			name:                 "reschedule from failure",
			startingAttemptCount: 1,
			expectedTasks: []chasm.MockTask{
				{Payload: &activitypb.ScheduleToStartTimeoutTask{}},
				{Payload: &activitypb.ActivityDispatchTask{}},
			},
			expectedRetryInterval:    2 * time.Second,
			expectedStartDelayBucket: activitypb.START_DELAY_BUCKET_NONE,
			retryPolicy:              defaultRetryPolicy,
			scheduleToStartTimeout:   defaultScheduleToStartTimeout,
			operationTag:             metrics.HistoryRespondActivityTaskFailedScope,
			counterMetric:            metrics.ActivityTaskFail.Name(),
		},
		{
			name:                 "retry preserves configured start delay bucket",
			startingAttemptCount: 1,
			expectedTasks: []chasm.MockTask{
				{Payload: &activitypb.ScheduleToStartTimeoutTask{}},
				{Payload: &activitypb.ActivityDispatchTask{}},
			},
			expectedRetryInterval:    2 * time.Second,
			startDelay:               5 * time.Minute,
			expectedStartDelayBucket: activitypb.START_DELAY_BUCKET_1M_10M,
			retryPolicy:              defaultRetryPolicy,
			scheduleToStartTimeout:   defaultScheduleToStartTimeout,
			operationTag:             metrics.HistoryRespondActivityTaskFailedScope,
			counterMetric:            metrics.ActivityTaskFail.Name(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}
			ctx.HandleNow = func(chasm.Component) time.Time { return defaultTime }
			attemptState := &activitypb.ActivityAttemptState{Count: tc.startingAttemptCount}
			outcome := &activitypb.ActivityOutcome{}

			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					ActivityType:            &commonpb.ActivityType{Name: "test-activity-type"},
					RetryPolicy:             defaultRetryPolicy,
					ScheduleToCloseTimeout:  durationpb.New(defaultScheduleToCloseTimeout),
					ScheduleToStartTimeout:  durationpb.New(tc.scheduleToStartTimeout),
					StartDelay:              durationpb.New(tc.startDelay),
					StartToCloseTimeout:     durationpb.New(defaultStartToCloseTimeout),
					Status:                  activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
					TaskQueue:               &taskqueuepb.TaskQueue{Name: "test-task-queue"},
					FirstAttemptStartedTime: timestamppb.New(defaultTime),
				},
				LastAttempt: chasm.NewDataField(ctx, attemptState),
				Outcome:     chasm.NewDataField(ctx, outcome),
			}

			event := rescheduleEvent{
				retryInterval: tc.expectedRetryInterval,
				failure:       createStartToCloseTimeoutFailure(),
				timeoutType:   tc.timeoutType,
			}

			err := TransitionRescheduled.Apply(activity, ctx, event)
			require.NoError(t, err)
			require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED, activity.Status)
			require.Equal(t, tc.startingAttemptCount+1, attemptState.Count)
			protorequire.ProtoEqual(t, durationpb.New(tc.expectedRetryInterval), attemptState.GetCurrentRetryInterval())

			// Verify attempt state failure details updated correctly
			lastFailureDetails := attemptState.GetLastFailureDetails()
			require.NotNil(t, lastFailureDetails.GetFailure())
			require.Equal(t, lastFailureDetails.GetTime(), attemptState.GetCompleteTime())
			// This should remain nil on intermediate retry attempts. The final attempt goes directly via TransitionTimedOut.
			require.Nil(t, outcome.GetVariant())

			// Verify added tasks
			require.Len(t, ctx.Tasks, len(tc.expectedTasks))
			for i, expectedTask := range tc.expectedTasks {
				actualTask := ctx.Tasks[i]

				switch expectedTask.Payload.(type) {
				case *activitypb.ActivityDispatchTask:
					dispatchTask, ok := actualTask.Payload.(*activitypb.ActivityDispatchTask)
					require.True(t, ok, "expected ActivityDispatchTask at index %d", i)
					require.Equal(t, defaultTime.Add(tc.expectedRetryInterval), actualTask.Attributes.ScheduledTime)
					require.Equal(t, activitypb.DISPATCH_REASON_RETRY, dispatchTask.GetDispatchReason())
					require.Equal(t, tc.expectedStartDelayBucket, dispatchTask.GetStartDelayBucket())
				case *activitypb.ScheduleToStartTimeoutTask:
					_, ok := actualTask.Payload.(*activitypb.ScheduleToStartTimeoutTask)
					require.True(t, ok, "expected ScheduleToStartTimeoutTask at index %d", i)
					require.Equal(t, defaultTime.Add(tc.scheduleToStartTimeout).Add(tc.expectedRetryInterval), actualTask.Attributes.ScheduledTime)
				default:
					t.Fatalf("unexpected task payload type at index %d: %T", i, actualTask.Payload)
				}

			}
		})
	}
}

func TestTransitionStarted(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	ctx.HandleNow = func(chasm.Component) time.Time { return defaultTime }
	ctx.GoCtx = headers.SetVersionsForTests(context.Background(), temporal.SDKVersion, headers.ClientNameGoSDK, "", "")
	attemptState := &activitypb.ActivityAttemptState{
		Count:       1,
		StartedTime: timestamppb.New(defaultTime),
	}
	outcome := &activitypb.ActivityOutcome{}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			RetryPolicy:            defaultRetryPolicy,
			ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
			ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
			StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		},
		LastAttempt: chasm.NewDataField(ctx, attemptState),
		Outcome:     chasm.NewDataField(ctx, outcome),
	}

	err := TransitionStarted.Apply(activity, ctx, &historyservice.RecordActivityTaskStartedRequest{
		PollRequest: &workflowservice.PollActivityTaskQueueRequest{
			Identity: "test-worker",
		},
		VersionDirective: &taskqueuespb.TaskVersionDirective{
			DeploymentVersion: &deploymentspb.WorkerDeploymentVersion{
				DeploymentName: "test-deployment",
				BuildId:        "test-build-1",
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_STARTED, activity.Status)
	require.EqualValues(t, 1, attemptState.Count)
	require.Equal(t, defaultTime, attemptState.StartedTime.AsTime())
	require.Equal(t, "test-worker", attemptState.LastWorkerIdentity)
	require.Equal(t, headers.ClientNameGoSDK, attemptState.SdkName)
	require.Equal(t, temporal.SDKVersion, attemptState.SdkVersion)

	// Verify last deployment version
	deploymentVersion := attemptState.GetLastDeploymentVersion()
	require.NotNil(t, deploymentVersion)
	require.Equal(t, "test-deployment", deploymentVersion.GetDeploymentName())
	require.Equal(t, "test-build-1", deploymentVersion.GetBuildId())

	// Verify added tasks
	require.Len(t, ctx.Tasks, 1)
	_, ok := ctx.Tasks[0].Payload.(*activitypb.StartToCloseTimeoutTask)
	require.True(t, ok, "expected ScheduleToStartTimeoutTask")
	require.Equal(t, defaultTime.Add(defaultStartToCloseTimeout), ctx.Tasks[0].Attributes.ScheduledTime)
}

func TestTransitionTimedout(t *testing.T) {
	testCases := []struct {
		name             string
		startStatus      activitypb.ActivityExecutionStatus
		timeoutType      enumspb.TimeoutType
		attemptCount     int32
		heartbeatDetails *commonpb.Payloads
		// hasStartedTime seeds the attempt with a StartedTime. It is stale (from a prior attempt)
		// when the activity is not currently running, e.g. during retry backoff.
		hasStartedTime bool
		// expectStartToCloseLatency is true only when an attempt was actively running at timeout.
		expectStartToCloseLatency bool
	}{
		{
			name:         "schedule to start timeout, never started",
			startStatus:  activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			timeoutType:  enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
			attemptCount: 1,
		},
		{
			name:         "schedule to close timeout from scheduled, never started",
			startStatus:  activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			timeoutType:  enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			attemptCount: 1,
		},
		{
			name:                      "schedule to close timeout from started status with heartbeat details",
			startStatus:               activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			timeoutType:               enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			attemptCount:              4,
			heartbeatDetails:          payloads.EncodeString("schedule-to-close-heartbeat"),
			hasStartedTime:            true,
			expectStartToCloseLatency: true,
		},
		{
			name:                      "start to close timeout with heartbeat details",
			startStatus:               activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			timeoutType:               enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			attemptCount:              5,
			heartbeatDetails:          payloads.EncodeString("start-to-close-heartbeat"),
			hasStartedTime:            true,
			expectStartToCloseLatency: true,
		},
		{
			name:             "heartbeat timeout with heartbeat details",
			startStatus:      activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			timeoutType:      enumspb.TIMEOUT_TYPE_HEARTBEAT,
			attemptCount:     2,
			heartbeatDetails: payloads.EncodeString("heartbeat-details"),
		},
		{
			name:         "heartbeat timeout without heartbeat details",
			startStatus:  activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			timeoutType:  enumspb.TIMEOUT_TYPE_HEARTBEAT,
			attemptCount: 2,
		},
		{
			name:                      "schedule to close timeout from started status",
			startStatus:               activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			timeoutType:               enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			attemptCount:              4,
			hasStartedTime:            true,
			expectStartToCloseLatency: true,
		},
		{
			name:                      "start to close timeout",
			startStatus:               activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			timeoutType:               enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			attemptCount:              5,
			hasStartedTime:            true,
			expectStartToCloseLatency: true,
		},
		{
			name:                      "heartbeat timeout",
			startStatus:               activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			timeoutType:               enumspb.TIMEOUT_TYPE_HEARTBEAT,
			attemptCount:              2,
			hasStartedTime:            true,
			expectStartToCloseLatency: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}
			attemptState := &activitypb.ActivityAttemptState{Count: tc.attemptCount}
			if tc.hasStartedTime {
				attemptState.StartedTime = timestamppb.New(defaultTime)
			}
			outcome := &activitypb.ActivityOutcome{}

			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					RetryPolicy:            defaultRetryPolicy,
					ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
					ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
					StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
					Status:                 tc.startStatus,
					TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
				},
				LastAttempt: chasm.NewDataField(ctx, attemptState),
				Outcome:     chasm.NewDataField(ctx, outcome),
			}
			if tc.heartbeatDetails != nil {
				activity.LastHeartbeat = chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{
					Details: tc.heartbeatDetails,
				})
			}

			controller := gomock.NewController(t)
			metricsHandler := metrics.NewMockHandler(controller)

			if tc.expectStartToCloseLatency {
				timerStartToCloseLatency := metrics.NewMockTimerIface(controller)
				timerStartToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
				metricsHandler.EXPECT().Timer(metrics.ActivityStartToCloseLatency.Name()).Return(timerStartToCloseLatency)
			}

			timerScheduleToCloseLatency := metrics.NewMockTimerIface(controller)
			timerScheduleToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
			metricsHandler.EXPECT().Timer(metrics.ActivityScheduleToCloseLatency.Name()).Return(timerScheduleToCloseLatency)

			timeoutTag := metrics.StringTag("timeout_type", tc.timeoutType.String())

			counterTimeout := metrics.NewMockCounterIface(controller)
			counterTimeout.EXPECT().Record(int64(1), timeoutTag).Times(1)
			metricsHandler.EXPECT().Counter(metrics.ActivityTimeout.Name()).Return(counterTimeout)

			counterTaskTimeout := metrics.NewMockCounterIface(controller)
			counterTaskTimeout.EXPECT().Record(int64(1), timeoutTag).Times(1)
			metricsHandler.EXPECT().Counter(metrics.ActivityTaskTimeout.Name()).Return(counterTaskTimeout)

			event := timeoutEvent{
				timeoutType:    tc.timeoutType,
				metricsHandler: metricsHandler,
				fromStatus:     tc.startStatus,
			}

			err := TransitionTimedOut.Apply(activity, ctx, event)
			require.NoError(t, err)
			require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, activity.Status)
			require.Equal(t, tc.attemptCount, attemptState.Count)

			switch tc.timeoutType {
			case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
				enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
				// Timeout failure is recorded in outcome but not attempt state
				require.Nil(t, attemptState.GetLastFailureDetails())
				require.Nil(t, attemptState.GetCompleteTime())
				outcomeFailure := outcome.GetFailed().GetFailure()
				require.NotNil(t, outcomeFailure)
				// The last heartbeat details must be surfaced on the timeout failure so callers can
				// inspect the activity's last reported progress.
				protorequire.ProtoEqual(t, tc.heartbeatDetails, outcomeFailure.GetTimeoutFailureInfo().GetLastHeartbeatDetails())
			case enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
				enumspb.TIMEOUT_TYPE_HEARTBEAT:
				// Timeout failure is recorded in attempt state only. TransitionTimedOut should only be called when there
				// are no more retries. Retries go through TransitionRescheduled.
				recordedFailure := attemptState.GetLastFailureDetails().GetFailure()
				require.NotNil(t, recordedFailure)
				require.NotNil(t, attemptState.GetLastFailureDetails().GetTime())
				require.NotNil(t, attemptState.GetCompleteTime())
				require.Nil(t, attemptState.GetCurrentRetryInterval())
				require.Nil(t, outcome.GetVariant())
				// The last heartbeat details must be surfaced on the timeout failure so callers can
				// inspect the activity's last reported progress.
				protorequire.ProtoEqual(t, tc.heartbeatDetails, recordedFailure.GetTimeoutFailureInfo().GetLastHeartbeatDetails())

			default:
				t.Fatalf("unexpected timeout type: %v", tc.timeoutType)
			}

			require.Empty(t, ctx.Tasks)
		})
	}
}

func TestTransitionCompleted(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	ctx.HandleNow = func(chasm.Component) time.Time { return defaultTime }
	attemptState := &activitypb.ActivityAttemptState{Count: 1}
	outcome := &activitypb.ActivityOutcome{}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
			RetryPolicy:            defaultRetryPolicy,
			ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
			ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
			StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
		},
		LastAttempt: chasm.NewDataField(ctx, attemptState),
		Outcome:     chasm.NewDataField(ctx, outcome),
	}

	payload := payloads.EncodeString("Done")

	controller := gomock.NewController(t)
	metricsHandler := metrics.NewMockHandler(controller)

	timerStartToCloseLatency := metrics.NewMockTimerIface(controller)
	timerStartToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
	metricsHandler.EXPECT().Timer(metrics.ActivityStartToCloseLatency.Name()).Return(timerStartToCloseLatency)

	timerScheduleToCloseLatency := metrics.NewMockTimerIface(controller)
	timerScheduleToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
	metricsHandler.EXPECT().Timer(metrics.ActivityScheduleToCloseLatency.Name()).Return(timerScheduleToCloseLatency)

	counterSuccess := metrics.NewMockCounterIface(controller)
	counterSuccess.EXPECT().Record(int64(1)).Times(1)
	metricsHandler.EXPECT().Counter(metrics.ActivitySuccess.Name()).Return(counterSuccess)

	req := &historyservice.RespondActivityTaskCompletedRequest{
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			Result:   payload,
			Identity: "worker",
		},
	}

	err := TransitionCompleted.Apply(activity, ctx, completeEvent{
		req:            req,
		metricsHandler: metricsHandler,
	})
	require.NoError(t, err)
	require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED, activity.Status)
	require.EqualValues(t, 1, attemptState.Count)
	require.Equal(t, "worker", attemptState.GetLastWorkerIdentity())
	require.NotNil(t, attemptState.GetCompleteTime())
	protorequire.ProtoEqual(t, payload, outcome.GetSuccessful().GetOutput())
}

func TestTransitionFailed(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	ctx.HandleNow = func(chasm.Component) time.Time { return defaultTime }
	attemptState := &activitypb.ActivityAttemptState{Count: 1}
	heartbeatState := &activitypb.ActivityHeartbeatState{}
	outcome := &activitypb.ActivityOutcome{}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
			RetryPolicy:            defaultRetryPolicy,
			ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
			ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
			StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
		},
		LastAttempt:   chasm.NewDataField(ctx, attemptState),
		LastHeartbeat: chasm.NewDataField(ctx, heartbeatState),
		Outcome:       chasm.NewDataField(ctx, outcome),
	}

	heartbeatDetails := payloads.EncodeString("Heartbeat")
	failure := &failurepb.Failure{
		Message: "Failed Activity",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type:         "Test",
			NonRetryable: true,
		}},
	}

	controller := gomock.NewController(t)
	metricsHandler := metrics.NewMockHandler(controller)

	timerStartToCloseLatency := metrics.NewMockTimerIface(controller)
	timerStartToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
	metricsHandler.EXPECT().Timer(metrics.ActivityStartToCloseLatency.Name()).Return(timerStartToCloseLatency)

	timerScheduleToCloseLatency := metrics.NewMockTimerIface(controller)
	timerScheduleToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
	metricsHandler.EXPECT().Timer(metrics.ActivityScheduleToCloseLatency.Name()).Return(timerScheduleToCloseLatency)

	counterFail := metrics.NewMockCounterIface(controller)
	counterFail.EXPECT().Record(int64(1)).Times(1)
	metricsHandler.EXPECT().Counter(metrics.ActivityFail.Name()).Return(counterFail)

	counterTaskFail := metrics.NewMockCounterIface(controller)
	counterTaskFail.EXPECT().Record(int64(1)).Times(1)
	metricsHandler.EXPECT().Counter(metrics.ActivityTaskFail.Name()).Return(counterTaskFail)

	req := &historyservice.RespondActivityTaskFailedRequest{
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			Failure:              failure,
			LastHeartbeatDetails: heartbeatDetails,
			Identity:             "worker",
		},
	}

	err := TransitionFailed.Apply(activity, ctx, failedEvent{
		req:            req,
		metricsHandler: metricsHandler,
	})

	require.NoError(t, err)
	require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_FAILED, activity.Status)
	require.EqualValues(t, 1, attemptState.Count)
	require.Equal(t, "worker", attemptState.GetLastWorkerIdentity())
	require.NotNil(t, attemptState.GetCompleteTime())
	protorequire.ProtoEqual(t, heartbeatDetails, heartbeatState.GetDetails())
	require.NotNil(t, heartbeatState.GetRecordedTime())
	protorequire.ProtoEqual(t, failure, attemptState.GetLastFailureDetails().GetFailure())
	require.NotNil(t, attemptState.GetLastFailureDetails().GetTime())
	require.Nil(t, outcome.GetFailed())
}

func TestTransitionTerminated(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	ctx.HandleNow = func(chasm.Component) time.Time { return defaultTime }
	attemptState := &activitypb.ActivityAttemptState{
		Count:              1,
		LastWorkerIdentity: "worker",
	}
	outcome := &activitypb.ActivityOutcome{}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
			RetryPolicy:            defaultRetryPolicy,
			ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
			ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
			StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
		},
		LastAttempt: chasm.NewDataField(ctx, attemptState),
		Outcome:     chasm.NewDataField(ctx, outcome),
	}

	controller := gomock.NewController(t)
	metricsHandler := metrics.NewMockHandler(controller)

	counterTerminate := metrics.NewMockCounterIface(controller)
	counterTerminate.EXPECT().Record(int64(1)).Times(1)
	metricsHandler.EXPECT().Counter(metrics.ActivityTerminate.Name()).Return(counterTerminate)

	identity := "terminator"
	req := chasm.TerminateComponentRequest{
		Reason:    "Test Termination",
		Identity:  identity,
		RequestID: "test-request-id",
	}

	err := TransitionTerminated.Apply(activity, ctx, terminateEvent{
		request:        req,
		metricsHandler: metricsHandler,
		fromStatus:     activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
	})
	require.NoError(t, err)
	require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED, activity.Status)
	require.EqualValues(t, 1, attemptState.Count)
	require.Equal(t, "worker", attemptState.GetLastWorkerIdentity())
	require.Equal(t, "test-request-id", activity.GetTerminateState().RequestId)

	expectedFailure := &failurepb.Failure{
		Message: "Test Termination",
		FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
			TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{
				Identity: identity,
			},
		},
	}
	protorequire.ProtoEqual(t, expectedFailure, outcome.GetFailed().GetFailure())
}

func TestTransitionCancelRequested(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	ctx.HandleNow = func(chasm.Component) time.Time { return defaultTime }
	attemptState := &activitypb.ActivityAttemptState{Count: 1}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			RetryPolicy:            defaultRetryPolicy,
			ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
			ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
			StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		},
		LastAttempt: chasm.NewDataField(ctx, attemptState),
	}

	err := TransitionCancelRequested.Apply(activity, ctx, &workflowservice.RequestCancelActivityExecutionRequest{
		RequestId: "cancel-request",
		Reason:    "Test Cancel Requested",
		Identity:  "worker",
	})
	require.NoError(t, err)
	require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED, activity.Status)

	cancelState := activity.CancelState

	require.Equal(t, "cancel-request", cancelState.GetRequestId())
	require.Equal(t, "worker", cancelState.GetIdentity())
	require.Equal(t, "Test Cancel Requested", cancelState.GetReason())
	require.NotNil(t, cancelState.GetRequestTime())
}

func TestTransitionCanceled(t *testing.T) {
	testCases := []struct {
		name       string
		fromStatus activitypb.ActivityExecutionStatus
		// hasStartedTime seeds the attempt with a StartedTime. It is stale (from a prior attempt)
		// when the activity is not currently running, e.g. during retry backoff.
		hasStartedTime bool
		// expectStartToCloseLatency is true only when an attempt was actively running at cancellation.
		expectStartToCloseLatency bool
	}{
		{
			name:                      "worker-confirmed cancellation of running attempt",
			fromStatus:                activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			hasStartedTime:            true,
			expectStartToCloseLatency: true,
		},
		{
			name:       "paused before first dispatch, never started",
			fromStatus: activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
		},
		{
			name:           "paused during retry backoff, stale StartedTime",
			fromStatus:     activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
			hasStartedTime: true,
		},
		{
			name:           "scheduled during retry backoff, stale StartedTime",
			fromStatus:     activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			hasStartedTime: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}
			ctx.HandleNow = func(chasm.Component) time.Time { return defaultTime }
			attemptState := &activitypb.ActivityAttemptState{Count: 1}
			if tc.hasStartedTime {
				attemptState.StartedTime = timestamppb.New(defaultTime)
			}
			outcome := &activitypb.ActivityOutcome{}
			identity := "canceler"

			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					RetryPolicy:            defaultRetryPolicy,
					ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
					ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
					StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
					// TransitionCanceled always runs from CANCEL_REQUESTED; fromStatus is the status
					// captured before the cancel request.
					Status:       activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
					ScheduleTime: timestamppb.New(defaultTime),
					TaskQueue:    &taskqueuepb.TaskQueue{Name: "test-task-queue"},
					CancelState:  &activitypb.ActivityCancelState{Identity: identity},
				},
				LastAttempt: chasm.NewDataField(ctx, attemptState),
				Outcome:     chasm.NewDataField(ctx, outcome),
			}

			controller := gomock.NewController(t)
			metricsHandler := metrics.NewMockHandler(controller)

			if tc.expectStartToCloseLatency {
				timerStartToCloseLatency := metrics.NewMockTimerIface(controller)
				timerStartToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
				metricsHandler.EXPECT().Timer(metrics.ActivityStartToCloseLatency.Name()).Return(timerStartToCloseLatency)
			}

			timerScheduleToCloseLatency := metrics.NewMockTimerIface(controller)
			timerScheduleToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
			metricsHandler.EXPECT().Timer(metrics.ActivityScheduleToCloseLatency.Name()).Return(timerScheduleToCloseLatency)

			counterCancel := metrics.NewMockCounterIface(controller)
			counterCancel.EXPECT().Record(int64(1)).Times(1)
			metricsHandler.EXPECT().Counter(metrics.ActivityCancel.Name()).Return(counterCancel)

			err := TransitionCanceled.Apply(activity, ctx, cancelEvent{
				details:        payloads.EncodeString("Details"),
				metricsHandler: metricsHandler,
				fromStatus:     tc.fromStatus,
			})
			require.NoError(t, err)
			require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED, activity.Status)

			expectedFailure := &failurepb.Failure{
				Message: "Activity canceled",
				FailureInfo: &failurepb.Failure_CanceledFailureInfo{
					CanceledFailureInfo: &failurepb.CanceledFailureInfo{
						Details:  payloads.EncodeString("Details"),
						Identity: identity,
					},
				},
			}
			protorequire.ProtoEqual(t, expectedFailure, outcome.GetFailed().GetFailure())
		})
	}
}

func TestTransitionResetClearsHeartbeat(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	ctx.HandleNow = func(chasm.Component) time.Time { return defaultTime }
	attemptState := &activitypb.ActivityAttemptState{Count: 2}
	heartbeatState := &activitypb.ActivityHeartbeatState{
		Details:      payloads.EncodeString("heartbeat-details"),
		RecordedTime: timestamppb.New(defaultTime),
	}

	act := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
			RetryPolicy:            defaultRetryPolicy,
			ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
			ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
			StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			ScheduleTime:           timestamppb.New(defaultTime),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
		},
		LastAttempt:   chasm.NewDataField(ctx, attemptState),
		LastHeartbeat: chasm.NewDataField(ctx, heartbeatState),
		Outcome:       chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
	}

	err := TransitionReset.Apply(act, ctx, resetEvent{resetTime: defaultTime, handler: metrics.NoopMetricsHandler})
	require.NoError(t, err)
	require.Nil(t, act.LastHeartbeat.Get(ctx).GetDetails())
	require.Nil(t, act.LastHeartbeat.Get(ctx).GetRecordedTime())
}

func TestDeferredResetClearsHeartbeat(t *testing.T) {
	testCases := []struct {
		name              string
		transition        chasm.Transition[activitypb.ActivityExecutionStatus, *Activity, rescheduleEvent]
		resetKeepPaused   bool
		expectedStatus    activitypb.ActivityExecutionStatus
		expectedTaskCount int
	}{
		{
			name:              "scheduled",
			transition:        TransitionResetAttemptFailedToScheduled,
			resetKeepPaused:   false,
			expectedStatus:    activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			expectedTaskCount: 1,
		},
		{
			name:              "paused",
			transition:        TransitionResetAttemptFailedToPaused,
			resetKeepPaused:   true,
			expectedStatus:    activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
			expectedTaskCount: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}
			ctx.HandleNow = func(chasm.Component) time.Time { return defaultTime }
			attemptState := &activitypb.ActivityAttemptState{
				Count:       3,
				StartedTime: timestamppb.New(defaultTime),
			}
			heartbeatState := &activitypb.ActivityHeartbeatState{
				Details:      payloads.EncodeString("heartbeat-details"),
				RecordedTime: timestamppb.New(defaultTime),
			}

			act := &Activity{
				ActivityState: &activitypb.ActivityState{
					ActivityType:            &commonpb.ActivityType{Name: "test-activity-type"},
					RetryPolicy:             defaultRetryPolicy,
					ScheduleToCloseTimeout:  durationpb.New(defaultScheduleToCloseTimeout),
					ScheduleToStartTimeout:  durationpb.New(0),
					StartToCloseTimeout:     durationpb.New(defaultStartToCloseTimeout),
					Status:                  activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
					ScheduleTime:            timestamppb.New(defaultTime),
					FirstAttemptStartedTime: timestamppb.New(defaultTime),
					TaskQueue:               &taskqueuepb.TaskQueue{Name: "test-task-queue"},
					ResetKeepPaused:         tc.resetKeepPaused,
				},
				LastAttempt:   chasm.NewDataField(ctx, attemptState),
				LastHeartbeat: chasm.NewDataField(ctx, heartbeatState),
				Outcome:       chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
			}

			err := tc.transition.Apply(act, ctx, rescheduleEvent{})
			require.NoError(t, err)
			require.Equal(t, tc.expectedStatus, act.Status)
			require.Equal(t, int32(1), attemptState.Count)
			require.Nil(t, attemptState.GetCurrentRetryInterval())
			require.Nil(t, act.LastHeartbeat.Get(ctx).GetDetails())
			require.Nil(t, act.LastHeartbeat.Get(ctx).GetRecordedTime())
			require.Len(t, ctx.Tasks, tc.expectedTaskCount)
		})
	}
}

// TestTransitionResetFromPaused verifies that TransitionReset applied to a PAUSED activity
// transitions it to SCHEDULED and adds a dispatch task so it can be picked up by a worker.
func TestTransitionResetFromPaused(t *testing.T) {
	testCases := []struct {
		name                   string
		scheduleToStartTimeout time.Duration
		expectedTaskCount      int
	}{
		{
			name:                   "with schedule-to-start timeout",
			scheduleToStartTimeout: defaultScheduleToStartTimeout,
			expectedTaskCount:      2, // ScheduleToStartTimeoutTask + ActivityDispatchTask
		},
		{
			name:                   "without schedule-to-start timeout",
			scheduleToStartTimeout: 0,
			expectedTaskCount:      1, // ActivityDispatchTask only
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}
			ctx.HandleNow = func(chasm.Component) time.Time { return defaultTime }
			attemptState := &activitypb.ActivityAttemptState{
				Count:                3,
				CurrentRetryInterval: durationpb.New(30 * time.Second),
			}

			act := &Activity{
				ActivityState: &activitypb.ActivityState{
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					RetryPolicy:            defaultRetryPolicy,
					ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
					ScheduleToStartTimeout: durationpb.New(tc.scheduleToStartTimeout),
					StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
					Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
					TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
					LastPauseState: &activitypb.ActivityPauseState{
						Identity: "test-identity",
						Reason:   "test reason",
					},
				},
				LastAttempt: chasm.NewDataField(ctx, attemptState),
				Outcome:     chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
			}

			err := TransitionReset.Apply(act, ctx, resetEvent{resetTime: defaultTime, handler: metrics.NoopMetricsHandler})
			require.NoError(t, err)
			require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED, act.Status)
			require.Equal(t, int32(1), attemptState.Count)
			require.Nil(t, attemptState.GetCurrentRetryInterval())
			require.Len(t, ctx.Tasks, tc.expectedTaskCount)

			// Last task is always the dispatch task
			_, ok := ctx.Tasks[tc.expectedTaskCount-1].Payload.(*activitypb.ActivityDispatchTask)
			require.True(t, ok, "expected ActivityDispatchTask as last task")
		})
	}
}

// TestTransitionResetClearsCurrentRetryInterval verifies that TransitionReset clears the retry
// interval so a reset activity is not delayed by a previous backoff period.
func TestTransitionResetClearsCurrentRetryInterval(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	ctx.HandleNow = func(chasm.Component) time.Time { return defaultTime }
	attemptState := &activitypb.ActivityAttemptState{
		Count:                2,
		CurrentRetryInterval: durationpb.New(30 * time.Second),
	}

	act := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
			RetryPolicy:            defaultRetryPolicy,
			ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
			ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
			StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
		},
		LastAttempt: chasm.NewDataField(ctx, attemptState),
		Outcome:     chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
	}

	err := TransitionReset.Apply(act, ctx, resetEvent{resetTime: defaultTime, handler: metrics.NoopMetricsHandler})
	require.NoError(t, err)
	require.Nil(t, attemptState.GetCurrentRetryInterval(), "TransitionReset must clear CurrentRetryInterval")
	require.Equal(t, int32(1), attemptState.Count, "TransitionReset must reset Count to 1")
}
