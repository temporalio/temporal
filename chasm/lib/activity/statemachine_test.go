package activity

import (
	"context"
	"fmt"
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
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
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

// defaultFailureSizeLimit is the retained-failure size limit used by tests in this package.
const defaultFailureSizeLimit = 1024

// testNamespaceEntry is the namespace entry Activity methods read off the chasm context in tests.
func testNamespaceEntry() *namespace.Namespace {
	return namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: "test-namespace-id", Name: "test-namespace"},
		&persistencespb.NamespaceConfig{},
		"test-cluster",
	)
}

// injectActivityContext adds the dependencies that Activity methods read off the chasm context:
// the activityContext (dynamic config) and the namespace entry.
func injectActivityContext(t *testing.T, ctx *chasm.MockMutableContext) {
	config := &Config{
		MutableStateActivityFailureSizeLimitError: dynamicconfig.GetIntPropertyFnFilteredByNamespace(defaultFailureSizeLimit),
	}
	ctx.GoCtx = context.WithValue(t.Context(), ctxKeyActivityContext, &activityContext{config: config})
	ctx.HandleNamespaceEntry = testNamespaceEntry
}

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
			injectActivityContext(t, ctx)
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
		// TODO: change this and serverside once versioning is supported in SAA.
		// LastDeploymentVersion represents the worker that actually accepted the task,
		// than when it's scheduled. WFA derives it from PollRequest via
		// DeploymentFromCapabilities, while this test uses VersionDirective.
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

	deploymentVersion := attemptState.GetLastDeploymentVersion()
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
		retryState       enumspb.RetryState
		attemptCount     int32
		heartbeatDetails *commonpb.Payloads
	}{
		{
			name:         "schedule to start timeout, never started",
			startStatus:  activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			timeoutType:  enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
			retryState:   enumspb.RETRY_STATE_TIMEOUT,
			attemptCount: 1,
		},
		{
			name:         "schedule to close timeout from scheduled, never started",
			startStatus:  activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			timeoutType:  enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			retryState:   enumspb.RETRY_STATE_TIMEOUT,
			attemptCount: 1,
		},
		{
			name:             "schedule to close timeout from started status with heartbeat details",
			startStatus:      activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			timeoutType:      enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			retryState:       enumspb.RETRY_STATE_TIMEOUT,
			attemptCount:     4,
			heartbeatDetails: payloads.EncodeString("schedule-to-close-heartbeat"),
		},
		{
			name:             "start to close timeout with heartbeat details",
			startStatus:      activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			timeoutType:      enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			retryState:       enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
			attemptCount:     5,
			heartbeatDetails: payloads.EncodeString("start-to-close-heartbeat"),
		},
		{
			name:             "heartbeat timeout with heartbeat details",
			startStatus:      activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			timeoutType:      enumspb.TIMEOUT_TYPE_HEARTBEAT,
			retryState:       enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
			attemptCount:     2,
			heartbeatDetails: payloads.EncodeString("heartbeat-details"),
		},
		{
			name:         "heartbeat timeout without heartbeat details",
			startStatus:  activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			timeoutType:  enumspb.TIMEOUT_TYPE_HEARTBEAT,
			retryState:   enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
			attemptCount: 2,
		},
		{
			name:         "schedule to close timeout from started status",
			startStatus:  activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			timeoutType:  enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			retryState:   enumspb.RETRY_STATE_TIMEOUT,
			attemptCount: 4,
		},
		{
			name:         "start to close timeout",
			startStatus:  activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			timeoutType:  enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			retryState:   enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
			attemptCount: 5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}
			attemptState := &activitypb.ActivityAttemptState{Count: tc.attemptCount}
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
				retryState:     tc.retryState,
				metricsHandler: metricsHandler,
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
				require.Equal(t, tc.timeoutType, outcomeFailure.GetTimeoutFailureInfo().GetTimeoutType())
				require.Equal(t, fmt.Sprintf(common.FailureReasonActivityTimeout, tc.timeoutType.String()), outcomeFailure.GetMessage())
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

			require.Equal(t, tc.retryState, outcome.GetRetryState())
			require.Equal(t, tc.retryState, activity.outcome(ctx).GetRetryState())
			require.Empty(t, ctx.Tasks)
		})
	}
}

// When a per-attempt timeout leaves too little time for another retry, the terminal ScheduleToClose
// failure must retain the prior attempt's failure even though the per-attempt failure is recorded first.
func TestTransitionTimedOutRetryWindowExhaustedChainsPriorFailure(t *testing.T) {
	// StartToClose and Heartbeat are the per-attempt timeouts that can request another retry.
	// They share this terminal path when the retry cannot fit inside ScheduleToClose.
	for _, timeoutType := range []enumspb.TimeoutType{
		enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		enumspb.TIMEOUT_TYPE_HEARTBEAT,
	} {
		t.Run(timeoutType.String(), func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}

			// This application failure ended the preceding attempt and caused the current attempt
			// to be scheduled. It is the useful underlying failure that the caller must receive.
			priorFailure := &failurepb.Failure{
				Message: "prior failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "AppFailure"},
				},
			}

			// Model the current attempt as started, with the preceding attempt's failure still in
			// LastFailureDetails. TransitionTimedOut must read that failure before recording the
			// current attempt's timeout in the same field.
			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					RetryPolicy:            defaultRetryPolicy,
					ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
					StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
					Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
					TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
				},
				LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
					Count:       3,
					StartedTime: timestamppb.New(defaultTime),
					LastFailureDetails: &activitypb.ActivityAttemptState_LastFailureDetails{
						Failure: priorFailure,
						Time:    timestamppb.New(defaultTime),
					},
				}),
				Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
			}

			// RETRY_STATE_TIMEOUT means the per-attempt timeout fired, but its next retry would
			// start after the ScheduleToClose deadline. The transition therefore first records the
			// per-attempt timeout, then closes the activity with a ScheduleToClose failure.
			require.NoError(t, TransitionTimedOut.Apply(activity, ctx, timeoutEvent{
				timeoutType:    timeoutType,
				retryState:     enumspb.RETRY_STATE_TIMEOUT,
				metricsHandler: metrics.NoopMetricsHandler,
			}))

			// The caller sees ScheduleToClose as the terminal timeout, with the application failure
			// from the preceding attempt—not the newly recorded per-attempt timeout—as its cause.
			terminal := activity.terminalFailure(ctx)
			require.Equal(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, terminal.GetTimeoutFailureInfo().GetTimeoutType())
			protorequire.ProtoEqual(t, priorFailure, terminal.GetCause())
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
	baseHandler := metrics.NewMockHandler(controller)
	enrichedHandler := metrics.NewMockHandler(controller)

	timerStartToCloseLatency := metrics.NewMockTimerIface(controller)
	timerStartToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
	enrichedHandler.EXPECT().Timer(metrics.ActivityStartToCloseLatency.Name()).Return(timerStartToCloseLatency)

	timerScheduleToCloseLatency := metrics.NewMockTimerIface(controller)
	timerScheduleToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
	enrichedHandler.EXPECT().Timer(metrics.ActivityScheduleToCloseLatency.Name()).Return(timerScheduleToCloseLatency)

	counterSuccess := metrics.NewMockCounterIface(controller)
	counterSuccess.EXPECT().Record(int64(1)).Times(1)
	enrichedHandler.EXPECT().Counter(metrics.ActivitySuccess.Name()).Return(counterSuccess)

	counterPayloadSize := metrics.NewMockCounterIface(controller)
	counterPayloadSize.EXPECT().Record(int64(payload.Size())).Times(1)
	baseHandler.EXPECT().Counter(metrics.ActivityPayloadSize.Name()).Return(counterPayloadSize)

	req := &historyservice.RespondActivityTaskCompletedRequest{
		CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
			Result:   payload,
			Identity: "worker",
		},
	}

	err := TransitionCompleted.Apply(activity, ctx, completeEvent{
		req:             req,
		baseHandler:     baseHandler,
		enrichedHandler: enrichedHandler,
	})
	require.NoError(t, err)
	require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED, activity.Status)
	require.EqualValues(t, 1, attemptState.Count)
	require.Equal(t, "worker", attemptState.GetLastWorkerIdentity())
	require.NotNil(t, attemptState.GetCompleteTime())
	protorequire.ProtoEqual(t, payload, outcome.GetSuccessful().GetOutput())
}

// TestTransitionCompleted_ForceCompleteWithNoAttempt covers force-completing an activity by ID
// before any worker has started it (RespondActivityTaskCompletedById), from both SCHEDULED and
// PAUSED — neither has an attempt in progress. Since no attempt ever started,
// ActivityStartToCloseLatency must not be emitted, mirroring the WFA behavior in
// respondactivitytaskcompleted/api.go of leaving attemptStartedTime zero when the started event
// is fabricated.
func TestTransitionCompleted_ForceCompleteWithNoAttempt(t *testing.T) {
	for _, fromStatus := range []activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
	} {
		t.Run(fromStatus.String(), func(t *testing.T) {
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
					Status:                 fromStatus,
					TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
				},
				LastAttempt: chasm.NewDataField(ctx, attemptState),
				Outcome:     chasm.NewDataField(ctx, outcome),
			}

			payload := payloads.EncodeString("Done")

			controller := gomock.NewController(t)
			baseHandler := metrics.NewMockHandler(controller)
			enrichedHandler := metrics.NewMockHandler(controller)

			// No attempt was ever started, so ActivityStartToCloseLatency must not be recorded.
			enrichedHandler.EXPECT().Timer(metrics.ActivityStartToCloseLatency.Name()).Times(0)

			timerScheduleToCloseLatency := metrics.NewMockTimerIface(controller)
			timerScheduleToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
			enrichedHandler.EXPECT().Timer(metrics.ActivityScheduleToCloseLatency.Name()).Return(timerScheduleToCloseLatency)

			counterSuccess := metrics.NewMockCounterIface(controller)
			counterSuccess.EXPECT().Record(int64(1)).Times(1)
			enrichedHandler.EXPECT().Counter(metrics.ActivitySuccess.Name()).Return(counterSuccess)

			counterPayloadSize := metrics.NewMockCounterIface(controller)
			counterPayloadSize.EXPECT().Record(int64(payload.Size())).Times(1)
			baseHandler.EXPECT().Counter(metrics.ActivityPayloadSize.Name()).Return(counterPayloadSize)

			req := &historyservice.RespondActivityTaskCompletedRequest{
				CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
					Result:   payload,
					Identity: "worker",
				},
			}

			err := TransitionCompleted.Apply(activity, ctx, completeEvent{
				req:             req,
				baseHandler:     baseHandler,
				enrichedHandler: enrichedHandler,
			})
			require.NoError(t, err)
			require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED, activity.Status)
		})
	}
}

func TestTransitionFailed(t *testing.T) {
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

	failure := &failurepb.Failure{
		Message: "Failed Activity",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type:         "Test",
			NonRetryable: true,
		}},
	}

	controller := gomock.NewController(t)
	baseHandler := metrics.NewMockHandler(controller)
	enrichedHandler := metrics.NewMockHandler(controller)

	timerStartToCloseLatency := metrics.NewMockTimerIface(controller)
	timerStartToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
	enrichedHandler.EXPECT().Timer(metrics.ActivityStartToCloseLatency.Name()).Return(timerStartToCloseLatency)

	timerScheduleToCloseLatency := metrics.NewMockTimerIface(controller)
	timerScheduleToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
	enrichedHandler.EXPECT().Timer(metrics.ActivityScheduleToCloseLatency.Name()).Return(timerScheduleToCloseLatency)

	counterFail := metrics.NewMockCounterIface(controller)
	counterFail.EXPECT().Record(int64(1)).Times(1)
	enrichedHandler.EXPECT().Counter(metrics.ActivityFail.Name()).Return(counterFail)

	counterTaskFail := metrics.NewMockCounterIface(controller)
	counterTaskFail.EXPECT().Record(int64(1)).Times(1)
	enrichedHandler.EXPECT().Counter(metrics.ActivityTaskFail.Name()).Return(counterTaskFail)

	counterPayloadSize := metrics.NewMockCounterIface(controller)
	counterPayloadSize.EXPECT().Record(int64(failure.Size())).Times(1)
	baseHandler.EXPECT().Counter(metrics.ActivityPayloadSize.Name()).Return(counterPayloadSize)

	req := &historyservice.RespondActivityTaskFailedRequest{
		FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
			Failure:  failure,
			Identity: "worker",
		},
	}

	err := TransitionFailed.Apply(activity, ctx, failedEvent{
		req:             req,
		retryState:      enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
		baseHandler:     baseHandler,
		enrichedHandler: enrichedHandler,
	})

	require.NoError(t, err)
	require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_FAILED, activity.Status)
	require.EqualValues(t, 1, attemptState.Count)
	require.Equal(t, "worker", attemptState.GetLastWorkerIdentity())
	require.NotNil(t, attemptState.GetCompleteTime())
	protorequire.ProtoEqual(t, failure, attemptState.GetLastFailureDetails().GetFailure())
	require.NotNil(t, attemptState.GetLastFailureDetails().GetTime())
	require.Nil(t, outcome.GetFailed())
	require.Equal(t, enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, outcome.GetRetryState())
	require.Equal(t, enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, activity.outcome(ctx).GetRetryState())
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

func TestTransitionCancelRequestedClearsDeferredOptionRestore(t *testing.T) {
	ctx := &chasm.MockMutableContext{}
	ctx.HandleNow = func(chasm.Component) time.Time { return defaultTime }
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			Status:              activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
			ResetRestoreOptions: true,
		},
	}

	err := TransitionCancelRequested.Apply(activity, ctx, &workflowservice.RequestCancelActivityExecutionRequest{})
	require.NoError(t, err)
	require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED, activity.Status)
	require.False(t, activity.ResetRestoreOptions)
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

// TestTransitionResetHeartbeat: reset keeps the heartbeat checkpoint unless the request asks for it
// to be discarded.
func TestTransitionResetHeartbeat(t *testing.T) {
	for _, resetHeartbeat := range []bool{false, true} {
		t.Run(fmt.Sprintf("resetHeartbeat=%v", resetHeartbeat), func(t *testing.T) {
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

			err := TransitionReset.Apply(act, ctx, resetEvent{
				req:            &workflowservice.ResetActivityExecutionRequest{ResetHeartbeat: resetHeartbeat},
				resetTime:      defaultTime,
				metricsHandler: metrics.NoopMetricsHandler,
			})
			require.NoError(t, err)
			if resetHeartbeat {
				require.Nil(t, act.LastHeartbeat.Get(ctx).GetDetails())
				require.Nil(t, act.LastHeartbeat.Get(ctx).GetRecordedTime())
			} else {
				protorequire.ProtoEqual(t, payloads.EncodeString("heartbeat-details"), act.LastHeartbeat.Get(ctx).GetDetails())
				require.Equal(t, timestamppb.New(defaultTime), act.LastHeartbeat.Get(ctx).GetRecordedTime())
			}
		})
	}
}

// TestDeferredResetHeartbeat: when the reset landed while a worker was running, the heartbeat
// checkpoint is discarded on the deferred landing only if the request asked for it, in which case
// the deferred intent is consumed.
func TestDeferredResetHeartbeat(t *testing.T) {
	testCases := []struct {
		name              string
		transition        chasm.Transition[activitypb.ActivityExecutionStatus, *Activity, rescheduleEvent]
		resetShouldPause  bool
		expectedStatus    activitypb.ActivityExecutionStatus
		expectedTaskCount int
	}{
		{
			name:              "scheduled",
			transition:        TransitionResetAttemptFailedToScheduled,
			resetShouldPause:  false,
			expectedStatus:    activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			expectedTaskCount: 1,
		},
		{
			name:              "paused",
			transition:        TransitionResetAttemptFailedToPaused,
			resetShouldPause:  true,
			expectedStatus:    activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
			expectedTaskCount: 0,
		},
	}

	for _, tc := range testCases {
		for _, shouldClearHeartbeat := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/resetShouldClearHeartbeat=%v", tc.name, shouldClearHeartbeat), func(t *testing.T) {
				ctx := &chasm.MockMutableContext{}
				ctx.HandleNow = func(chasm.Component) time.Time { return defaultTime }
				injectActivityContext(t, ctx)
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
						ActivityType:              &commonpb.ActivityType{Name: "test-activity-type"},
						RetryPolicy:               defaultRetryPolicy,
						ScheduleToCloseTimeout:    durationpb.New(defaultScheduleToCloseTimeout),
						ScheduleToStartTimeout:    durationpb.New(0),
						StartToCloseTimeout:       durationpb.New(defaultStartToCloseTimeout),
						Status:                    activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
						ScheduleTime:              timestamppb.New(defaultTime),
						FirstAttemptStartedTime:   timestamppb.New(defaultTime),
						TaskQueue:                 &taskqueuepb.TaskQueue{Name: "test-task-queue"},
						ResetShouldPause:          tc.resetShouldPause,
						ResetShouldClearHeartbeat: shouldClearHeartbeat,
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
				require.False(t, act.GetResetShouldClearHeartbeat(), "the deferred intent must be consumed")
				if shouldClearHeartbeat {
					require.Nil(t, act.LastHeartbeat.Get(ctx).GetDetails())
					require.Nil(t, act.LastHeartbeat.Get(ctx).GetRecordedTime())
				} else {
					protorequire.ProtoEqual(t, payloads.EncodeString("heartbeat-details"), act.LastHeartbeat.Get(ctx).GetDetails())
					require.Equal(t, timestamppb.New(defaultTime), act.LastHeartbeat.Get(ctx).GetRecordedTime())
				}
				require.Len(t, ctx.Tasks, tc.expectedTaskCount)
			})
		}
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

			err := TransitionReset.Apply(act, ctx, resetEvent{resetTime: defaultTime, metricsHandler: metrics.NoopMetricsHandler})
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

	err := TransitionReset.Apply(act, ctx, resetEvent{resetTime: defaultTime, metricsHandler: metrics.NoopMetricsHandler})
	require.NoError(t, err)
	require.Nil(t, attemptState.GetCurrentRetryInterval(), "TransitionReset must clear CurrentRetryInterval")
	require.Equal(t, int32(1), attemptState.Count, "TransitionReset must reset Count to 1")
}
