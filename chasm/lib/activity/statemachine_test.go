package activity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
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
		name                   string
		startingAttemptCount   int32
		expectedTasks          []chasm.MockTask
		scheduleToStartTimeout time.Duration
		scheduleToCloseTimeout time.Duration
	}{
		{
			name:                 "all timeouts set",
			startingAttemptCount: 0,
			expectedTasks: []chasm.MockTask{
				{Payload: &activitypb.ScheduleToStartTimeoutTask{}},
				{Payload: &activitypb.ScheduleToCloseTimeoutTask{}},
				{Payload: &activitypb.ActivityDispatchTask{}},
			},
			scheduleToStartTimeout: defaultScheduleToStartTimeout,
			scheduleToCloseTimeout: defaultScheduleToCloseTimeout,
		},
		{
			name:                 "schedule to start timeout not set",
			startingAttemptCount: 0,
			expectedTasks: []chasm.MockTask{
				{Payload: &activitypb.ScheduleToCloseTimeoutTask{}},
				{Payload: &activitypb.ActivityDispatchTask{}},
			},
			scheduleToStartTimeout: 0,
			scheduleToCloseTimeout: defaultScheduleToCloseTimeout,
		},
		{
			name:                 "schedule to close timeout not set",
			startingAttemptCount: 0,
			expectedTasks: []chasm.MockTask{
				{Payload: &activitypb.ScheduleToStartTimeoutTask{}},
				{Payload: &activitypb.ActivityDispatchTask{}},
			},
			scheduleToStartTimeout: defaultScheduleToStartTimeout,
			scheduleToCloseTimeout: 0,
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
					ScheduleToCloseTimeout: durationpb.New(tc.scheduleToCloseTimeout),
					ScheduleToStartTimeout: durationpb.New(tc.scheduleToStartTimeout),
					StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
					Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED,
					TaskQueue:              &taskqueue.TaskQueue{Name: "test-task-queue"},
				},
				RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
					Input: input,
				}),
				Attempt: chasm.NewDataField(ctx, attemptState),
				Outcome: chasm.NewDataField(ctx, outcome),
			}

			controller := gomock.NewController(t)
			metricsHandler := metrics.NewMockHandler(controller)
			payloadSize := input.Size()
			counter := metrics.NewMockCounterIface(controller)
			counter.EXPECT().Record(
				int64(payloadSize),
				metrics.OperationTag(metrics.HistoryRecordActivityTaskStartedScope),
				metrics.NamespaceTag("test-namespace"),
			).Times(1)
			metricsHandler.EXPECT().Counter(metrics.ActivityPayloadSize.Name()).Return(counter)

			event := scheduleEvent{
				handler:   metricsHandler,
				namespace: "test-namespace",
				inputSize: payloadSize,
			}

			err := TransitionScheduled.Apply(activity, ctx, event)
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
					require.Empty(t, actualTask.Attributes.ScheduledTime)
				case *activitypb.ScheduleToStartTimeoutTask:
					require.Equal(t, defaultTime.Add(tc.scheduleToStartTimeout), actualTask.Attributes.ScheduledTime)
				case *activitypb.ScheduleToCloseTimeoutTask:
					require.Equal(t, defaultTime.Add(tc.scheduleToCloseTimeout), actualTask.Attributes.ScheduledTime)
				default:
					t.Fatalf("unexpected task payload type at index %d: %T", i, actualTask.Payload)
				}

			}
		})
	}
}

func TestTransitionRescheduled(t *testing.T) {
	testCases := []struct {
		name                   string
		startingAttemptCount   int32
		expectedTasks          []chasm.MockTask
		expectedRetryInterval  time.Duration
		retryPolicy            *commonpb.RetryPolicy
		scheduleToStartTimeout time.Duration
		operationTag           string
		counterMetric          string
		timeoutType            enumspb.TimeoutType
	}{
		{
			name:                 "second attempt - timeout recorded",
			startingAttemptCount: 1,
			expectedTasks: []chasm.MockTask{
				{Payload: &activitypb.ScheduleToStartTimeoutTask{}},
				{Payload: &activitypb.ActivityDispatchTask{}},
			},
			expectedRetryInterval:  2 * time.Second,
			retryPolicy:            defaultRetryPolicy,
			scheduleToStartTimeout: defaultScheduleToStartTimeout,
			operationTag:           metrics.TimerActiveTaskActivityTimeoutScope,
			counterMetric:          metrics.ActivityTaskTimeout.Name(),
			timeoutType:            enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		},
		{
			name:                 "third attempt - timeout recorded",
			startingAttemptCount: 2,
			expectedTasks: []chasm.MockTask{
				{Payload: &activitypb.ScheduleToStartTimeoutTask{}},
				{Payload: &activitypb.ActivityDispatchTask{}},
			},
			expectedRetryInterval:  4 * time.Second,
			retryPolicy:            defaultRetryPolicy,
			scheduleToStartTimeout: defaultScheduleToStartTimeout,
			operationTag:           metrics.TimerActiveTaskActivityTimeoutScope,
			counterMetric:          metrics.ActivityTaskTimeout.Name(),
			timeoutType:            enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		},
		{
			name:                 "no schedule to start timeout",
			startingAttemptCount: 1,
			expectedTasks: []chasm.MockTask{
				{Payload: &activitypb.ActivityDispatchTask{}},
			},
			expectedRetryInterval:  2 * time.Second,
			retryPolicy:            defaultRetryPolicy,
			scheduleToStartTimeout: 0,
			operationTag:           metrics.TimerActiveTaskActivityTimeoutScope,
			counterMetric:          metrics.ActivityTaskTimeout.Name(),
			timeoutType:            enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		},
		{
			name:                 "heartbeat timeout - timeout recorded",
			startingAttemptCount: 1,
			expectedTasks: []chasm.MockTask{
				{Payload: &activitypb.ScheduleToStartTimeoutTask{}},
				{Payload: &activitypb.ActivityDispatchTask{}},
			},
			expectedRetryInterval:  2 * time.Second,
			retryPolicy:            defaultRetryPolicy,
			scheduleToStartTimeout: defaultScheduleToStartTimeout,
			operationTag:           metrics.TimerActiveTaskActivityTimeoutScope,
			counterMetric:          metrics.ActivityTaskTimeout.Name(),
			timeoutType:            enumspb.TIMEOUT_TYPE_HEARTBEAT,
		},

		{
			name:                 "reschedule from failure",
			startingAttemptCount: 1,
			expectedTasks: []chasm.MockTask{
				{Payload: &activitypb.ScheduleToStartTimeoutTask{}},
				{Payload: &activitypb.ActivityDispatchTask{}},
			},
			expectedRetryInterval:  2 * time.Second,
			retryPolicy:            defaultRetryPolicy,
			scheduleToStartTimeout: defaultScheduleToStartTimeout,
			operationTag:           metrics.HistoryRespondActivityTaskFailedScope,
			counterMetric:          metrics.ActivityTaskFail.Name(),
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
					ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
					RetryPolicy:            defaultRetryPolicy,
					ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
					ScheduleToStartTimeout: durationpb.New(tc.scheduleToStartTimeout),
					StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
					Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
					TaskQueue:              &taskqueue.TaskQueue{Name: "test-task-queue"},
				},
				Attempt: chasm.NewDataField(ctx, attemptState),
				Outcome: chasm.NewDataField(ctx, outcome),
			}

			controller := gomock.NewController(t)

			tags := []metrics.Tag{
				metrics.OperationTag(tc.operationTag),
				metrics.ActivityTypeTag("test-activity-type"),
				metrics.NamespaceTag("test-namespace"),
				metrics.UnsafeTaskQueueTag("test-task-queue"),
			}
			metricsHandler := metrics.NewMockHandler(controller)
			metricsHandler.EXPECT().WithTags(tags).Return(metricsHandler).Times(1)

			timerStartToCloseLatency := metrics.NewMockTimerIface(controller)
			timerStartToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
			metricsHandler.EXPECT().Timer(metrics.ActivityStartToCloseLatency.Name()).Return(timerStartToCloseLatency)

			counter := metrics.NewMockCounterIface(controller)
			if tc.operationTag == metrics.TimerActiveTaskActivityTimeoutScope {
				timeoutTag := metrics.StringTag("timeout_type", tc.timeoutType.String())
				counter.EXPECT().Record(int64(1), timeoutTag).Times(1)
			} else {
				counter.EXPECT().Record(int64(1)).Times(1)
			}
			metricsHandler.EXPECT().Counter(tc.counterMetric).Return(counter)

			event := rescheduleEvent{
				retryInterval:               tc.expectedRetryInterval,
				failure:                     createStartToCloseTimeoutFailure(),
				handler:                     metricsHandler,
				namespace:                   "test-namespace",
				breakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
				timeoutType:                 tc.timeoutType,
				operationTag:                tc.operationTag,
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
					_, ok := actualTask.Payload.(*activitypb.ActivityDispatchTask)
					require.True(t, ok, "expected ActivityDispatchTask at index %d", i)
					require.Equal(t, defaultTime.Add(tc.expectedRetryInterval), actualTask.Attributes.ScheduledTime)
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
	attemptState := &activitypb.ActivityAttemptState{Count: 1}
	outcome := &activitypb.ActivityOutcome{}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			RetryPolicy:            defaultRetryPolicy,
			ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
			ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
			StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		},
		Attempt: chasm.NewDataField(ctx, attemptState),
		Outcome: chasm.NewDataField(ctx, outcome),
	}

	err := TransitionStarted.Apply(activity, ctx, nil)
	require.NoError(t, err)
	require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_STARTED, activity.Status)
	require.EqualValues(t, 1, attemptState.Count)

	// Verify added tasks
	require.Len(t, ctx.Tasks, 1)
	_, ok := ctx.Tasks[0].Payload.(*activitypb.StartToCloseTimeoutTask)
	require.True(t, ok, "expected ScheduleToStartTimeoutTask")
	require.Equal(t, defaultTime.Add(defaultStartToCloseTimeout), ctx.Tasks[0].Attributes.ScheduledTime)
}

func TestTransitionTimedout(t *testing.T) {
	testCases := []struct {
		name         string
		startStatus  activitypb.ActivityExecutionStatus
		timeoutType  enumspb.TimeoutType
		attemptCount int32
	}{
		{
			name:         "schedule to start timeout",
			startStatus:  activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			timeoutType:  enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
			attemptCount: 2,
		},
		{
			name:         "schedule to close timeout from scheduled status",
			startStatus:  activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			timeoutType:  enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			attemptCount: 3,
		},
		{
			name:         "schedule to close timeout from started status",
			startStatus:  activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			timeoutType:  enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			attemptCount: 4,
		},
		{
			name:         "start to close timeout",
			startStatus:  activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			timeoutType:  enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			attemptCount: 5,
		},
		{
			name:         "heartbeat timeout",
			startStatus:  activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			timeoutType:  enumspb.TIMEOUT_TYPE_HEARTBEAT,
			attemptCount: 2,
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
					TaskQueue:              &taskqueue.TaskQueue{Name: "test-task-queue"},
				},
				Attempt: chasm.NewDataField(ctx, attemptState),
				Outcome: chasm.NewDataField(ctx, outcome),
			}

			controller := gomock.NewController(t)

			tags := []metrics.Tag{
				metrics.OperationTag(metrics.TimerActiveTaskActivityTimeoutScope),
				metrics.ActivityTypeTag("test-activity-type"),
				metrics.NamespaceTag("test-namespace"),
				metrics.UnsafeTaskQueueTag("test-task-queue"),
			}
			metricsHandler := metrics.NewMockHandler(controller)
			metricsHandler.EXPECT().WithTags(tags).Return(metricsHandler).Times(2)

			timerStartToCloseLatency := metrics.NewMockTimerIface(controller)
			timerStartToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
			metricsHandler.EXPECT().Timer(metrics.ActivityStartToCloseLatency.Name()).Return(timerStartToCloseLatency)

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
				timeoutType:                 tc.timeoutType,
				metricsHandler:              metricsHandler,
				namespaceName:               "test-namespace",
				breakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
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
				require.NotNil(t, outcome.GetFailed().GetFailure())
				// do something
			case enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
				enumspb.TIMEOUT_TYPE_HEARTBEAT:
				// Timeout failure is recorded in both attempt state and outcome. TransitionTimedOut should only be called when there
				// are no more retries. Retries go through TransitionRescheduled.
				require.NotNil(t, attemptState.GetLastFailureDetails().GetFailure())
				require.NotNil(t, attemptState.GetLastFailureDetails().GetTime())
				require.NotNil(t, attemptState.GetCompleteTime())
				require.Nil(t, attemptState.GetCurrentRetryInterval())

				failure, ok := outcome.GetVariant().(*activitypb.ActivityOutcome_Failed_)
				require.True(t, ok, "expected variant to be of type Failed")
				require.Nil(t, failure.Failed, "expected outcome.Failed to be nil since failure is recorded in attempt state")
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
			TaskQueue:              &taskqueue.TaskQueue{Name: "test-task-queue"},
		},
		Attempt: chasm.NewDataField(ctx, attemptState),
		Outcome: chasm.NewDataField(ctx, outcome),
	}

	payload := payloads.EncodeString("Done")

	controller := gomock.NewController(t)

	tags := []metrics.Tag{
		metrics.OperationTag(metrics.HistoryRespondActivityTaskCompletedScope),
		metrics.ActivityTypeTag("test-activity-type"),
		metrics.NamespaceTag("test-namespace"),
		metrics.UnsafeTaskQueueTag("test-task-queue"),
	}
	metricsHandler := metrics.NewMockHandler(controller)
	metricsHandler.EXPECT().WithTags(tags).Return(metricsHandler).Times(2)

	timerStartToCloseLatency := metrics.NewMockTimerIface(controller)
	timerStartToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
	metricsHandler.EXPECT().Timer(metrics.ActivityStartToCloseLatency.Name()).Return(timerStartToCloseLatency)

	timerScheduleToCloseLatency := metrics.NewMockTimerIface(controller)
	timerScheduleToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
	metricsHandler.EXPECT().Timer(metrics.ActivityScheduleToCloseLatency.Name()).Return(timerScheduleToCloseLatency)

	counterSuccess := metrics.NewMockCounterIface(controller)
	counterSuccess.EXPECT().Record(int64(1)).Times(1)
	metricsHandler.EXPECT().Counter(metrics.ActivitySuccess.Name()).Return(counterSuccess)

	counterPayloadSize := metrics.NewMockCounterIface(controller)
	counterPayloadSize.EXPECT().Record(
		int64(payload.Size()),
		metrics.OperationTag(metrics.HistoryRespondActivityTaskCompletedScope),
		metrics.NamespaceTag("test-namespace"),
	).Times(1)
	metricsHandler.EXPECT().Counter(metrics.ActivityPayloadSize.Name()).Return(counterPayloadSize)

	reqWithCtx := RequestWithContext[*historyservice.RespondActivityTaskCompletedRequest]{
		Request: &historyservice.RespondActivityTaskCompletedRequest{
			CompleteRequest: &workflowservice.RespondActivityTaskCompletedRequest{
				Result:   payload,
				Identity: "worker",
			},
		},
		MetricsHandler:              metricsHandler,
		NamespaceName:               "test-namespace",
		BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
	}

	err := TransitionCompleted.Apply(activity, ctx, reqWithCtx)
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
			TaskQueue:              &taskqueue.TaskQueue{Name: "test-task-queue"},
		},
		Attempt:       chasm.NewDataField(ctx, attemptState),
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

	tags := []metrics.Tag{
		metrics.OperationTag(metrics.HistoryRespondActivityTaskFailedScope),
		metrics.ActivityTypeTag("test-activity-type"),
		metrics.NamespaceTag("test-namespace"),
		metrics.UnsafeTaskQueueTag("test-task-queue"),
	}
	metricsHandler := metrics.NewMockHandler(controller)
	metricsHandler.EXPECT().WithTags(tags).Return(metricsHandler).Times(2)

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

	counterPayloadSize := metrics.NewMockCounterIface(controller)
	counterPayloadSize.EXPECT().Record(
		int64(failure.Size()),
		metrics.OperationTag(metrics.HistoryRespondActivityTaskFailedScope),
		metrics.NamespaceTag("test-namespace"),
	).Times(1)
	metricsHandler.EXPECT().Counter(metrics.ActivityPayloadSize.Name()).Return(counterPayloadSize)

	reqWithCtx := RequestWithContext[*historyservice.RespondActivityTaskFailedRequest]{
		Request: &historyservice.RespondActivityTaskFailedRequest{
			FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
				Failure:              failure,
				LastHeartbeatDetails: heartbeatDetails,
				Identity:             "worker",
			},
		},
		MetricsHandler:              metricsHandler,
		NamespaceName:               "test-namespace",
		BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
	}

	err := TransitionFailed.Apply(activity, ctx, reqWithCtx)

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
	attemptState := &activitypb.ActivityAttemptState{Count: 1}
	outcome := &activitypb.ActivityOutcome{}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			RetryPolicy:            defaultRetryPolicy,
			ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
			ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
			StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		},
		Attempt: chasm.NewDataField(ctx, attemptState),
		Outcome: chasm.NewDataField(ctx, outcome),
	}

	err := TransitionTerminated.Apply(activity, ctx, &activitypb.TerminateActivityExecutionRequest{
		FrontendRequest: &workflowservice.TerminateActivityExecutionRequest{
			Reason:   "Test Termination",
			Identity: "worker",
		},
	})
	require.NoError(t, err)
	require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED, activity.Status)
	require.EqualValues(t, 1, attemptState.Count)
	require.Equal(t, "worker", attemptState.GetLastWorkerIdentity())

	expectedFailure := &failurepb.Failure{
		Message:     "Test Termination",
		FailureInfo: &failurepb.Failure_TerminatedFailureInfo{},
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
		Attempt: chasm.NewDataField(ctx, attemptState),
	}

	err := TransitionCancelRequested.Apply(activity, ctx, &activitypb.CancelActivityExecutionRequest{
		FrontendRequest: &workflowservice.RequestCancelActivityExecutionRequest{
			RequestId: "cancel-request",
			Reason:    "Test Cancel Requested",
			Identity:  "worker",
		},
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
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
			TaskQueue:              &taskqueue.TaskQueue{Name: "test-task-queue"},
		},
		Attempt: chasm.NewDataField(ctx, attemptState),
		Outcome: chasm.NewDataField(ctx, outcome),
	}

	controller := gomock.NewController(t)

	tags := []metrics.Tag{
		metrics.OperationTag(metrics.HistoryRespondActivityTaskCanceledScope),
		metrics.ActivityTypeTag("test-activity-type"),
		metrics.NamespaceTag("test-namespace"),
		metrics.UnsafeTaskQueueTag("test-task-queue"),
	}
	metricsHandler := metrics.NewMockHandler(controller)
	metricsHandler.EXPECT().WithTags(tags).Return(metricsHandler).Times(2)

	timerStartToCloseLatency := metrics.NewMockTimerIface(controller)
	timerStartToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
	metricsHandler.EXPECT().Timer(metrics.ActivityStartToCloseLatency.Name()).Return(timerStartToCloseLatency)

	timerScheduleToCloseLatency := metrics.NewMockTimerIface(controller)
	timerScheduleToCloseLatency.EXPECT().Record(gomock.Any()).Times(1)
	metricsHandler.EXPECT().Timer(metrics.ActivityScheduleToCloseLatency.Name()).Return(timerScheduleToCloseLatency)

	counterCancel := metrics.NewMockCounterIface(controller)
	counterCancel.EXPECT().Record(int64(1)).Times(1)
	metricsHandler.EXPECT().Counter(metrics.ActivityCancel.Name()).Return(counterCancel)

	reqWithCtx := RequestWithContext[*historyservice.RespondActivityTaskCanceledRequest]{
		Request: &historyservice.RespondActivityTaskCanceledRequest{
			CancelRequest: &workflowservice.RespondActivityTaskCanceledRequest{
				Details: payloads.EncodeString("Details"),
			},
		},
		MetricsHandler:              metricsHandler,
		NamespaceName:               "test-namespace",
		BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
	}

	err := TransitionCanceled.Apply(activity, ctx, reqWithCtx)
	require.NoError(t, err)
	require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED, activity.Status)

	expectedFailure := &failurepb.Failure{
		FailureInfo: &failurepb.Failure_CanceledFailureInfo{
			CanceledFailureInfo: &failurepb.CanceledFailureInfo{
				Details: payloads.EncodeString("Details"),
			},
		},
	}
	protorequire.ProtoEqual(t, expectedFailure, outcome.GetFailed().GetFailure())
}
