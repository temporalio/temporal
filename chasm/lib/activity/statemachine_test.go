package activity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
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
			ctx := &chasm.MockMutableContext{}
			ctx.HandleNow = func(chasm.Component) time.Time { return defaultTime }
			attemptState := &activitypb.ActivityAttemptState{Count: tc.startingAttemptCount}
			outcome := &activitypb.ActivityOutcome{}

			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					RetryPolicy:            defaultRetryPolicy,
					ScheduleToCloseTimeout: durationpb.New(tc.scheduleToCloseTimeout),
					ScheduleToStartTimeout: durationpb.New(tc.scheduleToStartTimeout),
					StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
					Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED,
				},
				Attempt: chasm.NewDataField(ctx, attemptState),
				Outcome: chasm.NewDataField(ctx, outcome),
			}

			err := TransitionScheduled.Apply(activity, ctx, nil)
			require.NoError(t, err)
			require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED, activity.Status)
			require.EqualValues(t, 1, attemptState.Count)

			// Verify added tasks
			require.Len(t, ctx.Tasks, len(tc.expectedTasks))
			for i, expectedTask := range tc.expectedTasks {
				actualTask := ctx.Tasks[i]

				switch expectedTask.Payload.(type) {
				case *activitypb.ActivityDispatchTask:
					_, ok := actualTask.Payload.(*activitypb.ActivityDispatchTask)
					require.True(t, ok, "expected ActivityDispatchTask at index %d", i)
					require.Empty(t, 0, actualTask.Attributes.ScheduledTime)
				case *activitypb.ScheduleToStartTimeoutTask:
					_, ok := actualTask.Payload.(*activitypb.ScheduleToStartTimeoutTask)
					require.True(t, ok, "expected ScheduleToStartTimeoutTask at index %d", i)
					require.Equal(t, defaultTime.Add(tc.scheduleToStartTimeout), actualTask.Attributes.ScheduledTime)
				case *activitypb.ScheduleToCloseTimeoutTask:
					_, ok := actualTask.Payload.(*activitypb.ScheduleToCloseTimeoutTask)
					require.True(t, ok, "expected ScheduleToCloseTimeoutTask at index %d", i)
					require.Equal(t, defaultTime.Add(tc.scheduleToCloseTimeout), actualTask.Attributes.ScheduledTime)
				default:
					t.Fatalf("unexpected task payload type at index %d: %T", i, actualTask.Payload)
				}

			}
		})
	}
}

func TestTransitionScheduledFromInvalidAttempt(t *testing.T) {
	ctx := &chasm.MockMutableContext{}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			RetryPolicy:            defaultRetryPolicy,
			ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
			ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
			StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED,
		},
		Attempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{Count: 1}),
		Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
	}

	err := TransitionScheduled.Apply(activity, ctx, nil)
	require.Error(t, err)
}

func TestTransitionScheduledFromInvalidStatus(t *testing.T) {
	testCases := []struct {
		name           string
		startingStatus activitypb.ActivityExecutionStatus
	}{
		{
			name:           "from scheduled status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		},
		{
			name:           "from started status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		},
		{
			name:           "from cancel requested status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		},
		{
			name:           "from completed status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		},
		{
			name:           "from cancelled status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
		},
		{
			name:           "from terminated status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		},
		{
			name:           "from timed out status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}

			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					RetryPolicy:            defaultRetryPolicy,
					ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
					ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
					StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
					Status:                 tc.startingStatus,
				},
				Attempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{Count: 0}),
				Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
			}

			err := TransitionScheduled.Apply(activity, ctx, nil)
			require.Error(t, err)
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
					RetryPolicy:            defaultRetryPolicy,
					ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
					ScheduleToStartTimeout: durationpb.New(tc.scheduleToStartTimeout),
					StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
					Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
				},
				Attempt: chasm.NewDataField(ctx, attemptState),
				Outcome: chasm.NewDataField(ctx, outcome),
			}

			err := TransitionRescheduled.Apply(activity, ctx, nil)
			require.NoError(t, err)
			require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED, activity.Status)
			require.Equal(t, tc.startingAttemptCount+1, attemptState.Count)
			require.Equal(t, durationpb.New(tc.expectedRetryInterval), attemptState.GetCurrentRetryInterval())

			// Verify attempt state failure details updated correctly
			lastFailureDetails := attemptState.GetLastFailureDetails()
			require.NotNil(t, lastFailureDetails.GetLastFailureTime())
			require.NotNil(t, lastFailureDetails.GetLastFailure())
			require.Equal(t, lastFailureDetails.GetLastFailureTime(),
				attemptState.GetLastAttemptCompleteTime())
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

func TestTransitionRescheduledFromInvalidStatus(t *testing.T) {
	testCases := []struct {
		name           string
		startingStatus activitypb.ActivityExecutionStatus
	}{
		{
			name:           "from unspecified status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED,
		},
		{
			name:           "from scheduled status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		},
		{
			name:           "from cancel requested status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		},
		{
			name:           "from completed status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		},
		{
			name:           "from cancelled status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
		},
		{
			name:           "from terminated status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		},
		{
			name:           "from timed out status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}

			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					RetryPolicy:            defaultRetryPolicy,
					ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
					ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
					StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
					Status:                 tc.startingStatus,
				},
				Attempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{Count: 1}),
				Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
			}

			err := TransitionRescheduled.Apply(activity, ctx, nil)
			require.Error(t, err)
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

func TestTransitionStartedFromInvalidStatus(t *testing.T) {
	testCases := []struct {
		name           string
		startingStatus activitypb.ActivityExecutionStatus
	}{
		{
			name:           "from unspecified status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED,
		},
		{
			name:           "from started status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		},
		{
			name:           "from cancel requested status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		},
		{
			name:           "from completed status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		},
		{
			name:           "from cancelled status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
		},
		{
			name:           "from terminated status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		},
		{
			name:           "from timed out status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}

			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					RetryPolicy:            defaultRetryPolicy,
					ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
					ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
					StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
					Status:                 tc.startingStatus,
				},
				Attempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{Count: 1}),
				Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
			}

			err := TransitionStarted.Apply(activity, ctx, nil)
			require.Error(t, err)
		})
	}
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
			attemptCount: 1,
		},
		{
			name:         "start to close timeout on last attempt",
			startStatus:  activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			timeoutType:  enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
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
					RetryPolicy:            defaultRetryPolicy,
					ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
					ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
					StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
					Status:                 tc.startStatus,
				},
				Attempt: chasm.NewDataField(ctx, attemptState),
				Outcome: chasm.NewDataField(ctx, outcome),
			}

			err := TransitionTimedOut.Apply(activity, ctx, tc.timeoutType)
			require.NoError(t, err)
			require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, activity.Status)
			require.Equal(t, tc.attemptCount, attemptState.Count)

			switch tc.timeoutType {
			case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
				enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
				// Timeout failure is recorded in outcome but not attempt state
				require.Nil(t, attemptState.GetLastFailureDetails())
				require.Nil(t, attemptState.GetLastAttemptCompleteTime())
				require.NotNil(t, outcome.GetFailed().GetFailure())
				// do something
			case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
				isLastAttempt := tc.attemptCount >= defaultRetryPolicy.GetMaximumAttempts()

				// Timeout failure is recorded in attempt state
				require.NotNil(t, attemptState.GetLastFailureDetails().GetLastFailure())
				require.NotNil(t, attemptState.GetLastFailureDetails().GetLastFailureTime())
				require.NotNil(t, attemptState.GetLastAttemptCompleteTime())

				failure, ok := outcome.GetVariant().(*activitypb.ActivityOutcome_Failed_)
				if isLastAttempt {
					// On final attempt, the outcome should be failed, but the failure remains empty as it's already recorded in attempt state
					require.True(t, ok, "expected variant to be of type Failed")
					require.Nil(t, failure.Failed)
				} else {
					require.False(t, ok, "unexpected variant to be of type Failed")
					require.Nil(t, failure)
				}

			default:
				t.Fatalf("unexpected timeout type: %v", tc.timeoutType)
			}

			require.Empty(t, ctx.Tasks)
		})
	}
}

func TestTransitionTimeOutFromInvalidStatus(t *testing.T) {
	testCases := []struct {
		name           string
		startingStatus activitypb.ActivityExecutionStatus
	}{
		{
			name:           "from unspecified status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED,
		},
		{
			name:           "from cancel requested status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		},
		{
			name:           "from completed status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		},
		{
			name:           "from cancelled status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
		},
		{
			name:           "from terminated status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		},
		{
			name:           "from timed out status",
			startingStatus: activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}

			activity := &Activity{
				ActivityState: &activitypb.ActivityState{
					RetryPolicy:            defaultRetryPolicy,
					ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
					ScheduleToStartTimeout: durationpb.New(defaultScheduleToStartTimeout),
					StartToCloseTimeout:    durationpb.New(defaultStartToCloseTimeout),
					Status:                 tc.startingStatus,
				},
				Attempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{Count: 1}),
				Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
			}

			err := TransitionTimedOut.Apply(activity, ctx, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
			require.Error(t, err)
		})
	}
}
