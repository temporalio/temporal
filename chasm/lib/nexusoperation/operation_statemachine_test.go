package nexusoperation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	defaultTime                   = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	defaultScheduleToCloseTimeout = 10 * time.Minute
	defaultScheduleToStartTimeout = 5 * time.Minute
)

func newTestOperation() *Operation {
	return &Operation{
		OperationState: &nexusoperationpb.OperationState{
			Status:                 nexusoperationpb.OPERATION_STATUS_UNSPECIFIED,
			EndpointId:             "endpoint-id",
			Endpoint:               "test-endpoint",
			Service:                "test-service",
			Operation:              "test-operation",
			ScheduledTime:          timestamppb.New(defaultTime),
			ScheduleToCloseTimeout: durationpb.New(defaultScheduleToCloseTimeout),
			RequestId:              "request-id",
			Attempt:                0,
		},
	}
}

func TestTransitionScheduled(t *testing.T) {
	testCases := []struct {
		name                   string
		scheduleToCloseTimeout time.Duration
		scheduleToStartTimeout time.Duration
		expectedTasks          []chasm.MockTask
	}{
		{
			name:                   "schedules invocation and schedule-to-close timeout tasks",
			scheduleToCloseTimeout: defaultScheduleToCloseTimeout,
			expectedTasks: []chasm.MockTask{
				{
					Attributes: chasm.TaskAttributes{Destination: "test-endpoint"},
					Payload:    &nexusoperationpb.InvocationTask{Attempt: 1},
				},
				{
					Attributes: chasm.TaskAttributes{ScheduledTime: defaultTime.Add(defaultScheduleToCloseTimeout)},
					Payload:    &nexusoperationpb.ScheduleToCloseTimeoutTask{},
				},
			},
		},
		{
			name:                   "schedules invocation and schedule-to-start timeout tasks",
			scheduleToStartTimeout: defaultScheduleToStartTimeout,
			expectedTasks: []chasm.MockTask{
				{
					Attributes: chasm.TaskAttributes{Destination: "test-endpoint"},
					Payload:    &nexusoperationpb.InvocationTask{Attempt: 1},
				},
				{
					Attributes: chasm.TaskAttributes{ScheduledTime: defaultTime.Add(defaultScheduleToStartTimeout)},
					Payload:    &nexusoperationpb.ScheduleToStartTimeoutTask{},
				},
			},
		},
		{
			name:                   "schedules invocation and both timeout tasks",
			scheduleToCloseTimeout: defaultScheduleToCloseTimeout,
			scheduleToStartTimeout: defaultScheduleToStartTimeout,
			expectedTasks: []chasm.MockTask{
				{
					Attributes: chasm.TaskAttributes{Destination: "test-endpoint"},
					Payload:    &nexusoperationpb.InvocationTask{Attempt: 1},
				},
				{
					Attributes: chasm.TaskAttributes{ScheduledTime: defaultTime.Add(defaultScheduleToStartTimeout)},
					Payload:    &nexusoperationpb.ScheduleToStartTimeoutTask{},
				},
				{
					Attributes: chasm.TaskAttributes{ScheduledTime: defaultTime.Add(defaultScheduleToCloseTimeout)},
					Payload:    &nexusoperationpb.ScheduleToCloseTimeoutTask{},
				},
			},
		},
		{
			name: "schedules only invocation task when no timeouts set",
			expectedTasks: []chasm.MockTask{
				{
					Attributes: chasm.TaskAttributes{Destination: "test-endpoint"},
					Payload:    &nexusoperationpb.InvocationTask{Attempt: 1},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return defaultTime },
				},
			}

			operation := newTestOperation()
			operation.ScheduleToCloseTimeout = durationpb.New(tc.scheduleToCloseTimeout)
			operation.ScheduleToStartTimeout = durationpb.New(tc.scheduleToStartTimeout)

			err := TransitionScheduled.Apply(operation, ctx, EventScheduled{})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_SCHEDULED, operation.Status)

			require.Len(t, ctx.Tasks, len(tc.expectedTasks))
			for i, expectedTask := range tc.expectedTasks {
				actualTask := ctx.Tasks[i]
				require.Equal(t, expectedTask.Attributes, actualTask.Attributes)
				protorequire.ProtoEqual(t, expectedTask.Payload.(proto.Message), actualTask.Payload.(proto.Message))
			}
		})
	}
}

func TestTransitionAttemptFailed(t *testing.T) {
	testCases := []struct {
		name                 string
		startingAttemptCount int32
		expectedAttempt      int32
		minRetryInterval     time.Duration
		maxRetryInterval     time.Duration
		retryPolicy          backoff.RetryPolicy
	}{
		{
			name:                 "first retry",
			startingAttemptCount: 1,
			expectedAttempt:      1,
			minRetryInterval:     500 * time.Millisecond,  // With jitter, minimum is ~50% of base
			maxRetryInterval:     1500 * time.Millisecond, // With jitter, maximum is ~150% of base
			retryPolicy:          backoff.NewExponentialRetryPolicy(time.Second),
		},
		{
			name:                 "second retry",
			startingAttemptCount: 2,
			expectedAttempt:      2,
			minRetryInterval:     1 * time.Second,
			maxRetryInterval:     3 * time.Second,
			retryPolicy:          backoff.NewExponentialRetryPolicy(time.Second),
		},
		{
			name:                 "third retry",
			startingAttemptCount: 3,
			expectedAttempt:      3,
			minRetryInterval:     2 * time.Second,
			maxRetryInterval:     6 * time.Second,
			retryPolicy:          backoff.NewExponentialRetryPolicy(time.Second),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return defaultTime },
				},
			}

			operation := newTestOperation()
			operation.Attempt = tc.startingAttemptCount
			operation.Status = nexusoperationpb.OPERATION_STATUS_SCHEDULED

			failure := &failurepb.Failure{
				Message: "test failure",
			}

			event := EventAttemptFailed{
				Failure:     failure,
				RetryPolicy: tc.retryPolicy,
			}

			err := transitionAttemptFailed.Apply(operation, ctx, event)
			require.NoError(t, err)

			require.Equal(t, nexusoperationpb.OPERATION_STATUS_BACKING_OFF, operation.Status)
			require.Equal(t, tc.expectedAttempt, operation.Attempt)
			require.Equal(t, defaultTime, operation.LastAttemptCompleteTime.AsTime())
			require.Equal(t, failure, operation.LastAttemptFailure)
			require.NotNil(t, operation.NextAttemptScheduleTime)
			require.True(t, operation.NextAttemptScheduleTime.AsTime().After(defaultTime))

			// Verify retry interval is within expected range (due to jitter)
			actualInterval := operation.NextAttemptScheduleTime.AsTime().Sub(defaultTime)
			require.GreaterOrEqual(t, actualInterval, tc.minRetryInterval, "retry interval %v should be >= %v", actualInterval, tc.minRetryInterval)
			require.LessOrEqual(t, actualInterval, tc.maxRetryInterval, "retry interval %v should be <= %v", actualInterval, tc.maxRetryInterval)

			// Verify backoff task
			require.Len(t, ctx.Tasks, 1)
			backoffTask, ok := ctx.Tasks[0].Payload.(*nexusoperationpb.InvocationBackoffTask)
			require.True(t, ok, "expected InvocationBackoffTask")
			require.Equal(t, tc.expectedAttempt, backoffTask.Attempt)
			require.Equal(t, operation.NextAttemptScheduleTime.AsTime(), ctx.Tasks[0].Attributes.ScheduledTime)
		})
	}
}

func TestTransitionRescheduled(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	operation := newTestOperation()
	operation.Status = nexusoperationpb.OPERATION_STATUS_BACKING_OFF
	operation.Attempt = 2
	operation.NextAttemptScheduleTime = timestamppb.New(defaultTime.Add(time.Minute))

	event := EventRescheduled{}

	err := transitionRescheduled.Apply(operation, ctx, event)
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.OPERATION_STATUS_SCHEDULED, operation.Status)
	require.Equal(t, int32(3), operation.Attempt)

	// Verify NextAttemptScheduleTime was cleared
	require.Nil(t, operation.NextAttemptScheduleTime)

	// Verify invocation task
	require.Len(t, ctx.Tasks, 1)
	invTask, ok := ctx.Tasks[0].Payload.(*nexusoperationpb.InvocationTask)
	require.True(t, ok, "expected InvocationTask")
	require.Equal(t, int32(3), invTask.Attempt)
}

func TestTransitionStarted(t *testing.T) {
	defaultStartToCloseTimeout := 5 * time.Minute
	customStartTime := defaultTime.Add(time.Minute)

	testCases := []struct {
		name                string
		startToCloseTimeout time.Duration
		startTime           *time.Time
		pendingCancellation bool
		expectedTasks       []chasm.MockTask
	}{
		{
			name:                "emits start-to-close timeout task",
			startToCloseTimeout: defaultStartToCloseTimeout,
			expectedTasks: []chasm.MockTask{
				{
					Attributes: chasm.TaskAttributes{ScheduledTime: defaultTime.Add(defaultStartToCloseTimeout)},
					Payload:    &nexusoperationpb.StartToCloseTimeoutTask{},
				},
			},
		},
		{
			name:                "start-to-close timeout uses event StartTime",
			startToCloseTimeout: defaultStartToCloseTimeout,
			startTime:           &customStartTime,
			expectedTasks: []chasm.MockTask{
				{
					Attributes: chasm.TaskAttributes{ScheduledTime: customStartTime.Add(defaultStartToCloseTimeout)},
					Payload:    &nexusoperationpb.StartToCloseTimeoutTask{},
				},
			},
		},
		{
			name:                "schedules pending cancellation",
			pendingCancellation: true,
			expectedTasks: []chasm.MockTask{
				{
					Payload: &nexusoperationpb.CancellationTask{},
				},
			},
		},
		{
			name: "no tasks when timeout not set",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return defaultTime },
				},
			}

			operation := newTestOperation()
			operation.Status = nexusoperationpb.OPERATION_STATUS_SCHEDULED
			operation.Attempt = 1
			operation.StartToCloseTimeout = durationpb.New(tc.startToCloseTimeout)
			if tc.pendingCancellation {
				operation.Cancellation = chasm.NewComponentField[*Cancellation](nil, newCancellation(
					&nexusoperationpb.CancellationState{
						Status:        nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED,
						RequestedTime: timestamppb.New(defaultTime),
					},
				))
			}

			err := TransitionStarted.Apply(operation, ctx, EventStarted{
				OperationToken: "test-token",
				StartTime:      tc.startTime,
			})
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_STARTED, operation.Status)

			require.Len(t, ctx.Tasks, len(tc.expectedTasks))
			for i, expectedTask := range tc.expectedTasks {
				actualTask := ctx.Tasks[i]
				require.Equal(t, expectedTask.Attributes, actualTask.Attributes)
				protorequire.ProtoEqual(t, expectedTask.Payload.(proto.Message), actualTask.Payload.(proto.Message))
			}
		})
	}
}

func TestTransitionSucceeded(t *testing.T) {
	customCompleteTime := defaultTime.Add(time.Minute)

	testCases := []struct {
		name               string
		completeTime       *time.Time
		expectedClosedTime time.Time
	}{
		{
			name:               "uses default time",
			expectedClosedTime: defaultTime,
		},
		{
			name:               "uses event CompleteTime",
			completeTime:       &customCompleteTime,
			expectedClosedTime: customCompleteTime,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return defaultTime },
				},
			}

			operation := newTestOperation()
			operation.Status = nexusoperationpb.OPERATION_STATUS_STARTED

			err := TransitionSucceeded.Apply(operation, ctx, EventSucceeded{
				CompleteTime: tc.completeTime,
			})
			require.NoError(t, err)

			require.Equal(t, nexusoperationpb.OPERATION_STATUS_SUCCEEDED, operation.Status)
			require.Equal(t, tc.expectedClosedTime, operation.ClosedTime.AsTime())
			require.Empty(t, ctx.Tasks)
		})
	}
}

func TestTransitionFailed(t *testing.T) {
	customCompleteTime := defaultTime.Add(time.Minute)
	failure := &failurepb.Failure{Message: "test failure"}

	testCases := []struct {
		name                          string
		startStatus                   nexusoperationpb.OperationStatus
		completeTime                  *time.Time
		expectedClosedTime            time.Time
		expectedLastAttemptFailure    *failurepb.Failure
		expectLastAttemptCompleteTime bool
	}{
		{
			name:                          "from scheduled records last attempt failure",
			startStatus:                   nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			expectedClosedTime:            defaultTime,
			expectedLastAttemptFailure:    failure,
			expectLastAttemptCompleteTime: true,
		},
		{
			name:               "from started does not record last attempt failure",
			startStatus:        nexusoperationpb.OPERATION_STATUS_STARTED,
			expectedClosedTime: defaultTime,
		},
		{
			name:               "uses event CompleteTime",
			startStatus:        nexusoperationpb.OPERATION_STATUS_STARTED,
			completeTime:       &customCompleteTime,
			expectedClosedTime: customCompleteTime,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return defaultTime },
				},
			}

			operation := newTestOperation()
			operation.Status = tc.startStatus

			err := TransitionFailed.Apply(operation, ctx, EventFailed{
				Failure:      failure,
				CompleteTime: tc.completeTime,
			})
			require.NoError(t, err)

			require.Equal(t, nexusoperationpb.OPERATION_STATUS_FAILED, operation.Status)
			require.Equal(t, tc.expectedClosedTime, operation.ClosedTime.AsTime())
			require.Nil(t, operation.NextAttemptScheduleTime)
			require.Empty(t, ctx.Tasks)

			if tc.expectLastAttemptCompleteTime {
				require.Equal(t, defaultTime, operation.LastAttemptCompleteTime.AsTime())
			}
			protorequire.ProtoEqual(t, tc.expectedLastAttemptFailure, operation.LastAttemptFailure)
		})
	}
}

func TestTransitionCanceled(t *testing.T) {
	customCompleteTime := defaultTime.Add(time.Minute)
	failure := &failurepb.Failure{Message: "canceled"}

	testCases := []struct {
		name                          string
		startStatus                   nexusoperationpb.OperationStatus
		completeTime                  *time.Time
		expectedClosedTime            time.Time
		expectedLastAttemptFailure    *failurepb.Failure
		expectLastAttemptCompleteTime bool
	}{
		{
			name:                          "from scheduled records last attempt failure",
			startStatus:                   nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			expectedClosedTime:            defaultTime,
			expectedLastAttemptFailure:    failure,
			expectLastAttemptCompleteTime: true,
		},
		{
			name:               "from started does not record last attempt failure",
			startStatus:        nexusoperationpb.OPERATION_STATUS_STARTED,
			expectedClosedTime: defaultTime,
		},
		{
			name:               "uses event CompleteTime",
			startStatus:        nexusoperationpb.OPERATION_STATUS_STARTED,
			completeTime:       &customCompleteTime,
			expectedClosedTime: customCompleteTime,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{
				MockContext: chasm.MockContext{
					HandleNow: func(chasm.Component) time.Time { return defaultTime },
				},
			}

			operation := newTestOperation()
			operation.Status = tc.startStatus

			err := TransitionCanceled.Apply(operation, ctx, EventCanceled{
				Failure:      failure,
				CompleteTime: tc.completeTime,
			})
			require.NoError(t, err)

			require.Equal(t, nexusoperationpb.OPERATION_STATUS_CANCELED, operation.Status)
			require.Equal(t, tc.expectedClosedTime, operation.ClosedTime.AsTime())
			require.Nil(t, operation.NextAttemptScheduleTime)
			require.Empty(t, ctx.Tasks)

			if tc.expectLastAttemptCompleteTime {
				require.Equal(t, defaultTime, operation.LastAttemptCompleteTime.AsTime())
			}
			protorequire.ProtoEqual(t, tc.expectedLastAttemptFailure, operation.LastAttemptFailure)
		})
	}
}

func TestTransitionTimedOut(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	operation := newTestOperation()
	operation.Status = nexusoperationpb.OPERATION_STATUS_STARTED

	err := TransitionTimedOut.Apply(operation, ctx, EventTimedOut{})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.OPERATION_STATUS_TIMED_OUT, operation.Status)
	require.Equal(t, defaultTime, operation.ClosedTime.AsTime())
	require.Empty(t, ctx.Tasks)
}
