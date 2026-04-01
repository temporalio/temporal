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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	defaultTime                   = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	defaultScheduleToCloseTimeout = 10 * time.Minute
)

func newTestOperation() *Operation {
	ctx := &chasm.MockMutableContext{}
	op := &Operation{
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
	op.Outcome = chasm.NewDataField(ctx, &nexusoperationpb.OperationOutcome{})
	return op
}

func TestTransitionScheduled(t *testing.T) {
	testCases := []struct {
		name                   string
		scheduleToCloseTimeout time.Duration
		expectedTasks          []chasm.MockTask
	}{
		{
			name:                   "schedules invocation and timeout tasks",
			scheduleToCloseTimeout: defaultScheduleToCloseTimeout,
			expectedTasks: []chasm.MockTask{
				{Payload: &nexusoperationpb.InvocationTask{}},
				{Payload: &nexusoperationpb.ScheduleToCloseTimeoutTask{}},
			},
		},
		{
			name:                   "schedules only invocation task when timeout not set",
			scheduleToCloseTimeout: 0,
			expectedTasks: []chasm.MockTask{
				{Payload: &nexusoperationpb.InvocationTask{}},
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
			event := EventScheduled{}

			err := transitionScheduled.Apply(operation, ctx, event)
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.OPERATION_STATUS_SCHEDULED, operation.Status)

			// Verify added tasks
			require.Len(t, ctx.Tasks, len(tc.expectedTasks))
			for i, expectedTask := range tc.expectedTasks {
				actualTask := ctx.Tasks[i]

				require.IsType(t, expectedTask.Payload, actualTask.Payload, "expected %T at index %d, got %T",
					expectedTask.Payload, i, actualTask.Payload)

				switch expectedTask.Payload.(type) {
				case *nexusoperationpb.InvocationTask:
					invTask, ok := actualTask.Payload.(*nexusoperationpb.InvocationTask)
					require.True(t, ok, "expected InvocationTask at index %d", i)
					require.Equal(t, int32(0), invTask.Attempt)
					require.Empty(t, actualTask.Attributes.ScheduledTime)
				case *nexusoperationpb.ScheduleToCloseTimeoutTask:
					timeoutTask, ok := actualTask.Payload.(*nexusoperationpb.ScheduleToCloseTimeoutTask)
					require.True(t, ok, "expected ScheduleToCloseTimeoutTask at index %d", i)
					require.Equal(t, int32(0), timeoutTask.Attempt)
					require.Equal(t, defaultTime.Add(tc.scheduleToCloseTimeout), actualTask.Attributes.ScheduledTime)
				default:
					t.Fatalf("unexpected task payload type at index %d: %T", i, actualTask.Payload)
				}
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
			startingAttemptCount: 0,
			expectedAttempt:      1,
			minRetryInterval:     500 * time.Millisecond,  // With jitter, minimum is ~50% of base
			maxRetryInterval:     1500 * time.Millisecond, // With jitter, maximum is ~150% of base
			retryPolicy:          backoff.NewExponentialRetryPolicy(time.Second),
		},
		{
			name:                 "second retry",
			startingAttemptCount: 1,
			expectedAttempt:      2,
			minRetryInterval:     1 * time.Second,
			maxRetryInterval:     3 * time.Second,
			retryPolicy:          backoff.NewExponentialRetryPolicy(time.Second),
		},
		{
			name:                 "third retry",
			startingAttemptCount: 2,
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
	require.Equal(t, int32(2), operation.Attempt)

	// Verify NextAttemptScheduleTime was cleared
	require.Nil(t, operation.NextAttemptScheduleTime)

	// Verify invocation task
	require.Len(t, ctx.Tasks, 1)
	invTask, ok := ctx.Tasks[0].Payload.(*nexusoperationpb.InvocationTask)
	require.True(t, ok, "expected InvocationTask")
	require.Equal(t, int32(2), invTask.Attempt)
}

func TestTransitionStarted(t *testing.T) {
	testCases := []struct {
		name            string
		startStatus     nexusoperationpb.OperationStatus
		startingAttempt int32
		expectedAttempt int32
		operationToken  string
	}{
		{
			name:            "started from scheduled",
			startStatus:     nexusoperationpb.OPERATION_STATUS_SCHEDULED,
			startingAttempt: 0,
			expectedAttempt: 1,
			operationToken:  "test-token-1",
		},
		{
			name:            "started from backing off",
			startStatus:     nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
			startingAttempt: 3,
			expectedAttempt: 3, // Attempt should not increment when coming from BACKING_OFF
			operationToken:  "test-token-retry",
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
			operation.Attempt = tc.startingAttempt

			// Set NextAttemptScheduleTime if starting from BACKING_OFF to verify it gets cleared
			if tc.startStatus == nexusoperationpb.OPERATION_STATUS_BACKING_OFF {
				operation.NextAttemptScheduleTime = timestamppb.New(defaultTime.Add(time.Minute))
			}

			event := EventStarted{
				OperationToken: tc.operationToken,
				FromBackingOff: tc.startStatus == nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
			}

			err := transitionStarted.Apply(operation, ctx, event)
			require.NoError(t, err)

			require.Equal(t, nexusoperationpb.OPERATION_STATUS_STARTED, operation.Status)
			require.Equal(t, tc.expectedAttempt, operation.Attempt)
			require.Equal(t, tc.operationToken, operation.OperationToken)
			require.Equal(t, defaultTime, operation.LastAttemptCompleteTime.AsTime())
			require.Nil(t, operation.LastAttemptFailure)

			// Verify NextAttemptScheduleTime is cleared when leaving BACKING_OFF
			require.Nil(t, operation.NextAttemptScheduleTime)

			// No tasks should be emitted
			require.Empty(t, ctx.Tasks)
		})
	}
}

func TestTransitionSucceeded(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	operation := newTestOperation()
	operation.Status = nexusoperationpb.OPERATION_STATUS_STARTED

	err := transitionSucceeded.Apply(operation, ctx, EventSucceeded{})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.OPERATION_STATUS_SUCCEEDED, operation.Status)
	require.Empty(t, ctx.Tasks)
}

func TestTransitionFailed(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	operation := newTestOperation()
	operation.Status = nexusoperationpb.OPERATION_STATUS_STARTED

	err := transitionFailed.Apply(operation, ctx, EventFailed{})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.OPERATION_STATUS_FAILED, operation.Status)
	require.Empty(t, ctx.Tasks)
}

func TestTransitionCanceled(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	operation := newTestOperation()
	operation.Status = nexusoperationpb.OPERATION_STATUS_STARTED

	err := TransitionCanceled.Apply(operation, ctx, EventCanceled{})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.OPERATION_STATUS_CANCELED, operation.Status)
	require.Empty(t, ctx.Tasks)
}

func TestTransitionTimedOut(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	operation := newTestOperation()
	operation.Status = nexusoperationpb.OPERATION_STATUS_SCHEDULED

	err := transitionTimedOut.Apply(operation, ctx, EventTimedOut{})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.OPERATION_STATUS_TIMED_OUT, operation.Status)
	require.Empty(t, ctx.Tasks)
}

func TestTransitionTerminated(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	operation := newTestOperation()
	operation.Status = nexusoperationpb.OPERATION_STATUS_SCHEDULED

	event := EventTerminated{
		chasm.TerminateComponentRequest{
			RequestID: "terminate-request-id",
			Reason:    "test reason",
			Identity:  "test-identity",
		},
	}

	err := TransitionTerminated.Apply(operation, ctx, event)
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.OPERATION_STATUS_TERMINATED, operation.Status)
	protorequire.ProtoEqual(t, &nexusoperationpb.NexusOperationTerminateState{
		RequestId: "terminate-request-id",
		Identity:  "test-identity",
	}, operation.TerminateState)

	// Verify outcome failure is set with terminated info and reason as message.
	protorequire.ProtoEqual(t, &nexusoperationpb.OperationOutcome{
		Variant: &nexusoperationpb.OperationOutcome_Failed_{
			Failed: &nexusoperationpb.OperationOutcome_Failed{
				Failure: &failurepb.Failure{
					Message: "test reason",
					FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
						TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{},
					},
				},
			},
		},
	}, operation.Outcome.Get(ctx))

	require.Empty(t, ctx.Tasks)
}
