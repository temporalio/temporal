package nexusoperation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTransitionCancellationScheduled(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	cancellation := newCancellation(&nexusoperationpb.CancellationState{Status: nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED})

	err := TransitionCancellationScheduled.Apply(cancellation, ctx, EventCancellationScheduled{
		Destination: "test-endpoint",
	})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SCHEDULED, cancellation.Status)
	require.Equal(t, int32(1), cancellation.Attempt)

	// Verify cancellation task with correct destination.
	require.Len(t, ctx.Tasks, 1)
	task := ctx.Tasks[0]
	require.Equal(t, "test-endpoint", task.Attributes.Destination)
	require.Empty(t, task.Attributes.ScheduledTime)
	cancellationTask, ok := task.Payload.(*nexusoperationpb.CancellationTask)
	require.True(t, ok, "expected CancellationTask")
	require.Equal(t, int32(1), cancellationTask.Attempt)
}

func TestTransitionCancellationAttemptFailed(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	retryDelay := 5 * time.Second
	cancellation := newCancellation(&nexusoperationpb.CancellationState{Status: nexusoperationpb.CANCELLATION_STATUS_SCHEDULED, Attempt: 2})
	failure := &failurepb.Failure{Message: "transient error"}

	err := transitionCancellationAttemptFailed.Apply(cancellation, ctx, EventCancellationAttemptFailed{
		Failure:     failure,
		RetryPolicy: backoff.NewConstantDelayRetryPolicy(retryDelay),
	})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF, cancellation.Status)
	require.Equal(t, defaultTime, cancellation.LastAttemptCompleteTime.AsTime())
	protorequire.ProtoEqual(t, failure, cancellation.LastAttemptFailure)
	require.Equal(t, defaultTime.Add(retryDelay), cancellation.NextAttemptScheduleTime.AsTime())

	// Verify backoff task.
	require.Len(t, ctx.Tasks, 1)
	backoffTask, ok := ctx.Tasks[0].Payload.(*nexusoperationpb.CancellationBackoffTask)
	require.True(t, ok, "expected CancellationBackoffTask")
	require.Equal(t, int32(2), backoffTask.Attempt)
	require.Equal(t, defaultTime.Add(retryDelay), ctx.Tasks[0].Attributes.ScheduledTime)
	require.Empty(t, ctx.Tasks[0].Attributes.Destination)
}

func TestTransitionCancellationRescheduled(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	op := newTestOperation()
	cancellation := newCancellation(&nexusoperationpb.CancellationState{
		Status:                  nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF,
		Attempt:                 2,
		NextAttemptScheduleTime: timestamppb.New(defaultTime.Add(time.Minute)),
	})
	cancellation.Operation = chasm.NewMockParentPtr[*Operation](op)

	err := transitionCancellationRescheduled.Apply(cancellation, ctx, EventCancellationRescheduled{})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SCHEDULED, cancellation.Status)
	require.Equal(t, int32(3), cancellation.Attempt)
	require.Nil(t, cancellation.NextAttemptScheduleTime)

	// Verify cancellation task with correct destination.
	require.Len(t, ctx.Tasks, 1)
	task := ctx.Tasks[0]
	require.Equal(t, "test-endpoint", task.Attributes.Destination)
	require.Empty(t, task.Attributes.ScheduledTime)
	cancellationTask, ok := task.Payload.(*nexusoperationpb.CancellationTask)
	require.True(t, ok, "expected CancellationTask")
	require.Equal(t, int32(3), cancellationTask.Attempt)
}

func TestTransitionCancellationFailed(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	cancellation := newCancellation(&nexusoperationpb.CancellationState{Status: nexusoperationpb.CANCELLATION_STATUS_SCHEDULED})
	failure := &failurepb.Failure{Message: "permanent failure"}

	err := TransitionCancellationFailed.Apply(cancellation, ctx, EventCancellationFailed{
		Failure: failure,
	})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_FAILED, cancellation.Status)
	require.Equal(t, defaultTime, cancellation.LastAttemptCompleteTime.AsTime())
	protorequire.ProtoEqual(t, failure, cancellation.LastAttemptFailure)
	// Terminal state - no tasks.
	require.Empty(t, ctx.Tasks)
}

func TestTransitionCancellationSucceeded(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	cancellation := newCancellation(&nexusoperationpb.CancellationState{
		Status:             nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
		Attempt:            1,
		LastAttemptFailure: &failurepb.Failure{Message: "previous attempt failed"},
	})

	err := TransitionCancellationSucceeded.Apply(cancellation, ctx, EventCancellationSucceeded{})
	require.NoError(t, err)

	require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED, cancellation.Status)
	require.Equal(t, defaultTime, cancellation.LastAttemptCompleteTime.AsTime())
	// LastAttemptFailure should be cleared on success.
	require.Nil(t, cancellation.LastAttemptFailure)
	// Terminal state - no tasks.
	require.Empty(t, ctx.Tasks)
}

func TestCancellationLifecycleState(t *testing.T) {
	testCases := []struct {
		status   nexusoperationpb.CancellationStatus
		expected chasm.LifecycleState
	}{
		{nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED, chasm.LifecycleStateRunning},
		{nexusoperationpb.CANCELLATION_STATUS_SCHEDULED, chasm.LifecycleStateRunning},
		{nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF, chasm.LifecycleStateRunning},
		{nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED, chasm.LifecycleStateCompleted},
		{nexusoperationpb.CANCELLATION_STATUS_FAILED, chasm.LifecycleStateFailed},
		{nexusoperationpb.CANCELLATION_STATUS_TIMED_OUT, chasm.LifecycleStateFailed},
	}

	for _, tc := range testCases {
		t.Run(tc.status.String(), func(t *testing.T) {
			cancellation := newCancellation(&nexusoperationpb.CancellationState{Status: tc.status})
			require.Equal(t, tc.expected, cancellation.LifecycleState(nil))
		})
	}
}

func TestCancellationStateMachineState(t *testing.T) {
	cancellation := newCancellation(&nexusoperationpb.CancellationState{Status: nexusoperationpb.CANCELLATION_STATUS_SCHEDULED})
	require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SCHEDULED, cancellation.StateMachineState())

	cancellation.SetStateMachineState(nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED)
	require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED, cancellation.StateMachineState())
}

func TestCancellationFullLifecycle(t *testing.T) {
	// Test a full lifecycle: scheduled → attempt failed → backing off → (rescheduled →) attempt failed → succeeded.
	// We can't test Scheduled/Rescheduled transitions directly due to ParentPtr requirements,
	// but we can test the retry and terminal transitions in sequence.

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return defaultTime },
		},
	}

	cancellation := newCancellation(&nexusoperationpb.CancellationState{Status: nexusoperationpb.CANCELLATION_STATUS_SCHEDULED, Attempt: 1})

	// First attempt fails with retryable error.
	retryPolicy := backoff.NewExponentialRetryPolicy(time.Second)
	err := transitionCancellationAttemptFailed.Apply(cancellation, ctx, EventCancellationAttemptFailed{
		Failure:     &failurepb.Failure{Message: "transient"},
		RetryPolicy: retryPolicy,
	})
	require.NoError(t, err)
	require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF, cancellation.Status)
	require.Len(t, ctx.Tasks, 1)
	backoffTask, ok := ctx.Tasks[0].Payload.(*nexusoperationpb.CancellationBackoffTask)
	require.True(t, ok)
	require.Equal(t, int32(1), backoffTask.Attempt)

	// Simulate rescheduling by manually transitioning to SCHEDULED (since we can't call
	// transitionCancellationRescheduled without ParentPtr).
	cancellation.Status = nexusoperationpb.CANCELLATION_STATUS_SCHEDULED
	cancellation.Attempt = 2
	cancellation.NextAttemptScheduleTime = nil

	// Second attempt succeeds.
	ctx.Tasks = nil // Reset tasks.
	err = TransitionCancellationSucceeded.Apply(cancellation, ctx, EventCancellationSucceeded{})
	require.NoError(t, err)
	require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED, cancellation.Status)
	require.Nil(t, cancellation.LastAttemptFailure)
	require.Empty(t, ctx.Tasks)
}
