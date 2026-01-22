package nexusoperation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	defaultCancellationTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
)

func newTestCancellation() *Cancellation {
	return &Cancellation{
		CancellationState: &nexusoperationpb.CancellationState{
			Status:           nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED,
			Attempt:          0,
			RequestedTime:    nil,
			RequestedEventId: 123,
		},
	}
}

func newTestCancellationContext() *chasm.MockMutableContext {
	return &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time {
				return defaultCancellationTime
			},
		},
	}
}

func TestTransitionCancellationScheduled(t *testing.T) {
	ctx := newTestCancellationContext()
	cancellation := newTestCancellation()

	event := EventCancellationScheduled{}

	err := transitionCancellationScheduled.Apply(cancellation, ctx, event)
	require.NoError(t, err)

	// Verify state transition
	require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SCHEDULED, cancellation.Status)

	// Verify requested time was recorded
	require.NotNil(t, cancellation.RequestedTime)
	require.Equal(t, defaultCancellationTime, cancellation.RequestedTime.AsTime())

	// Verify CancellationTask was emitted
	require.Len(t, ctx.Tasks, 1)
	task := ctx.Tasks[0]
	require.IsType(t, &nexusoperationpb.CancellationTask{}, task.Payload)
	cancelTask := task.Payload.(*nexusoperationpb.CancellationTask)
	require.Equal(t, int32(0), cancelTask.Attempt)
	require.Equal(t, time.Time{}, task.Attributes.ScheduledTime) // Immediate execution
}

func TestTransitionCancellationScheduled_InvalidFromState(t *testing.T) {
	ctx := newTestCancellationContext()
	cancellation := newTestCancellation()
	cancellation.Status = nexusoperationpb.CANCELLATION_STATUS_SCHEDULED // Invalid from state

	event := EventCancellationScheduled{}

	err := transitionCancellationScheduled.Apply(cancellation, ctx, event)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid transition")
}

func TestTransitionCancellationRescheduled(t *testing.T) {
	ctx := newTestCancellationContext()
	cancellation := newTestCancellation()
	cancellation.Status = nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF
	cancellation.Attempt = 2
	cancellation.NextAttemptScheduleTime = timestamppb.New(defaultCancellationTime.Add(5 * time.Second))

	event := EventCancellationRescheduled{}

	err := transitionCancellationRescheduled.Apply(cancellation, ctx, event)
	require.NoError(t, err)

	// Verify state transition
	require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SCHEDULED, cancellation.Status)

	// Verify next attempt schedule time was cleared
	require.Nil(t, cancellation.NextAttemptScheduleTime)

	// Verify CancellationTask was emitted with current attempt
	require.Len(t, ctx.Tasks, 1)
	task := ctx.Tasks[0]
	require.IsType(t, &nexusoperationpb.CancellationTask{}, task.Payload)
	cancelTask := task.Payload.(*nexusoperationpb.CancellationTask)
	require.Equal(t, int32(2), cancelTask.Attempt)
}

func TestTransitionCancellationAttemptFailed(t *testing.T) {
	testCases := []struct {
		name             string
		initialAttempt   int32
		minRetryInterval time.Duration
		maxRetryInterval time.Duration
	}{
		{
			name:             "first retry",
			initialAttempt:   0,
			minRetryInterval: 500 * time.Millisecond,
			maxRetryInterval: 1500 * time.Millisecond,
		},
		{
			name:             "second retry",
			initialAttempt:   1,
			minRetryInterval: 1 * time.Second,
			maxRetryInterval: 3 * time.Second,
		},
		{
			name:             "third retry",
			initialAttempt:   2,
			minRetryInterval: 2 * time.Second,
			maxRetryInterval: 6 * time.Second,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newTestCancellationContext()
			cancellation := newTestCancellation()
			cancellation.Status = nexusoperationpb.CANCELLATION_STATUS_SCHEDULED
			cancellation.Attempt = tc.initialAttempt

			failure := &failurepb.Failure{
				Message: "temporary error",
			}
			retryPolicy := backoff.NewExponentialRetryPolicy(time.Second)

			event := EventCancellationAttemptFailed{
				Failure:     failure,
				RetryPolicy: retryPolicy,
			}

			err := transitionCancellationAttemptFailed.Apply(cancellation, ctx, event)
			require.NoError(t, err)

			// Verify state transition
			require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF, cancellation.Status)

			// Verify attempt was incremented
			require.Equal(t, tc.initialAttempt+1, cancellation.Attempt)

			// Verify last attempt complete time was set
			require.NotNil(t, cancellation.LastAttemptCompleteTime)
			require.Equal(t, defaultCancellationTime, cancellation.LastAttemptCompleteTime.AsTime())

			// Verify failure was stored
			require.NotNil(t, cancellation.LastAttemptFailure)
			require.Equal(t, "temporary error", cancellation.LastAttemptFailure.Message)

			// Verify next attempt schedule time was set with backoff (accounting for jitter)
			require.NotNil(t, cancellation.NextAttemptScheduleTime)
			actualInterval := cancellation.NextAttemptScheduleTime.AsTime().Sub(defaultCancellationTime)
			require.True(t, actualInterval >= tc.minRetryInterval,
				"retry interval %v should be >= %v", actualInterval, tc.minRetryInterval)
			require.True(t, actualInterval <= tc.maxRetryInterval,
				"retry interval %v should be <= %v", actualInterval, tc.maxRetryInterval)

			// Verify CancellationBackoffTask was emitted
			require.Len(t, ctx.Tasks, 1)
			task := ctx.Tasks[0]
			require.IsType(t, &nexusoperationpb.CancellationBackoffTask{}, task.Payload)
			backoffTask := task.Payload.(*nexusoperationpb.CancellationBackoffTask)
			require.Equal(t, tc.initialAttempt+1, backoffTask.Attempt)
			require.Equal(t, cancellation.NextAttemptScheduleTime.AsTime(), task.Attributes.ScheduledTime)
		})
	}
}

func TestTransitionCancellationAttemptFailed_ClearsLastFailure(t *testing.T) {
	ctx := newTestCancellationContext()
	cancellation := newTestCancellation()
	cancellation.Status = nexusoperationpb.CANCELLATION_STATUS_SCHEDULED
	cancellation.Attempt = 0
	// Set a previous failure that should be cleared during recordAttempt
	cancellation.LastAttemptFailure = &failurepb.Failure{
		Message: "old failure",
	}

	newFailure := &failurepb.Failure{
		Message: "new failure",
	}
	retryPolicy := backoff.NewExponentialRetryPolicy(time.Second)

	event := EventCancellationAttemptFailed{
		Failure:     newFailure,
		RetryPolicy: retryPolicy,
	}

	err := transitionCancellationAttemptFailed.Apply(cancellation, ctx, event)
	require.NoError(t, err)

	// Verify the new failure replaced the old one
	require.NotNil(t, cancellation.LastAttemptFailure)
	require.Equal(t, "new failure", cancellation.LastAttemptFailure.Message)
}

func TestTransitionCancellationFailed(t *testing.T) {
	testCases := []struct {
		name       string
		fromStatus nexusoperationpb.CancellationStatus
	}{
		{
			name:       "from unspecified",
			fromStatus: nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED,
		},
		{
			name:       "from scheduled",
			fromStatus: nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newTestCancellationContext()
			cancellation := newTestCancellation()
			cancellation.Status = tc.fromStatus
			cancellation.Attempt = 3

			failure := &failurepb.Failure{
				Message: "non-retryable error",
			}

			event := EventCancellationFailed{
				Failure: failure,
			}

			err := transitionCancellationFailed.Apply(cancellation, ctx, event)
			require.NoError(t, err)

			// Verify state transition to terminal state
			require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_FAILED, cancellation.Status)

			// Verify attempt was incremented
			require.Equal(t, int32(4), cancellation.Attempt)

			// Verify last attempt complete time was set
			require.NotNil(t, cancellation.LastAttemptCompleteTime)
			require.Equal(t, defaultCancellationTime, cancellation.LastAttemptCompleteTime.AsTime())

			// Verify failure was stored
			require.NotNil(t, cancellation.LastAttemptFailure)
			require.Equal(t, "non-retryable error", cancellation.LastAttemptFailure.Message)

			// Verify no tasks emitted (terminal state)
			require.Empty(t, ctx.Tasks)
		})
	}
}

func TestTransitionCancellationFailed_InvalidFromState(t *testing.T) {
	ctx := newTestCancellationContext()
	cancellation := newTestCancellation()
	cancellation.Status = nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF // Invalid from state

	failure := &failurepb.Failure{
		Message: "non-retryable error",
	}

	event := EventCancellationFailed{

		Failure: failure,
	}

	err := transitionCancellationFailed.Apply(cancellation, ctx, event)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid transition")
}

func TestTransitionCancellationSucceeded(t *testing.T) {
	ctx := newTestCancellationContext()
	cancellation := newTestCancellation()
	cancellation.Status = nexusoperationpb.CANCELLATION_STATUS_SCHEDULED
	cancellation.Attempt = 2

	event := EventCancellationSucceeded{}

	err := transitionCancellationSucceeded.Apply(cancellation, ctx, event)
	require.NoError(t, err)

	// Verify state transition to terminal state
	require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED, cancellation.Status)

	// Verify attempt was incremented
	require.Equal(t, int32(3), cancellation.Attempt)

	// Verify last attempt complete time was set
	require.NotNil(t, cancellation.LastAttemptCompleteTime)
	require.Equal(t, defaultCancellationTime, cancellation.LastAttemptCompleteTime.AsTime())

	// Verify last attempt failure was cleared (successful completion)
	require.Nil(t, cancellation.LastAttemptFailure)

	// Verify no tasks emitted (terminal state)
	require.Empty(t, ctx.Tasks)
}

func TestTransitionCancellationSucceeded_InvalidFromState(t *testing.T) {
	ctx := newTestCancellationContext()
	cancellation := newTestCancellation()
	cancellation.Status = nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED // Invalid from state

	event := EventCancellationSucceeded{}

	err := transitionCancellationSucceeded.Apply(cancellation, ctx, event)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid transition")
}

// Test that transitionCancellationScheduled validates from states
func TestTransitionCancellationScheduled_StateValidation(t *testing.T) {
	allStatuses := []nexusoperationpb.CancellationStatus{
		nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED,
		nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
		nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF,
		nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED,
		nexusoperationpb.CANCELLATION_STATUS_FAILED,
		nexusoperationpb.CANCELLATION_STATUS_TIMED_OUT,
		nexusoperationpb.CANCELLATION_STATUS_BLOCKED,
	}

	for _, status := range allStatuses {
		t.Run(status.String(), func(t *testing.T) {
			ctx := newTestCancellationContext()
			cancellation := newTestCancellation()
			cancellation.Status = status

			event := EventCancellationScheduled{}
			err := transitionCancellationScheduled.Apply(cancellation, ctx, event)

			if status == nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// Test that transitionCancellationRescheduled validates from states
func TestTransitionCancellationRescheduled_StateValidation(t *testing.T) {
	allStatuses := []nexusoperationpb.CancellationStatus{
		nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED,
		nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
		nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF,
		nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED,
		nexusoperationpb.CANCELLATION_STATUS_FAILED,
		nexusoperationpb.CANCELLATION_STATUS_TIMED_OUT,
		nexusoperationpb.CANCELLATION_STATUS_BLOCKED,
	}

	for _, status := range allStatuses {
		t.Run(status.String(), func(t *testing.T) {
			ctx := newTestCancellationContext()
			cancellation := newTestCancellation()
			cancellation.Status = status

			event := EventCancellationRescheduled{}
			err := transitionCancellationRescheduled.Apply(cancellation, ctx, event)

			if status == nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// Test that transitionCancellationAttemptFailed validates from states
func TestTransitionCancellationAttemptFailed_StateValidation(t *testing.T) {
	allStatuses := []nexusoperationpb.CancellationStatus{
		nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED,
		nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
		nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF,
		nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED,
		nexusoperationpb.CANCELLATION_STATUS_FAILED,
		nexusoperationpb.CANCELLATION_STATUS_TIMED_OUT,
		nexusoperationpb.CANCELLATION_STATUS_BLOCKED,
	}

	for _, status := range allStatuses {
		t.Run(status.String(), func(t *testing.T) {
			ctx := newTestCancellationContext()
			cancellation := newTestCancellation()
			cancellation.Status = status

			event := EventCancellationAttemptFailed{
				Failure:     &failurepb.Failure{Message: "error"},
				RetryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
			}
			err := transitionCancellationAttemptFailed.Apply(cancellation, ctx, event)

			if status == nexusoperationpb.CANCELLATION_STATUS_SCHEDULED {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// Test that transitionCancellationSucceeded validates from states
func TestTransitionCancellationSucceeded_StateValidation(t *testing.T) {
	allStatuses := []nexusoperationpb.CancellationStatus{
		nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED,
		nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
		nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF,
		nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED,
		nexusoperationpb.CANCELLATION_STATUS_FAILED,
		nexusoperationpb.CANCELLATION_STATUS_TIMED_OUT,
		nexusoperationpb.CANCELLATION_STATUS_BLOCKED,
	}

	for _, status := range allStatuses {
		t.Run(status.String(), func(t *testing.T) {
			ctx := newTestCancellationContext()
			cancellation := newTestCancellation()
			cancellation.Status = status

			event := EventCancellationSucceeded{}
			err := transitionCancellationSucceeded.Apply(cancellation, ctx, event)

			if status == nexusoperationpb.CANCELLATION_STATUS_SCHEDULED {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// Test transitionCancellationFailed accepts multiple from states
func TestTransitionCancellationFailed_MultipleFromStates(t *testing.T) {
	validFromStates := []nexusoperationpb.CancellationStatus{
		nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED,
		nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
	}

	for _, fromStatus := range validFromStates {
		t.Run(fromStatus.String(), func(t *testing.T) {
			ctx := newTestCancellationContext()
			cancellation := newTestCancellation()
			cancellation.Status = fromStatus

			event := EventCancellationFailed{

				Failure: &failurepb.Failure{Message: "error"},
			}

			err := transitionCancellationFailed.Apply(cancellation, ctx, event)
			require.NoError(t, err)
			require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_FAILED, cancellation.Status)
		})
	}
}
