package callback

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestValidTransitions(t *testing.T) {
	// Setup
	currentTime := time.Now().UTC()
	callback := &Callback{
		CallbackState: &callbackspb.CallbackState{
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{
						Url: "http://address:999/path/to/callback?query=string",
					},
				},
			},
		},
	}
	callback.SetStateMachineState(callbackspb.CALLBACK_STATUS_SCHEDULED)

	t.Run("TransitionAttemptFailed", func(t *testing.T) {
		mctx := &chasm.MockMutableContext{}
		mctx.HandleNow = func(chasm.Component) time.Time { return currentTime }

		err := TransitionAttemptFailed.Apply(callback, mctx, EventAttemptFailed{
			Time:        currentTime,
			Err:         errors.New("error message"),
			RetryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
		})
		require.NoError(t, err)

		// Assert info object is updated.
		require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, callback.StateMachineState())
		require.Equal(t, int32(1), callback.Attempt)
		require.Equal(t, "error message", callback.LastAttemptFailure.Message)
		require.False(t, callback.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
		require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
		dt := currentTime.Add(time.Second).Sub(callback.NextAttemptScheduleTime.AsTime())
		require.Less(t, dt, time.Millisecond*200)

		// Because of the retry policy, the first failure isn't terminal.
		_, hasTermFailure := callback.TerminalFailure.TryGet(mctx)
		require.False(t, hasTermFailure)

		// Assert backoff task is generated
		require.Len(t, mctx.Tasks, 1)
		require.IsType(t, &callbackspb.BackoffTask{}, mctx.Tasks[0].Payload)
	})

	t.Run("TransitionRescheduled", func(t *testing.T) {
		mctx := &chasm.MockMutableContext{}
		mctx.HandleNow = func(chasm.Component) time.Time { return currentTime }

		err := TransitionRescheduled.Apply(callback, mctx, EventRescheduled{})
		require.NoError(t, err)

		// Assert info object is only partially updated.
		require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, callback.StateMachineState())
		// Unmodified
		require.Equal(t, int32(1), callback.Attempt)
		require.Equal(t, "error message", callback.LastAttemptFailure.Message)
		require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
		require.Nil(t, callback.NextAttemptScheduleTime)
		_, hasTermFailure := callback.TerminalFailure.TryGet(mctx)
		require.False(t, hasTermFailure)

		// Assert callback task is generated.
		require.Len(t, mctx.Tasks, 1)
		require.IsType(t, &callbackspb.InvocationTask{}, mctx.Tasks[0].Payload)
	})

	// Store the pre-succeeded state to test Failed later.
	dup := &Callback{
		CallbackState: proto.Clone(callback.CallbackState).(*callbackspb.CallbackState),
	}
	dup.Status = callback.StateMachineState()

	t.Run("TransitionSucceeded", func(t *testing.T) {
		currentTime = currentTime.Add(time.Second)
		mctx := &chasm.MockMutableContext{}
		mctx.HandleNow = func(chasm.Component) time.Time { return currentTime }

		err := TransitionSucceeded.Apply(callback, mctx, EventSucceeded{Time: currentTime})
		require.NoError(t, err)

		// Assert info object is updated.
		require.Equal(t, callbackspb.CALLBACK_STATUS_SUCCEEDED, callback.StateMachineState())
		require.Equal(t, int32(2), callback.Attempt)
		require.Nil(t, callback.LastAttemptFailure)
		require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
		require.Nil(t, callback.NextAttemptScheduleTime)

		// TerminalFailure may explicitly be set to nil.
		termFailureValue, hasTermFailure := callback.TerminalFailure.TryGet(mctx)
		require.True(t, !hasTermFailure || termFailureValue == nil)

		// Assert no task is generated on success transition
		require.Empty(t, mctx.Tasks)
	})

	// Reset back to the scheduled state.
	callback = dup
	// Increment the time to ensure it's updated in the transition
	currentTime = currentTime.Add(time.Second)

	t.Run("TransitionFailed", func(t *testing.T) {
		mctx := &chasm.MockMutableContext{}
		mctx.HandleNow = func(chasm.Component) time.Time { return currentTime }

		err := TransitionFailed.Apply(callback, mctx, EventFailed{Time: currentTime, Err: errors.New("failed")})
		require.NoError(t, err)

		// Assert info object is updated.
		require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, callback.StateMachineState())
		require.Equal(t, int32(2), callback.Attempt)
		require.Equal(t, "failed", callback.LastAttemptFailure.Message)
		require.True(t, callback.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
		require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
		require.Nil(t, callback.NextAttemptScheduleTime)

		// Check the TerminalFailure field is set.
		require.NotNil(t, callback.TerminalFailure, "TerminalFailure not set")
		got := callback.TerminalFailure.Get(mctx)
		want := callback.LastAttemptFailure
		require.True(t, proto.Equal(want, got), "TerminalFailure not as expected. Got %v, want %v.", got, want)

		// Assert no tasks generated. In terminal state.
		require.Empty(t, mctx.Tasks)
	})
}

func TestTerminatedTransition(t *testing.T) {
	initialCallbackState := &callbackspb.CallbackState{
		RegistrationTime: timestamppb.New(time.Now()),
		Callback: &callbackspb.Callback{
			Variant: &callbackspb.Callback_Nexus_{
				Nexus: &callbackspb.Callback_Nexus{
					Url: "http://address:999/path",
				},
			},
		},
	}
	initialCallback := &Callback{
		CallbackState: initialCallbackState,
	}

	for _, src := range []callbackspb.CallbackStatus{
		callbackspb.CALLBACK_STATUS_STANDBY,
		callbackspb.CALLBACK_STATUS_SCHEDULED,
		callbackspb.CALLBACK_STATUS_BACKING_OFF,
	} {
		t.Run("from_"+src.String(), func(t *testing.T) {
			currentTime := time.Now().UTC()
			mctx := &chasm.MockMutableContext{}
			mctx.HandleNow = func(chasm.Component) time.Time { return currentTime }

			cb := &Callback{
				CallbackState: proto.Clone(initialCallbackState).(*callbackspb.CallbackState),
			}
			cb.SetStateMachineState(src)

			err := TransitionTerminated.Apply(cb, mctx, EventTerminated{})
			require.NoError(t, err)

			// Confirm expected state changes.
			require.Equal(t, callbackspb.CALLBACK_STATUS_TERMINATED, cb.StateMachineState())
			require.Equal(t, currentTime, cb.GetCloseTime().AsTime())
			// Other fields remain the same.
			require.True(t, proto.Equal(initialCallbackState.Callback, cb.Callback))
			require.True(t, proto.Equal(initialCallbackState.RegistrationTime, cb.RegistrationTime))
			require.Equal(t, initialCallback.LastAttemptFailure, cb.LastAttemptFailure)

			// Confirm the Callback's terminal failure reason is set.
			termFailure := cb.TerminalFailure.Get(mctx)
			require.Equal(t, "callback execution terminated", termFailure.Message)

			// No new tasks were generated.
			require.Empty(t, mctx.Tasks)
		})
	}

	// Terminal states. Confirm the request should be rejected by CHASM.
	for _, src := range []callbackspb.CallbackStatus{
		callbackspb.CALLBACK_STATUS_SUCCEEDED,
		callbackspb.CALLBACK_STATUS_FAILED,
		callbackspb.CALLBACK_STATUS_TERMINATED,
	} {
		t.Run("from_"+src.String(), func(t *testing.T) {
			mctx := &chasm.MockMutableContext{}

			cb := &Callback{
				CallbackState: proto.Clone(initialCallbackState).(*callbackspb.CallbackState),
			}
			cb.SetStateMachineState(src)

			err := TransitionTerminated.Apply(cb, mctx, EventTerminated{})
			require.ErrorContains(t, err, "invalid transition from")
		})
	}
}

func TestSaveResult_TerminatedWhileInFlight(t *testing.T) {
	// If the callback was terminated while an invocation was in-flight,
	// saveResult should drop the result silently.
	cb := &Callback{
		CallbackState: &callbackspb.CallbackState{
			Status: callbackspb.CALLBACK_STATUS_TERMINATED,
		},
	}
	mctx := &chasm.MockMutableContext{}
	_, err := cb.saveResult(mctx, saveResultInput{
		result:      invocationResultOK{},
		retryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
	})
	require.NoError(t, err)
	require.Equal(t, callbackspb.CALLBACK_STATUS_TERMINATED, cb.StateMachineState())
}
