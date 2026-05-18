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

		require.Nil(t, callback.CloseTime)
		_, ok := callback.TerminalFailure.TryGet(mctx)
		require.False(t, ok)

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
		require.Nil(t, callback.CloseTime)
		require.Nil(t, callback.NextAttemptScheduleTime)
		_, ok := callback.TerminalFailure.TryGet(mctx)
		require.False(t, ok)

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
		require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
		require.Nil(t, callback.NextAttemptScheduleTime)
		require.Equal(t, currentTime, callback.CloseTime.AsTime())

		// The LastAttemptFailure and related data remain unchanged.
		lastFailure := callback.LastAttemptFailure
		require.NotNil(t, lastFailure)
		require.Equal(t, "error message", lastFailure.Message)
		require.False(t, lastFailure.GetApplicationFailureInfo().NonRetryable)

		_, ok := callback.TerminalFailure.TryGet(mctx)
		require.False(t, ok)

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

		err := TransitionFailed.Apply(callback, mctx, EventFailed{Time: currentTime, Err: errors.New("new failure msg")})
		require.NoError(t, err)

		// Assert info object is updated.
		require.Equal(t, callbackspb.CALLBACK_STATUS_FAILED, callback.StateMachineState())
		require.Equal(t, int32(2), callback.Attempt)
		require.Equal(t, currentTime, callback.LastAttemptCompleteTime.AsTime())
		require.Nil(t, callback.NextAttemptScheduleTime)
		require.Equal(t, currentTime, callback.CloseTime.AsTime())

		// Assert LastAttemptFailure is the latest, and final failure.
		lastFailure := callback.LastAttemptFailure
		require.Equal(t, "new failure msg", lastFailure.Message)
		require.True(t, lastFailure.GetApplicationFailureInfo().NonRetryable)

		// Assert the TerminalFailure field is not set.
		// (It will only have a value on externa failures, like timeouts.)
		_, ok := callback.TerminalFailure.TryGet(mctx)
		require.False(t, ok)

		// Assert no tasks generated. In terminal state.
		require.Empty(t, mctx.Tasks)
	})
}

func TestTerminatedTransition(t *testing.T) {
	initialCallbackState := &callbackspb.CallbackState{
		Status:           callbackspb.CALLBACK_STATUS_SCHEDULED,
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

	emptyTerminateEvent := EventTerminated{}
	assertEmptyEventResults := func(t *testing.T, mctx *chasm.MockMutableContext, cb *Callback) {
		// Confirm default reason, but no additional metadata.
		require.Equal(t, callbackspb.CALLBACK_STATUS_TERMINATED, cb.GetStatus())
		termFailure := cb.TerminalFailure.Get(mctx)
		require.Equal(t, "callback execution terminated", termFailure.Message)
		require.Nil(t, termFailure.GetTerminatedFailureInfo())
	}

	populatedTerminateEvent := EventTerminated{
		Identity: "user-supplied identity",
		Reason:   "user-supplied reason",
	}
	assertPopulatedEventResults := func(t *testing.T, mctx *chasm.MockMutableContext, cb *Callback) {
		// Confirm user-supplied reason and identity are available.
		require.Equal(t, callbackspb.CALLBACK_STATUS_TERMINATED, cb.GetStatus())
		termFailure := cb.TerminalFailure.Get(mctx)
		require.Equal(t, "user-supplied reason", termFailure.Message)
		gotTermFailureInfo := termFailure.GetTerminatedFailureInfo()
		require.NotNil(t, gotTermFailureInfo)
		require.Equal(t, "user-supplied identity", gotTermFailureInfo.Identity)
	}

	tests := []struct {
		Name       string
		FromStatus callbackspb.CallbackStatus
		Event      EventTerminated
		Prepare    func(*Callback)
		Assert     func(*testing.T, *chasm.MockMutableContext, *Callback)
	}{
		// Transitions with no Reason/Identity supplied for termination.
		{
			Name:       "in standby",
			FromStatus: callbackspb.CALLBACK_STATUS_STANDBY,
			Event:      emptyTerminateEvent,
			Assert:     assertEmptyEventResults,
		},
		{
			Name:       "in scheduled, with identity supplied",
			FromStatus: callbackspb.CALLBACK_STATUS_SCHEDULED,
			Event:      emptyTerminateEvent,
			Assert:     assertEmptyEventResults,
		},
		{
			Name:       "in backing off, with identity supplied",
			FromStatus: callbackspb.CALLBACK_STATUS_BACKING_OFF,
			Event:      emptyTerminateEvent,
			Assert:     assertEmptyEventResults,
		},

		// With the Reason/Identity supplied.
		{
			Name:       "in standby, with identity supplied",
			FromStatus: callbackspb.CALLBACK_STATUS_STANDBY,
			Event:      populatedTerminateEvent,
			Assert:     assertPopulatedEventResults,
		},
		{
			Name:       "in scheduled, with identity supplied",
			FromStatus: callbackspb.CALLBACK_STATUS_SCHEDULED,
			Event:      populatedTerminateEvent,
			Assert:     assertPopulatedEventResults,
		},
		{
			Name:       "in backing off, with identity supplied",
			FromStatus: callbackspb.CALLBACK_STATUS_BACKING_OFF,
			Event:      populatedTerminateEvent,
			Assert:     assertPopulatedEventResults,
		},
	}
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			currentTime := time.Now().UTC()
			mctx := &chasm.MockMutableContext{}
			mctx.HandleNow = func(chasm.Component) time.Time { return currentTime }

			// Configure
			cb := &Callback{
				CallbackState: proto.Clone(initialCallbackState).(*callbackspb.CallbackState),
			}
			cb.SetStateMachineState(test.FromStatus)

			// Call
			err := TransitionTerminated.Apply(cb, mctx, test.Event)
			require.NoError(t, err)

			// Confirm expected state changes.
			require.Equal(t, callbackspb.CALLBACK_STATUS_TERMINATED, cb.StateMachineState())
			require.Equal(t, currentTime, cb.GetCloseTime().AsTime())
			// Other fields remain the same.
			require.True(t, proto.Equal(initialCallbackState.Callback, cb.Callback))
			require.True(t, proto.Equal(initialCallbackState.RegistrationTime, cb.RegistrationTime))
			require.Equal(t, initialCallback.LastAttemptFailure, cb.LastAttemptFailure)

			// No new tasks were generated.
			require.Empty(t, mctx.Tasks)

			test.Assert(t, mctx, cb)
		})
	}
}
