package callback

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
)

func TestOutcome(t *testing.T) {
	terminalFailure := &failurepb.Failure{Message: "terminal"}
	lastAttemptFailure := &failurepb.Failure{Message: "last attempt"}

	cases := []struct {
		name   string
		status callbackspb.CallbackStatus
		// Set the Callback's state before the assert function is called.
		initCallback func(*chasm.MockMutableContext, *Callback)
		assert       func(*testing.T, *Callback, chasm.Context)
	}{
		{
			name:   "unspecified is non-terminal",
			status: callbackspb.CALLBACK_STATUS_UNSPECIFIED,
			assert: func(t *testing.T, cb *Callback, ctx chasm.Context) {
				require.Nil(t, cb.Outcome(ctx))
			},
		},
		{
			name:   "standby is non-terminal",
			status: callbackspb.CALLBACK_STATUS_STANDBY,
			assert: func(t *testing.T, cb *Callback, ctx chasm.Context) {
				require.Nil(t, cb.Outcome(ctx))
			},
		},
		{
			name:   "scheduled is non-terminal",
			status: callbackspb.CALLBACK_STATUS_SCHEDULED,
			assert: func(t *testing.T, cb *Callback, ctx chasm.Context) {
				require.Nil(t, cb.Outcome(ctx))
			},
		},
		{
			name:   "backing off is non-terminal, even with a last attempt failure",
			status: callbackspb.CALLBACK_STATUS_BACKING_OFF,
			initCallback: func(_ *chasm.MockMutableContext, cb *Callback) {
				cb.LastAttemptFailure = lastAttemptFailure
			},
			assert: func(t *testing.T, cb *Callback, ctx chasm.Context) {
				require.Nil(t, cb.Outcome(ctx))
			},
		},
		{
			name:   "succeeded",
			status: callbackspb.CALLBACK_STATUS_SUCCEEDED,
			assert: func(t *testing.T, cb *Callback, ctx chasm.Context) {
				outcome := cb.Outcome(ctx)
				require.NotNil(t, outcome)
				require.NotNil(t, outcome.GetSuccess())
				require.Nil(t, outcome.GetFailure())
			},
		},
		{
			name:   "failed reports the terminal failure",
			status: callbackspb.CALLBACK_STATUS_FAILED,
			initCallback: func(mctx *chasm.MockMutableContext, cb *Callback) {
				cb.LastAttemptFailure = lastAttemptFailure
				cb.TerminalFailure = chasm.NewDataField(mctx, terminalFailure)
			},
			assert: func(t *testing.T, cb *Callback, ctx chasm.Context) {
				outcome := cb.Outcome(ctx)
				require.NotNil(t, outcome)
				require.Nil(t, outcome.GetSuccess())
				require.Equal(t, terminalFailure, outcome.GetFailure())
			},
		},
		{
			name:   "failed falls back to the last attempt failure when TerminalFailure is unset",
			status: callbackspb.CALLBACK_STATUS_FAILED,
			initCallback: func(_ *chasm.MockMutableContext, cb *Callback) {
				// Mimics a callback persisted before TerminalFailure was introduced.
				cb.LastAttemptFailure = lastAttemptFailure
			},
			assert: func(t *testing.T, cb *Callback, ctx chasm.Context) {
				outcome := cb.Outcome(ctx)
				require.NotNil(t, outcome)
				require.Nil(t, outcome.GetSuccess())
				require.Equal(t, lastAttemptFailure, outcome.GetFailure())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mctx := &chasm.MockMutableContext{}
			cb := &Callback{CallbackState: &callbackspb.CallbackState{}}
			cb.SetStateMachineState(tc.status)
			if tc.initCallback != nil {
				tc.initCallback(mctx, cb)
			}
			tc.assert(t, cb, mctx)
		})
	}
}

// TestOutcomeAfterTransitions verifies Outcome against state produced by the actual transitions,
// rather than hand-built component state.
func TestOutcomeAfterTransitions(t *testing.T) {
	testTime := time.Now().UTC()
	errFailedDelivery := errors.New("failed to deliver callback")

	newScheduledCallback := func() *Callback {
		cb := &Callback{
			CallbackState: &callbackspb.CallbackState{
				Callback: &callbackspb.Callback{
					Variant: &callbackspb.Callback_Nexus_{
						Nexus: &callbackspb.Callback_Nexus{
							Url: "http://address:8888/path/to/callback",
						},
					},
				},
			},
		}
		cb.SetStateMachineState(callbackspb.CALLBACK_STATUS_SCHEDULED)
		return cb
	}

	t.Run("succeeded", func(t *testing.T) {
		mctx := &chasm.MockMutableContext{}
		cb := newScheduledCallback()
		require.Nil(t, cb.Outcome(mctx))

		require.NoError(t, TransitionSucceeded.Apply(cb, mctx, EventSucceeded{Time: testTime}))

		outcome := cb.Outcome(mctx)
		require.NotNil(t, outcome)
		require.NotNil(t, outcome.GetSuccess())
	})

	t.Run("failed", func(t *testing.T) {
		mctx := &chasm.MockMutableContext{}
		cb := newScheduledCallback()
		require.Nil(t, cb.Outcome(mctx))

		require.NoError(t, TransitionFailed.Apply(cb, mctx, EventFailed{
			Time: testTime,
			Err:  errFailedDelivery,
		}))

		outcome := cb.Outcome(mctx)
		require.NotNil(t, outcome)
		require.Nil(t, outcome.GetSuccess())
		require.Equal(t, errFailedDelivery.Error(), outcome.GetFailure().GetMessage())
		require.True(t, outcome.GetFailure().GetApplicationFailureInfo().GetNonRetryable())
	})

	t.Run("attempt failed is not terminal", func(t *testing.T) {
		mctx := &chasm.MockMutableContext{}
		cb := newScheduledCallback()

		require.NoError(t, TransitionAttemptFailed.Apply(cb, mctx, EventAttemptFailed{
			Time:        testTime,
			Err:         errFailedDelivery,
			RetryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
		}))

		require.Equal(t, callbackspb.CALLBACK_STATUS_BACKING_OFF, cb.StateMachineState())
		require.Nil(t, cb.Outcome(mctx))
	})
}
