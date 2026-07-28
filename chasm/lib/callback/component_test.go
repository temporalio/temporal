package callback

import (
	"testing"

	callbackpb "go.temporal.io/api/callback/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestOutcome(t *testing.T) {
	terminalFailure := &failurepb.Failure{Message: "terminal"}
	lastAttemptFailure := &failurepb.Failure{Message: "last attempt"}

	cases := []struct {
		name   string
		status callbackspb.CallbackStatus
		// Set the Callback's state before Outcome is called.
		initCallback func(*chasm.MockMutableContext, *Callback)
		expected     *callbackpb.CallbackOutcome
	}{
		{
			name:   "unspecified is non-terminal",
			status: callbackspb.CALLBACK_STATUS_UNSPECIFIED,
		},
		{
			name:   "standby is non-terminal",
			status: callbackspb.CALLBACK_STATUS_STANDBY,
		},
		{
			name:   "scheduled is non-terminal",
			status: callbackspb.CALLBACK_STATUS_SCHEDULED,
		},
		{
			name:   "backing off is non-terminal, even with a last attempt failure",
			status: callbackspb.CALLBACK_STATUS_BACKING_OFF,
			initCallback: func(_ *chasm.MockMutableContext, cb *Callback) {
				cb.LastAttemptFailure = lastAttemptFailure
			},
		},
		{
			name:   "succeeded",
			status: callbackspb.CALLBACK_STATUS_SUCCEEDED,
			expected: &callbackpb.CallbackOutcome{
				Value: &callbackpb.CallbackOutcome_Success{Success: &emptypb.Empty{}},
			},
		},
		{
			name:   "failed reports the terminal failure",
			status: callbackspb.CALLBACK_STATUS_FAILED,
			initCallback: func(mctx *chasm.MockMutableContext, cb *Callback) {
				cb.LastAttemptFailure = lastAttemptFailure
				cb.TerminalFailure = chasm.NewDataField(mctx, terminalFailure)
			},
			expected: &callbackpb.CallbackOutcome{
				Value: &callbackpb.CallbackOutcome_Failure{Failure: terminalFailure},
			},
		},
		{
			name:   "failed falls back to the last attempt failure when TerminalFailure is unset",
			status: callbackspb.CALLBACK_STATUS_FAILED,
			initCallback: func(_ *chasm.MockMutableContext, cb *Callback) {
				// Mimics a callback persisted before TerminalFailure was introduced.
				cb.LastAttemptFailure = lastAttemptFailure
			},
			expected: &callbackpb.CallbackOutcome{
				Value: &callbackpb.CallbackOutcome_Failure{Failure: lastAttemptFailure},
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
			protorequire.ProtoEqual(t, tc.expected, cb.Outcome(mctx))
		})
	}
}
