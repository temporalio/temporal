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
		name string

		// Callback state to set.
		status             callbackspb.CallbackStatus
		lastAttemptFailure *failurepb.Failure
		terminalFailure    *failurepb.Failure

		want *callbackpb.CallbackOutcome
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
			name:               "backing off is non-terminal, even with a last attempt failure",
			status:             callbackspb.CALLBACK_STATUS_BACKING_OFF,
			lastAttemptFailure: lastAttemptFailure,
		},
		{
			name:   "succeeded",
			status: callbackspb.CALLBACK_STATUS_SUCCEEDED,
			want: &callbackpb.CallbackOutcome{
				Value: &callbackpb.CallbackOutcome_Success{Success: &emptypb.Empty{}},
			},
		},
		{
			name:               "failed reports the terminal failure",
			status:             callbackspb.CALLBACK_STATUS_FAILED,
			lastAttemptFailure: lastAttemptFailure,
			terminalFailure:    terminalFailure,
			want: &callbackpb.CallbackOutcome{
				Value: &callbackpb.CallbackOutcome_Failure{Failure: terminalFailure},
			},
		},
		{
			name:               "failed falls back to the last attempt failure when TerminalFailure is unset",
			status:             callbackspb.CALLBACK_STATUS_FAILED,
			lastAttemptFailure: lastAttemptFailure,
			terminalFailure:    nil,
			want: &callbackpb.CallbackOutcome{
				Value: &callbackpb.CallbackOutcome_Failure{Failure: lastAttemptFailure},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mctx := &chasm.MockMutableContext{}
			cb := &Callback{
				CallbackState: &callbackspb.CallbackState{
					LastAttemptFailure: tc.lastAttemptFailure,
				},
			}
			cb.SetStateMachineState(tc.status)
			if tc.terminalFailure != nil {
				cb.TerminalFailure = chasm.NewDataField(mctx, tc.terminalFailure)
			}

			protorequire.ProtoEqual(t, tc.want, cb.Outcome(mctx))
		})
	}
}
