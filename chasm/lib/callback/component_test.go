package callback

import (
	"testing"

	"github.com/stretchr/testify/require"
	callbackpb "go.temporal.io/api/callback/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/protoutils"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestAPIState(t *testing.T) {
	cases := []struct {
		name    string
		status  callbackspb.CallbackStatus
		want    enumspb.CallbackState
		wantErr bool
	}{
		{
			name:    "unspecified is an error",
			status:  callbackspb.CALLBACK_STATUS_UNSPECIFIED,
			want:    enumspb.CALLBACK_STATE_UNSPECIFIED,
			wantErr: true,
		},
		{
			name:   "standby",
			status: callbackspb.CALLBACK_STATUS_STANDBY,
			want:   enumspb.CALLBACK_STATE_STANDBY,
		},
		{
			name:   "scheduled",
			status: callbackspb.CALLBACK_STATUS_SCHEDULED,
			want:   enumspb.CALLBACK_STATE_SCHEDULED,
		},
		{
			name:   "backing off",
			status: callbackspb.CALLBACK_STATUS_BACKING_OFF,
			want:   enumspb.CALLBACK_STATE_BACKING_OFF,
		},
		{
			name:   "failed",
			status: callbackspb.CALLBACK_STATUS_FAILED,
			want:   enumspb.CALLBACK_STATE_FAILED,
		},
		{
			name:   "succeeded",
			status: callbackspb.CALLBACK_STATUS_SUCCEEDED,
			want:   enumspb.CALLBACK_STATE_SUCCEEDED,
		},
	}

	// Guards against a new CallbackStatus being added without a corresponding API mapping.
	covered := make([]callbackspb.CallbackStatus, 0, len(cases))
	for _, tc := range cases {
		covered = append(covered, tc.status)
		t.Run(tc.name, func(t *testing.T) {
			cb := &Callback{CallbackState: &callbackspb.CallbackState{}}
			cb.SetStateMachineState(tc.status)

			state, err := cb.APIState()
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, state)
		})
	}
	require.ElementsMatch(t, protoutils.EnumValues[callbackspb.CallbackStatus](), covered)
}

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
