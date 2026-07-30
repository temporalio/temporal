package callback

import (
	"testing"

	"github.com/stretchr/testify/require"
	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/testing/protorequire"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
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

// apiCallbackVariants covers every variant temporal.api.common.v1.Callback defines, so that a variant
// added to the API surface shows up here as a case that has to be classified rather than silently
// falling into the unsupported bucket.
var apiCallbackVariants = map[string]struct {
	callback *commonpb.Callback
	// Whether CHASM can persist this variant. Persistable variants must round trip.
	persistable bool
}{
	"nexus": {
		callback: &commonpb.Callback{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url:    "http://localhost:8080/cb",
					Header: map[string]string{"key": "value"},
				},
			},
		},
		persistable: true,
	},
	"worker": {
		callback: &commonpb.Callback{
			Variant: &commonpb.Callback_Worker_{
				Worker: &commonpb.Callback_Worker{
					TaskQueueName: "task-queue",
					Service:       "HTTPAdapter",
					Operation:     "DeliverAsWebhook",
					SourceContext: &commonpb.Payload{Data: []byte("ctx")},
				},
			},
		},
		persistable: true,
	},
	"internal": {
		callback: &commonpb.Callback{
			Variant: &commonpb.Callback_Internal_{Internal: &commonpb.Callback_Internal{}},
		},
	},
	"unset": {callback: &commonpb.Callback{}},
}

func TestFromAPICallback(t *testing.T) {
	for name, tc := range apiCallbackVariants {
		t.Run(name, func(t *testing.T) {
			got, err := FromAPICallback(tc.callback)
			if !tc.persistable {
				var invalidArgErr *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalidArgErr)
				require.Contains(t, err.Error(), "unsupported callback variant")
				return
			}
			require.NoError(t, err)

			// Round tripping is what keeps the forked callbackspb.Callback honest against the API type.
			roundTripped, err := (&Callback{
				CallbackState: &callbackspb.CallbackState{Callback: got},
			}).ToAPICallback()
			require.NoError(t, err)
			protorequire.ProtoEqual(t, tc.callback, roundTripped)
		})
	}

	t.Run("carries links independently of the variant", func(t *testing.T) {
		links := []*commonpb.Link{{Variant: &commonpb.Link_WorkflowEvent_{
			WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns", WorkflowId: "wf-id"},
		}}}
		got, err := FromAPICallback(&commonpb.Callback{
			Links:   links,
			Variant: &commonpb.Callback_Worker_{Worker: &commonpb.Callback_Worker{Service: "svc"}},
		})
		require.NoError(t, err)
		require.Len(t, got.GetLinks(), 1)
	})

	t.Run("does not alias the request's maps, slices or payloads", func(t *testing.T) {
		req := &commonpb.Callback{
			Links: []*commonpb.Link{{}},
			Variant: &commonpb.Callback_Worker_{
				Worker: &commonpb.Callback_Worker{SourceContext: &commonpb.Payload{Data: []byte("ctx")}},
			},
		}
		got, err := FromAPICallback(req)
		require.NoError(t, err)

		got.Links[0] = nil
		got.GetWorker().GetSourceContext().Data = []byte("mutated")

		require.NotNil(t, req.GetLinks()[0])
		reqWorker := req.GetVariant().(*commonpb.Callback_Worker_).Worker
		require.Equal(t, []byte("ctx"), reqWorker.GetSourceContext().GetData())
	})
}

// TestToAPICallbackUnsupportedVariant covers the reverse direction's fallthrough. A persisted variant
// that cannot be projected onto the API is a server bug, not a bad request, so it reports Internal.
func TestToAPICallbackUnsupportedVariant(t *testing.T) {
	cb := &Callback{CallbackState: &callbackspb.CallbackState{
		Callback: &callbackspb.Callback{},
	}}
	_, err := cb.ToAPICallback()
	var internalErr *serviceerror.Internal
	require.ErrorAs(t, err, &internalErr)
	require.Contains(t, err.Error(), "unsupported CHASM callback type")
}

// TestLoadInvocationArgsRejectsWorkerVariant pins the current lack of support: a Worker callback can be
// persisted and described, but invoking one fails the task as unprocessable rather than retrying.
func TestLoadInvocationArgsRejectsWorkerVariant(t *testing.T) {
	chasmCB, err := FromAPICallback(&commonpb.Callback{
		Variant: &commonpb.Callback_Worker_{
			Worker: &commonpb.Callback_Worker{TaskQueueName: "task-queue", Service: "svc", Operation: "op"},
		},
	})
	require.NoError(t, err)

	// The variant is rejected before CompletionSource is resolved, so an unset parent pointer is fine
	// here: reaching it would panic and fail this test.
	cb := &Callback{CallbackState: &callbackspb.CallbackState{Callback: chasmCB}}

	_, err = cb.loadInvocationArgs(&chasm.MockMutableContext{}, nil)
	var unprocessableErr *queueserrors.UnprocessableTaskError
	require.ErrorAs(t, err, &unprocessableErr)
	require.Contains(t, err.Error(), "unprocessable callback variant")
	require.Contains(t, err.Error(), "Callback_Worker_")
}
