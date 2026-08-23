package callback

import (
	"testing"

	"github.com/stretchr/testify/require"
	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestFromAPICallback(t *testing.T) {
	// Set of API Callback variants to test.
	apiCallbackVariants := map[string]struct {
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
						TaskQueueName: "completions-task-queue",
						Service:       "HTTPAdapter",
						Operation:     "DeliverAsWebhook",
						SourceContext: &commonpb.Payload{Data: []byte("...")},
					},
				},
			},
			persistable: true,
		},
		"internal": {
			callback: &commonpb.Callback{
				Variant: &commonpb.Callback_Internal_{Internal: &commonpb.Callback_Internal{}},
			},
			// Not defined in the CHASM callback proto.
			persistable: false,
		},
		"unset": {callback: &commonpb.Callback{}},
	}

	t.Run("RoundTripped", func(t *testing.T) {
		for name, tc := range apiCallbackVariants {
			t.Run(name, func(t *testing.T) {
				got, err := FromAPICallback(tc.callback)

				// Error case, for invalid or unknown API callbacks.
				if !tc.persistable {
					var invalidArgErr *serviceerror.InvalidArgument
					require.ErrorAs(t, err, &invalidArgErr)
					require.ErrorContains(t, err, "unsupported callback variant")
					return
				}
				require.NoError(t, err)

				// Verify round-tripping the proto produces the same result.
				chasmComponent := &Callback{
					CallbackState: &callbackspb.CallbackState{
						Callback: got,
					},
				}
				roundTripped, err := chasmComponent.ToAPICallback()
				require.NoError(t, err)
				protorequire.ProtoEqual(t, tc.callback, roundTripped)
			})
		}
	})

	t.Run("LinksPersistedForVariants", func(t *testing.T) {
		links := []*commonpb.Link{
			{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "ns", WorkflowId: "wf-id"},
				},
			},
			{
				Variant: &commonpb.Link_Callback_{
					Callback: &commonpb.Link_Callback{
						Execution: &commonpb.Execution{
							Type:       enumspb.EXECUTION_TYPE_NEXUS_OPERATION,
							BusinessId: "nexus-operation-id",
							RunId:      "run-id",
						},
						RequestId: "request-id",
					},
				},
			},
		}

		for name, tc := range apiCallbackVariants {
			// This test only applies to callbacks that can be converted.
			if !tc.persistable {
				continue
			}

			t.Run(name, func(t *testing.T) {
				cbWithLinks := common.CloneProto(tc.callback)
				cbWithLinks.Links = links

				got, err := FromAPICallback(cbWithLinks)
				require.NoError(t, err)

				// Verify links were converted in the process.
				gotLinks := got.GetLinks()
				require.Len(t, gotLinks, 2)
				require.NotNil(t, gotLinks[0].GetWorkflowEvent())
				require.NotNil(t, gotLinks[1].GetCallback())

				// Verify that a deep copy was used. (Different references.)
				require.NotSame(t, links[0], gotLinks[0])
				require.NotSame(t, links[1], gotLinks[1])
			})
		}
	})
}

// Test the error case when the CHASM callback has a bogus value.
// The positive cases are covered in the round-tripping scenarios
// in TestFromAPICallback.
func TestToAPICallbackUnsupportedVariant(t *testing.T) {
	cb := &Callback{CallbackState: &callbackspb.CallbackState{
		Callback: &callbackspb.Callback{},
	}}
	_, err := cb.ToAPICallback()
	var internalErr *serviceerror.Internal
	require.ErrorAs(t, err, &internalErr)
	require.ErrorContains(t, err, "unsupported CHASM callback type")
}

// A callback whose variant this server doesn't know how to invoke can still be persisted (by a server that
// does, or by a future version), so its invocation task has to be rejected rather than crash.
func TestLoadInvocationArgsUnsupportedVariant(t *testing.T) {
	cb := &Callback{
		CallbackState: &callbackspb.CallbackState{
			Callback: &callbackspb.Callback{},
		},
	}
	_, err := cb.loadInvocationArgs(&chasm.MockMutableContext{}, nil)

	var unprocessableErr *queueserrors.UnprocessableTaskError
	require.ErrorAs(t, err, &unprocessableErr)
	require.ErrorContains(t, err, "unprocessable callback variant")
}

// Verify the setResult method sets the "result" field based on the Callback state.
func TestSetResult(t *testing.T) {
	lastAttemptFailure := &failurepb.Failure{Message: "last attempt"}

	cases := []struct {
		name string

		// Callback state to set.
		status             callbackspb.CallbackStatus
		lastAttemptFailure *failurepb.Failure

		// The CallbackInfo expected after setResult.
		want *callbackpb.CallbackInfo
	}{
		{
			name:   "unspecified is non-terminal",
			status: callbackspb.CALLBACK_STATUS_UNSPECIFIED,
			want:   &callbackpb.CallbackInfo{},
		},
		{
			name:   "standby is non-terminal",
			status: callbackspb.CALLBACK_STATUS_STANDBY,
			want:   &callbackpb.CallbackInfo{},
		},
		{
			name:   "scheduled is non-terminal",
			status: callbackspb.CALLBACK_STATUS_SCHEDULED,
			want:   &callbackpb.CallbackInfo{},
		},
		{
			name:               "backing off is non-terminal, even with a last attempt failure",
			status:             callbackspb.CALLBACK_STATUS_BACKING_OFF,
			lastAttemptFailure: lastAttemptFailure,
			want:               &callbackpb.CallbackInfo{},
		},
		{
			name:   "succeeded",
			status: callbackspb.CALLBACK_STATUS_SUCCEEDED,
			want: &callbackpb.CallbackInfo{
				Result: &callbackpb.CallbackInfo_Success{Success: &emptypb.Empty{}},
			},
		},
		{
			name:               "failed reports the terminal failure",
			status:             callbackspb.CALLBACK_STATUS_FAILED,
			lastAttemptFailure: lastAttemptFailure,
			want: &callbackpb.CallbackInfo{
				Result: &callbackpb.CallbackInfo_Failure{Failure: lastAttemptFailure},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cb := &Callback{
				CallbackState: &callbackspb.CallbackState{
					Status:             tc.status,
					LastAttemptFailure: tc.lastAttemptFailure,
				},
			}

			var cbInfo callbackpb.CallbackInfo
			cb.setResult(&cbInfo)

			protorequire.ProtoEqual(t, tc.want, &cbInfo)
			if gotFailure := cbInfo.GetFailure(); gotFailure != nil {
				require.NotSame(t, tc.lastAttemptFailure, gotFailure)
			}
		})
	}
}
