package callback

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
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

// Asserts that CHASM Callbacks do not support the new Worker callback variant.
func TestWorkerCallbacksNotSupported(t *testing.T) {
	apiCb := &commonpb.Callback{
		Variant: &commonpb.Callback_Worker_{
			Worker: &commonpb.Callback_Worker{},
		},
	}
	chasmCB, err := FromAPICallback(apiCb)
	require.NoError(t, err)

	cb := &Callback{
		CallbackState: &callbackspb.CallbackState{
			Callback: chasmCB,
		},
	}
	_, err = cb.loadInvocationArgs(&chasm.MockMutableContext{}, nil)

	var unprocessableErr *queueserrors.UnprocessableTaskError
	require.ErrorAs(t, err, &unprocessableErr)
	require.ErrorContains(t, err, "unprocessable callback variant")
	require.ErrorContains(t, err, "Callback_Worker_")
}
