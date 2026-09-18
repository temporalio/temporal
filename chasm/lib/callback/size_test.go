package callback

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
)

// callbackspb.Callback is a field-number-identical fork of commonpb.Callback, so the two
// serialize to the same number of bytes. The aggregate size limit relies on this: the limit is
// enforced against the API proto (which is what callers supply and what
// callbacks.Validator.ValidateAdditions measures), but the bytes actually persisted are the
// CHASM proto. If the two ever diverge, the enforced budget stops matching the stored one.
func TestAPIAndChasmCallbackSizesAgree(t *testing.T) {
	testCases := []struct {
		name string
		cb   *commonpb.Callback
	}{
		{
			name: "nexus minimal",
			cb: &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/cb"}}},
		},
		{
			name: "nexus with headers",
			cb: &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url:    "https://nexus.example.cluster.tmprl.cloud:7243/namespaces/ex/nexus/callback",
					Header: map[string]string{"content-type": "application/json", "x-custom": "value"},
				}}},
		},
		{
			name: "nexus handler with large source context",
			cb: &commonpb.Callback{Variant: &commonpb.Callback_NexusHandler_{
				NexusHandler: &commonpb.Callback_NexusHandler{
					TaskQueueName: "wc-queue",
					Service:       "CompletionService",
					Operation:     "DeliverAsWebhook",
					SourceContext: &commonpb.Payload{
						Metadata: map[string][]byte{"encoding": []byte("json/plain")},
						Data:     make([]byte, 4096),
					},
				}}},
		},
		{
			name: "with embedded links",
			cb: &commonpb.Callback{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/cb"}},
				Links: []*commonpb.Link{{Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: &commonpb.Link_WorkflowEvent{
						Namespace:  "ns",
						WorkflowId: "wid",
						RunId:      "3f1c1b0e-0000-4000-8000-000000000000",
					}}}},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			chasmCB, err := FromAPICallback(tc.cb)
			require.NoError(t, err)
			require.Equal(t, tc.cb.Size(), chasmCB.Size(),
				"API and CHASM callback protos must serialize to the same size")
		})
	}
}
