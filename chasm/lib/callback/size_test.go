package callback

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
)

// We run validation against the commonpb.Callback proto, but that is converted into
// a nearly identical callbackspb.Callback proto. Verify the sizes agree, and that
// the CHASM conversion doesn't include any unexpected data.
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

			// Convert the user-supplied commonpb.Callback into the CHASM callbackspb.Callback.
			chasmCB, err := FromAPICallback(tc.cb)
			require.NoError(t, err)

			require.Equal(t, tc.cb.Size(), chasmCB.Size(),
				"API and CHASM callback protos must serialize to the same size")
		})
	}
}
