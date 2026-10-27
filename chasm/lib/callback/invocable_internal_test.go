package callback

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"google.golang.org/protobuf/proto"
)

func TestGetHistoryRequest_SuccessPayload(t *testing.T) {
	value := &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("binary/custom"), "key-id": []byte("k")},
		Data:     []byte("v"),
	}
	cases := []struct {
		name   string
		result any
		want   *commonpb.Payload
	}{
		{name: "nil", result: nil, want: commonnexus.NilResultPayload()},
		{name: "typed-nil", result: (*commonpb.Payload)(nil), want: commonnexus.NilResultPayload()},
		{name: "value", result: value, want: value},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inv := invocableInternal{completion: nexusrpc.CompleteOperationOptions{Result: tc.result}}
			req, err := inv.getHistoryRequest(nil, "request-id")
			require.NoError(t, err)

			// Round-trip through the wire, where a nil oneof message becomes an empty one.
			b, err := proto.Marshal(req)
			require.NoError(t, err)
			got := &historyservice.CompleteNexusOperationChasmRequest{}
			require.NoError(t, proto.Unmarshal(b, got))
			require.True(t, proto.Equal(tc.want, got.GetSuccess()), "got %v", got.GetSuccess())
		})
	}
}
