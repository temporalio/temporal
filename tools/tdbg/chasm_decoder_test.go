package tdbg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDecodeNode_DecodesCallbackState(t *testing.T) {
	registry, err := newChasmRegistry(log.NewNoopLogger())
	require.NoError(t, err)

	blob, err := serialization.Encode(&callbackspb.CallbackState{
		RequestId:        "request-id",
		RegistrationTime: timestamppb.New(time.Now().UTC()),
	})
	require.NoError(t, err)

	node, err := decodeNode(&persistencespb.ChasmNode{
		Metadata: &persistencespb.ChasmNodeMetadata{
			Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
				ComponentAttributes: &persistencespb.ChasmComponentAttributes{
					TypeId:   chasm.CallbackComponentID,
					Detached: true,
				},
			},
		},
		Data: blob,
	}, registry)
	require.NoError(t, err)
	require.Equal(t, "component", node.NodeType)
	require.NotNil(t, node.DecodedData)
	require.Contains(t, string(node.DecodedData), "\"requestId\":\"request-id\"")
	require.Nil(t, node.RawData)
}
