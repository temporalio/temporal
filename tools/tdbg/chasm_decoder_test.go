package tdbg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	activitylib "go.temporal.io/server/chasm/lib/activity"
	activitypb "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
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

func TestDecodeNode_DecodesActivityState(t *testing.T) {
	registry, err := newChasmRegistry(log.NewNoopLogger())
	require.NoError(t, err)

	blob, err := serialization.Encode(&activitylib.Activity{
		ActivityState: &activitypb.ActivityState{
			Status: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		},
	})
	require.NoError(t, err)

	node, err := decodeNode(&persistencespb.ChasmNode{
		Metadata: &persistencespb.ChasmNodeMetadata{
			Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
				ComponentAttributes: &persistencespb.ChasmComponentAttributes{
					TypeId:   activitylib.ArchetypeID,
					Detached: true,
				},
			},
		},
		Data: blob,
	}, registry)
	require.NoError(t, err)
	require.Equal(t, "component", node.NodeType)
	require.NotNil(t, node.DecodedData)
	require.Contains(t, string(node.DecodedData), "\"status\":\"ACTIVITY_EXECUTION_STATUS_SCHEDULED\"")
	require.Nil(t, node.RawData)
}
