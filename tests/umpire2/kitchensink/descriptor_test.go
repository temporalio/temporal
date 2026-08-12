package kitchensink

import (
	"testing"

	"github.com/stretchr/testify/require"
	v1kitchensink "go.temporal.io/server/tests/umpirev1/kitchensink"
	"google.golang.org/protobuf/proto"
)

func TestDescriptorUsesUmpire2Namespace(t *testing.T) {
	require.Equal(t, "umpire2/kitchen_sink.proto", File_kitchen_sink_proto.Path())
	require.Equal(t, "temporal.omes.umpire2.kitchen_sink", string(File_kitchen_sink_proto.Package()))
}

func TestDescriptorPreservesKitchenSinkWireSchema(t *testing.T) {
	v1Payload, err := proto.Marshal(&v1kitchensink.ExecuteNexusOperation{
		Endpoint:  "endpoint",
		Operation: "operation",
	})
	require.NoError(t, err)
	v2Payload, err := proto.Marshal(&ExecuteNexusOperation{
		Endpoint:  "endpoint",
		Operation: "operation",
	})
	require.NoError(t, err)

	require.Equal(t, v1Payload, v2Payload)
}
