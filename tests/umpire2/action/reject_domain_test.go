package action

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	umpirefw "go.temporal.io/server/common/testing/umpire"
)

func TestReflectStartParamsIncludesEnumAndPayloadDomains(t *testing.T) {
	params := reflectStartParams(&workflowservice.StartNexusOperationExecutionRequest{})
	byPath := make(map[string]umpirefw.Domain, len(params))
	for _, param := range params {
		byPath[param.Path] = param.Domain
	}

	require.IsType(t, &umpirefw.EnumDomain{}, byPath["id_reuse_policy"])
	require.IsType(t, &umpirefw.EnumDomain{}, byPath["id_conflict_policy"])
	require.IsType(t, &umpirefw.PayloadDomain{}, byPath["input"])
}

func TestProtoValueSupportsReflectedEnumAndPayloadMutations(t *testing.T) {
	request := &workflowservice.StartNexusOperationExecutionRequest{Input: &commonpb.Payload{Data: []byte("input")}}
	message := request.ProtoReflect()
	enumField := message.Descriptor().Fields().ByName("id_reuse_policy")
	payloadField := message.Descriptor().Fields().ByName("input")

	message.Set(enumField, protoValue(enumField, int32(1000)))
	message.Set(payloadField, protoValue(payloadField, &commonpb.Payload{Data: []byte("changed")}))

	require.EqualValues(t, 1000, request.GetIdReusePolicy())
	require.Equal(t, []byte("changed"), request.GetInput().GetData())
}
