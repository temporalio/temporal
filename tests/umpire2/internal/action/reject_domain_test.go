package action

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/protorequire"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestReflectStartParamsIncludesEnumAndPayloadDomains(t *testing.T) {
	params := reflectStartParams(&workflowservice.StartNexusOperationExecutionRequest{})
	byPath := make(map[string]umpirefw.Domain, len(params))
	for _, param := range params {
		byPath[param.Path] = param.Domain
	}

	require.IsType(t, &umpirefw.ValidatorDomain{}, byPath["id_reuse_policy"])
	require.IsType(t, &umpirefw.ValidatorDomain{}, byPath["id_conflict_policy"])
	require.IsType(t, &umpirefw.ValidatorDomain{}, byPath["input"])
	require.IsType(t, &umpirefw.UnsupportedDomain{}, byPath["search_attributes"])
}

func TestPayloadDomainMutationsAndNormalizationDoNotExposeRawData(t *testing.T) {
	domain := newPayloadDomain(8)
	payload := &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("json/plain")},
		Data:     []byte("secret"),
	}
	variants := domain.Variants()
	require.Len(t, variants, 3)
	for _, variant := range variants {
		mutated, ok := variant.Mutate(payload).(*commonpb.Payload)
		require.True(t, ok)
		require.NotSame(t, payload, mutated)
	}

	normalized, err := domain.Normalize(payload)
	require.NoError(t, err)
	require.NotContains(t, normalized, "secret")
	require.Contains(t, normalized, "sha256:")
	_, err = domain.Normalize(&commonpb.Payload{Data: []byte("123456789")})
	require.ErrorIs(t, err, umpirefw.ErrDomainValue)
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

func TestStartValidatorDomainsUseServerValidationAndCloneMutableValues(t *testing.T) {
	params := reflectStartParams(&workflowservice.StartNexusOperationExecutionRequest{})
	byPath := make(map[string]umpirefw.Domain, len(params))
	for _, param := range params {
		byPath[param.Path] = param.Domain
	}

	duration := durationpb.New(200 * 365 * 24 * time.Hour)
	normalized, err := byPath["schedule_to_close_timeout"].(umpirefw.NormalizingDomain).Normalize(duration)
	require.NoError(t, err)
	require.Contains(t, normalized, "duration:sha256:")
	protorequire.ProtoEqual(t, durationpb.New(200*365*24*time.Hour), duration)
	_, err = byPath["schedule_to_close_timeout"].(umpirefw.NormalizingDomain).Normalize(durationpb.New(-time.Second))
	require.ErrorIs(t, err, umpirefw.ErrDomainValue)

	payload := payloads.MustEncodeSingle("value")
	normalized, err = byPath["input"].(umpirefw.NormalizingDomain).Normalize(payload)
	require.NoError(t, err)
	require.Contains(t, normalized, "payload:sha256:")
	_, err = byPath["input"].(umpirefw.NormalizingDomain).Normalize(&commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("unknown")},
	})
	require.ErrorIs(t, err, umpirefw.ErrDomainValue)

	normalized, err = byPath["id_reuse_policy"].(umpirefw.NormalizingDomain).Normalize(int32(enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE))
	require.NoError(t, err)
	require.NotEmpty(t, normalized)
}

func TestTemporalLinkAndSignedIntegerValidatorAdapters(t *testing.T) {
	linkDomain, err := newLinkCollectionValidatorDomain(2, 1024)
	require.NoError(t, err)
	valid := []*commonpb.Link{{Variant: &commonpb.Link_Activity_{Activity: &commonpb.Link_Activity{
		Namespace: "namespace", ActivityId: "activity", RunId: "run",
	}}}}
	normalized, err := linkDomain.Normalize(valid)
	require.NoError(t, err)
	require.Contains(t, normalized, "link:sha256:")
	_, err = linkDomain.Normalize([]*commonpb.Link{{}})
	require.ErrorIs(t, err, umpirefw.ErrDomainValue)

	integerDomain, err := newSignedIntegerValidatorDomain(-2, 4)
	require.NoError(t, err)
	normalized, err = integerDomain.Normalize(int32(3))
	require.NoError(t, err)
	require.Equal(t, "3", normalized)
	_, err = integerDomain.Normalize(int64(5))
	require.ErrorIs(t, err, umpirefw.ErrDomainValue)
}
