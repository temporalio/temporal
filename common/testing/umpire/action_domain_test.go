package umpire

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestCanonicalProtoDigestIsDeterministicAndNonSecret(t *testing.T) {
	left, err := structpb.NewStruct(map[string]any{"second": "secret", "first": 1.0})
	require.NoError(t, err)
	right, err := structpb.NewStruct(map[string]any{"first": 1.0, "second": "secret"})
	require.NoError(t, err)

	leftDigest, err := CanonicalProtoDigest("payload", left)
	require.NoError(t, err)
	rightDigest, err := CanonicalProtoDigest("payload", right)
	require.NoError(t, err)
	require.Equal(t, leftDigest, rightDigest)
	require.True(t, strings.HasPrefix(leftDigest, "payload:sha256:"))
	require.NotContains(t, leftDigest, "secret")

	otherLabel, err := CanonicalProtoDigest("failure", left)
	require.NoError(t, err)
	require.NotEqual(t, leftDigest, otherLabel)
}

func TestCanonicalProtoDigestRejectsInvalidInput(t *testing.T) {
	_, err := CanonicalProtoDigest("", &wrapperspb.StringValue{Value: "value"})
	require.ErrorIs(t, err, ErrDomainValue)
	_, err = CanonicalProtoDigest("payload", nil)
	require.ErrorIs(t, err, ErrDomainValue)
	_, err = CanonicalProtoDigest("payload", &wrapperspb.StringValue{Value: string([]byte{0xff})})
	require.ErrorIs(t, err, ErrDomainValue)
}

func TestEnumDomainProducesOutOfDomainVariantAndCanonicalValue(t *testing.T) {
	domain, err := NewEnumDomain([]int32{0, 1, 2})
	require.NoError(t, err)
	variants := domain.Variants()
	require.Len(t, variants, 1)
	require.Equal(t, OutOfRange, variants[0].Class)
	require.EqualValues(t, 3, variants[0].Mutate(int32(1)))
	normalized, err := domain.Normalize(int32(2))
	require.NoError(t, err)
	require.Equal(t, "2", normalized)
	_, err = domain.Normalize(int32(3))
	require.ErrorIs(t, err, ErrDomainValue)
}

func TestIntegerDomainProducesBoundariesAndNormalizes(t *testing.T) {
	domain, err := NewIntegerDomain(-2, 4)
	require.NoError(t, err)
	variants := domain.Variants()
	require.EqualValues(t, -3, variants[0].Mutate(int64(0)))
	require.EqualValues(t, 5, variants[1].Mutate(int64(0)))
	normalized, err := domain.Normalize(int32(3))
	require.NoError(t, err)
	require.Equal(t, "3", normalized)
	_, err = NewIntegerDomain(5, 4)
	require.ErrorIs(t, err, ErrDomainValue)
	_, err = NewIntegerDomain(math.MinInt64, math.MaxInt64)
	require.NoError(t, err)
}

func TestPayloadDomainMutationsAndNormalizationDoNotExposeRawData(t *testing.T) {
	domain := NewPayloadDomain(8)
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
	require.ErrorIs(t, err, ErrDomainValue)
}

func TestUnsupportedDomainIsExplicit(t *testing.T) {
	domain := NewUnsupportedDomain("validator registry unavailable")
	require.Empty(t, domain.Variants())
	_, err := domain.Normalize("value")
	require.ErrorIs(t, err, ErrUnsupportedDomain)
	require.ErrorContains(t, err, "validator registry unavailable")
	require.NotErrorIs(t, err, ErrDomainValue)
}

func TestValidatorRegistryValidatesRegistrationAndLookup(t *testing.T) {
	base, err := NewIntegerDomain(0, 10)
	require.NoError(t, err)
	registration := ValidatorRegistration{
		Key:    "message.field",
		Domain: base,
		Normalize: func(value any) (string, error) {
			return base.Normalize(value)
		},
	}
	registry, err := NewValidatorRegistry(registration)
	require.NoError(t, err)
	domain, err := registry.Domain("message.field")
	require.NoError(t, err)
	normalized, err := domain.Normalize(int32(4))
	require.NoError(t, err)
	require.Equal(t, "4", normalized)
	require.Len(t, domain.Variants(), 2)

	_, err = registry.Domain("missing")
	require.ErrorIs(t, err, ErrUnsupportedDomain)
	_, err = NewValidatorRegistry(ValidatorRegistration{})
	require.ErrorIs(t, err, ErrUnsupportedDomain)
	_, err = NewValidatorRegistry(registration, registration)
	require.ErrorContains(t, err, "duplicate validator")
}

func TestValidatorDomainClonesMutableValuesBeforeNormalization(t *testing.T) {
	base := NewPayloadDomain(32)
	registry, err := NewValidatorRegistry(ValidatorRegistration{
		Key:    "message.payload",
		Domain: base,
		Clone: func(value any) any {
			return clonePayload(value)
		},
		Normalize: func(value any) (string, error) {
			payload := value.(*commonpb.Payload)
			payload.Data[0] = 'X'
			return base.Normalize(payload)
		},
	})
	require.NoError(t, err)
	domain, err := registry.Domain("message.payload")
	require.NoError(t, err)
	payload := &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte("secret")}
	normalized, err := domain.Normalize(payload)
	require.NoError(t, err)
	require.NotContains(t, normalized, "secret")
	require.Equal(t, []byte("secret"), payload.Data)
}

func TestValidatorRegistrySupportsConcurrentReads(t *testing.T) {
	base, err := NewIntegerDomain(0, 10)
	require.NoError(t, err)
	registry, err := NewValidatorRegistry(ValidatorRegistration{
		Key: "message.field", Domain: base, Normalize: base.Normalize,
	})
	require.NoError(t, err)
	var wait sync.WaitGroup
	results := make(chan error, 32)
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			domain, err := registry.Domain("message.field")
			if err != nil {
				results <- err
				return
			}
			value, err := domain.Normalize(int64(5))
			if err != nil {
				results <- err
				return
			}
			if value != "5" {
				results <- fmt.Errorf("unexpected normalized value %q", value)
				return
			}
			results <- nil
		}()
	}
	wait.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
}
