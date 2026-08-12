package umpire

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
)

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
