package chasm

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/testvars"
)

func TestInternalKeyConverter(t *testing.T) {
	tv := testvars.New(t)

	key := EntityKey{
		NamespaceID: tv.NamespaceID().String(),
		BusinessID:  tv.Any().String(),
		EntityID:    tv.Any().String(),
	}
	archetype := tv.Any().String()

	internalKey, err := DefaultInternalKeyConverter.ToInternalKey(key, archetype)
	require.NoError(t, err)

	convertedKey, err := DefaultInternalKeyConverter.FromInternalKey(internalKey, archetype)
	require.NoError(t, err)
	require.Equal(t, key, convertedKey)
}
