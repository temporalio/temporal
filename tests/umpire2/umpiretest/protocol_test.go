package umpiretest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanonicalProtocolIsCached(t *testing.T) {
	first, err := CanonicalProtocol()
	require.NoError(t, err)
	second, err := CanonicalProtocol()
	require.NoError(t, err)
	require.Same(t, first, second)
}
