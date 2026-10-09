package nexus

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReservedHeaderKeys(t *testing.T) {
	require.Empty(t, ReservedHeaderKeys(nil))
	require.Empty(t, ReservedHeaderKeys(map[string]string{"key": "v", "x-temporal-foo": "v"}))
	require.Equal(t,
		[]string{"temporal-a", "temporal-b"},
		ReservedHeaderKeys(map[string]string{"temporal-b": "v", "key": "v", "temporal-a": "v"}),
	)
}
