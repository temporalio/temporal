package failpoint_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/failpoint"
)

func TestKey(t *testing.T) {
	t.Parallel()

	scoped := key1.ForScope("ns-1")
	require.Equal(t, "1", key1.Name())
	require.Empty(t, key1.Scope())
	require.Equal(t, "1", key1.String())
	require.Equal(t, "1", scoped.Name())
	require.Equal(t, "ns-1", scoped.Scope())
	require.Equal(t, "1@ns-1", scoped.String())
	require.NotEqual(t, key1, scoped)
}

func TestBoundaryKeys(t *testing.T) {
	t.Parallel()

	require.Equal(t, "grpc:/test.Service/Method", failpoint.GRPCKey("/test.Service/Method").Name())
	require.Equal(t, "http:POST /callback", failpoint.HTTPKey("POST /callback").Name())
}

func TestNewKey_PanicsOnDuplicateName(t *testing.T) {
	t.Parallel()

	require.Panics(t, func() { failpoint.NewKey("1") })
}
