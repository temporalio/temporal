//go:build !test_dep

package buggify

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/testx/failpoint"
)

// Release builds never randomize settings, even with a seed in the environment.
func TestWrapDynamicConfigClient(t *testing.T) {
	t.Setenv(failpoint.BuggifySeedEnv, "1")
	client := dynamicconfig.NewMemoryClient()
	wrapped, startup, err := WrapDynamicConfigClient(client, log.NewNoopLogger())
	require.NoError(t, err)
	require.Same(t, client, wrapped)
	require.Equal(t, Startup{}, startup)
}
