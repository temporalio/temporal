package wci

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
)

// Values from a dynamic config file arrive as []any, which is where merging over the default bit.
func TestInProcessComputeProvidersReplacesTheDefault(t *testing.T) {
	get := func(v any) []string {
		client := dynamicconfig.StaticClient{}
		if v != nil {
			client[InProcessComputeProviders.Key()] = v
		}
		return InProcessComputeProviders.Get(dynamicconfig.NewCollection(client, log.NewNoopLogger()))()
	}
	require.Equal(t, []string{"test-invoke", "test-worker-set"}, get(nil), "unset keeps the default")
	require.Equal(t, []string{"subprocess"}, get([]any{"subprocess"}), "a shorter list replaces it")
	require.Empty(t, get([]any{}), "an empty list turns in-process off")
}
