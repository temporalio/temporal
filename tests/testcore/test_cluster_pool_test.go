package testcore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
)

func TestGlobalOverridesSurviveTestCleanup(t *testing.T) {
	var dcClient *dynamicconfig.MemoryClient

	t.Run("create", func(t *testing.T) {
		impl := newTemporal(t, &TemporalParams{
			ClusterMetadataConfig: &cluster.Config{},
		})
		dcClient = impl.dcClient
	})
	// "create" subtest finished — its t.Cleanup has run
	// but we expect global dynamic config overrides to still be in place.
	for k, v := range defaultDynamicConfigOverrides {
		got := dcClient.GetValue(k)
		require.NotEmpty(t, got, "key %s missing after cleanup", k)
		require.Equal(t, v, got[0].Value, "key %s wrong after cleanup", k)
	}
}
