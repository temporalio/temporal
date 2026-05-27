package testcore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/membership/static"
	"go.temporal.io/server/common/primitives"
)

func TestGlobalOverridesSurviveTestCleanup(t *testing.T) {
	var dcClient *dynamicconfig.MemoryClient

	t.Run("create", func(t *testing.T) {
		impl := newTemporal(t, &TemporalParams{
			ClusterMetadataConfig: &cluster.Config{},
			HostsByProtocolByService: map[transferProtocol]map[primitives.ServiceName]static.Hosts{
				httpProtocol: {
					primitives.FrontendService: {
						All: []string{"127.0.0.1:0"},
					},
				},
			},
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
