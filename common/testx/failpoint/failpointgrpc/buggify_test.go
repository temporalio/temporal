package failpointgrpc_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
	"go.temporal.io/testx/failpoint/failpointgrpc"
	"go.temporal.io/testx/failpoint/failpointpb"
	"google.golang.org/protobuf/proto"
)

func TestListFailpoints_ReportsBuggifyCounters(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	b := buggify.New(registry)
	require.NoError(t, b.Set(buggify.Config{
		Seed: 1,
		Keys: []buggify.Key{{Match: "stats.z*", ActivationProbability: 1}, {Match: "stats.*"}},
	}))
	// Creating the server with WithBuggify starts the counters.
	server := failpointgrpc.NewServer(registry, failpointgrpc.WithBuggify(b))
	for _, name := range []string{"z0", "a", "z1", "z0"} {
		require.False(t, failpoint.ExecuteSwitch(registry, statsKeys.Key(name)))
	}

	resp, err := server.ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	want := []*failpointpb.FallbackKeyInfo{
		{Name: "stats.a", Executions: 1},
		{Name: "stats.z0", Activated: true, Executions: 2},
		{Name: "stats.z1", Activated: true, Executions: 1},
	}
	require.Len(t, resp.FallbackKeys, len(want))
	for i, info := range want {
		require.True(t, proto.Equal(info, resp.FallbackKeys[i]), resp.FallbackKeys[i])
	}
}

// BUGGIFY counts executions only once a service asks for it, so that a registry without a service
// doesn't pay for counters on every execution of a selected name. NewServer with WithBuggify
// enables counting immediately, even before BUGGIFY is configured.
func TestServer_StartsTheBuggifyCounters(t *testing.T) {
	t.Parallel()
	enable := func(t *testing.T, b *buggify.Buggify) {
		require.NoError(t, b.Set(buggify.Config{
			Seed: 1,
			Keys: []buggify.Key{{Match: "stats.*", ActivationProbability: 1}},
		}))
	}
	key := statsKeys.Key("x")

	t.Run("no service", func(t *testing.T) {
		registry := failpoint.NewRegistry()
		b := buggify.New(registry)
		enable(t, b)
		require.False(t, failpoint.ExecuteSwitch(registry, key))
		require.Equal(t, []buggify.NameStats{{Name: "stats.x", Activated: true}}, b.Stats())
	})

	t.Run("BUGGIFY enabled before the service", func(t *testing.T) {
		registry := failpoint.NewRegistry()
		b := buggify.New(registry)
		enable(t, b)
		failpointgrpc.NewServer(registry, failpointgrpc.WithBuggify(b))
		require.False(t, failpoint.ExecuteSwitch(registry, key))
		require.Equal(t, []buggify.NameStats{{Name: "stats.x", Activated: true, Executions: 1}}, b.Stats())
	})

	t.Run("BUGGIFY enabled after the service", func(t *testing.T) {
		registry := failpoint.NewRegistry()
		b := buggify.New(registry)
		failpointgrpc.NewServer(registry, failpointgrpc.WithBuggify(b))
		enable(t, b)
		require.False(t, failpoint.ExecuteSwitch(registry, key))
		require.Equal(t, []buggify.NameStats{{Name: "stats.x", Activated: true, Executions: 1}}, b.Stats())
	})
}
