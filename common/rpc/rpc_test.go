package rpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestHandleMembershipChangeEvictsConnections(t *testing.T) {
	// When hosts are removed from the membership ring, their cached
	// gRPC connections should be evicted from the cache.

	cfg := &config.Config{
		Services: map[string]config.Service{
			string(primitives.HistoryService): {
				RPC: config.RPC{},
			},
		},
	}

	factory := NewFactory(
		cfg,
		primitives.HistoryService,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		nil,
		"",
		"",
		0,
		nil,
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		nil,
		nil, // no monitor needed for this test
	)

	// Create connections to multiple hosts
	host1 := "127.0.0.1:7234"
	host2 := "127.0.0.1:7235"
	host3 := "127.0.0.1:7236"

	factory.CreateHistoryGRPCConnection(host1)
	factory.CreateHistoryGRPCConnection(host2)
	factory.CreateHistoryGRPCConnection(host3)

	// Verify all connections are cached
	require.NotNil(t, factory.interNodeGrpcConnections.Get(host1))
	require.NotNil(t, factory.interNodeGrpcConnections.Get(host2))
	require.NotNil(t, factory.interNodeGrpcConnections.Get(host3))

	// Simulate membership change - host1 and host2 removed
	event := &membership.ChangedEvent{
		HostsRemoved: []membership.HostInfo{
			membership.NewHostInfoFromAddress(host1),
			membership.NewHostInfoFromAddress(host2),
		},
	}

	factory.HandleMembershipChange(event)

	// host1 and host2 should be evicted, host3 should remain
	require.Nil(t, factory.interNodeGrpcConnections.Get(host1), "removed host should be evicted")
	require.Nil(t, factory.interNodeGrpcConnections.Get(host2), "removed host should be evicted")
	require.NotNil(t, factory.interNodeGrpcConnections.Get(host3), "unaffected host should remain")
}
