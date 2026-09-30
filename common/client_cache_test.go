package common

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

func TestClientCacheRefreshesShutdownGRPCConnection(t *testing.T) {
	var connections []*grpc.ClientConn
	cache := NewClientCache(nil, func(string) (any, *grpc.ClientConn, error) {
		connection, err := grpc.NewClient(
			"passthrough:///unused",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, nil, err
		}
		connections = append(connections, connection)
		return connection, connection, nil
	}, log.NewNoopLogger())
	t.Cleanup(cache.EvictAll)

	first, err := cache.GetClientForClientKey("matching:7235")
	require.NoError(t, err)
	require.Len(t, connections, 1)

	same, err := cache.GetClientForClientKey("matching:7235")
	require.NoError(t, err)
	require.Same(t, first, same)
	require.Len(t, connections, 1)

	require.NoError(t, connections[0].Close())
	replacement, err := cache.GetClientForClientKey("matching:7235")
	require.NoError(t, err)
	require.NotSame(t, first, replacement)
	require.Len(t, connections, 2)

	cache.Evict("matching:7235")
	require.Equal(t, connectivity.Shutdown, connections[1].GetState())
}
