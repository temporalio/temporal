package client

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMatchingClientCacheProviderReleasesLiveConnection(t *testing.T) {
	entry, connection := newMatchingClientCacheTestEntry(t)
	require.True(t, entry.IsValid())
	require.NoError(t, entry.Release())
	require.Equal(t, connectivity.Shutdown, connection.GetState())
	require.False(t, entry.IsValid())
}

func TestMatchingClientCacheProviderAcceptsClosedConnection(t *testing.T) {
	entry, connection := newMatchingClientCacheTestEntry(t)
	require.NoError(t, connection.Close())
	require.False(t, entry.IsValid())
	require.NoError(t, entry.Release())
}

func newMatchingClientCacheTestEntry(t *testing.T) (common.ClientCacheEntry, *grpc.ClientConn) {
	t.Helper()
	connection, err := grpc.NewClient(
		"passthrough:///unused",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if connection.GetState() != connectivity.Shutdown {
			require.NoError(t, connection.Close())
		}
	})

	provider := newMatchingClientCacheProvider(func(string) *grpc.ClientConn {
		return connection
	})
	entry, err := provider("matching:7235")
	require.NoError(t, err)
	require.NotNil(t, entry.Client)
	return entry, connection
}
