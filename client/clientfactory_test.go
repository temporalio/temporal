package client

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMatchingClientCacheProviderConnectionValidity(t *testing.T) {
	for _, closeBeforeRelease := range []bool{false, true} {
		name := "release closes live connection"
		if closeBeforeRelease {
			name = "release accepts closed connection"
		}
		t.Run(name, func(t *testing.T) {
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
			require.True(t, entry.IsValid())

			if closeBeforeRelease {
				require.NoError(t, connection.Close())
				require.False(t, entry.IsValid())
			}
			require.NoError(t, entry.Release())
			require.False(t, entry.IsValid())
		})
	}
}
