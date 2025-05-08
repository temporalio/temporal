package nettest_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/nettest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
)

func TestRPCFactory_GetFrontendGRPCServerOptions(t *testing.T) {
	t.Parallel()

	rpcFactory := newRPCFactory()
	opts, err := rpcFactory.GetFrontendGRPCServerOptions()
	require.NoError(t, err)
	assert.Empty(t, opts)
}

func TestRPCFactory_GetInternodeGRPCServerOptions(t *testing.T) {
	t.Parallel()

	rpcFactory := newRPCFactory()
	opts, err := rpcFactory.GetInternodeGRPCServerOptions()
	require.NoError(t, err)
	assert.Empty(t, opts)
}

func TestRPCFactory_CreateHistoryGRPCConnection(t *testing.T) {
	t.Parallel()

	testDialer(t, "localhost", func(rpcFactory *nettest.RPCFactory) *grpc.ClientConn {
		return rpcFactory.CreateHistoryGRPCConnection("localhost")
	})
}

func TestRPCFactory_CreateMatchingGRPCConnection(t *testing.T) {
	t.Parallel()

	testDialer(t, "localhost", func(rpcFactory *nettest.RPCFactory) *grpc.ClientConn {
		return rpcFactory.CreateMatchingGRPCConnection("localhost")
	})
}

func TestRPCFactory_CreateLocalFrontendGRPCConnection(t *testing.T) {
	t.Parallel()

	testDialer(t, ":0", func(rpcFactory *nettest.RPCFactory) *grpc.ClientConn {
		return rpcFactory.CreateLocalFrontendGRPCConnection()
	})
}

func TestRPCFactory_CreateRemoteFrontendGRPCConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		rpcAddress  string
		wantTarget  string
		wantErr     bool
		errContains string
	}{
		{
			name:       "simple host:port",
			rpcAddress: "localhost:8080",
			wantTarget: "localhost:8080",
			wantErr:    false,
		},
		{
			name:       "ip:port",
			rpcAddress: "127.0.0.1:8080",
			wantTarget: "127.0.0.1:8080",
			wantErr:    false,
		},
		{
			name:       "passthrough URL",
			rpcAddress: "passthrough:///host:8080",
			wantTarget: "passthrough:///host:8080",
			wantErr:    false,
		},
		{
			name:       "dns URL",
			rpcAddress: "dns:///host:8080",
			wantTarget: "dns:///host:8080",
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create a context with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Create RPCFactory with custom dial options for DNS URLs
			var dialOptions []grpc.DialOption
			if strings.HasPrefix(tt.rpcAddress, "dns:///") {
				// Use a custom resolver for DNS URLs
				dialOptions = append(dialOptions, grpc.WithResolvers(&mockResolver{}))
			}
			rpcFactory := newRPCFactory(dialOptions...)

			errs := make(chan error, 1)

			go func() {
				_, err := rpcFactory.GetGRPCListener().Accept()
				errs <- err
			}()

			conn := rpcFactory.CreateRemoteFrontendGRPCConnection(tt.rpcAddress)

			if tt.wantErr {
				assert.Nil(t, conn, "Expected nil connection for error case")
			} else {
				assert.NotNil(t, conn, "Expected non-nil connection for success case")
				if conn != nil {
					conn.Connect()
					select {
					case err := <-errs:
						require.NoError(t, err)
					case <-ctx.Done():
						t.Fatal("Test timed out waiting for connection")
					}
					assert.Equal(t, tt.wantTarget, conn.Target())
					assert.NoError(t, conn.Close())
				}
			}
		})
	}
}

func testDialer(t *testing.T, target string, dial func(rpcFactory *nettest.RPCFactory) *grpc.ClientConn) {
	t.Helper()

	t.Run("HappyPath", func(t *testing.T) {
		t.Parallel()

		rpcFactory := newRPCFactory()
		errs := make(chan error, 1)

		go func() {
			_, err := rpcFactory.GetGRPCListener().Accept()
			errs <- err
		}()

		conn := dial(rpcFactory)
		conn.Connect()
		require.NoError(t, <-errs)
		assert.Equal(t, target, conn.Target())
		assert.NoError(t, conn.Close())
	})
}

func newRPCFactory(dialOptions ...grpc.DialOption) *nettest.RPCFactory {
	return nettest.NewRPCFactory(nettest.NewListener(nettest.NewPipe()), dialOptions...)
}

// mockResolver is a custom resolver for DNS URLs
type mockResolver struct{}

func (r *mockResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	// Return a mock resolver that immediately resolves to localhost
	cc.UpdateState(resolver.State{
		Addresses: []resolver.Address{{Addr: "localhost:8080"}},
	})
	return &mockResolver{}, nil
}

func (r *mockResolver) Scheme() string {
	return "dns"
}

func (r *mockResolver) ResolveNow(resolver.ResolveNowOptions) {}

func (r *mockResolver) Close() {}
