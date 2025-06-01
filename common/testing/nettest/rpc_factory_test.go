package nettest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/nettest"
	"google.golang.org/grpc"
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

	testDialer(t, "localhost", func(rpcFactory *nettest.RPCFactory) *grpc.ClientConn {
		return rpcFactory.CreateRemoteFrontendGRPCConnection("localhost")
	})
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
