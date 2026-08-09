package history

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

const testConnAddr rpcAddress = "conn-pool-test-host:1234"

func newLazyConn(t *testing.T, addr string) *grpc.ClientConn {
	// grpc.NewClient is lazy, so this address never has to be reachable.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return conn
}

// newSharedConnFactory mimics the RPCFactory internode cache: one connection per
// host, shared by every pool and re-dialled once it has been shut down.
func newSharedConnFactory(t *testing.T) *common.MockRPCFactory {
	factory := common.NewMockRPCFactory(gomock.NewController(t))
	var conn *grpc.ClientConn

	factory.EXPECT().CreateHistoryGRPCConnection(gomock.Any()).DoAndReturn(
		func(addr string) *grpc.ClientConn {
			if conn == nil || conn.GetState() == connectivity.Shutdown {
				conn = newLazyConn(t, addr)
			}
			return conn
		}).AnyTimes()

	return factory
}

func newTestConnectionPool(t *testing.T, factory RPCFactory) *connectionPoolImpl[grpc.ClientConnInterface] {
	ctrl := gomock.NewController(t)
	resolver := membership.NewMockServiceResolver(ctrl)
	resolver.EXPECT().AddListener(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	resolver.EXPECT().RemoveListener(gomock.Any()).Return(nil).AnyTimes()
	resolver.EXPECT().Members().Return(nil).AnyTimes()

	return NewConnectionPool(
		resolver,
		factory,
		func(cc grpc.ClientConnInterface) grpc.ClientConnInterface { return cc },
		log.NewNoopLogger(),
		func() time.Duration { return time.Minute },
	)
}

// The factory shares one connection per host, so a caller that loses the cache
// race holds the very connection the winner stored. Closing it would shut down
// the connection every sibling pool is using, for no reason.
func TestConnectionPool_LostCreateRaceKeepsSharedConnUsable(t *testing.T) {
	factory := common.NewMockRPCFactory(gomock.NewController(t))
	pool := newTestConnectionPool(t, factory)
	t.Cleanup(pool.Close)

	conn := newLazyConn(t, string(testConnAddr))
	factory.EXPECT().CreateHistoryGRPCConnection(gomock.Any()).DoAndReturn(
		func(addr string) *grpc.ClientConn {
			// Stand in for another caller that won the race while this one was
			// dialing, so getOrCreateClientConn takes its "already cached" branch.
			pool.conns.Store(rpcAddress(addr), clientConnection[grpc.ClientConnInterface]{
				grpcClient: conn,
				grpcConn:   conn,
			})
			return conn
		}).AnyTimes()

	cc := pool.getOrCreateClientConn(testConnAddr)

	require.NotEqual(t, connectivity.Shutdown, cc.grpcConn.GetState(),
		"pool closed the shared connection another caller had already cached")

	v, ok := pool.conns.Load(testConnAddr)
	require.True(t, ok, "expected a pooled connection")
	require.Same(t, cc.grpcConn, v.(clientConnection[grpc.ClientConnInterface]).grpcConn)
}

// Every history client built on one RPCFactory shares a connection per host, so
// a pool evicting a departed host closes the connection its siblings still hold.
// A sibling has to re-dial rather than keep serving the shut-down connection.
func TestConnectionPool_SiblingPoolRedialsAfterPeerClose(t *testing.T) {
	factory := newSharedConnFactory(t)
	poolA := newTestConnectionPool(t, factory)
	poolB := newTestConnectionPool(t, factory)
	t.Cleanup(poolA.Close)
	t.Cleanup(poolB.Close)

	shared := poolA.getOrCreateClientConn(testConnAddr).grpcConn
	require.Same(t, shared, poolB.getOrCreateClientConn(testConnAddr).grpcConn,
		"pools built on one factory should share a connection per host")

	// poolA reaps the host after it leaves the ring.
	require.NoError(t, shared.Close())

	got := poolB.getOrCreateClientConn(testConnAddr).grpcConn
	require.NotSame(t, shared, got, "poolB kept the connection poolA closed")
}
