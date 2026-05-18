package history

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

func newTestConn(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	// grpc.NewClient is lazy and does not dial until an RPC is issued, so this
	// is safe to call with unreachable addresses in tests.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return conn
}

func TestConnectionPool_CloseConn(t *testing.T) {
	ctrl := gomock.NewController(t)
	resolver := membership.NewMockServiceResolver(ctrl)
	rpcFactory := NewMockRPCFactory(ctrl)
	rpcFactory.EXPECT().CreateHistoryGRPCConnection(gomock.Any()).DoAndReturn(
		func(addr string) *grpc.ClientConn { return newTestConn(t, addr) },
	).AnyTimes()
	pool := NewConnectionPool(resolver, rpcFactory, historyservice.NewHistoryServiceClient, log.NewNoopLogger())

	cc := pool.getOrCreateClientConn("addr:7235")
	require.NotEqual(t, connectivity.Shutdown, cc.grpcConn.GetState())

	pool.closeConn("addr:7235")
	require.Eventually(t, func() bool {
		return cc.grpcConn.GetState() == connectivity.Shutdown
	}, 2*time.Second, 10*time.Millisecond, "closed gRPC conn should reach Shutdown state")

	pool.mu.Lock()
	_, stillCached := pool.mu.conns["addr:7235"]
	pool.mu.Unlock()
	require.False(t, stillCached, "closed addr should be removed from the pool")

	require.NotPanics(t, func() { pool.closeConn("addr:7235") })
	require.NotPanics(t, func() { pool.closeConn("never-existed") })
}
