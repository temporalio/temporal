package rpc

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// The cache is keyed by host address, and grpc.NewClient is lazy: no dial
// happens until the first RPC, which these tests never make. So the addresses
// below are only map keys and never have to be reachable.
const (
	hostA = "cache-test-host-a:1234"
	hostB = "cache-test-host-b:1234"
)

func newCacheTestFactory() *RPCFactory {
	return NewFactory(nil, "tester", log.NewNoopLogger(), metrics.NoopMetricsHandler, nil, "", "", 0, nil, nil, nil, nil, nil)
}

func cachedConn(f *RPCFactory, hostName string) (*grpc.ClientConn, bool) {
	f.internodeGRPCConnections.RLock()
	defer f.internodeGRPCConnections.RUnlock()
	c, ok := f.internodeGRPCConnections.conns[hostName]
	return c, ok
}

func cachedConnCount(f *RPCFactory) int {
	f.internodeGRPCConnections.RLock()
	defer f.internodeGRPCConnections.RUnlock()
	return len(f.internodeGRPCConnections.conns)
}

// dial returns nil after logging Fatal, which a no-op logger does not act on.
// Caching that nil would make every later lookup dereference it.
func TestInternodeConnCache_DoesNotCacheFailedDial(t *testing.T) {
	f := newCacheTestFactory()

	// A control character makes gRPC's target parsing fail, so dial returns nil.
	const badHost = "cache-test-host-\x7f"
	require.Nil(t, f.CreateHistoryGRPCConnection(badHost))

	_, cached := cachedConn(f, badHost)
	require.False(t, cached, "a failed dial must not be cached")
	require.Nil(t, f.CreateHistoryGRPCConnection(badHost))
}

func TestInternodeConnCache_SharesConnPerHost(t *testing.T) {
	f := newCacheTestFactory()

	c1 := f.CreateHistoryGRPCConnection(hostA)
	require.NotNil(t, c1)
	require.Same(t, c1, f.CreateHistoryGRPCConnection(hostA))

	// Different host address gets its own connection.
	require.NotSame(t, c1, f.CreateHistoryGRPCConnection(hostB))
}

func TestInternodeConnCache_ReplacesShutdownConn(t *testing.T) {
	f := newCacheTestFactory()

	c1 := f.CreateHistoryGRPCConnection(hostA)
	require.NoError(t, c1.Close()) // simulate the downstream cache closing it on membership change
	require.Equal(t, connectivity.Shutdown, c1.GetState())

	c2 := f.CreateHistoryGRPCConnection(hostA)
	require.NotSame(t, c1, c2)
	require.NotEqual(t, connectivity.Shutdown, c2.GetState())
}

func TestInternodeConnCache_CleanupRemovesShutdownConns(t *testing.T) {
	f := newCacheTestFactory()

	live := f.CreateHistoryGRPCConnection(hostA)
	dead := f.CreateHistoryGRPCConnection(hostB)
	require.NoError(t, dead.Close())

	f.cleanupInternodeConns()

	_, deadOK := cachedConn(f, hostB)
	require.False(t, deadOK, "shut-down connection should be swept")
	liveConn, liveOK := cachedConn(f, hostA)
	require.True(t, liveOK, "live connection should be retained")
	require.Same(t, live, liveConn)
}

func TestInternodeConnCache_ConcurrentCreate(t *testing.T) {
	f := newCacheTestFactory()

	const n = 32
	got := make([]*grpc.ClientConn, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			got[i] = f.CreateHistoryGRPCConnection(hostA)
		}()
	}
	wg.Wait()

	require.Equal(t, 1, cachedConnCount(f))
	cached, ok := cachedConn(f, hostA)
	require.True(t, ok)
	for i, conn := range got {
		require.Same(t, cached, conn, "caller %d got a connection the cache discarded", i)
	}
}
