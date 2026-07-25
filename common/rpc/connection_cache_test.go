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

func (d *RPCFactory) cachedConn(hostName string) (*grpc.ClientConn, bool) {
	d.internodeGRPCConnections.RLock()
	defer d.internodeGRPCConnections.RUnlock()
	c, ok := d.internodeGRPCConnections.conns[hostName]
	return c, ok
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

	_, deadOK := f.cachedConn(hostB)
	require.False(t, deadOK, "shut-down connection should be swept")
	liveConn, liveOK := f.cachedConn(hostA)
	require.True(t, liveOK, "live connection should be retained")
	require.Same(t, live, liveConn)
}

func TestInternodeConnCache_ConcurrentCreate(t *testing.T) {
	f := newCacheTestFactory()

	const n = 32
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			f.CreateHistoryGRPCConnection(hostA)
		}()
	}
	wg.Wait()

	f.internodeGRPCConnections.RLock()
	require.Len(t, f.internodeGRPCConnections.conns, 1)
	f.internodeGRPCConnections.RUnlock()
}
