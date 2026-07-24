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

	c1 := f.CreateHistoryGRPCConnection("127.0.0.1:7234")
	require.NotNil(t, c1)
	require.Same(t, c1, f.CreateHistoryGRPCConnection("127.0.0.1:7234"))

	// Different host address gets its own connection.
	require.NotSame(t, c1, f.CreateHistoryGRPCConnection("127.0.0.1:7235"))
}

func TestInternodeConnCache_ReplacesShutdownConn(t *testing.T) {
	f := newCacheTestFactory()

	c1 := f.CreateHistoryGRPCConnection("127.0.0.1:7234")
	require.NoError(t, c1.Close()) // simulate the downstream cache closing it on membership change
	require.Equal(t, connectivity.Shutdown, c1.GetState())

	c2 := f.CreateHistoryGRPCConnection("127.0.0.1:7234")
	require.NotSame(t, c1, c2)
	require.NotEqual(t, connectivity.Shutdown, c2.GetState())
}

func TestInternodeConnCache_CleanupRemovesShutdownConns(t *testing.T) {
	f := newCacheTestFactory()

	live := f.CreateHistoryGRPCConnection("127.0.0.1:7234")
	dead := f.CreateHistoryGRPCConnection("127.0.0.1:7235")
	require.NoError(t, dead.Close())

	f.cleanupInternodeConns()

	_, deadOK := f.cachedConn("127.0.0.1:7235")
	require.False(t, deadOK, "shut-down connection should be swept")
	liveConn, liveOK := f.cachedConn("127.0.0.1:7234")
	require.True(t, liveOK, "live connection should be retained")
	require.Same(t, live, liveConn)
}

func TestInternodeConnCache_ConcurrentCreate(t *testing.T) {
	f := newCacheTestFactory()

	const n = 32
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			f.CreateHistoryGRPCConnection("127.0.0.1:7234")
		}()
	}
	wg.Wait()

	f.internodeGRPCConnections.RLock()
	require.Len(t, f.internodeGRPCConnections.conns, 1)
	f.internodeGRPCConnections.RUnlock()
}
