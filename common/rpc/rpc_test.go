package rpc

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/grpc"
)

func TestCreateLocalFrontendGRPCConnectionIsCached(t *testing.T) {
	f := NewFactory(nil, primitives.FrontendService, log.NewNoopLogger(),
		metrics.NoopMetricsHandler, nil, "localhost:7233", "", 0, nil, nil, nil, nil, nil)
	first := f.CreateLocalFrontendGRPCConnection()
	second := f.CreateLocalFrontendGRPCConnection()
	require.NotNil(t, first)
	require.Same(t, first, second, "expected the internal frontend connection to be reused")
	t.Cleanup(func() { _ = first.Close() })
}

func TestCreateRemoteFrontendGRPCConnection_CachedPerAddress(t *testing.T) {
	f := NewFactory(nil, primitives.FrontendService, log.NewNoopLogger(),
		metrics.NoopMetricsHandler, nil, "localhost:7233", "", 0, nil, nil, nil, nil, nil)

	firstA := f.CreateRemoteFrontendGRPCConnection("remote-a:7233")
	secondA := f.CreateRemoteFrontendGRPCConnection("remote-a:7233")
	require.NotNil(t, firstA)
	require.Same(t, firstA, secondA, "expected repeated calls for the same address to reuse the connection")

	connB := f.CreateRemoteFrontendGRPCConnection("remote-b:7233")
	require.NotNil(t, connB)
	require.NotSame(t, firstA, connB, "expected different addresses to get distinct connections")

	t.Cleanup(func() {
		_ = firstA.Close()
		_ = connB.Close()
	})
}

// Concurrent first callers for one rpcAddress converge on a single connection.
func TestCreateRemoteFrontendGRPCConnection_ConcurrentSameAddressConverges(t *testing.T) {
	f := NewFactory(nil, primitives.FrontendService, log.NewNoopLogger(),
		metrics.NoopMetricsHandler, nil, "localhost:7233", "", 0, nil, nil, nil, nil, nil)

	const goroutines = 20
	conns := make([]*grpc.ClientConn, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func(i int) {
			defer wg.Done()
			conns[i] = f.CreateRemoteFrontendGRPCConnection("remote-concurrent:7233")
		}(i)
	}
	wg.Wait()

	require.NotNil(t, conns[0])
	for i := 1; i < goroutines; i++ {
		require.Same(t, conns[0], conns[i], "expected all concurrent callers to converge on the same cached connection")
	}
	t.Cleanup(func() { _ = conns[0].Close() })
}
