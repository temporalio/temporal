package rpc

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

func TestCreateLocalFrontendGRPCConnectionIsCached(t *testing.T) {
	f := NewFactory(nil, primitives.FrontendService, log.NewNoopLogger(),
		metrics.NoopMetricsHandler, nil, "localhost:7233", "", 0, nil, nil, nil, nil, nil, nil)
	first := f.CreateLocalFrontendGRPCConnection()
	second := f.CreateLocalFrontendGRPCConnection()
	require.NotNil(t, first)
	require.Same(t, first, second, "expected the internal frontend connection to be reused")
	t.Cleanup(func() { _ = first.Close() })
}

func TestCreateRemoteFrontendGRPCConnection_CachedPerAddress(t *testing.T) {
	f := NewFactory(nil, primitives.FrontendService, log.NewNoopLogger(),
		metrics.NoopMetricsHandler, nil, "localhost:7233", "", 0, nil, nil, nil, nil, nil, nil)

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
		metrics.NoopMetricsHandler, nil, "localhost:7233", "", 0, nil, nil, nil, nil, nil, nil)

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

// A connection requested after Close must come back already shut down. Otherwise a component
// still winding down during server shutdown dials a fresh connection that nothing will ever
// close, leaking its goroutines (and everything they reference) for the life of the process.
func TestRPCFactory_ConnectionsRequestedAfterCloseAreShutdown(t *testing.T) {
	newFactory := func() *RPCFactory {
		return NewFactory(nil, primitives.FrontendService, log.NewNoopLogger(),
			metrics.NoopMetricsHandler, nil, "localhost:7233", "", 0, nil, nil, nil, nil, nil, nil)
	}

	for name, create := range map[string]func(*RPCFactory) *grpc.ClientConn{
		"History":        func(f *RPCFactory) *grpc.ClientConn { return f.CreateHistoryGRPCConnection(hostA) },
		"Matching":       func(f *RPCFactory) *grpc.ClientConn { return f.CreateMatchingGRPCConnection(hostA) },
		"RemoteFrontend": func(f *RPCFactory) *grpc.ClientConn { return f.CreateRemoteFrontendGRPCConnection("remote-a:7233") },
		"LocalFrontend":  func(f *RPCFactory) *grpc.ClientConn { return f.CreateLocalFrontendGRPCConnection() },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFactory()
			f.Close()

			conn := create(f)
			require.NotNil(t, conn)
			t.Cleanup(func() { _ = conn.Close() })
			require.Equal(t, connectivity.Shutdown, conn.GetState(),
				"a connection created after Close must be shut down")
		})
	}
}

// Close racing with connection creation must not leave any connection live: each one is
// either cached in time for Close to shut it down, or created after Close and shut down
// immediately.
func TestRPCFactory_CloseRacingCreateLeavesNoLiveConnections(t *testing.T) {
	f := NewFactory(nil, primitives.FrontendService, log.NewNoopLogger(),
		metrics.NoopMetricsHandler, nil, "localhost:7233", "", 0, nil, nil, nil, nil, nil, nil)

	const goroutines = 8
	closed := make(chan struct{})
	conns := make([][]*grpc.ClientConn, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func() {
			defer wg.Done()
			for n := 0; ; n++ {
				done := false
				select {
				case <-closed:
					done = true // make one more round of calls after Close has returned
				default:
				}
				host := fmt.Sprintf("race-host-%d-%d:1234", i, n)
				conns[i] = append(conns[i],
					f.CreateHistoryGRPCConnection(host),
					f.CreateRemoteFrontendGRPCConnection(host),
				)
				if done {
					return
				}
			}
		}()
	}

	f.Close()
	close(closed)
	wg.Wait()

	for i, perGoroutine := range conns {
		for _, conn := range perGoroutine {
			require.NotNil(t, conn)
			require.Equal(t, connectivity.Shutdown, conn.GetState(),
				"goroutine %d got a connection that outlived Close", i)
		}
	}
}
