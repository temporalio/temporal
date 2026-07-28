package rpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/grpc/connectivity"
)

const localFrontendTarget = "127.0.0.1:9999"

// gRPC connections are lazy, so nothing has to be listening on the target.
func newTestFactory() *RPCFactory {
	return NewFactory(nil, "tester", log.NewNoopLogger(), metrics.NoopMetricsHandler,
		nil, localFrontendTarget, "", 0, nil, nil, nil, nil, nil)
}

func TestRPCFactoryClose_ShutsDownDialedConns(t *testing.T) {
	t.Parallel()

	f := newTestFactory()
	first := f.dial("127.0.0.1:1234", nil)
	second := f.dial("127.0.0.1:5678", nil)
	require.NotNil(t, first)
	require.NotNil(t, second)
	require.NotEqual(t, connectivity.Shutdown, first.GetState())

	f.Close()

	require.Equal(t, connectivity.Shutdown, first.GetState())
	require.Equal(t, connectivity.Shutdown, second.GetState())
}

func TestRPCFactoryClose_Idempotent(t *testing.T) {
	t.Parallel()

	f := newTestFactory()
	conn := f.dial("127.0.0.1:1234", nil)
	require.NotNil(t, conn)

	f.Close()
	f.Close()

	require.Equal(t, connectivity.Shutdown, conn.GetState())
}

func TestRPCFactoryClose_ConnDialedAfterCloseIsClosed(t *testing.T) {
	t.Parallel()

	f := newTestFactory()
	f.Close()

	conn := f.dial("127.0.0.1:1234", nil)
	require.NotNil(t, conn)
	require.Equal(t, connectivity.Shutdown, conn.GetState())
}

func TestRPCFactoryClose_DoesNotFailOnConnClosedByOwner(t *testing.T) {
	t.Parallel()

	f := newTestFactory()
	// Warn never fails a test on its own, so count the matches instead.
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	warned := logger.Expect(testlogger.Warn, "Failed to close gRPC connection")
	f.logger = logger

	conn := f.dial("127.0.0.1:1234", nil)
	require.NotNil(t, conn)
	require.NoError(t, conn.Close())

	f.Close()

	require.Equal(t, connectivity.Shutdown, conn.GetState())
	require.Zero(t, warned.MatchCount(),
		"closing a connection its owner already closed must not warn")
}

// Connections closed by their owner must not stay reachable from the factory,
// otherwise every host that ever left the ring is retained for the process
// lifetime.
func TestRPCFactoryDial_DropsConnsAlreadyShutDown(t *testing.T) {
	t.Parallel()

	f := newTestFactory()
	defer f.Close()

	evicted := f.dial("127.0.0.1:1234", nil)
	require.NotNil(t, evicted)
	require.NoError(t, evicted.Close())

	replacement := f.dial("127.0.0.1:5678", nil)
	require.NotNil(t, replacement)

	// Look the keys up directly: handing a live *grpc.ClientConn to a testify
	// equality helper makes it walk gRPC's mutable internals and races with the
	// connection's own goroutines.
	f.connsLock.Lock()
	_, evictedTracked := f.conns[evicted]
	_, replacementTracked := f.conns[replacement]
	tracked := f.conns
	f.connsLock.Unlock()

	require.False(t, evictedTracked, "a connection closed by its owner should be swept")
	require.True(t, replacementTracked)
	require.Len(t, tracked, 1)
}

// The local frontend target is fixed, so every caller shares one connection.
// Dialing per call is what leaked a connection on every request that needed one.
func TestRPCFactoryLocalFrontendConn_Reused(t *testing.T) {
	t.Parallel()

	f := newTestFactory()
	defer f.Close()

	first := f.CreateLocalFrontendGRPCConnection()
	second := f.CreateLocalFrontendGRPCConnection()
	require.NotNil(t, first)
	require.Same(t, first, second)

	f.connsLock.Lock()
	conns := len(f.conns)
	f.connsLock.Unlock()
	require.Equal(t, 1, conns)

	f.Close()
	require.Equal(t, connectivity.Shutdown, first.GetState())
}
