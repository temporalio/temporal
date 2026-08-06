package rpc

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/grpc/connectivity"
)

const localFrontendTarget = "127.0.0.1:9999"

// gRPC connections are lazy, so nothing has to be listening on the target.
func newTestFactory(logger log.Logger) *RPCFactory {
	return NewFactory(nil, "tester", logger, metrics.NoopMetricsHandler,
		nil, localFrontendTarget, "", 0, nil, nil, nil, nil, nil)
}

func TestRPCFactoryClose_ShutsDownDialedConns(t *testing.T) {
	t.Parallel()

	f := newTestFactory(log.NewNoopLogger())
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

	f := newTestFactory(log.NewNoopLogger())
	conn := f.dial("127.0.0.1:1234", nil)
	require.NotNil(t, conn)

	f.Close()
	f.Close()

	require.Equal(t, connectivity.Shutdown, conn.GetState())
	require.Nil(t, f.conns, "Close must release the tracked connections")
}

func TestRPCFactoryClose_ConnDialedAfterCloseIsClosed(t *testing.T) {
	t.Parallel()

	f := newTestFactory(log.NewNoopLogger())
	f.Close()

	conn := f.dial("127.0.0.1:1234", nil)
	require.NotNil(t, conn)
	require.Equal(t, connectivity.Shutdown, conn.GetState())
}

func TestRPCFactoryClose_DoesNotFailOnConnClosedByOwner(t *testing.T) {
	t.Parallel()

	// Warn never fails a test on its own, so assert on the expectation.
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	warned := logger.Expect(testlogger.Warn, "Failed to close gRPC connection")
	f := newTestFactory(logger)

	conn := f.dial("127.0.0.1:1234", nil)
	require.NotNil(t, conn)
	require.NoError(t, conn.Close())

	f.Close()

	require.Equal(t, connectivity.Shutdown, conn.GetState())
	require.False(t, warned.Matched(),
		"closing a connection its owner already closed must not warn")
}

func TestRPCFactoryDial_DropsConnsAlreadyShutDown(t *testing.T) {
	t.Parallel()

	f := newTestFactory(log.NewNoopLogger())
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
func TestRPCFactoryLocalFrontendConn_Reused(t *testing.T) {
	t.Parallel()

	f := newTestFactory(log.NewNoopLogger())

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

// A ring change can dial while the fx stop hook is closing the factory, so
// trackConn and Close both take connsLock.
func TestRPCFactoryDial_DoesNotRaceClose(t *testing.T) {
	t.Parallel()

	for range 50 {
		f := newTestFactory(log.NewNoopLogger())

		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Go(func() {
				<-start
				for j := range 5 {
					f.dial("127.0.0.1:"+strconv.Itoa(20000+i*10+j), nil)
				}
			})
		}
		wg.Go(func() { <-start; f.Close() })
		close(start)
		wg.Wait()
		f.Close()
	}
}
