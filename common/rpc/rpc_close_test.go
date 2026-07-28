package rpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/grpc/connectivity"
)

// newTestFactory builds a factory with just the fields dial and Close need.
// gRPC connections are lazy, so nothing has to be listening on the target.
func newTestFactory() *RPCFactory {
	return &RPCFactory{
		logger:         log.NewNoopLogger(),
		metricsHandler: metrics.NoopMetricsHandler,
	}
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

func TestRPCFactoryClose_ToleratesConnClosedByOwner(t *testing.T) {
	t.Parallel()

	f := newTestFactory()
	conn := f.dial("127.0.0.1:1234", nil)
	require.NotNil(t, conn)
	require.NoError(t, conn.Close())

	f.Close()

	require.Equal(t, connectivity.Shutdown, conn.GetState())
}

// Connections closed by their owner must not stay reachable from the factory,
// otherwise every host that ever left the ring is retained for the process
// lifetime.
func TestRPCFactoryDial_DropsConnsAlreadyShutDown(t *testing.T) {
	t.Parallel()

	f := newTestFactory()
	evicted := f.dial("127.0.0.1:1234", nil)
	require.NotNil(t, evicted)
	require.NoError(t, evicted.Close())

	replacement := f.dial("127.0.0.1:5678", nil)
	require.NotNil(t, replacement)

	f.connsLock.Lock()
	defer f.connsLock.Unlock()
	require.NotContains(t, f.conns, evicted)
	require.Contains(t, f.conns, replacement)
}
