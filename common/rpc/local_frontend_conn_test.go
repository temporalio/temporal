package rpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
)

// TestCreateLocalFrontendGRPCConnectionIsCached verifies that the internal
// frontend connection is dialed once and reused across calls.
//
// Callers of CreateLocalFrontendGRPCConnection never close the connection it
// returns, so dialing a fresh one per call leaked a grpc.ClientConn — and the
// background goroutines it owns — on every RPC that went through it. Its HTTP
// sibling, CreateLocalFrontendHTTPClient, has always been cached this way.
func TestCreateLocalFrontendGRPCConnectionIsCached(t *testing.T) {
	f := NewFactory(
		nil,
		primitives.FrontendService,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		nil,
		"localhost:7233",
		"",
		0,
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	first := f.CreateLocalFrontendGRPCConnection()
	second := f.CreateLocalFrontendGRPCConnection()

	require.NotNil(t, first)
	require.Same(t, first, second,
		"expected the internal frontend connection to be reused; a new connection per call leaks goroutines")

	t.Cleanup(func() { _ = first.Close() })
}
