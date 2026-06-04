package nexustest

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/testing/freeport"
)

func AllocListenAddress() string {
	return fmt.Sprintf("localhost:%d", freeport.MustGetFreePort())
}

func NewNexusServer(t *testing.T, listenAddr string, handler nexus.Handler) {
	// Create listener
	listener, err := net.Listen("tcp", listenAddr)
	require.NoError(t, err, "Nexus test server failed to listen on %s", listenAddr)

	// Create HTTP handler and server
	hh := nexusrpc.NewHTTPHandler(nexusrpc.HandlerOptions{
		Handler: handler,
	})
	srv := &http.Server{Addr: listenAddr, Handler: hh}

	// Start server
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(listener)
	}()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Shutdown server gracefully
		err = srv.Shutdown(ctx)
		if err != nil {
			// Graceful shutdown failed, force close
			require.ErrorIs(t, err, context.DeadlineExceeded, "Nexus test server graceful shutdown failed")
			require.NoError(t, srv.Close(), "Nexus test server force close after graceful shutdown timeout")
		}

		// Wait for server to exit gracefully
		select {
		case err := <-errCh:
			require.ErrorIs(t, err, http.ErrServerClosed, "Nexus test server Serve returned unexpected error")
		case <-time.After(time.Second):
			require.Fail(t, "Nexus test server Serve did not exit after shutdown")
		}
	})
}

type Handler struct {
	nexus.UnimplementedHandler
	OnStartOperation  func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error)
	OnCancelOperation func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error
}

func (h Handler) StartOperation(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
	return h.OnStartOperation(ctx, service, operation, input, options)
}

func (h Handler) CancelOperation(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
	return h.OnCancelOperation(ctx, service, operation, token, options)
}
