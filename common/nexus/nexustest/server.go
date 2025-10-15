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
	hh := nexusrpc.NewHTTPHandler(nexusrpc.HandlerOptions{
		Handler: handler,
	})
	srv := &http.Server{Addr: listenAddr, Handler: hh}
	listener, err := net.Listen("tcp", listenAddr)
	require.NoError(t, err)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(listener)
	}()

	t.Cleanup(func() {
		// Graceful shutdown
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = srv.Shutdown(ctx)
		if ctx.Err() != nil {
			require.NoError(t, err)
			require.ErrorIs(t, <-errCh, http.ErrServerClosed)
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
