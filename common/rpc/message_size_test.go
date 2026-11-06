package rpc

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/testcontext"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

func TestServerReceivesLargeRequest(t *testing.T) {
	t.Parallel()

	factory := &RPCFactory{}
	for _, tc := range []struct {
		name    string
		options func() ([]grpc.ServerOption, error)
	}{
		{name: "frontend", options: factory.GetFrontendGRPCServerOptions},
		{name: "internode", options: factory.GetInternodeGRPCServerOptions},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			options, err := tc.options()
			require.NoError(t, err)
			server := grpc.NewServer(options...)
			listener := bufconn.Listen(1024 * 1024)
			t.Cleanup(func() { _ = listener.Close() })

			// Exceed gRPC's 4 MiB default to verify the server receive limit is raised.
			service := strings.Repeat("x", 5*1024*1024)
			healthServer := health.NewServer()
			healthServer.SetServingStatus(service, healthpb.HealthCheckResponse_SERVING)
			healthpb.RegisterHealthServer(server, healthServer)

			serveErr := make(chan error, 1)
			go func() { serveErr <- server.Serve(listener) }()
			t.Cleanup(func() {
				server.Stop()
				require.NoError(t, <-serveErr)
			})

			conn, err := grpc.NewClient("passthrough:///message-size-test",
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return listener.DialContext(ctx)
				}),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })

			response, err := healthpb.NewHealthClient(conn).Check(testcontext.For(t), &healthpb.HealthCheckRequest{Service: service})
			require.NoError(t, err)
			require.Equal(t, healthpb.HealthCheckResponse_SERVING, response.GetStatus())
		})
	}
}
