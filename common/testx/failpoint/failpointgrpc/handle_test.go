package failpointgrpc_test

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/await"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/failpointgrpc"
	"go.temporal.io/testx/failpoint/failpointpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestHandle runs the same steps against an in-process service and a remote one.
func TestHandle(t *testing.T) {
	t.Parallel()

	t.Run("local", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		testHandle(t, registry, failpointgrpc.NewLocalClient(failpointgrpc.NewServer(registry)))
	})
	t.Run("remote", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		grpcServer := grpc.NewServer()
		failpointpb.RegisterFailpointServiceServer(grpcServer, failpointgrpc.NewServer(registry))
		go func() { _ = grpcServer.Serve(listener) }()
		t.Cleanup(grpcServer.Stop)
		conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		testHandle(t, registry, failpointpb.NewFailpointServiceClient(conn))
	})
}

func testHandle(t *testing.T, registry *failpoint.Registry, client failpointpb.FailpointServiceClient) {
	t.Helper()
	ctx := t.Context()
	handle := failpointgrpc.NewHandle(client, key1.ForScope("ns"))

	require.NoError(t, handle.Return(ctx, "injected"))
	got, set := failpoint.Execute(registry, key1.ForScope("ns"))
	require.True(t, set)
	require.EqualError(t, got, "injected")
	_, set = failpoint.Execute(registry, key1.ForScope("other"))
	require.False(t, set, "the handle is scoped")

	require.NoError(t, handle.Panic(ctx, "boom", failpoint.Trigger{MaxFires: 1}))
	require.PanicsWithError(t, "boom", func() { _, _ = failpoint.Execute(registry, key1.ForScope("ns")) })

	require.NoError(t, handle.Block(ctx))
	executed := make(chan struct{})
	go func() {
		defer close(executed)
		_, _ = failpoint.Execute(registry, key1.ForScope("ns"))
	}()
	require.NoError(t, handle.Wait(ctx))

	require.NoError(t, handle.Disable())
	await.Rcv(t, executed)
	require.Empty(t, registry.Infos())
	require.Equal(t, codes.NotFound, status.Code(handle.Wait(ctx)))
}
