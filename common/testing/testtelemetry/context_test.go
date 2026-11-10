package testtelemetry

import (
	"context"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/testcontext"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

func TestNameMetadata(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		opts    []testcontext.Option
		timeout time.Duration
	}{
		{name: "default", timeout: testcontext.DefaultTimeout()},
		{name: "custom", opts: []testcontext.Option{testcontext.WithTimeout(time.Second)}, timeout: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				testcontext.For(t, tc.opts...)
				AttachTestName(t)
				AttachTestName(t)
				ctx := testcontext.For(t)
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.Equal(t, start.Add(tc.timeout), deadline)
				for _, ctx := range []context.Context{ctx, testcontext.For(t)} {
					md, ok := metadata.FromOutgoingContext(ctx)
					require.True(t, ok)
					require.Equal(t, []string{t.Name()}, md.Get("temporal-test-name"))
				}
			})
		})
	}
}

func TestEnsureRemainingPreservesTestNameMetadata(t *testing.T) {
	t.Parallel()

	AttachTestName(t)
	ctx := testcontext.For(t)
	refreshed := testcontext.EnsureRemaining(ctx, t, testcontext.DefaultTimeout()+10*time.Second)

	require.NotSame(t, ctx, refreshed, "context should have been replaced")
	md, ok := metadata.FromOutgoingContext(refreshed)
	require.True(t, ok)
	require.Equal(t, []string{t.Name()}, md.Get("temporal-test-name"))
}

func TestDecoratorReceivesTestNameMetadata(t *testing.T) {
	t.Parallel()

	AttachTestName(t)
	type key struct{}
	var names []string
	testcontext.AttachDecorator(t, key{}, func(ctx context.Context) context.Context {
		md, _ := metadata.FromOutgoingContext(ctx)
		names = md.Get("temporal-test-name")
		return ctx
	})

	require.Equal(t, []string{t.Name()}, names)
}

func TestTestNameReachesExternalClient(t *testing.T) {
	t.Parallel()

	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	names := make(chan []string, 1)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		names <- md.Get("temporal-test-name")
		return handler(ctx, req)
	}))
	healthpb.RegisterHealthServer(server, health.NewServer())
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()
	client, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	AttachTestName(t)
	ctx := testcontext.For(t)
	_, err = healthpb.NewHealthClient(client).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{t.Name()}, <-names)
}
