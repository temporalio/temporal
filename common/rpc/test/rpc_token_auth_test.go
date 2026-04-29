package rpc

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/testservice/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestTokenAuthHeader_SentOnRemoteConnection(t *testing.T) {
	t.Parallel()

	const expectedToken = "my-test-jwt-token"

	var capturedAuthHeader atomic.Value
	interceptor := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if vals := md.Get("authorization"); len(vals) > 0 {
				capturedAuthHeader.Store(vals[0])
			}
		}
		return handler(ctx, req)
	}

	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor))
	testservice.RegisterTestServiceServer(server, &TestServiceServerHandler{})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	address := listener.Addr().String()

	tokenFile := filepath.Join(t.TempDir(), "token.jwt")
	require.NoError(t, os.WriteFile(tokenFile, []byte(expectedToken), 0600))

	tokenProvider := &auth.FileTokenProvider{
		TokenFiles: map[string]string{
			"127.0.0.1": tokenFile,
		},
	}

	testCfg := &config.Config{
		Services: map[string]config.Service{
			"frontend": {RPC: config.RPC{GRPCPort: 0, BindOnIP: localhostIPv4}},
		},
	}
	factory := rpc.NewFactory(
		testCfg,
		primitives.FrontendService,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler, nil,
		"dns:///127.0.0.1:0", "dns:///127.0.0.1:0", 0, nil,
		nil, nil, nil,
		tokenProvider,
	)

	conn := factory.CreateRemoteFrontendGRPCConnection(address)
	defer conn.Close()

	client := testservice.NewTestServiceClient(conn)
	resp, err := client.SendHello(context.Background(), &testservice.SendHelloRequest{Name: "test"})
	require.NoError(t, err)
	require.Contains(t, resp.Message, "test")

	captured, ok := capturedAuthHeader.Load().(string)
	require.True(t, ok, "authorization header should have been captured")
	require.Equal(t, "Bearer "+expectedToken, captured)
}

func TestTokenAuthHeader_NotSentWhenNoProvider(t *testing.T) {
	t.Parallel()

	var capturedAuthHeader atomic.Value
	interceptor := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if vals := md.Get("authorization"); len(vals) > 0 {
				capturedAuthHeader.Store(vals[0])
			}
		}
		return handler(ctx, req)
	}

	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor))
	testservice.RegisterTestServiceServer(server, &TestServiceServerHandler{})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	address := listener.Addr().String()

	testCfg := &config.Config{
		Services: map[string]config.Service{
			"frontend": {RPC: config.RPC{GRPCPort: 0, BindOnIP: localhostIPv4}},
		},
	}
	factory := rpc.NewFactory(
		testCfg,
		primitives.FrontendService,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler, nil,
		"dns:///127.0.0.1:0", "dns:///127.0.0.1:0", 0, nil,
		nil, nil, nil,
		nil,
	)

	conn := factory.CreateRemoteFrontendGRPCConnection(address)
	defer conn.Close()

	client := testservice.NewTestServiceClient(conn)
	resp, err := client.SendHello(context.Background(), &testservice.SendHelloRequest{Name: "test"})
	require.NoError(t, err)
	require.Contains(t, resp.Message, "test")

	_, ok := capturedAuthHeader.Load().(string)
	require.False(t, ok, "authorization header should not have been sent")
}
