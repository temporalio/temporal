//revive:disable:package-directory-mismatch matches the existing convention in this directory.
package rpc

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/testservice/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
			"remote-cluster": tokenFile,
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

	conn := factory.CreateRemoteFrontendGRPCConnection("remote-cluster", address)
	defer func() { _ = conn.Close() }()

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

	conn := factory.CreateRemoteFrontendGRPCConnection("remote-cluster", address)
	defer func() { _ = conn.Close() }()

	client := testservice.NewTestServiceClient(conn)
	resp, err := client.SendHello(context.Background(), &testservice.SendHelloRequest{Name: "test"})
	require.NoError(t, err)
	require.Contains(t, resp.Message, "test")

	_, ok := capturedAuthHeader.Load().(string)
	require.False(t, ok, "authorization header should not have been sent")
}

// Stand-in for the production ClaimMapper rejection path: validates that the bearer header is inspectable server-side and can drive an auth decision.
func TestTokenAuthHeader_ReceiverRejectsWrongToken(t *testing.T) {
	t.Parallel()

	const expectedToken = "expected-jwt-token"

	rejectInterceptor := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}
		vals := md.Get("authorization")
		if len(vals) == 0 || vals[0] != "Bearer "+expectedToken {
			return nil, status.Error(codes.PermissionDenied, "invalid auth token")
		}
		return handler(ctx, req)
	}

	server := grpc.NewServer(grpc.UnaryInterceptor(rejectInterceptor))
	testservice.RegisterTestServiceServer(server, &TestServiceServerHandler{})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	address := listener.Addr().String()

	tokenFile := filepath.Join(t.TempDir(), "token.jwt")
	require.NoError(t, os.WriteFile(tokenFile, []byte("wrong-token"), 0600))

	tokenProvider := &auth.FileTokenProvider{
		TokenFiles: map[string]string{"remote-cluster": tokenFile},
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

	conn := factory.CreateRemoteFrontendGRPCConnection("remote-cluster", address)
	defer func() { _ = conn.Close() }()

	client := testservice.NewTestServiceClient(conn)
	_, err = client.SendHello(context.Background(), &testservice.SendHelloRequest{Name: "test"})
	require.Error(t, err)
	var permErr *serviceerror.PermissionDenied
	require.ErrorAs(t, err, &permErr)
	require.Contains(t, permErr.Message, "invalid auth token")
}

func TestTokenAuthHeader_StrictModeRejectsEmptyToken(t *testing.T) {
	t.Parallel()

	server := grpc.NewServer()
	testservice.RegisterTestServiceServer(server, &TestServiceServerHandler{})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	address := listener.Addr().String()

	tokenProvider := &auth.FileTokenProvider{TokenFiles: map[string]string{}}

	testCfg := &config.Config{
		Global: config.Global{
			RequireRemoteClusterAuth: true,
		},
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

	conn := factory.CreateRemoteFrontendGRPCConnection("unknown-cluster", address)
	defer func() { _ = conn.Close() }()

	client := testservice.NewTestServiceClient(conn)
	_, err = client.SendHello(context.Background(), &testservice.SendHelloRequest{Name: "test"})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unauthenticated, st.Code())
}
