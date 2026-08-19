//revive:disable:package-directory-mismatch matches the existing convention in this directory.
package rpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
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
	"go.temporal.io/server/common/rpc/auth/authtest"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/tests/testutils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type tlsEnv struct {
	serverOpt grpc.ServerOption
	provider  *encryption.FixedTLSConfigProvider
}

// newTLSEnv generates a self-signed cert chain for 127.0.0.1 and returns the matching
// server option and client-side TLSConfigProvider. Both halves trust the same chain.
func newTLSEnv(t *testing.T) tlsEnv {
	t.Helper()
	chain, err := testutils.GenerateTestChain(t.TempDir(), localhostIPv4)
	require.NoError(t, err)

	serverCreds, err := credentials.NewServerTLSFromFile(chain.CertPubFile, chain.CertKeyFile)
	require.NoError(t, err)

	caBytes, err := os.ReadFile(chain.CaPubFile)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(caBytes))

	return tlsEnv{
		serverOpt: grpc.Creds(serverCreds),
		provider: &encryption.FixedTLSConfigProvider{
			RemoteClusterClientConfigs: map[string]*tls.Config{
				localhostIPv4: {ServerName: localhostIPv4, RootCAs: pool},
			},
		},
	}
}

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

	env := newTLSEnv(t)
	server := grpc.NewServer(env.serverOpt, grpc.UnaryInterceptor(interceptor))
	testservice.RegisterTestServiceServer(server, &TestServiceServerHandler{})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	address := listener.Addr().String()

	tokenProvider := authtest.StaticTokenProvider(expectedToken)

	testCfg := &config.Config{
		Services: map[string]config.Service{
			"frontend": {RPC: config.RPC{GRPCPort: 0, BindOnIP: localhostIPv4}},
		},
	}
	factory := rpc.NewFactory(
		testCfg,
		primitives.FrontendService,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler, env.provider,
		"dns:///127.0.0.1:0", "dns:///127.0.0.1:0", 0, nil,
		nil, nil, nil,
		tokenProvider,
	)

	conn := factory.CreateRemoteFrontendGRPCConnection(address)
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

	env := newTLSEnv(t)
	server := grpc.NewServer(env.serverOpt, grpc.UnaryInterceptor(interceptor))
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
		metrics.NoopMetricsHandler, env.provider,
		"dns:///127.0.0.1:0", "dns:///127.0.0.1:0", 0, nil,
		nil, nil, nil,
		nil,
	)

	conn := factory.CreateRemoteFrontendGRPCConnection(address)
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

	env := newTLSEnv(t)
	server := grpc.NewServer(env.serverOpt, grpc.UnaryInterceptor(rejectInterceptor))
	testservice.RegisterTestServiceServer(server, &TestServiceServerHandler{})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	address := listener.Addr().String()

	tokenProvider := authtest.StaticTokenProvider("wrong-token")

	testCfg := &config.Config{
		Services: map[string]config.Service{
			"frontend": {RPC: config.RPC{GRPCPort: 0, BindOnIP: localhostIPv4}},
		},
	}
	factory := rpc.NewFactory(
		testCfg,
		primitives.FrontendService,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler, env.provider,
		"dns:///127.0.0.1:0", "dns:///127.0.0.1:0", 0, nil,
		nil, nil, nil,
		tokenProvider,
	)

	conn := factory.CreateRemoteFrontendGRPCConnection(address)
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

	env := newTLSEnv(t)
	server := grpc.NewServer(env.serverOpt)
	testservice.RegisterTestServiceServer(server, &TestServiceServerHandler{})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	address := listener.Addr().String()

	tokenProvider := authtest.StaticTokenProvider("")

	testCfg := &config.Config{
		Global: config.Global{
			Authorization: config.Authorization{
				RemoteClusterAuth: config.RemoteClusterAuth{Require: true},
			},
		},
		Services: map[string]config.Service{
			"frontend": {RPC: config.RPC{GRPCPort: 0, BindOnIP: localhostIPv4}},
		},
	}
	factory := rpc.NewFactory(
		testCfg,
		primitives.FrontendService,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler, env.provider,
		"dns:///127.0.0.1:0", "dns:///127.0.0.1:0", 0, nil,
		nil, nil, nil,
		tokenProvider,
	)

	conn := factory.CreateRemoteFrontendGRPCConnection(address)
	defer func() { _ = conn.Close() }()

	client := testservice.NewTestServiceClient(conn)
	_, err = client.SendHello(context.Background(), &testservice.SendHelloRequest{Name: "test"})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unauthenticated, st.Code())
}

// Asserts production TokenCredentials refuse to attach to a plaintext dial.
// gRPC validates this at NewClient construction; the bearer never gets a chance to leave.
func TestTokenAuthHeader_PlaintextDialRejectedByCredentials(t *testing.T) {
	t.Parallel()

	creds := auth.NewTokenCredentials("authorization", func(context.Context) (string, error) {
		return "any-token", nil
	})

	_, err := grpc.NewClient(
		"127.0.0.1:0",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(creds),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "transport level security")
}
