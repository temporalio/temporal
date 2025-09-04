package rpc

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/rpc/interceptor"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	// DefaultServiceConfig is a default gRPC connection service config which enables DNS round robin between IPs.
	// To use DNS resolver, a "dns:///" prefix should be applied to the hostPort.
	// https://github.com/grpc/grpc/blob/master/doc/naming.md
	DefaultServiceConfig = `{"loadBalancingConfig": [{"round_robin":{}}]}`

	// MaxBackoffDelay is a maximum interval between reconnect attempts.
	MaxBackoffDelay = 10 * time.Second

	// MaxHTTPAPIRequestBytes is the maximum number of bytes an HTTP API request
	// can have. This is currently set to the max gRPC request size.
	MaxHTTPAPIRequestBytes = 4 * 1024 * 1024

	// MaxNexusAPIRequestBodyBytes is the maximum number of bytes a Nexus HTTP API request can have. Because the body is
	// read into a Payload object, this is currently set to the max Payload size. Content headers are transformed to
	// Payload metadata and contribute to the Payload size as well. A separate limit is enforced on top of this.
	MaxNexusAPIRequestBodyBytes = 2 * 1024 * 1024

	// minConnectTimeout is the minimum amount of time we are willing to give a connection to complete.
	minConnectTimeout = 20 * time.Second

	// maxInternodeRecvPayloadSize indicates the internode max receive payload size.
	maxInternodeRecvPayloadSize = 128 * 1024 * 1024 // 128 Mb
)

// Dial creates a client connection to the given target with default options.
// The hostName syntax is defined in
// https://github.com/grpc/grpc/blob/master/doc/naming.md.
// dns resolver is used by default
func Dial(
	hostName string,
	tlsConfig *tls.Config,
	logger log.Logger,
	metricsHandler metrics.Handler,
	opts ...grpc.DialOption,
) (*grpc.ClientConn, error) {
	var grpcSecureOpt grpc.DialOption
	if tlsConfig == nil {
		grpcSecureOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
	} else {
		grpcSecureOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	// gRPC maintains connection pool inside grpc.ClientConn.
	// This connection pool has auto reconnect feature.
	// If connection goes down, gRPC will try to reconnect using exponential backoff strategy:
	// https://github.com/grpc/grpc/blob/master/doc/connection-backoff.md.
	// Default MaxDelay is 120 seconds which is too high.
	var cp = grpc.ConnectParams{
		Backoff:           backoff.DefaultConfig,
		MinConnectTimeout: minConnectTimeout,
	}
	cp.Backoff.MaxDelay = MaxBackoffDelay

	dtrace := newDialTracer(hostName, metricsHandler, logger)

	contextDialer := func(ctx context.Context, s string) (net.Conn, error) {
		// Keep the existing gRPC behavior by using OS defaults for TCP keepalive settings.
		// We are on Go 1.23+ and can use KeepAliveConfig directly instead of the old KeepAlive/Control hacks.
		dialer := &net.Dialer{
			KeepAliveConfig: net.KeepAliveConfig{
				Enable: true,
			},
		}

		var ndt *networkDialTrace
		ctx, ndt = dtrace.beginNetworkDial(ctx)
		conn, dialErr := dialer.DialContext(ctx, "tcp", s)
		dtrace.endNetworkDial(ndt, dialErr)
		return conn, dialErr
	}

	dialOptions := []grpc.DialOption{
		grpcSecureOpt,
		grpc.WithContextDialer(contextDialer),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxInternodeRecvPayloadSize)),
		grpc.WithChainUnaryInterceptor(
			headersInterceptor,
			metrics.NewClientMetricsTrailerPropagatorInterceptor(logger),
			errorInterceptor,
		),
		grpc.WithChainStreamInterceptor(
			interceptor.StreamErrorInterceptor,
		),
		grpc.WithDefaultServiceConfig(DefaultServiceConfig),
		grpc.WithDisableServiceConfig(),
		grpc.WithConnectParams(cp),
	}
	dialOptions = append(dialOptions, opts...)

	return grpc.NewClient(hostName, dialOptions...)
}

func errorInterceptor(
	ctx context.Context,
	method string,
	req, reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	err = serviceerrors.FromStatus(status.Convert(err))
	return err
}

func headersInterceptor(
	ctx context.Context,
	method string,
	req, reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	ctx = headers.Propagate(ctx)
	return invoker(ctx, method, req, reply, cc, opts...)
}
