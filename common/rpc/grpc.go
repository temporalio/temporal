package rpc

import (
	"context"
	"crypto/tls"
	"errors"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/rpc/interceptor"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
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

	// ResourceExhaustedCauseHeader will be added to rpc response if request returns ResourceExhausted error.
	// Value of this header will be ResourceExhaustedCause.
	ResourceExhaustedCauseHeader = "X-Resource-Exhausted-Cause"

	// ResourceExhaustedScopeHeader will be added to rpc response if request returns ResourceExhausted error.
	// Value of this header will be the scope of exhausted resource.
	ResourceExhaustedScopeHeader = "X-Resource-Exhausted-Scope"
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

	dialOptions := []grpc.DialOption{
		grpcSecureOpt,
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

	cc, err := grpc.NewClient(hostName, dialOptions...)
	if err != nil {
		metrics.DialErrorCount.With(metricsHandler).Record(1)
		return nil, err
	}
	watchDialAttempts(cc, metricsHandler)
	return cc, nil
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

func NewFrontendServiceErrorInterceptor(
	logger log.Logger,
) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		_ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {

		resp, err := handler(ctx, req)

		if err == nil {
			return resp, err
		}

		// mask some internal service errors at frontend
		switch err.(type) {
		case *serviceerrors.ShardOwnershipLost:
			err = serviceerror.NewUnavailable("shard unavailable, please backoff and retry")
		case *serviceerror.DataLoss:
			err = serviceerror.NewUnavailable("internal history service error")
		}

		addHeadersForResourceExhausted(ctx, logger, err)

		return resp, err
	}
}

func addHeadersForResourceExhausted(ctx context.Context, logger log.Logger, err error) {
	var reErr *serviceerror.ResourceExhausted
	if errors.As(err, &reErr) {
		headerErr := grpc.SetHeader(ctx, metadata.Pairs(
			ResourceExhaustedCauseHeader, reErr.Cause.String(),
			ResourceExhaustedScopeHeader, reErr.Scope.String(),
		))
		if headerErr != nil {
			logger.Error("Failed to add Resource-Exhausted headers to response", tag.Error(headerErr))
		}
	}
}

// connState is the tiny interface we use in tests. *grpc.ClientConn satisfies it.
type connState interface {
	GetState() connectivity.State
	WaitForStateChange(context.Context, connectivity.State) bool
}

// watchDialAttempts watches the gRPC dial attempts and records metrics
func watchDialAttempts(cc *grpc.ClientConn, mh metrics.Handler) <-chan struct{} {
	return watchDialAttemptsImpl(cc, mh)
}

// watchDialAttemptsImpl is the implementation of watchDialAttempts to make it easier to test.
// It returns a channel that is closed when the shutdown is received.
func watchDialAttemptsImpl(cc connState, mh metrics.Handler) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		// gRPC’s state machine can emit CONNECTING multiple times in a single attempt.
		// inAttempt is used to only start timing when we enter CONNECTING from a non-connecting
		// state, and stop when we leave it for a terminal state (READY, TRANSIENT_FAILURE, SHUTDOWN)
		var inAttempt bool
		// start is the start time for the current attempt
		var start time.Time

		for {
			st := cc.GetState()

			switch st {
			case connectivity.Idle:
				// No action needed for idle state
			case connectivity.Connecting:
				// If we just entered CONNECTING and are not already timing,
				// this marks the start of a new attempt.
				if !inAttempt {
					inAttempt = true
					start = time.Now()
					metrics.DialAttemptsCount.With(mh).Record(1)
				}
			case connectivity.Ready:
				if inAttempt {
					// Attempt succeeded
					metrics.DialSuccessCount.With(mh).Record(1)
					metrics.DialConnectLatency.With(mh).Record(time.Since(start))
					inAttempt = false
				}
			case connectivity.TransientFailure:
				if inAttempt {
					// Attempt failed
					metrics.DialErrorCount.With(mh).Record(1)
					metrics.DialConnectLatency.With(mh).Record(time.Since(start))
					inAttempt = false
				}
			case connectivity.Shutdown:
				// If we shut down while timing an attempt (e.g., CONNECTING -> SHUTDOWN),
				// count it as a failed attempt and exit.
				if inAttempt {
					metrics.DialErrorCount.With(mh).Record(1)
					metrics.DialConnectLatency.With(mh).Record(time.Since(start))
				}
				return
			}

			// Block until the state changes.
			if !cc.WaitForStateChange(context.Background(), st) {
				return
			}
		}
	}()
	return done
}
