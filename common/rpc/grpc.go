// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package rpc

import (
	"context"
	"crypto/tls"
	"errors"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/rpc/interceptor"
	serviceerrors "go.temporal.io/server/common/serviceerror"
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

	// MaxHTTPAPIRequestBytes is the maximum number of bytes a Nexus HTTP API request can have. Because the body is
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
// e.g. to use dns resolver, a "dns:///" prefix should be applied to the target.
func Dial(hostName string, tlsConfig *tls.Config, logger log.Logger, interceptors ...grpc.UnaryClientInterceptor) (*grpc.ClientConn, error) {
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
			append(
				interceptors,
				headersInterceptor,
				metrics.NewClientMetricsTrailerPropagatorInterceptor(logger),
				errorInterceptor,
			)...,
		),
		grpc.WithChainStreamInterceptor(
			interceptor.StreamErrorInterceptor,
		),
		grpc.WithDefaultServiceConfig(DefaultServiceConfig),
		grpc.WithDisableServiceConfig(),
		grpc.WithConnectParams(cp),
	}

	return grpc.Dial(
		hostName,
		dialOptions...,
	)
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

func addHeadersForResourceExhausted(ctx context.Context, logger log.Logger, err error) {
	var reErr *serviceerror.ResourceExhausted
	if errors.As(err, &reErr) {
		headerErr := grpc.SetHeader(ctx, metadata.Pairs(
			ResourceExhaustedCauseHeader, reErr.Cause.String(),
			ResourceExhaustedScopeHeader, reErr.Scope.String(),
		))
		if headerErr != nil {
			logger.Error("Failed to add Resource-Exhausted headers to response", tag.Error(err))
		}
	}
}

func NewServiceErrorInterceptor(
	logger log.Logger,
) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		_ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {

		resp, err := handler(ctx, req)
		if err != nil {
			addHeadersForResourceExhausted(ctx, logger, err)
		}
		return resp, serviceerror.ToStatus(err).Err()
	}

}

func FrontendErrorInterceptor(
	ctx context.Context,
	req interface{},
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	resp, err := handler(ctx, req)

	// mask some internal errors at frontend
	if _, ok := err.(*serviceerrors.ShardOwnershipLost); ok {
		err = serviceerror.NewUnavailable("shard unavailable, please backoff and retry")
	}
	return resp, serviceerror.ToStatus(err).Err()
}
