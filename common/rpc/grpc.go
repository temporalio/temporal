// Copyright (c) 2020 Temporal Technologies, Inc.

package rpc

import (
	"context"

	"github.com/gogo/status"
	"go.temporal.io/temporal-proto/serviceerror"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal/common/headers"
)

const (
	// DefaultServiceConfig is a default gRPC connection service config which enables DNS round robin between IPs.
	// To use DNS resolver, a "dns:///" prefix should be applied to the hostPort
	// https://github.com/grpc/grpc/blob/master/doc/naming.md
	DefaultServiceConfig = `{"loadBalancingConfig": [{"round_robin":{}}]}`
)

// Dial creates a client connection to the given target with default options.
// The hostName syntax is defined in
// https://github.com/grpc/grpc/blob/master/doc/naming.md.
// e.g. to use dns resolver, a "dns:///" prefix should be applied to the target.
func Dial(hostName string) (*grpc.ClientConn, error) {
	return grpc.Dial(hostName,
		grpc.WithInsecure(),
		grpc.WithChainUnaryInterceptor(
			versionHeadersInterceptor,
			errorInterceptor),
		grpc.WithDefaultServiceConfig(DefaultServiceConfig),
		grpc.WithDisableServiceConfig(),
	)
}

func errorInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	err = serviceerror.FromStatus(status.Convert(err))
	return err
}

func versionHeadersInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ctx = headers.PropagateVersions(ctx)
	return invoker(ctx, method, req, reply, cc, opts...)
}
