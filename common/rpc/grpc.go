package rpc

import (
	"context"

	"github.com/gogo/status"
	"go.temporal.io/temporal-proto/serviceerror"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal/common/headers"
)

// Dial creates a client connection to the given target with default options.
func Dial(hostName string) (*grpc.ClientConn, error) {
	return grpc.Dial(hostName,
		grpc.WithInsecure(),
		grpc.WithChainUnaryInterceptor(
			versionHeadersInterceptor,
			errorInterceptor))
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
