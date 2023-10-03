package sdk

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type HeadersProvider interface {
	GetHeaders(ctx context.Context) (map[string]string, error)
}

func HeadersProviderInterceptor(headersProvider HeadersProvider) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		headers, err := headersProvider.GetHeaders(ctx)
		if err != nil {
			return err
		}
		for k, v := range headers {
			ctx = metadata.AppendToOutgoingContext(ctx, k, v)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
