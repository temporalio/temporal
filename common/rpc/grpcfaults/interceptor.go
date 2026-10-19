package grpcfaults

import (
	"context"

	"go.temporal.io/server/common/faultinjection"
	"google.golang.org/grpc"
)

// UnaryServerInterceptor returns a unary server interceptor that checks for
// dynamically registered fault injection callbacks before and after the handler.
//
// This allows tests and configured generators to register callbacks that can
// inspect requests/responses and inject faults on demand.
//
// Behavior:
// - If no request generator is registered, the handler proceeds normally.
// - Request callbacks are checked before the handler. A matched fault can skip the handler.
// - Response callbacks are checked after the handler with its response and error. A matched fault can replace them.
// - If no callbacks match, the handler's response and error are returned.
func UnaryServerInterceptor(generator Generator) grpc.UnaryServerInterceptor {
	if generator == nil {
		return nil
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return faultinjection.Invoke(ctx, generator, info.FullMethod, req, func() (any, error) { return handler(ctx, req) })
	}
}
