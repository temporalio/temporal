package grpcfaults

import (
	"context"

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
		// Check before the handler. A fault can short-circuit the call.
		if outcome := generator.GenerateRequest(ctx, info.FullMethod, req); outcome != nil {
			return outcome.Response, outcome.Error
		}

		// Call the handler.
		resp, err := handler(ctx, req)

		// Check after the handler. A fault can replace the response or error.
		if outcome := generator.GenerateResponse(ctx, info.FullMethod, req, resp, err); outcome != nil {
			return outcome.Response, outcome.Error
		}
		return resp, err
	}
}
