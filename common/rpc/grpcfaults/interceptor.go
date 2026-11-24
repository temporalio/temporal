package grpcfaults

import (
	"context"

	"google.golang.org/grpc"
)

// UnaryServerInterceptor returns a unary server interceptor that checks the generator for faults
// before and after the handler.
//
// This allows tests to inspect requests/responses and inject faults on demand.
//
// Behavior:
// - If no generator is configured, no interceptor is installed.
// - The generator is checked before the handler. A fault error skips the handler.
// - The generator is checked after the handler with its response and error. A fault error replaces them.
// - If no fault matches, the handler's response and error are returned.
func UnaryServerInterceptor(generator Generator) grpc.UnaryServerInterceptor {
	if generator == nil {
		return nil
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Check before the handler. A fault can short-circuit the call.
		if faultErr := generator.GenerateRequest(ctx, info.FullMethod, req); faultErr != nil {
			return nil, faultErr
		}

		// Call the handler.
		resp, err := handler(ctx, req)

		// Check after the handler. A fault can replace the response or error.
		if faultErr := generator.GenerateResponse(ctx, info.FullMethod, req, resp, err); faultErr != nil {
			return nil, faultErr
		}
		return resp, err
	}
}
