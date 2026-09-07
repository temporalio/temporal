package grpcfaults

import (
	"context"

	"google.golang.org/grpc"
)

// Generator checks for gRPC faults before and after a handler runs.
type Generator interface {
	GenerateRequest(ctx context.Context, fullMethod string, req any) *Outcome
	GenerateResponse(ctx context.Context, fullMethod string, req, resp any, err error) *Outcome
}

// UnaryServerInterceptor returns a unary server interceptor that checks for
// dynamically registered fault injection callbacks before and after the handler.
//
// This allows tests and configured generators to register callbacks that can
// inspect requests/responses and inject faults on demand.
//
// Behavior:
// - If no request generator is registered, the handler proceeds normally.
// - Request callbacks are checked before the handler. If matched, the handler is skipped.
// - Response callbacks are checked after the handler with actual resp/err. If matched, returned values are used.
// - If no callbacks match, the handler's response/error is returned.
func UnaryServerInterceptor(generator Generator) grpc.UnaryServerInterceptor {
	if generator == nil {
		return nil
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Check before handler (can short-circuit)
		if outcome := generator.GenerateRequest(ctx, info.FullMethod, req); outcome != nil {
			return outcome.Response, outcome.Error
		}

		// Call handler
		resp, err := handler(ctx, req)

		// Check after handler (can modify response/error)
		if outcome := generator.GenerateResponse(ctx, info.FullMethod, req, resp, err); outcome != nil {
			return outcome.Response, outcome.Error
		}
		return resp, err
	}
}
