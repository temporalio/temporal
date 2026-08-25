package faultinjection

import (
	"context"

	"google.golang.org/grpc"
)

// Generator checks for RPC faults before and after a handler runs.
type Generator interface {
	GenerateRequest(context.Context, string, any) (bool, any, error)
	GenerateResponse(context.Context, string, any, any, error) (bool, any, error)
}

// GRPCUnaryServerInterceptor returns a unary server interceptor that checks for
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
func GRPCUnaryServerInterceptor(generator Generator) grpc.UnaryServerInterceptor {
	if generator == nil {
		return nil
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Check before handler (can short-circuit)
		if matched, resp, err := generator.GenerateRequest(ctx, info.FullMethod, req); matched {
			return resp, err
		}

		// Call handler
		resp, err := handler(ctx, req)

		// Check after handler (can modify response/error)
		if matched, newResp, newErr := generator.GenerateResponse(ctx, info.FullMethod, req, resp, err); matched {
			return newResp, newErr
		}
		return resp, err
	}
}
