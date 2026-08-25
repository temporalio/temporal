//go:build test_dep

package faultinjection

import (
	"context"

	"go.temporal.io/server/common/testing/testhooks"
	"google.golang.org/grpc"
)

// GRPCUnaryServerInterceptor returns a unary server interceptor that checks for
// dynamically registered fault injection callbacks before and after the handler.
//
// This is primarily used for testing, allowing tests to register callbacks that
// can inspect requests/responses and inject faults on demand.
//
// Behavior:
// - If no request generator is registered, the handler proceeds normally.
// - Request callbacks are checked before the handler. If matched, the handler is skipped.
// - Response callbacks are checked after the handler with actual resp/err. If matched, returned values are used.
// - If no callbacks match, the handler's response/error is returned.
func GRPCUnaryServerInterceptor(testHooks testhooks.TestHooks) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Check before handler (can short-circuit)
		if generate, ok := testhooks.Get(testHooks, testhooks.RPCRequestFaultGenerator, testhooks.GlobalScope); ok {
			if matched, resp, err := generate(ctx, info.FullMethod, req); matched {
				return resp, err
			}
		}

		// Call handler
		resp, err := handler(ctx, req)

		// Check after handler (can modify response/error)
		if generate, ok := testhooks.Get(testHooks, testhooks.RPCResponseFaultGenerator, testhooks.GlobalScope); ok {
			if matched, newResp, newErr := generate(ctx, info.FullMethod, req, resp, err); matched {
				return newResp, newErr
			}
		}

		return resp, err
	}
}
