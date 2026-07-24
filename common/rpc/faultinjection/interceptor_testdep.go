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
// - If no generator is registered, the handler proceeds normally.
// - Callbacks are checked before handler (resp=nil, err=nil). If matched, handler is skipped.
// - Callbacks are checked after handler with actual resp/err. If matched, returned values are used.
// - If no callbacks match, the handler's response/error is returned.
func GRPCUnaryServerInterceptor(testHooks testhooks.TestHooks) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		generate, ok := testhooks.Get(testHooks, testhooks.RPCFaultGenerator, testhooks.GlobalScope)
		if !ok {
			return handler(ctx, req)
		}

		// Check before handler (can short-circuit)
		if matched, resp, err := generate(ctx, info.FullMethod, req, nil, nil); matched {
			return resp, err
		}

		// Call handler
		resp, err := handler(ctx, req)

		// Check after handler (can modify response/error)
		if matched, newResp, newErr := generate(ctx, info.FullMethod, req, resp, err); matched {
			return newResp, newErr
		}

		return resp, err
	}
}
