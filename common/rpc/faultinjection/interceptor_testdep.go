//go:build test_dep

package faultinjection

import (
	"context"

	"go.temporal.io/server/common/namespace"
	rpcinterceptor "go.temporal.io/server/common/rpc/interceptor"
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
		if generate, ok := getRequestFaultGenerator(testHooks, req); ok {
			if matched, resp, err := generate(ctx, info.FullMethod, req); matched {
				return resp, err
			}
		}
		if generate, ok := testhooks.Get(testHooks, testhooks.RPCRequestFaultGenerator, testhooks.GlobalScope); ok {
			if matched, resp, err := generate(ctx, info.FullMethod, req); matched {
				return resp, err
			}
		}

		// Call handler
		resp, err := handler(ctx, req)

		// Check after handler (can modify response/error)
		if generate, ok := getResponseFaultGenerator(testHooks, req); ok {
			if matched, newResp, newErr := generate(ctx, info.FullMethod, req, resp, err); matched {
				return newResp, newErr
			}
		}
		if generate, ok := testhooks.Get(testHooks, testhooks.RPCResponseFaultGenerator, testhooks.GlobalScope); ok {
			if matched, newResp, newErr := generate(ctx, info.FullMethod, req, resp, err); matched {
				return newResp, newErr
			}
		}
		return resp, err
	}
}

func getRequestFaultGenerator(testHooks testhooks.TestHooks, req any) (func(context.Context, string, any) (bool, any, error), bool) {
	if namespaceID, ok := namespaceIDFromRequest(req); ok {
		return testhooks.Get(testHooks, testhooks.RPCRequestFaultGeneratorByNamespaceID, namespaceID)
	}
	if namespaceName, ok := namespaceNameFromRequest(req); ok {
		return testhooks.Get(testHooks, testhooks.RPCRequestFaultGeneratorByNamespaceName, namespaceName)
	}
	return nil, false
}

func getResponseFaultGenerator(testHooks testhooks.TestHooks, req any) (func(context.Context, string, any, any, error) (bool, any, error), bool) {
	if namespaceID, ok := namespaceIDFromRequest(req); ok {
		return testhooks.Get(testHooks, testhooks.RPCResponseFaultGeneratorByNamespaceID, namespaceID)
	}
	if namespaceName, ok := namespaceNameFromRequest(req); ok {
		return testhooks.Get(testHooks, testhooks.RPCResponseFaultGeneratorByNamespaceName, namespaceName)
	}
	return nil, false
}

func namespaceIDFromRequest(req any) (namespace.ID, bool) {
	request, ok := req.(rpcinterceptor.NamespaceIDGetter)
	if !ok || request.GetNamespaceId() == "" {
		return "", false
	}
	return namespace.ID(request.GetNamespaceId()), true
}

func namespaceNameFromRequest(req any) (namespace.Name, bool) {
	request, ok := req.(rpcinterceptor.NamespaceNameGetter)
	if !ok || request.GetNamespace() == "" {
		return "", false
	}
	return namespace.Name(request.GetNamespace()), true
}
