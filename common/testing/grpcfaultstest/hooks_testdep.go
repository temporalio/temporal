//go:build test_dep

package grpcfaultstest

import (
	"context"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/grpcfaults"
	rpcinterceptor "go.temporal.io/server/common/rpc/interceptor"
	commontesthooks "go.temporal.io/server/common/testing/testhooks"
)

type adapter struct {
	testHooks commontesthooks.TestHooks
}

// NewCallbackGenerator creates a CallbackGenerator connected to namespace-scoped test hooks.
func NewCallbackGenerator(testHooks commontesthooks.TestHooks) *grpcfaults.CallbackGenerator {
	return grpcfaults.NewCallbackGeneratorWithHooks(adapter{testHooks: testHooks})
}

func (a adapter) InstallRequestCallback(scope grpcfaults.Scope, callback grpcfaults.RequestCallback) func() {
	switch {
	case scope.NamespaceID != "":
		return commontesthooks.Set(a.testHooks, commontesthooks.GRPCRequestFaultGeneratorByNamespaceID, callback, scope.NamespaceID)
	case scope.NamespaceName != "":
		return commontesthooks.Set(a.testHooks, commontesthooks.GRPCRequestFaultGeneratorByNamespaceName, callback, scope.NamespaceName)
	default:
		return func() {}
	}
}

func (a adapter) InstallResponseCallback(scope grpcfaults.Scope, callback grpcfaults.ResponseCallback) func() {
	switch {
	case scope.NamespaceID != "":
		return commontesthooks.Set(a.testHooks, commontesthooks.GRPCResponseFaultGeneratorByNamespaceID, callback, scope.NamespaceID)
	case scope.NamespaceName != "":
		return commontesthooks.Set(a.testHooks, commontesthooks.GRPCResponseFaultGeneratorByNamespaceName, callback, scope.NamespaceName)
	default:
		return func() {}
	}
}

// NewGenerator creates a Generator backed by namespace-scoped test hooks.
func NewGenerator(testHooks commontesthooks.TestHooks) grpcfaults.Generator {
	return adapter{testHooks: testHooks}
}

func (a adapter) GenerateRequest(ctx context.Context, fullMethod string, req any) *grpcfaults.Outcome {
	if generate, ok := getRequestFaultGenerator(a.testHooks, req); ok {
		return generate(ctx, fullMethod, req)
	}
	return nil
}

func (a adapter) GenerateResponse(ctx context.Context, fullMethod string, req, resp any, err error) *grpcfaults.Outcome {
	if generate, ok := getResponseFaultGenerator(a.testHooks, req); ok {
		return generate(ctx, fullMethod, req, resp, err)
	}
	return nil
}

func getRequestFaultGenerator(testHooks commontesthooks.TestHooks, req any) (grpcfaults.RequestCallback, bool) {
	if namespaceID, ok := namespaceIDFromRequest(req); ok {
		return commontesthooks.Get(testHooks, commontesthooks.GRPCRequestFaultGeneratorByNamespaceID, namespaceID)
	}
	if namespaceName, ok := namespaceNameFromRequest(req); ok {
		return commontesthooks.Get(testHooks, commontesthooks.GRPCRequestFaultGeneratorByNamespaceName, namespaceName)
	}
	return nil, false
}

func getResponseFaultGenerator(testHooks commontesthooks.TestHooks, req any) (grpcfaults.ResponseCallback, bool) {
	if namespaceID, ok := namespaceIDFromRequest(req); ok {
		return commontesthooks.Get(testHooks, commontesthooks.GRPCResponseFaultGeneratorByNamespaceID, namespaceID)
	}
	if namespaceName, ok := namespaceNameFromRequest(req); ok {
		return commontesthooks.Get(testHooks, commontesthooks.GRPCResponseFaultGeneratorByNamespaceName, namespaceName)
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
