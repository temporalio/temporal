//go:build test_dep

package faultinjection

import (
	"context"

	"go.temporal.io/server/common/testing/testhooks"
)

type testHookGenerator struct {
	testHooks testhooks.TestHooks
}

// NewTestHookGenerator creates a Generator backed by namespace-scoped test hooks.
func NewTestHookGenerator(testHooks testhooks.TestHooks) Generator {
	return testHookGenerator{testHooks: testHooks}
}

func (g testHookGenerator) GenerateRequest(ctx context.Context, fullMethod string, req any) (bool, any, error) {
	if generate, ok := getRequestFaultGenerator(g.testHooks, req); ok {
		return generate(ctx, fullMethod, req)
	}
	return false, nil, nil
}

func (g testHookGenerator) GenerateResponse(ctx context.Context, fullMethod string, req, resp any, err error) (bool, any, error) {
	if generate, ok := getResponseFaultGenerator(g.testHooks, req); ok {
		return generate(ctx, fullMethod, req, resp, err)
	}
	return false, nil, nil
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
