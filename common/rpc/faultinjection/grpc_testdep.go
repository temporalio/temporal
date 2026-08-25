//go:build test_dep

package faultinjection

import (
	"context"

	"go.temporal.io/server/common/testing/testhooks"
)

// NewTestRPCFaultGenerator creates an RPCFaultGenerator connected to namespace-scoped test hooks.
func NewTestRPCFaultGenerator(testHooks testhooks.TestHooks) *RPCFaultGenerator {
	generator := NewRPCFaultGenerator()
	generator.installHook = func(scope rpcCallbackScope) func() {
		switch {
		case scope.NamespaceID != "" && scope.stage == rpcFaultStageRequest:
			return testhooks.Set(testHooks, testhooks.RPCRequestFaultGeneratorByNamespaceID, func(ctx context.Context, fullMethod string, req any) (bool, any, error) {
				return generator.generate(ctx, fullMethod, scope, req, nil, nil)
			}, scope.NamespaceID)
		case scope.NamespaceID != "":
			return testhooks.Set(testHooks, testhooks.RPCResponseFaultGeneratorByNamespaceID, func(ctx context.Context, fullMethod string, req, resp any, err error) (bool, any, error) {
				return generator.generate(ctx, fullMethod, scope, req, resp, err)
			}, scope.NamespaceID)
		case scope.NamespaceName != "" && scope.stage == rpcFaultStageRequest:
			return testhooks.Set(testHooks, testhooks.RPCRequestFaultGeneratorByNamespaceName, func(ctx context.Context, fullMethod string, req any) (bool, any, error) {
				return generator.generate(ctx, fullMethod, scope, req, nil, nil)
			}, scope.NamespaceName)
		case scope.NamespaceName != "":
			return testhooks.Set(testHooks, testhooks.RPCResponseFaultGeneratorByNamespaceName, func(ctx context.Context, fullMethod string, req, resp any, err error) (bool, any, error) {
				return generator.generate(ctx, fullMethod, scope, req, resp, err)
			}, scope.NamespaceName)
		default:
			return func() {}
		}
	}
	return generator
}
