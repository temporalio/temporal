//go:build test_dep

package grpcfaultstest

import (
	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/testing/faultinjectiontest"
	commontesthooks "go.temporal.io/server/common/testing/testhooks"
)

// NewCallbackGenerator creates a CallbackGenerator connected to namespace-scoped test hooks.
func NewCallbackGenerator(testHooks commontesthooks.TestHooks) *grpcfaults.CallbackGenerator {
	return grpcfaults.NewCallbackGeneratorWithHooks(newAdapter(testHooks))
}

// NewGenerator creates a Generator backed by namespace-scoped test hooks.
func NewGenerator(testHooks commontesthooks.TestHooks) grpcfaults.Generator {
	return newAdapter(testHooks)
}

func newAdapter(testHooks commontesthooks.TestHooks) faultinjectiontest.Adapter[any, any] {
	return faultinjectiontest.NewAdapter(testHooks, faultinjectiontest.HookKeys[any, any]{
		Request: faultinjectiontest.Keys[grpcfaults.RequestCallback]{
			ByNamespaceID:   commontesthooks.GRPCRequestFaultGeneratorByNamespaceID,
			ByNamespaceName: commontesthooks.GRPCRequestFaultGeneratorByNamespaceName,
		},
		Response: faultinjectiontest.Keys[grpcfaults.ResponseCallback]{
			ByNamespaceID:   commontesthooks.GRPCResponseFaultGeneratorByNamespaceID,
			ByNamespaceName: commontesthooks.GRPCResponseFaultGeneratorByNamespaceName,
		},
	})
}
