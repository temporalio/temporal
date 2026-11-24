package grpcfaultstest

import (
	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/testing/faultinjectiontest"
	commontesthooks "go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/testx/failpoint"
)

// NewCallbackGenerator creates a CallbackGenerator connected to namespace-scoped test hooks.
// Without test hooks, it creates a CallbackGenerator without them.
func NewCallbackGenerator(testHooks commontesthooks.TestHooks) *grpcfaults.CallbackGenerator {
	if !testHooks.Enabled() {
		return grpcfaults.NewCallbackGenerator()
	}
	return grpcfaults.NewCallbackGeneratorWithHooks(newAdapter(testHooks))
}

// NewGenerator creates a Generator backed by namespace-scoped test hooks. It returns nil without
// test hooks, so no fault interceptor is installed.
func NewGenerator(testHooks commontesthooks.TestHooks) grpcfaults.Generator {
	if !testHooks.Enabled() {
		return nil
	}
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
		Failpoint: failpoint.GRPCKey,
	})
}
