package testcore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	rpcfaultinjection "go.temporal.io/server/common/rpc/faultinjection"
)

// RPCRequestFault determines whether a fault should be injected before an RPC handler runs.
// Return the error to inject, or nil to not inject a fault.
type RPCRequestFault func(req any) error

// RPCResponseFault determines whether a fault should be injected after an RPC handler runs.
// Return the error to inject, or nil to preserve the handler response and error.
type RPCResponseFault func(req, resp any, handlerErr error) error

// InjectRPCRequestFault registers a pre-handler fault injection scoped to this test's namespace.
// Requests match either the namespace ID or name filter, depending on which
// namespace field they expose. Requests without either field are ignored.
// Returns a cleanup function that disables the fault.
func (e *TestEnv) InjectRPCRequestFault(fault RPCRequestFault) func() {
	scope := rpcfaultinjection.RPCFaultScope{
		NamespaceID:   e.nsID,
		NamespaceName: e.nsName,
	}
	return injectRPCFault(e.t, func(inject func(any, error) (bool, any, error)) func() {
		return e.GetTestCluster().Host().GetRPCFaultGenerator().RegisterRequestCallback(scope, func(_ context.Context, _ string, req any) (bool, any, error) {
			return inject(req, fault(req))
		})
	})
}

// InjectRPCResponseFault registers a post-handler fault injection scoped to this test's namespace.
// Requests match either the namespace ID or name filter, depending on which
// namespace field they expose. Requests without either field are ignored.
// Returns a cleanup function that disables the fault.
func (e *TestEnv) InjectRPCResponseFault(fault RPCResponseFault) func() {
	scope := rpcfaultinjection.RPCFaultScope{
		NamespaceID:   e.nsID,
		NamespaceName: e.nsName,
	}
	return injectRPCFault(e.t, func(inject func(any, error) (bool, any, error)) func() {
		return e.GetTestCluster().Host().GetRPCFaultGenerator().RegisterResponseCallback(scope, func(_ context.Context, _ string, req, resp any, err error) (bool, any, error) {
			return inject(req, fault(req, resp, err))
		})
	})
}

func injectRPCFault(t testing.TB, register func(func(any, error) (bool, any, error)) func()) func() {
	t.Helper()

	var fired atomic.Bool
	var logMu sync.Mutex
	loggingEnabled := true

	unregister := register(func(req any, injectedErr error) (bool, any, error) {
		if injectedErr != nil {
			fired.Store(true)
			logMu.Lock()
			if loggingEnabled {
				t.Logf("Fault injection fired: %T", req)
			}
			logMu.Unlock()
			return true, nil, injectedErr
		}
		return false, nil, nil
	})

	t.Cleanup(func() {
		logMu.Lock()
		loggingEnabled = false
		logMu.Unlock()
		unregister()
		if !fired.Load() {
			t.Error("fault injection was registered but never fired - the fault was never injected")
		}
	})

	return unregister
}
