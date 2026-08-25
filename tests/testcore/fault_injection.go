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

// RPCFaultScope identifies a namespace by ID, name, or both.
type RPCFaultScope = rpcfaultinjection.RPCFaultScope

// InjectRPCRequestFault registers a pre-handler fault injection that applies to all services
// (frontend, history, matching). The fault function determines which requests
// trigger a fault and what error to return.
//
// Prefer [TestEnv.InjectRPCRequestFault], which scopes the fault to the test's
// namespace. Direct use requires a non-empty [RPCFaultScope]. Only unary RPCs
// are intercepted; streaming RPCs are unaffected.
//
// Returns a cleanup function that disables the fault injection when called.
// The test fails if the fault is never injected before the test completes.
//
// Example:
//
//	testcore.InjectRPCRequestFault(s.T(), s.GetTestCluster(),
//	    testcore.RPCFaultScope{NamespaceID: "namespace-id"},
//	    func(req any) error {
//	        if _, ok := req.(*matchingservice.AddWorkflowTaskRequest); ok {
//	            return serviceerror.NewNotFound("injected fault")
//	        }
//	        return nil
//	    })
func InjectRPCRequestFault(t testing.TB, tc *TestCluster, scope RPCFaultScope, fault RPCRequestFault) func() {
	return injectRPCFault(t, scope, func(inject func(any, error) (bool, any, error)) func() {
		return tc.Host().GetRPCFaultGenerator().RegisterRequestCallback(scope, func(_ context.Context, _ string, req any) (bool, any, error) {
			return inject(req, fault(req))
		})
	})
}

// InjectRPCResponseFault registers a post-handler fault injection that applies to all services
// (frontend, history, matching). The fault function receives the handler response and error.
// Returning an error discards the handler response and returns the injected error.
//
// Prefer [TestEnv.InjectRPCResponseFault], which scopes the fault to the test's
// namespace. Direct use requires a non-empty [RPCFaultScope]. Only unary RPCs
// are intercepted; streaming RPCs are unaffected.
//
// Returns a cleanup function that disables the fault injection when called.
// The test fails if the fault is never injected before the test completes.
func InjectRPCResponseFault(t testing.TB, tc *TestCluster, scope RPCFaultScope, fault RPCResponseFault) func() {
	return injectRPCFault(t, scope, func(inject func(any, error) (bool, any, error)) func() {
		return tc.Host().GetRPCFaultGenerator().RegisterResponseCallback(scope, func(_ context.Context, _ string, req, resp any, err error) (bool, any, error) {
			return inject(req, fault(req, resp, err))
		})
	})
}

func injectRPCFault(t testing.TB, scope RPCFaultScope, register func(func(any, error) (bool, any, error)) func()) func() {
	t.Helper()

	if scope.NamespaceID == "" && scope.NamespaceName == "" {
		t.Fatal("RPC fault injection requires a namespace scope")
		return func() {}
	}

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
