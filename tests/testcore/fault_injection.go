package testcore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"go.temporal.io/server/common/namespace"
	rpcfaultinjection "go.temporal.io/server/common/rpc/faultinjection"
)

// RPCRequestFault determines whether a fault should be injected before an RPC handler runs.
// Return the error to inject, or nil to not inject a fault.
type RPCRequestFault func(req any) error

// RPCResponseFault determines whether a fault should be injected after an RPC handler runs.
// Return the error to inject, or nil to preserve the handler response and error.
type RPCResponseFault func(req, resp any, handlerErr error) error

// RPCFaultOption configures the behavior of RPC fault injection.
type RPCFaultOption func(*rpcFaultOptions)

type rpcFault func(req, resp any, err error) error

type rpcFaultCallback func(req, resp any, err error) (bool, any, error)

type registerRPCFault func(*rpcfaultinjection.RPCFaultGenerator, namespace.ID, namespace.Name, rpcFaultCallback) func()

type rpcFaultOptions struct {
	namespaceID   string
	namespaceName string
}

func (o rpcFaultOptions) namespaceScopes() (namespace.ID, namespace.Name, bool) {
	namespaceID := namespace.ID(o.namespaceID)
	namespaceName := namespace.Name(o.namespaceName)
	return namespaceID, namespaceName, namespaceID != "" || namespaceName != ""
}

// WithNamespaceID matches requests that expose the given namespace ID.
// When combined with [WithNamespaceName], requests that expose an ID use the ID scope.
func WithNamespaceID(id string) RPCFaultOption {
	return func(o *rpcFaultOptions) {
		o.namespaceID = id
	}
}

// WithNamespaceName matches requests that expose the given namespace name.
// When combined with [WithNamespaceID], requests without an ID use the name scope.
func WithNamespaceName(name string) RPCFaultOption {
	return func(o *rpcFaultOptions) {
		o.namespaceName = name
	}
}

// InjectRPCRequestFault registers a pre-handler fault injection that applies to all services
// (frontend, history, matching). The fault function determines which requests
// trigger a fault and what error to return.
//
// Prefer [TestEnv.InjectRPCRequestFault], which scopes the fault to the test's
// namespace. Direct use requires [WithNamespaceID] or [WithNamespaceName]. Only
// unary RPCs are intercepted; streaming RPCs are unaffected.
//
// Returns a cleanup function that disables the fault injection when called.
// The test fails if the fault is never injected before the test completes.
//
// Example:
//
//	testcore.InjectRPCRequestFault(s.T(), s.GetTestCluster(),
//	    func(req any) error {
//	        if _, ok := req.(*matchingservice.AddWorkflowTaskRequest); ok {
//	            return serviceerror.NewNotFound("injected fault")
//	        }
//	        return nil
//	    }, testcore.WithNamespaceID("namespace-id"))
func InjectRPCRequestFault(t testing.TB, tc *TestCluster, fault RPCRequestFault, opts ...RPCFaultOption) func() {
	return injectRPCFault(t, tc, func(req, _ any, _ error) error {
		return fault(req)
	}, registerRPCRequestFault, opts...)
}

// InjectRPCResponseFault registers a post-handler fault injection that applies to all services
// (frontend, history, matching). The fault function receives the handler response and error.
// Returning an error discards the handler response and returns the injected error.
//
// Prefer [TestEnv.InjectRPCResponseFault], which scopes the fault to the test's
// namespace. Direct use requires [WithNamespaceID] or [WithNamespaceName]. Only
// unary RPCs are intercepted; streaming RPCs are unaffected.
//
// Returns a cleanup function that disables the fault injection when called.
// The test fails if the fault is never injected before the test completes.
func InjectRPCResponseFault(t testing.TB, tc *TestCluster, fault RPCResponseFault, opts ...RPCFaultOption) func() {
	return injectRPCFault(t, tc, rpcFault(fault), registerRPCResponseFault, opts...)
}

func registerRPCRequestFault(generator *rpcfaultinjection.RPCFaultGenerator, namespaceID namespace.ID, namespaceName namespace.Name, callback rpcFaultCallback) func() {
	return generator.RegisterRequestCallback(namespaceID, namespaceName, func(_ context.Context, _ string, req any) (bool, any, error) {
		return callback(req, nil, nil)
	})
}

func registerRPCResponseFault(generator *rpcfaultinjection.RPCFaultGenerator, namespaceID namespace.ID, namespaceName namespace.Name, callback rpcFaultCallback) func() {
	return generator.RegisterResponseCallback(namespaceID, namespaceName, func(_ context.Context, _ string, req, resp any, err error) (bool, any, error) {
		return callback(req, resp, err)
	})
}

func injectRPCFault(t testing.TB, tc *TestCluster, fault rpcFault, register registerRPCFault, opts ...RPCFaultOption) func() {
	t.Helper()

	var options rpcFaultOptions
	for _, opt := range opts {
		opt(&options)
	}
	namespaceID, namespaceName, ok := options.namespaceScopes()
	if !ok {
		t.Fatal("RPC fault injection requires WithNamespaceID or WithNamespaceName")
		return func() {}
	}

	generator := tc.Host().GetFaultInjector()
	if generator == nil {
		t.Fatal("fault injector is nil")
		return func() {}
	}

	var fired atomic.Bool
	var logMu sync.Mutex
	loggingEnabled := true

	unregister := register(generator, namespaceID, namespaceName, func(req, resp any, err error) (bool, any, error) {
		if injectedErr := fault(req, resp, err); injectedErr != nil {
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
