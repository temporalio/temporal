package testcore

import (
	"context"
	"sync/atomic"
	"testing"
)

// RPCFault determines whether a fault should be injected for a given RPC.
// It receives the request, response, and error.
// For pre-handler calls, resp and err are nil.
// Return the error to inject, or nil to not inject a fault.
type RPCFault func(req, resp any, err error) error

// InjectRPCFault registers a fault injection that applies to all services
// (frontend, history, matching). The fault function determines which requests
// trigger a fault and what error to return.
//
// The fault function is called twice per RPC: before the handler (resp=nil, err=nil)
// and after. Returning an error before handler short-circuits; returning after
// modifies the response.
//
// Returns a cleanup function that disables the fault injection when called.
// The test fails if the fault is never injected before the test completes.
//
// Example:
//
//	testcore.InjectRPCFault(s.T(), s.GetTestCluster(),
//	    func(req, _ any, _ error) error {
//	        r, ok := req.(*matchingservice.AddWorkflowTaskRequest)
//	        if ok {
//	            return serviceerror.NewNotFound("injected fault")
//	        }
//	        return nil
//	    })
func InjectRPCFault(t testing.TB, tc *TestCluster, fault RPCFault) func() {
	t.Helper()

	generator := tc.Host().GetFaultInjector()
	if generator == nil {
		t.Fatal("fault injector is nil")
		return func() {}
	}

	var fired atomic.Bool
	var cleared atomic.Bool

	unregister := generator.RegisterCallback(func(ctx context.Context, fullMethod string, req, resp any, err error) (bool, any, error) {
		if cleared.Load() {
			return false, nil, nil
		}

		if injectedErr := fault(req, resp, err); injectedErr != nil {
			fired.Store(true)
			t.Logf("Fault injection fired: %T", req)
			return true, nil, injectedErr
		}
		return false, nil, nil
	})

	t.Cleanup(func() {
		unregister()
		if !fired.Load() {
			t.Error("fault injection was registered but never fired - the fault was never injected")
		}
	})

	return func() {
		cleared.Store(true)
	}
}
