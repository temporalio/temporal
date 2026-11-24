package httpfaultstest

import (
	"net/http"

	"go.temporal.io/server/common/rpc/httpfaults"
	"go.temporal.io/server/common/testing/faultinjectiontest"
	commontesthooks "go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/testx/failpoint"
)

// NewCallbackGenerator returns a generator that uses namespace-scoped test hooks. Without test
// hooks, it returns a callback generator without them.
func NewCallbackGenerator(testHooks commontesthooks.TestHooks) *httpfaults.CallbackGenerator {
	if !testHooks.Enabled() {
		return httpfaults.NewCallbackGenerator()
	}
	return httpfaults.NewCallbackGeneratorWithHooks(newAdapter(testHooks))
}

// NewGenerator returns a generator that uses namespace-scoped test hooks. It returns nil without
// test hooks, so no HTTP faults are injected.
func NewGenerator(testHooks commontesthooks.TestHooks) httpfaults.Generator {
	if !testHooks.Enabled() {
		return nil
	}
	return newAdapter(testHooks)
}

func newAdapter(testHooks commontesthooks.TestHooks) faultinjectiontest.Adapter[*httpfaults.Request, *http.Response] {
	return faultinjectiontest.NewAdapter(testHooks, faultinjectiontest.HookKeys[*httpfaults.Request, *http.Response]{
		Request: faultinjectiontest.Keys[httpfaults.RequestCallback]{
			ByNamespaceID:   commontesthooks.HTTPRequestFaultGeneratorByNamespaceID,
			ByNamespaceName: commontesthooks.HTTPRequestFaultGeneratorByNamespaceName,
		},
		Response: faultinjectiontest.Keys[httpfaults.ResponseCallback]{
			ByNamespaceID:   commontesthooks.HTTPResponseFaultGeneratorByNamespaceID,
			ByNamespaceName: commontesthooks.HTTPResponseFaultGeneratorByNamespaceName,
		},
		Failpoint: failpoint.HTTPKey,
	})
}
