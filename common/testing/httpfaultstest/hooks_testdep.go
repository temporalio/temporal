//go:build test_dep

package httpfaultstest

import (
	"net/http"

	"go.temporal.io/server/common/rpc/httpfaults"
	"go.temporal.io/server/common/testing/faultinjectiontest"
	commontesthooks "go.temporal.io/server/common/testing/testhooks"
)

// NewCallbackGenerator returns a generator that uses namespace-scoped test hooks.
func NewCallbackGenerator(testHooks commontesthooks.TestHooks) *httpfaults.CallbackGenerator {
	return httpfaults.NewCallbackGeneratorWithHooks(newAdapter(testHooks))
}

// NewGenerator returns a generator that uses namespace-scoped test hooks.
func NewGenerator(testHooks commontesthooks.TestHooks) httpfaults.Generator {
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
	})
}
