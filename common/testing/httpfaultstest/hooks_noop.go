//go:build !test_dep

package httpfaultstest

import (
	"go.temporal.io/server/common/rpc/httpfaults"
	commontesthooks "go.temporal.io/server/common/testing/testhooks"
)

// NewCallbackGenerator returns a callback generator without test hooks.
func NewCallbackGenerator(commontesthooks.TestHooks) *httpfaults.CallbackGenerator {
	return httpfaults.NewCallbackGenerator()
}

// NewGenerator returns nil without the test_dep build tag.
func NewGenerator(commontesthooks.TestHooks) httpfaults.Generator {
	return nil
}
