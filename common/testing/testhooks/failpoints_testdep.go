//go:build test_dep

package testhooks

import (
	"go.temporal.io/server/testx/failpoint"
	testxhooks "go.temporal.io/testx/testhooks"
)

func NewTestHooks() TestHooks {
	return TestHooks{TestHooks: testxhooks.NewTestHooks(), failpoints: failpoint.NewRegistry()}
}

// Failpoints returns the failpoint registry, so TestHooks can be used as a failpoint.Source.
func (th TestHooks) Failpoints() failpoint.Registry {
	return th.failpoints
}
