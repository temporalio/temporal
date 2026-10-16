//go:build !test_dep

package testhooks

import (
	"go.temporal.io/server/testx/failpoint"
	testxhooks "go.temporal.io/testx/testhooks"
)

func NewTestHooks() TestHooks {
	return TestHooks{TestHooks: testxhooks.NewTestHooks()}
}

// Failpoints returns nil. Failpoints never fire in production mode.
func (TestHooks) Failpoints() failpoint.Registry {
	return nil
}
