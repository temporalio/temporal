//go:build !test_dep

package faultinjection

import "go.temporal.io/server/common/testing/testhooks"

// NewTestHookGenerator returns nil when test hooks are disabled.
func NewTestHookGenerator(testhooks.TestHooks) Generator {
	return nil
}
