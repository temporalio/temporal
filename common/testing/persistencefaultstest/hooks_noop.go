//go:build !test_dep

package persistencefaultstest

import (
	"go.temporal.io/server/common/config"
	commontesthooks "go.temporal.io/server/common/testing/testhooks"
)

// WithFailpoints returns fiConfig unchanged when test hooks are disabled.
func WithFailpoints(fiConfig *config.FaultInjection, _ commontesthooks.TestHooks) *config.FaultInjection {
	return fiConfig
}
