//go:build test_dep

package persistencefaultstest

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/rpc/faultinjection"
	"go.temporal.io/server/common/testing/failpoint"
	commontesthooks "go.temporal.io/server/common/testing/testhooks"
)

// WithFailpoints returns a copy of fiConfig whose injector also executes the failpoint for each
// persistence store method, scoped to the request's namespace ID if it has one. An error from the
// failpoint fails the method. The failpoint runs after the injector of fiConfig, if any.
// Without a failpoint registry, for example for zero TestHooks, fiConfig is returned unchanged.
func WithFailpoints(fiConfig *config.FaultInjection, testHooks commontesthooks.TestHooks) *config.FaultInjection {
	if testHooks.Failpoints() == nil {
		return fiConfig
	}
	var result config.FaultInjection
	if fiConfig != nil {
		result = *fiConfig
	}
	previous := result.Injector
	result.Injector = func(target config.FaultInjectionTarget) error {
		if previous != nil {
			if err := previous(target); err != nil {
				return err
			}
		}
		key := failpoint.PersistenceKey(string(target.Store), target.Method)
		if nsID := faultinjection.ScopeFromRequest(target.Request).NamespaceID; nsID != "" {
			key = key.ForNamespace(nsID)
		}
		return failpoint.Checkpoint(testHooks, key)
	}
	return &result
}
