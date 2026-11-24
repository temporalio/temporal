package persistencefaultstest

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/rpc/faultinjection"
	"go.temporal.io/server/common/testing/failpoints"
	commontesthooks "go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/testx/failpoint"
)

// WithFailpoints returns a copy of fiConfig whose injector also executes the failpoint for each
// persistence store method, scoped to the request's namespace ID if it has one. An error from the
// failpoint fails the method. The failpoint runs after the injector of fiConfig, if any.
// Without a failpoint registry, for example for zero TestHooks or a build without the test_dep
// tag, fiConfig is returned unchanged. Fault injection configured in fiConfig is a feature of all
// builds, so the caller still installs it; only the failpoints require test_dep.
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
		key := failpoints.PersistenceKey(string(target.Store), target.Method)
		if nsID := faultinjection.ScopeFromRequest(target.Request).NamespaceID; nsID != "" {
			key = key.ForScope(nsID.String())
		}
		return failpoint.Checkpoint(testHooks, key)
	}
	return &result
}
