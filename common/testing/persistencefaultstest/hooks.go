package persistencefaultstest

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/testing/failpoints"
	"go.temporal.io/testx/failpoint"
)

// WithFailpoints returns a copy of fiConfig whose injector also executes the failpoint for each
// persistence store method, scoped to the request's namespace ID if it has one. An error from the
// failpoint fails the method. The failpoint runs after the injector of fiConfig, if any.
// Without a failpoint registry, because failpoints are not enabled, fiConfig is returned
// unchanged. Fault injection configured in fiConfig is a feature of all builds, so the caller
// still installs it; only the failpoints require the startup switch.
func WithFailpoints(fiConfig *config.FaultInjection, source *failpoints.Source) *config.FaultInjection {
	if source.Failpoints() == nil {
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
		if target.NamespaceID != "" {
			key = key.ForScope(target.NamespaceID)
		}
		return failpoint.Checkpoint(source, key)
	}
	return &result
}
