package persistencefailpoints

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/serverfailpoints"
	"go.temporal.io/testx/failpoint"
)

// NewInjector returns a fault injector that executes the failpoint for each persistence store
// method, scoped to the namespace ID of the request if it has one. An error from the failpoint
// fails the method. It returns nil if failpoints are not enabled.
func NewInjector(source *failpoint.Registry) config.FaultInjector {
	if source == nil {
		return nil
	}
	return func(target config.FaultInjectionTarget) error {
		// Building the key allocates, so skip it while nothing can fire.
		if !source.MayFire() {
			return nil
		}
		key := serverfailpoints.PersistenceKey(string(target.Store), target.Method)
		if namespaceID := persistence.RequestNamespaceID(target.Request); namespaceID != "" {
			key = key.ForScope(namespaceID)
		}
		return failpoint.CheckError(source, key)
	}
}
