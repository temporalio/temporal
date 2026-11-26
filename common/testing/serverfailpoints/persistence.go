package serverfailpoints

import "go.temporal.io/testx/failpoint"

var persistenceKeys = failpoint.ReserveKeyPrefix[error]("persistence:")

// PersistenceKey returns the key of the failpoint that fires before a persistence store method
// runs, for example PersistenceKey("ExecutionStore", "UpdateWorkflowExecution").
func PersistenceKey(store, method string) failpoint.Key[error] {
	return persistenceKeys.Key(store + "." + method)
}
