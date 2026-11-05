package faultinjection

import (
	"go.temporal.io/server/common/persistence"
)

var _ persistence.RunAlreadyExistsChecker = faultInjectionExecutionStore{}

func (d faultInjectionExecutionStore) SupportsCheckRunAlreadyExists() bool {
	checker, ok := d.ExecutionStore.(persistence.RunAlreadyExistsChecker)
	return ok && checker.SupportsCheckRunAlreadyExists()
}
