package telemetry

import (
	"go.temporal.io/server/common/persistence"
)

var _ persistence.RunAlreadyExistsChecker = telemetryExecutionStore{}

func (d telemetryExecutionStore) SupportsCheckRunAlreadyExists() bool {
	checker, ok := d.ExecutionStore.(persistence.RunAlreadyExistsChecker)
	return ok && checker.SupportsCheckRunAlreadyExists()
}
