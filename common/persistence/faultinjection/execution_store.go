package faultinjection

import (
	"context"

	"go.temporal.io/server/common/persistence"
)

var _ persistence.RunAlreadyExistsChecker = faultInjectionExecutionStore{}

func (d faultInjectionExecutionStore) SupportsCheckRunAlreadyExists() bool {
	checker, ok := d.ExecutionStore.(persistence.RunAlreadyExistsChecker)
	return ok && checker.SupportsCheckRunAlreadyExists()
}

// SubmitWorkflowExecutionUpdate injects faults only before submission, so an injected error
// never hides an accepted write.
func (d faultInjectionExecutionStore) SubmitWorkflowExecutionUpdate(
	ctx context.Context,
	request *persistence.InternalUpdateWorkflowExecutionRequest,
) (persistence.WorkflowExecutionUpdateReceipt, error) {
	if err := d.generator.generate("SubmitWorkflowExecutionUpdate", request).inject(func() error { return nil }); err != nil {
		return nil, err
	}
	return d.ExecutionStore.SubmitWorkflowExecutionUpdate(ctx, request)
}
