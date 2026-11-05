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

func (d faultInjectionExecutionStore) SubmitWorkflowExecutionUpdate(
	ctx context.Context,
	request *persistence.InternalUpdateWorkflowExecutionRequest,
) (persistence.WorkflowExecutionUpdateReceipt, error) {
	// Inject before admission so rejection cannot conceal an accepted write.
	if err := d.generator.generate("SubmitWorkflowExecutionUpdate", request).inject(func() error { return nil }); err != nil {
		return nil, err
	}
	return d.ExecutionStore.SubmitWorkflowExecutionUpdate(ctx, request)
}
