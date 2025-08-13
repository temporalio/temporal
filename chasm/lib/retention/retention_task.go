package retention

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
)

type RetentionTaskExecutor struct{}

func NewRetentionTaskExecutor() *RetentionTaskExecutor {
	return &RetentionTaskExecutor{}
}

func (r *RetentionTaskExecutor) Execute(
	ctx chasm.MutableContext,
	retention *Retention,
	_ chasm.TaskAttributes,
	_ *persistencespb.RetentionTask,
) error {
	retention.Closed = true
	return nil
}

func (r *RetentionTaskExecutor) Validate(
	ctx chasm.Context,
	retention *Retention,
	_ chasm.TaskAttributes,
	_ *persistencespb.RetentionTask,
) (bool, error) {
	if retention.Closed {
		return false, nil
	}

	return true, nil
}
