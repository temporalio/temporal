package replication

import (
	"time"

	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	ExecutableNoopTask struct {
		ExecutableTask
	}
)

var _ ctasks.Task = (*ExecutableNoopTask)(nil)
var _ TrackableExecutableTask = (*ExecutableNoopTask)(nil)

func NewExecutableNoopTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
) *ExecutableNoopTask {
	return &ExecutableNoopTask{
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.NoopTaskScope,
			taskCreationTime,
			time.Now().UTC(),
		),
	}
}

func (e *ExecutableNoopTask) Execute() error {
	return nil
}

func (e *ExecutableNoopTask) HandleErr(err error) error {
	return err
}
