package replication

import (
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	ExecutableUnknownTask struct {
		ExecutableTask
		task any
	}
)

var _ ctasks.Task = (*ExecutableUnknownTask)(nil)
var _ TrackableExecutableTask = (*ExecutableUnknownTask)(nil)

func NewExecutableUnknownTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	task any,
) *ExecutableUnknownTask {
	return &ExecutableUnknownTask{
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.UnknownTaskScope,
			taskCreationTime,
			time.Now().UTC(),
		),
		task: task,
	}
}

func (e *ExecutableUnknownTask) Execute() error {
	return serviceerror.NewInvalidArgument(
		fmt.Sprintf("unknown task, ID: %v, task: %v", e.TaskID(), e.task),
	)
}

func (e *ExecutableUnknownTask) HandleErr(err error) error {
	return err
}

func (e *ExecutableUnknownTask) IsRetryableError(err error) bool {
	return false
}
