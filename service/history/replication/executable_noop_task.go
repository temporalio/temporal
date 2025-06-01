package replication

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	ExecutableNoopTask struct {
		ExecutableTask
	}
)

const (
	noopTaskID = "noop-task-id"
)

var _ ctasks.Task = (*ExecutableNoopTask)(nil)
var _ TrackableExecutableTask = (*ExecutableNoopTask)(nil)

func NewExecutableNoopTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	sourceClusterName string,
	sourceShardKey ClusterShardKey,
) *ExecutableNoopTask {
	return &ExecutableNoopTask{
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.NoopTaskScope,
			taskCreationTime,
			time.Now().UTC(),
			sourceClusterName,
			sourceShardKey,
			enumsspb.TASK_PRIORITY_UNSPECIFIED,
			nil,
		),
	}
}

func (e *ExecutableNoopTask) QueueID() interface{} {
	return noopTaskID
}

func (e *ExecutableNoopTask) Execute() error {
	return nil
}

func (e *ExecutableNoopTask) HandleErr(err error) error {
	return err
}

func (e *ExecutableNoopTask) MarkPoisonPill() error {
	return nil
}
