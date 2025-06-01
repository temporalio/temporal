package replication

import (
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	ExecutableUnknownTask struct {
		ProcessToolBox

		ExecutableTask
		task any
	}
)

const (
	unknownTaskID = "unknown-task-id"
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
		ProcessToolBox: processToolBox,

		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.UnknownTaskScope,
			taskCreationTime,
			time.Now().UTC(),
			"sourceCluster",
			ClusterShardKey{
				ClusterID: 0,
				ShardID:   0,
			},
			enumsspb.TASK_PRIORITY_UNSPECIFIED,
			nil,
		),
		task: task,
	}
}

func (e *ExecutableUnknownTask) QueueID() interface{} {
	return unknownTaskID
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

func (e *ExecutableUnknownTask) MarkPoisonPill() error {
	e.Logger.Error("unable to enqueue unknown replication task to DLQ",
		tag.Task(e.task),
		tag.TaskID(e.ExecutableTask.TaskID()),
	)
	return nil
}
