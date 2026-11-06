package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*ReleaseLimiterTask)(nil)

type ReleaseLimiterTask struct {
	definition.WorkflowKey
	VisibilityTimestamp time.Time
	TaskID              int64
	Limiters            []*taskqueuespb.LimiterRef
}

func (a *ReleaseLimiterTask) GetKey() Key {
	return NewImmediateKey(a.TaskID)
}

func (a *ReleaseLimiterTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *ReleaseLimiterTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *ReleaseLimiterTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *ReleaseLimiterTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

func (a *ReleaseLimiterTask) GetCategory() Category {
	return CategoryTransfer
}

func (a *ReleaseLimiterTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TRANSFER_RELEASE_LIMITER
}
