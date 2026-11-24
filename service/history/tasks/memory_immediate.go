package tasks

import (
	"context"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

// MemoryImmediateTask runs a best-effort callback without persistence or retries.
type MemoryImmediateTask struct {
	definition.WorkflowKey
	TaskID              int64
	VisibilityTimestamp time.Time
	Callback            func(context.Context)
}

var _ Task = (*MemoryImmediateTask)(nil)

func (t *MemoryImmediateTask) GetKey() Key                  { return NewImmediateKey(t.TaskID) }
func (t *MemoryImmediateTask) GetTaskID() int64             { return t.TaskID }
func (t *MemoryImmediateTask) SetTaskID(id int64)           { t.TaskID = id }
func (t *MemoryImmediateTask) GetVisibilityTime() time.Time { return t.VisibilityTimestamp }
func (t *MemoryImmediateTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}
func (t *MemoryImmediateTask) GetCategory() Category      { return CategoryMemoryImmediate }
func (t *MemoryImmediateTask) GetType() enumsspb.TaskType { return enumsspb.TASK_TYPE_UNSPECIFIED }

func (t *MemoryImmediateTask) Run(ctx context.Context) { t.Callback(ctx) }
func (t *MemoryImmediateTask) Abort()                  {}
