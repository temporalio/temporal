package tasks

import (
	"fmt"
	"sync/atomic"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
	ctasks "go.temporal.io/server/common/tasks"
)

var (
	_ Task           = (*ChasmMemoryWakeupTask)(nil)
	_ HasArchetypeID = (*ChasmMemoryWakeupTask)(nil)
)

type (
	// ChasmMemoryWakeupTask is an in-memory timer that wakes up a cached CHASM execution.
	ChasmMemoryWakeupTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
		ArchetypeID         uint32

		state atomic.Uint32 // of type ctasks.State
	}
)

func (t *ChasmMemoryWakeupTask) GetKey() Key {
	return NewKey(t.VisibilityTimestamp, t.TaskID)
}

func (t *ChasmMemoryWakeupTask) GetVersion() int64 {
	return t.Version
}

func (t *ChasmMemoryWakeupTask) SetVersion(version int64) {
	t.Version = version
}

func (t *ChasmMemoryWakeupTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *ChasmMemoryWakeupTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *ChasmMemoryWakeupTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *ChasmMemoryWakeupTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

func (t *ChasmMemoryWakeupTask) GetArchetypeID() uint32 {
	return t.ArchetypeID
}

func (t *ChasmMemoryWakeupTask) GetCategory() Category {
	return CategoryMemoryTimer
}

func (t *ChasmMemoryWakeupTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_CHASM_MEMORY_WAKEUP
}

// Cancel marks the task cancelled so the in-memory queue drops it.
func (t *ChasmMemoryWakeupTask) Cancel() {
	t.state.Store(uint32(ctasks.TaskStateCancelled))
}

func (t *ChasmMemoryWakeupTask) State() ctasks.State {
	return ctasks.State(t.state.Load())
}

func (t *ChasmMemoryWakeupTask) String() string {
	return fmt.Sprintf("ChasmMemoryWakeupTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v}",
		t.WorkflowKey.String(),
		t.VisibilityTimestamp,
		t.TaskID,
	)
}
