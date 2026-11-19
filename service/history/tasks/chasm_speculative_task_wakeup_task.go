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
	_ Task           = (*ChasmSpeculativeTaskWakeupTask)(nil)
	_ HasArchetypeID = (*ChasmSpeculativeTaskWakeupTask)(nil)
)

type (
	// ChasmSpeculativeTaskWakeupTask is an in-memory timer that runs the due tasks added by
	// speculative CHASM transitions on an execution.
	ChasmSpeculativeTaskWakeupTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
		ArchetypeID         uint32

		state atomic.Uint32 // of type ctasks.State
	}
)

func (t *ChasmSpeculativeTaskWakeupTask) GetKey() Key {
	return NewKey(t.VisibilityTimestamp, t.TaskID)
}

func (t *ChasmSpeculativeTaskWakeupTask) GetVersion() int64 {
	return t.Version
}

func (t *ChasmSpeculativeTaskWakeupTask) SetVersion(version int64) {
	t.Version = version
}

func (t *ChasmSpeculativeTaskWakeupTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *ChasmSpeculativeTaskWakeupTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *ChasmSpeculativeTaskWakeupTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *ChasmSpeculativeTaskWakeupTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

func (t *ChasmSpeculativeTaskWakeupTask) GetArchetypeID() uint32 {
	return t.ArchetypeID
}

func (t *ChasmSpeculativeTaskWakeupTask) GetCategory() Category {
	return CategoryMemoryTimer
}

func (t *ChasmSpeculativeTaskWakeupTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_CHASM_SPECULATIVE_TASK_WAKEUP
}

// Cancel marks the task cancelled so the in-memory queue drops it.
func (t *ChasmSpeculativeTaskWakeupTask) Cancel() {
	t.state.Store(uint32(ctasks.TaskStateCancelled))
}

func (t *ChasmSpeculativeTaskWakeupTask) State() ctasks.State {
	return ctasks.State(t.state.Load())
}

func (t *ChasmSpeculativeTaskWakeupTask) String() string {
	return fmt.Sprintf("ChasmSpeculativeTaskWakeupTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v}",
		t.WorkflowKey.String(),
		t.VisibilityTimestamp,
		t.TaskID,
	)
}
