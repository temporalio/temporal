package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
)

var (
	_ Task           = (*TimeSkippingTimerTask)(nil)
	_ HasArchetypeID = (*TimeSkippingTimerTask)(nil)
)

type (
	// TimeSkippingTimerTask wakes a workflow when the fast-forward configured
	// on its TimeSkippingConfig should take effect.
	TimeSkippingTimerTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		VersionedTransition *persistencespb.VersionedTransition
		TaskID              int64
		// ArchetypeID identifies the execution archetype (workflow or a CHASM component) this task
		// targets. It is required to acquire the correct execution context at firing time, since the
		// context cache is keyed by archetype. Always set at generation from the mutable state's
		// archetype (chasm.WorkflowArchetypeID for workflows).
		ArchetypeID uint32
	}
)

func (t *TimeSkippingTimerTask) GetKey() Key {
	return NewKey(t.VisibilityTimestamp, t.TaskID)
}

func (t *TimeSkippingTimerTask) GetArchetypeID() uint32 {
	return t.ArchetypeID
}

func (t *TimeSkippingTimerTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *TimeSkippingTimerTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *TimeSkippingTimerTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *TimeSkippingTimerTask) SetVisibilityTime(visibilityTime time.Time) {
	t.VisibilityTimestamp = visibilityTime
}

func (t *TimeSkippingTimerTask) GetCategory() Category {
	return CategoryTimer
}

func (t *TimeSkippingTimerTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TIMESKIPPING_TIMER
}

func (t *TimeSkippingTimerTask) String() string {
	vt := t.VersionedTransition
	return fmt.Sprintf("TimeSkippingTimerTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, FailoverVersion: %v, TransitionCount: %v, ArchetypeID: %v}",
		t.WorkflowKey.String(),
		t.VisibilityTimestamp,
		t.TaskID,
		vt.GetNamespaceFailoverVersion(),
		vt.GetTransitionCount(),
		t.ArchetypeID,
	)
}
