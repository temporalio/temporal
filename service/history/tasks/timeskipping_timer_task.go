package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*TimeSkippingTimerTask)(nil)

type (
	// TimeSkippingTimerTask wakes a workflow when the fast-forward configured
	// on its TimeSkippingConfig should take effect.
	//
	// Stamp identifies the fast-forward this task targets. It is matched against
	// TimeSkippingInfo.FastForwardInfo.Stamp at firing time to detect superseded tasks:
	// re-applying the fast-forward emits a new task with a bumped Stamp, and the old task is
	// silently dropped on mismatch. Version is the namespace failover version, validated via
	// CheckTaskVersion. Both are event-free, so the task works for workflow and CHASM executions.
	TimeSkippingTimerTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
		Stamp               int32
	}
)

func (t *TimeSkippingTimerTask) GetKey() Key {
	return NewKey(t.VisibilityTimestamp, t.TaskID)
}

func (t *TimeSkippingTimerTask) GetVersion() int64 {
	return t.Version
}

func (t *TimeSkippingTimerTask) SetVersion(version int64) {
	t.Version = version
}

func (t *TimeSkippingTimerTask) GetStamp() int32 {
	return t.Stamp
}

func (t *TimeSkippingTimerTask) SetStamp(stamp int32) {
	t.Stamp = stamp
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
	return fmt.Sprintf("TimeSkippingTimerTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, Version: %v, Stamp: %v}",
		t.WorkflowKey.String(),
		t.VisibilityTimestamp,
		t.TaskID,
		t.Version,
		t.Stamp,
	)
}
