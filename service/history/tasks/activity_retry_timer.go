package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*ActivityRetryTimerTask)(nil)

type (
	ActivityRetryTimerTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		EventID             int64
		Version             int64
		Attempt             int32
		Stamp               int32
	}
)

func (r *ActivityRetryTimerTask) GetKey() Key {
	return NewKey(r.VisibilityTimestamp, r.TaskID)
}

func (r *ActivityRetryTimerTask) GetVersion() int64 {
	return r.Version
}

func (r *ActivityRetryTimerTask) SetVersion(version int64) {
	r.Version = version
}

func (r *ActivityRetryTimerTask) GetTaskID() int64 {
	return r.TaskID
}

func (r *ActivityRetryTimerTask) SetTaskID(id int64) {
	r.TaskID = id
}

func (r *ActivityRetryTimerTask) GetVisibilityTime() time.Time {
	return r.VisibilityTimestamp
}

func (r *ActivityRetryTimerTask) SetVisibilityTime(t time.Time) {
	r.VisibilityTimestamp = t
}

func (r *ActivityRetryTimerTask) GetCategory() Category {
	return CategoryTimer
}

func (r *ActivityRetryTimerTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER
}

func (r *ActivityRetryTimerTask) GetStamp() int32 {
	return r.Stamp
}

func (r *ActivityRetryTimerTask) SetStamp(stamp int32) {
	r.Stamp = stamp
}

func (r *ActivityRetryTimerTask) String() string {
	return fmt.Sprintf("ActivityRetryTimerTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, EventID: %v, Version: %v, Attempt: %v, Stamp: %v}",
		r.WorkflowKey.String(),
		r.VisibilityTimestamp,
		r.TaskID,
		r.EventID,
		r.Version,
		r.Attempt,
		r.Stamp,
	)
}
