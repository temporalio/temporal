package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
)

// StateMachineTask is a base for all tasks emitted by hierarchical state machines.
type StateMachineTask struct {
	definition.WorkflowKey
	VisibilityTimestamp time.Time
	TaskID              int64
	Info                *persistencespb.StateMachineTaskInfo
}

var _ HasStateMachineTaskType = &StateMachineTask{}

func (t *StateMachineTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *StateMachineTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *StateMachineTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *StateMachineTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

func (t *StateMachineTask) StateMachineTaskType() string {
	return t.Info.Type
}

// StateMachineOutboundTask is a task on the outbound queue.
type StateMachineOutboundTask struct {
	StateMachineTask
	Destination string
}

// GetDestination is used for grouping outbound tasks into a per source namespace and destination scheduler and in multi-cursor predicates.
func (t *StateMachineOutboundTask) GetDestination() string {
	return t.Destination
}

func (*StateMachineOutboundTask) GetCategory() Category {
	return CategoryOutbound
}

func (*StateMachineOutboundTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_STATE_MACHINE_OUTBOUND
}

func (t *StateMachineOutboundTask) GetKey() Key {
	return NewImmediateKey(t.TaskID)
}

var _ Task = &StateMachineOutboundTask{}
var _ HasDestination = &StateMachineOutboundTask{}

// StateMachineCallbackTask is a generic timer task that can be emitted by any hierarchical state machine.
type StateMachineTimerTask struct {
	definition.WorkflowKey
	VisibilityTimestamp time.Time
	TaskID              int64
	Version             int64
}

func (*StateMachineTimerTask) GetCategory() Category {
	return CategoryTimer
}

func (*StateMachineTimerTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_STATE_MACHINE_TIMER
}

func (t *StateMachineTimerTask) GetKey() Key {
	return NewKey(t.VisibilityTimestamp, t.TaskID)
}

func (t *StateMachineTimerTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *StateMachineTimerTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *StateMachineTimerTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *StateMachineTimerTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

var _ Task = &StateMachineTimerTask{}
