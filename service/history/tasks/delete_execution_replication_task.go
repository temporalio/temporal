package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*DeleteExecutionReplicationTask)(nil)
var _ HasArchetypeID = (*DeleteExecutionReplicationTask)(nil)

type DeleteExecutionReplicationTask struct {
	definition.WorkflowKey
	VisibilityTimestamp time.Time
	TaskID              int64
	ArchetypeID         uint32
}

func (a *DeleteExecutionReplicationTask) GetKey() Key {
	return NewImmediateKey(a.TaskID)
}

func (a *DeleteExecutionReplicationTask) GetVersion() int64 {
	return 0
}

func (a *DeleteExecutionReplicationTask) SetVersion(_ int64) {
}

func (a *DeleteExecutionReplicationTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *DeleteExecutionReplicationTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *DeleteExecutionReplicationTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *DeleteExecutionReplicationTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

func (a *DeleteExecutionReplicationTask) GetCategory() Category {
	return CategoryReplication
}

func (a *DeleteExecutionReplicationTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_REPLICATION_DELETE_EXECUTION
}

func (a *DeleteExecutionReplicationTask) GetArchetypeID() uint32 {
	return a.ArchetypeID
}
