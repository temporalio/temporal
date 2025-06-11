package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*SyncActivityTask)(nil)

type (
	SyncActivityTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
		ScheduledEventID    int64
		TargetClusters      []string
	}
)

func (a *SyncActivityTask) GetKey() Key {
	return NewImmediateKey(a.TaskID)
}

func (a *SyncActivityTask) GetVersion() int64 {
	return a.Version
}

func (a *SyncActivityTask) SetVersion(version int64) {
	a.Version = version
}

func (a *SyncActivityTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *SyncActivityTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *SyncActivityTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *SyncActivityTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

func (a *SyncActivityTask) GetCategory() Category {
	return CategoryReplication
}

func (a *SyncActivityTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY
}
