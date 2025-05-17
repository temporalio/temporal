package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*HistoryReplicationTask)(nil)

type (
	HistoryReplicationTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		FirstEventID        int64
		NextEventID         int64
		Version             int64
		NewRunID            string
		TargetClusters      []string

		// deprecated
		BranchToken       []byte
		NewRunBranchToken []byte
	}
)

func (a *HistoryReplicationTask) GetKey() Key {
	return NewImmediateKey(a.TaskID)
}

func (a *HistoryReplicationTask) GetVersion() int64 {
	return a.Version
}

func (a *HistoryReplicationTask) SetVersion(version int64) {
	a.Version = version
}

func (a *HistoryReplicationTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *HistoryReplicationTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *HistoryReplicationTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *HistoryReplicationTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

func (a *HistoryReplicationTask) GetCategory() Category {
	return CategoryReplication
}

func (a *HistoryReplicationTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_REPLICATION_HISTORY
}
