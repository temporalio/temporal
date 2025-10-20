package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*SyncWorkflowStateTask)(nil)

type (
	SyncWorkflowStateTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		// TODO: validate this version in source task converter
		Version            int64
		Priority           enumsspb.TaskPriority
		TargetClusters     []string
		IsForceReplication bool
	}
)

func (a *SyncWorkflowStateTask) GetKey() Key {
	return NewImmediateKey(a.TaskID)
}

func (a *SyncWorkflowStateTask) GetVersion() int64 {
	return a.Version
}

func (a *SyncWorkflowStateTask) SetVersion(version int64) {
	a.Version = version
}

func (a *SyncWorkflowStateTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *SyncWorkflowStateTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *SyncWorkflowStateTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *SyncWorkflowStateTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

func (a *SyncWorkflowStateTask) GetCategory() Category {
	return CategoryReplication
}

func (a *SyncWorkflowStateTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE
}
