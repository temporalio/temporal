package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*SyncVersionedTransitionTask)(nil)
var _ HasArchetypeID = (*SyncVersionedTransitionTask)(nil)

type (
	SyncVersionedTransitionTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		ArchetypeID         uint32
		Priority            enumsspb.TaskPriority
		TargetClusters      []string

		VersionedTransition    *persistencespb.VersionedTransition
		FirstEventVersion      int64
		FirstEventID           int64                          // First event ID of version transition
		NextEventID            int64                          // Next event ID after version transition
		LastVersionHistoryItem *historyspb.VersionHistoryItem // Last version history item of version transition when version transition does not have associated events
		NewRunID               string
		IsFirstTask            bool
		IsForceReplication     bool

		TaskEquivalents []Task
	}
)

func (a *SyncVersionedTransitionTask) GetKey() Key {
	return NewImmediateKey(a.TaskID)
}

func (a *SyncVersionedTransitionTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *SyncVersionedTransitionTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *SyncVersionedTransitionTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *SyncVersionedTransitionTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

func (a *SyncVersionedTransitionTask) GetCategory() Category {
	return CategoryReplication
}

func (a *SyncVersionedTransitionTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_REPLICATION_SYNC_VERSIONED_TRANSITION
}

func (a *SyncVersionedTransitionTask) GetArchetypeID() uint32 {
	return a.ArchetypeID
}

func (a *SyncVersionedTransitionTask) String() string {
	return fmt.Sprintf("SyncVersionedTransitionTask{WorkflowKey: %v, TaskID: %v, Priority: %v, VersionedTransition: %v, FirstEventID: %v, FirstEventVersion: %v, NextEventID: %v, NewRunID: %v}",
		a.WorkflowKey, a.TaskID, a.Priority, a.VersionedTransition, a.FirstEventID, a.FirstEventVersion, a.NextEventID, a.NewRunID)
}
