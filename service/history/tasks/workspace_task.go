package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	workspacepb "go.temporal.io/api/workspace/v1"
	"go.temporal.io/server/common/definition"
)

// WorkspaceCreateAndAcquireTask creates a standalone workspace CHASM execution
// and acquires the writer lock. Created when a workflow first schedules an
// activity with a workspace_id that doesn't exist.
type WorkspaceCreateAndAcquireTask struct {
	definition.WorkflowKey
	VisibilityTimestamp time.Time
	TaskID              int64
	WorkspaceID         string // namespace-scoped workspace business ID
}

var _ Task = (*WorkspaceCreateAndAcquireTask)(nil)

func (t *WorkspaceCreateAndAcquireTask) GetKey() Key            { return NewImmediateKey(t.TaskID) }
func (t *WorkspaceCreateAndAcquireTask) GetTaskID() int64        { return t.TaskID }
func (t *WorkspaceCreateAndAcquireTask) SetTaskID(id int64)      { t.TaskID = id }
func (t *WorkspaceCreateAndAcquireTask) GetVisibilityTime() time.Time { return t.VisibilityTimestamp }
func (t *WorkspaceCreateAndAcquireTask) SetVisibilityTime(ts time.Time) { t.VisibilityTimestamp = ts }
func (t *WorkspaceCreateAndAcquireTask) GetCategory() Category  { return CategoryTransfer }
func (t *WorkspaceCreateAndAcquireTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TRANSFER_WORKSPACE_CREATE_AND_ACQUIRE
}

// WorkspaceSyncAndReleaseTask pushes accumulated diffs from the workflow's
// local WorkspaceRef to the standalone workspace CHASM execution and releases
// the writer lock. Created on workflow completion or explicit release.
type WorkspaceSyncAndReleaseTask struct {
	definition.WorkflowKey
	VisibilityTimestamp time.Time
	TaskID              int64
	WorkspaceID         string
	WorkflowID          string // the workflow releasing
	RunID               string
	NewVersion          int64
	DiffRecords         []*workspacepb.DiffRecord
}

var _ Task = (*WorkspaceSyncAndReleaseTask)(nil)

func (t *WorkspaceSyncAndReleaseTask) GetKey() Key            { return NewImmediateKey(t.TaskID) }
func (t *WorkspaceSyncAndReleaseTask) GetTaskID() int64        { return t.TaskID }
func (t *WorkspaceSyncAndReleaseTask) SetTaskID(id int64)      { t.TaskID = id }
func (t *WorkspaceSyncAndReleaseTask) GetVisibilityTime() time.Time { return t.VisibilityTimestamp }
func (t *WorkspaceSyncAndReleaseTask) SetVisibilityTime(ts time.Time) { t.VisibilityTimestamp = ts }
func (t *WorkspaceSyncAndReleaseTask) GetCategory() Category  { return CategoryTransfer }
func (t *WorkspaceSyncAndReleaseTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TRANSFER_WORKSPACE_SYNC_AND_RELEASE
}

// WorkspaceForkTask creates a new standalone workspace CHASM execution from
// an existing one (zero-copy fork). Created when a child workflow gets a
// workspace grant with COPY mode.
type WorkspaceForkTask struct {
	definition.WorkflowKey
	VisibilityTimestamp   time.Time
	TaskID                int64
	SourceWorkspaceID     string
	TargetWorkspaceID     string
}

var _ Task = (*WorkspaceForkTask)(nil)

func (t *WorkspaceForkTask) GetKey() Key            { return NewImmediateKey(t.TaskID) }
func (t *WorkspaceForkTask) GetTaskID() int64        { return t.TaskID }
func (t *WorkspaceForkTask) SetTaskID(id int64)      { t.TaskID = id }
func (t *WorkspaceForkTask) GetVisibilityTime() time.Time { return t.VisibilityTimestamp }
func (t *WorkspaceForkTask) SetVisibilityTime(ts time.Time) { t.VisibilityTimestamp = ts }
func (t *WorkspaceForkTask) GetCategory() Category  { return CategoryTransfer }
func (t *WorkspaceForkTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TRANSFER_WORKSPACE_FORK
}
