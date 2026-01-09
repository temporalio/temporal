package tasks

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*ParentClosePolicyTask)(nil)

type (
	// ParentClosePolicyTask is a transfer task to apply parent close policy to a child workflow.
	ParentClosePolicyTask struct {
		// WorkflowKey is the parent workflow that generated this task.
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64

		// Target* fields identify the child workflow to apply the policy to
		TargetNamespaceID string
		TargetWorkflowID  string
		TargetRunID       string // FirstExecutionRunID - allows terminate across CAN chains
		ParentClosePolicy enumspb.ParentClosePolicy
	}
)

func (t *ParentClosePolicyTask) GetKey() Key {
	return NewImmediateKey(t.TaskID)
}

func (t *ParentClosePolicyTask) GetVersion() int64 {
	return t.Version
}

func (t *ParentClosePolicyTask) SetVersion(version int64) {
	t.Version = version
}

func (t *ParentClosePolicyTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *ParentClosePolicyTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *ParentClosePolicyTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *ParentClosePolicyTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

func (t *ParentClosePolicyTask) GetCategory() Category {
	return CategoryTransfer
}

func (t *ParentClosePolicyTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TRANSFER_PARENT_CLOSE_POLICY
}

func (t *ParentClosePolicyTask) String() string {
	return fmt.Sprintf("ParentClosePolicyTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, Version: %v, TargetNamespaceID: %v, TargetWorkflowID: %v, TargetRunID: %v, ParentClosePolicy: %v}",
		t.WorkflowKey.String(),
		t.VisibilityTimestamp,
		t.TaskID,
		t.Version,
		t.TargetNamespaceID,
		t.TargetWorkflowID,
		t.TargetRunID,
		t.ParentClosePolicy,
	)
}
