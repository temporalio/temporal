package definition

import (
	"fmt"
)

type (
	// WorkflowKey is the combinations which represent a workflow
	WorkflowKey struct {
		NamespaceID string
		WorkflowID  string
		RunID       string
	}
)

// NewWorkflowKey create a new WorkflowKey
func NewWorkflowKey(
	namespaceID string,
	workflowID string,
	runID string,
) WorkflowKey {
	return WorkflowKey{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
}

func (k *WorkflowKey) GetNamespaceID() string {
	return k.NamespaceID
}

func (k *WorkflowKey) GetWorkflowID() string {
	return k.WorkflowID
}

func (k *WorkflowKey) GetRunID() string {
	return k.RunID
}

func (k *WorkflowKey) String() string {
	return fmt.Sprintf("%v/%v/%v", k.NamespaceID, k.WorkflowID, k.RunID)
}
