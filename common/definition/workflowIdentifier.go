package definition

type (
	// WorkflowIdentifier is the combinations which represent a workflow
	WorkflowIdentifier struct {
		NamespaceID string
		WorkflowID  string
		RunID       string
	}
)

// NewWorkflowIdentifier create a new WorkflowIdentifier
func NewWorkflowIdentifier(namespaceID string, workflowID string, runID string) WorkflowIdentifier {
	return WorkflowIdentifier{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
}
