package chasm

const (
	WorkflowLibraryName   = "workflow"
	WorkflowComponentName = "workflow"
)

var (
	WorkflowArchetype   = FullyQualifiedName(WorkflowLibraryName, WorkflowComponentName)
	WorkflowArchetypeID = GenerateTypeID(WorkflowArchetype)
)
