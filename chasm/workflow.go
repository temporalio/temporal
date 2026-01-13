package chasm

const (
	WorkflowLibraryName   = "workflow"
	WorkflowComponentName = "workflow"
)

var (
	WorkflowArchetype   = Archetype(FullyQualifiedName(WorkflowLibraryName, WorkflowComponentName))
	WorkflowArchetypeID = ArchetypeID(GenerateTypeID(WorkflowArchetype))
)
