package chasm

const (
	WorkflowLibraryName   = "workflow"
	WorkflowComponentName = "workflow"
)

var (
	WorkflowArchetype   = Archetype(fullyQualifiedName(WorkflowLibraryName, WorkflowComponentName))
	WorkflowArchetypeID = ArchetypeID(generateTypeID(WorkflowArchetype))
)
