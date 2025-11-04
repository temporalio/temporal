package chasm

const (
	WorkflowLibraryName   = "workflow"
	WorkflowComponentName = "Workflow"
)

var (
	WorkflowArchetype   Archetype   = Archetype(fullyQualifiedName(WorkflowLibraryName, WorkflowComponentName))
	WorkflowArchetypeID ArchetypeID = generateTypeID(WorkflowArchetype)
)
