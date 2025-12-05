package chasm

// This file defines constants for Scheduler which is special to the CHASM framework
// because it shares the same ID space with Workflow for backwards compatibility reasons.

const (
	SchedulerLibraryName   = "scheduler"
	SchedulerComponentName = "scheduler"
)

var (
	SchedulerArchetype   = Archetype(fullyQualifiedName(SchedulerLibraryName, SchedulerComponentName))
	SchedulerArchetypeID = ArchetypeID(generateTypeID(SchedulerArchetype))
)
