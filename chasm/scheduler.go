package chasm

// Scheduler's library and component name constants are exported here, as they
// are used as part of old VisibilityManager queries (e.g., visibility queries
// out-of-band of CHASM). Most components shouldn't have to define, or export,
// these names.
const (
	SchedulerLibraryName   = "scheduler"
	SchedulerComponentName = "scheduler"
)

var (
	SchedulerArchetype   = Archetype(fullyQualifiedName(SchedulerLibraryName, SchedulerComponentName))
	SchedulerArchetypeID = ArchetypeID(generateTypeID(SchedulerArchetype))
)
