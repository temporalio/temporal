package chasm

// Archetype is the fully qualified name of the root component of a CHASM execution.
type Archetype = string

// ArchetypeID is CHASM framework's internal ID for an Archetype.
type ArchetypeID = uint32

const (
	// UnspecifiedArchetypeID is a reserved special ArchetypeID value indicating that the
	// ArchetypeID is not specified.
	// This typically happens when:
	// 1. The chasm tree is not yet initialized with a root component,
	// 2. If it's a field in a persisted record, it means the record is persisted before archetypeID
	// was introduced (basically Workflow).
	UnspecifiedArchetypeID ArchetypeID = 0
)
