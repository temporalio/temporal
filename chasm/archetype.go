package chasm

// Archetype is the fully qualified name of the root component of a CHASM execution.
type Archetype = string

// ArchetypeID is CHASM framework's internal ID for an Archetype.
type ArchetypeID = uint32

const (
	// ArchetypeAny is a special value that matches any archetype.
	// TODO: deprecate this constant and always specify the actual archetypeID of the execution.
	ArchetypeAny Archetype = "__any__"
)
