package chasm

type Archetype string

func (a Archetype) String() string {
	return string(a)
}

const (
	// ArchetypeAny is a special value that matches any archetype.
	ArchetypeAny Archetype = "__any__"
)
