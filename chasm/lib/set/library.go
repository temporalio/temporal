package set

import (
	"go.temporal.io/server/chasm"
)

const (
	libraryName            = "set"
	stringSetComponentName = "string_set"
)

// Library registers the Set implementations so that any process decoding a tree that holds one can
// resolve its type. It has no handlers or tasks.
type Library struct {
	chasm.UnimplementedLibrary
}

// NewNilLibrary registers persisted component types for offline readers.
func NewNilLibrary() *Library {
	return &Library{}
}

func (l *Library) Name() string {
	return libraryName
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*stringSet](stringSetComponentName),
	}
}
