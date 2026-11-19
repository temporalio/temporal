package historydlq

import "go.temporal.io/server/chasm"

const (
	libraryName        = "historydlq"
	entryComponentName = "entry"
)

type Library struct {
	chasm.UnimplementedLibrary
}

func NewNilLibrary() *Library { return &Library{} }

func (l *Library) Name() string { return libraryName }

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Entry](entryComponentName),
	}
}
