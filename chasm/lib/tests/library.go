package tests

import "go.temporal.io/server/chasm"

type (
	library struct {
		chasm.UnimplementedLibrary
	}
)

var Library = &library{}

func (l *library) Name() string {
	return "tests"
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*PayloadStore]("payloadStore"),
	}
}
