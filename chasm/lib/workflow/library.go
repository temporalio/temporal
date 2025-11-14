package workflow

import "go.temporal.io/server/chasm"

type (
	Library struct {
		chasm.UnimplementedLibrary
	}
)

func NewLibrary() *Library {
	return &Library{}
}

func (l *Library) Name() string {
	return "workflow"
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Workflow]("Workflow"),
	}
}
