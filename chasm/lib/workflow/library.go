package workflow

import (
	"go.temporal.io/server/chasm"
)

type Library struct {
	chasm.UnimplementedLibrary

	eventRegistry EventRegistry
}

func NewLibrary(eventRegistry EventRegistry) *Library {
	return &Library{
		eventRegistry: eventRegistry,
	}
}

func (l *Library) Name() string {
	return chasm.WorkflowLibraryName
}

const eventRegistryChasmCtxKey = "eventRegistryKey"

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Workflow](chasm.WorkflowComponentName, chasm.WithContextValues(map[any]any{
			eventRegistryChasmCtxKey: l.eventRegistry,
		})),
	}
}
