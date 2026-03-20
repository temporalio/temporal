package workflow

import (
	"go.temporal.io/server/chasm"
)

type Library struct {
	chasm.UnimplementedLibrary

	registry *Registry
}

func NewLibrary(registry *Registry) *Library {
	return &Library{
		registry: registry,
	}
}

func (l *Library) Name() string {
	return chasm.WorkflowLibraryName
}

type chasmCtxKey struct{}

var eventRegistryChasmCtxKey chasmCtxKey = chasmCtxKey{}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Workflow](chasm.WorkflowComponentName, chasm.WithContextValues(map[any]any{
			eventRegistryChasmCtxKey: l.registry,
		})),
	}
}

// SetEventRegistryOnContext injects the event registry into a CHASM context. This is primarily
// useful for tests that construct MockMutableContext directly.
func SetEventRegistryOnContext[C chasm.Context](ctx C, registry *Registry) C {
	return chasm.ContextWithValue(ctx, eventRegistryChasmCtxKey, registry)
}
