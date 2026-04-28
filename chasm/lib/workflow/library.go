package workflow

import (
	"go.temporal.io/server/chasm"
)

type library struct {
	chasm.UnimplementedLibrary

	registry *Registry
}

func newLibrary(registry *Registry) *library {
	return &library{
		registry: registry,
	}
}

// NewLibrary creates a new CHASM library for the workflow package.
func NewLibrary(registry *Registry) chasm.Library {
	return newLibrary(registry)
}

func (l *library) Name() string {
	return chasm.WorkflowLibraryName
}

type workflowContext struct {
	registry *Registry
}

type ctxKeyWorkflowContextType struct{}

var ctxKeyWorkflowContext = ctxKeyWorkflowContextType{}

func workflowContextFromChasm(ctx chasm.Context) *workflowContext {
	wc, ok := ctx.Value(ctxKeyWorkflowContext).(*workflowContext)
	if !ok {
		return nil
	}
	return wc
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Workflow](chasm.WorkflowComponentName, chasm.WithContextValues(map[any]any{
			ctxKeyWorkflowContext: &workflowContext{registry: l.registry},
		})),
		chasm.NewRegistrableComponent[*WorkflowUpdate]("update"),
	}
}

// SetEventRegistryOnContext injects the event registry into a CHASM context. This is primarily
// useful for tests that construct MockMutableContext directly.
func SetEventRegistryOnContext[C chasm.Context](ctx C, registry *Registry) C {
	return chasm.ContextWithValue(ctx, ctxKeyWorkflowContext, &workflowContext{registry: registry})
}
