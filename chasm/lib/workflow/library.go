package workflow

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
)

type library struct {
	chasm.UnimplementedLibrary

	registry                    *Registry
	workflowServiceNexusHandler *workflowServiceNexusHandler
	config                      Config
	saMapperProvider            searchattribute.MapperProvider
	saValidator                 *searchattribute.Validator
}

func newLibrary(
	registry *Registry,
	namespaceRegistry namespace.Registry,
	config Config,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
) *library {
	return &library{
		registry:         registry,
		config:           config,
		saMapperProvider: saMapperProvider,
		saValidator:      saValidator,
		workflowServiceNexusHandler: &workflowServiceNexusHandler{
			namespaceRegistry: namespaceRegistry,
		},
	}
}

// NewLibrary creates a new CHASM library for the workflow package.
// Use newLibrary (via fx) for the full setup including Nexus services.
func NewLibrary(registry *Registry) chasm.Library {
	return &library{registry: registry}
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
	}
}

// SetEventRegistryOnContext injects the event registry into a CHASM context. This is primarily
// useful for tests that construct MockMutableContext directly.
func SetEventRegistryOnContext[C chasm.Context](ctx C, registry *Registry) C {
	return chasm.ContextWithValue(ctx, ctxKeyWorkflowContext, &workflowContext{registry: registry})
}

func (l *library) NexusServices() []*nexus.Service {
	if l.workflowServiceNexusHandler == nil {
		return nil
	}
	return []*nexus.Service{
		mustNewWorkflowServiceNexusHandler(l.workflowServiceNexusHandler),
	}
}

func (l *library) NexusServiceProcessors() []*chasm.NexusServiceProcessor {
	if l.workflowServiceNexusHandler == nil {
		return nil
	}
	return []*chasm.NexusServiceProcessor{
		NewWorkflowServiceNexusServiceProcessor(l.config, l.saMapperProvider, l.saValidator),
	}
}
