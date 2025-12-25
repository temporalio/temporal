package workflow

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
)

type ComponentOnlyLibrary struct {
	chasm.UnimplementedLibrary
}

func NewComponentOnlyLibrary() *ComponentOnlyLibrary {
	return &ComponentOnlyLibrary{}
}

func (l *ComponentOnlyLibrary) Name() string {
	return chasm.WorkflowLibraryName
}

func (l *ComponentOnlyLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Workflow](chasm.WorkflowComponentName),
	}
}

type Library struct {
	ComponentOnlyLibrary
	workflowServiceNexusHandler *workflowServiceNexusHandler
	config                      Config
	saMapperProvider            searchattribute.MapperProvider
	saValidator                 *searchattribute.Validator
}

func NewLibrary(
	namespaceRegistry namespace.Registry,
	config Config,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
) *Library {
	return &Library{
		config:           config,
		saMapperProvider: saMapperProvider,
		saValidator:      saValidator,
		workflowServiceNexusHandler: &workflowServiceNexusHandler{
			namespaceRegistry: namespaceRegistry,
		},
	}
}

func (l *Library) NexusServices() []*nexus.Service {
	return []*nexus.Service{
		mustNewWorkflowServiceNexusHandler(l.workflowServiceNexusHandler),
	}
}

func (l *Library) NexusServiceProcessors() []*chasm.NexusServiceProcessor {
	return []*chasm.NexusServiceProcessor{
		NewWorkflowServiceNexusServiceProcessor(l.config, l.saMapperProvider, l.saValidator),
	}
}
