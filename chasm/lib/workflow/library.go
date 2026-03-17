package workflow

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/workflow/workflowregistry"
)

type (
	Library struct {
		chasm.UnimplementedLibrary
	}
)

func NewLibrary() *Library {
	return &Library{}
}

func (l *Library) Name() string {
	return chasm.WorkflowLibraryName
}

const mapKeyWorkflow = "workflow"

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Workflow](chasm.WorkflowComponentName, chasm.WithContextValues(map[any]any{
			mapKeyWorkflow: workflowregistry.Registry,
		})),
	}
}
