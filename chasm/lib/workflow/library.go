package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/workflow/command"
)

type (
	Library struct {
		chasm.UnimplementedLibrary
		commandHandlers *command.Registry
	}
)

func NewLibrary() *Library {
	return &Library{
		commandHandlers: command.NewRegistry(),
	}
}

func (l *Library) Name() string {
	return chasm.WorkflowLibraryName
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Workflow](chasm.WorkflowComponentName),
	}
}

// RegisterHandler registers a command handler for the given command type.
func (l *Library) RegisterHandler(t enumspb.CommandType, handler command.Handler) error {
	return l.commandHandlers.Register(t, handler)
}

// CommandRegistry returns the command handler registry.
func (l *Library) CommandRegistry() *command.Registry {
	return l.commandHandlers
}
