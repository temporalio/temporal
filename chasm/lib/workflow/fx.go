package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.workflow",
	fx.Provide(NewRegistry),
	fx.Provide(newLibrary),
	fx.Invoke(func(registry *chasm.Registry, library *library) error {
		return registry.Register(library)
	}),
	fx.Invoke(func(registry *Registry) error {
		return registry.Register(activityEventLibrary{})
	}),
)

// activityEventLibrary is a [Library] that registers activity task event definitions
// with the workflow event registry. It has no command handlers.
type activityEventLibrary struct{}

func (activityEventLibrary) CommandHandlers() map[enumspb.CommandType]CommandHandler {
	return nil
}

func (activityEventLibrary) EventDefinitions() []EventDefinition {
	return []EventDefinition{
		ActivityTaskScheduledEventDefinition{},
		ActivityTaskStartedEventDefinition{},
		ActivityTaskCompletedEventDefinition{},
		ActivityTaskFailedEventDefinition{},
		ActivityTaskTimedOutEventDefinition{},
		ActivityTaskCancelRequestedEventDefinition{},
		ActivityTaskCanceledEventDefinition{},
	}
}
