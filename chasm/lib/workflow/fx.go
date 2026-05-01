package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.workflow",
	fx.Provide(NewRegistry),
	fx.Provide(newLibrary),
	fx.Invoke(func(
		chasmRegistry *chasm.Registry,
		library *library,
		config *nexusoperation.Config,
	) error {
		if err := library.registry.Register(
			newNexusLibrary(config, chasmRegistry.NexusEndpointProcessor),
		); err != nil {
			return err
		}
		return chasmRegistry.Register(library)
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
