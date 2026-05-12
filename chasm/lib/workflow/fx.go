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
// and command handlers with the workflow event registry.
type activityEventLibrary struct{}

func (activityEventLibrary) CommandHandlers() map[enumspb.CommandType]CommandHandler {
	h := &activityCommandHandler{}
	return map[enumspb.CommandType]CommandHandler{
		enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK: h.handleScheduleCommand,
	}
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
