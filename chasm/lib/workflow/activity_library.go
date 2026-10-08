package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
)

// activityLibrary holds the command handlers and event definitions for activities that are CHASM
// components of the workflow. It is not registered by the server, which runs workflow activities
// in mutable state.
type activityLibrary struct {
	config Config
}

// NewActivityLibrary returns the command handlers and event definitions for workflow activities
// that are CHASM components of the workflow.
func NewActivityLibrary(config Config) Library {
	return &activityLibrary{config: config}
}

func (l *activityLibrary) CommandHandlers() map[enumspb.CommandType]CommandHandler {
	h := &activityCommandHandler{config: l.config}
	return map[enumspb.CommandType]CommandHandler{
		enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK: h.handleScheduleCommand,
	}
}

func (l *activityLibrary) EventDefinitions() []EventDefinition {
	return []EventDefinition{
		ActivityTaskScheduledEventDefinition{},
		ActivityTaskStartedEventDefinition{},
		ActivityTaskCompletedEventDefinition{},
		ActivityTaskFailedEventDefinition{},
		ActivityTaskTimedOutEventDefinition{},
		ActivityTaskCanceledEventDefinition{},
	}
}
