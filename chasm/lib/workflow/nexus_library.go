package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
)

type nexusLibrary struct {
	config         *nexusoperation.Config
	nexusProcessor *chasm.NexusEndpointProcessor
}

func newNexusLibrary(config *nexusoperation.Config, nexusProcessor *chasm.NexusEndpointProcessor) *nexusLibrary {
	return &nexusLibrary{config: config, nexusProcessor: nexusProcessor}
}

func (l *nexusLibrary) CommandHandlers() map[enumspb.CommandType]CommandHandler {
	h := &nexusCommandHandler{config: l.config, nexusProcessor: l.nexusProcessor}
	return map[enumspb.CommandType]CommandHandler{
		enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION:       h.handleScheduleCommand,
		enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION: h.handleCancelCommand,
	}
}

func (l *nexusLibrary) EventDefinitions() []EventDefinition {
	return []EventDefinition{
		ScheduledEventDefinition{},
		CancelRequestedEventDefinition{},
		CancelRequestCompletedEventDefinition{},
		CancelRequestFailedEventDefinition{},
		StartedEventDefinition{},
		CompletedEventDefinition{},
		FailedEventDefinition{},
		CanceledEventDefinition{},
		TimedOutEventDefinition{},
	}
}
