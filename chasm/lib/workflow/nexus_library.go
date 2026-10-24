package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/log"
)

type nexusLibrary struct {
	config          *nexusoperation.Config
	nexusProcessor  *chasm.NexusEndpointProcessor
	throttledLogger log.ThrottledLogger
}

func newNexusLibrary(
	config *nexusoperation.Config,
	nexusProcessor *chasm.NexusEndpointProcessor,
	throttledLogger log.ThrottledLogger,
) *nexusLibrary {
	return &nexusLibrary{config: config, nexusProcessor: nexusProcessor, throttledLogger: throttledLogger}
}

func (l *nexusLibrary) CommandHandlers() map[enumspb.CommandType]CommandHandler {
	h := &nexusCommandHandler{config: l.config, nexusProcessor: l.nexusProcessor, throttledLogger: l.throttledLogger}
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
