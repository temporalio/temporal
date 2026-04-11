package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
)

type library struct {
	config         *nexusoperation.Config
	nexusProcessor *chasm.NexusEndpointProcessor
}

func newLibrary(config *nexusoperation.Config, nexusProcessor *chasm.NexusEndpointProcessor) *library {
	return &library{config: config, nexusProcessor: nexusProcessor}
}

func (l *library) CommandHandlers() map[enumspb.CommandType]chasmworkflow.CommandHandler {
	h := &commandHandler{config: l.config, nexusProcessor: l.nexusProcessor}
	return map[enumspb.CommandType]chasmworkflow.CommandHandler{
		enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION:       h.handleScheduleCommand,
		enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION: h.handleCancelCommand,
	}
}

func (l *library) EventDefinitions() []chasmworkflow.EventDefinition {
	return []chasmworkflow.EventDefinition{
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
