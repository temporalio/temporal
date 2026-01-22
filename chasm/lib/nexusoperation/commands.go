package nexusoperation

import (
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/workflow/command"
	commonnexus "go.temporal.io/server/common/nexus"
)

type commandHandler struct {
	config           *Config
	endpointRegistry commonnexus.EndpointRegistry
}

func RegisterCommandHandlers(
	registry *command.Registry,
	endpointRegistry commonnexus.EndpointRegistry,
	config *Config,
) error {
	h := &commandHandler{config: config, endpointRegistry: endpointRegistry}
	if err := registry.Register(
		enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
		h.handleScheduleCommand,
	); err != nil {
		return err
	}
	return registry.Register(
		enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION,
		h.handleCancelCommand,
	)
}

func (ch *commandHandler) handleScheduleCommand(
	chasmCtx chasm.MutableContext,
	ms command.Backend,
	validator command.Validator,
	workflowTaskCompletedEventID int64,
	cmd *commandpb.Command,
) error {
	// TODO: Implement CHASM nexus operation scheduling
	return serviceerror.NewUnimplemented("CHASM nexus operation scheduling not yet implemented")
}

func (ch *commandHandler) handleCancelCommand(
	chasmCtx chasm.MutableContext,
	ms command.Backend,
	validator command.Validator,
	workflowTaskCompletedEventID int64,
	cmd *commandpb.Command,
) error {
	// TODO: Implement CHASM nexus operation cancellation
	return serviceerror.NewUnimplemented("CHASM nexus operation cancellation not yet implemented")
}
