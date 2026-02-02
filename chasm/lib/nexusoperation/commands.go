package nexusoperation

import (
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/workflow/command"
)

func registerCommandHandlers(registry *command.Registry) error {
	if err := registry.Register(
		enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
		handleScheduleCommand,
	); err != nil {
		return err
	}
	return registry.Register(
		enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION,
		handleCancelCommand,
	)
}

func handleScheduleCommand(
	chasmCtx chasm.MutableContext,
	validator command.Validator,
	cmd *commandpb.Command,
	opts command.HandlerOptions,
) error {
	// TODO: Implement CHASM nexus operation scheduling
	return serviceerror.NewUnimplemented("CHASM nexus operation scheduling not yet implemented")
}

func handleCancelCommand(
	chasmCtx chasm.MutableContext,
	validator command.Validator,
	cmd *commandpb.Command,
	opts command.HandlerOptions,
) error {
	// TODO: Implement CHASM nexus operation cancellation
	return serviceerror.NewUnimplemented("CHASM nexus operation cancellation not yet implemented")
}
