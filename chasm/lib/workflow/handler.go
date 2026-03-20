package workflow

import (
	"errors"

	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
)

// ErrCommandNotSupported is returned by a [CommandHandler] when the command type is registered but not supported;
// for example, because of a disabled feature flag.
var ErrCommandNotSupported = errors.New("command not supported")

type CommandHandlerOptions struct {
	WorkflowTaskCompletedEventID int64
}

// CommandHandler is a function for handling a workflow command as part of processing a RespondWorkflowTaskCompleted
// worker request.
type CommandHandler func(
	chasmCtx chasm.MutableContext,
	wf *Workflow,
	validator Validator,
	command *commandpb.Command,
	opts CommandHandlerOptions,
) error

// Validator is a helper for validating workflow commands.
type Validator interface {
	// IsValidPayloadSize validates that a payload size is within the configured limits.
	IsValidPayloadSize(size int) bool
}

// FailWorkflowTaskError is an error that can be returned from a [CommandHandler] to fail the current workflow task and
// optionally terminate the entire workflow.
type FailWorkflowTaskError struct {
	Cause             enumspb.WorkflowTaskFailedCause
	Message           string
	TerminateWorkflow bool
}

func (e FailWorkflowTaskError) Error() string { return e.Message }
