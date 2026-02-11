package command

import (
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
)

type HandlerOptions struct {
	WorkflowTaskCompletedEventID int64
}

// Handler is a function for handling a workflow command as part of processing a RespondWorkflowTaskCompleted
// worker request.
type Handler func(
	chasmCtx chasm.MutableContext,
	wf *chasmworkflow.Workflow,
	validator Validator,
	command *commandpb.Command,
	opts HandlerOptions,
) error

// Validator is a helper for validating workflow commands.
type Validator interface {
	// IsValidPayloadSize validates that a payload size is within the configured limits.
	IsValidPayloadSize(size int) bool
}

// FailWorkflowTaskError is an error that can be returned from a [Handler] to fail the current workflow task and
// optionally terminate the entire workflow.
type FailWorkflowTaskError struct {
	Cause             enumspb.WorkflowTaskFailedCause
	Message           string
	TerminateWorkflow bool
}

func (e FailWorkflowTaskError) Error() string { return e.Message }
