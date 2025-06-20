package workflow

import (
	"context"
	"errors"
	"fmt"

	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// ErrDuplicateRegistration is returned by a [CommandHandlerRegistry] when it detects duplicate registration.
var ErrDuplicateRegistration = errors.New("duplicate registration")

// FailWorkflowTaskError is an error that can be returned from a [CommandHandler] to fail the current workflow task and
// optionally terminate the entire workflow.
type FailWorkflowTaskError struct {
	// The cause to set on the WorkflowTaskFailed event.
	Cause             enumspb.WorkflowTaskFailedCause
	Message           string
	TerminateWorkflow bool
}

func (e FailWorkflowTaskError) Error() string {
	return e.Message
}

// CommandHandler is a function for handling a workflow command as part of processing a RespondWorkflowTaskCompleted
// worker request.
type CommandHandler func(
	context.Context,
	historyi.MutableState,
	CommandValidator,
	int64,
	*commandpb.Command,
) error

// CommandHandlerRegistry maintains a mapping of command type to [CommandHandler].
type CommandHandlerRegistry struct {
	handlers map[enumspb.CommandType]CommandHandler
}

// CommandValidator is a helper for validating workflow commands.
type CommandValidator interface {
	// IsValidPayloadSize validates that a payload size is within the configured limits.
	IsValidPayloadSize(size int) bool
}

// NewCommandHandlerRegistry creates a new [CommandHandlerRegistry].
func NewCommandHandlerRegistry() *CommandHandlerRegistry {
	return &CommandHandlerRegistry{
		handlers: make(map[enumspb.CommandType]CommandHandler),
	}
}

// Register registers a [CommandHandler] for a given command type.
// Returns an [ErrDuplicateRegistration] if a handler for the given command is already registered.
// All registration is expected to happen in a single thread on process initialization.
func (r *CommandHandlerRegistry) Register(t enumspb.CommandType, handler CommandHandler) error {
	if existing, ok := r.handlers[t]; ok {
		return fmt.Errorf("%w: command handler for %v: %v", ErrDuplicateRegistration, t, existing)
	}
	r.handlers[t] = handler
	return nil
}

// Handler returns a [CommandHandler] for a given type and a boolean indicating whether it was found.
func (r *CommandHandlerRegistry) Handler(t enumspb.CommandType) (handler CommandHandler, ok bool) {
	handler, ok = r.handlers[t]
	return
}
