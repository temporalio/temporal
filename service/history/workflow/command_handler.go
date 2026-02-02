package workflow

import (
	"context"
	"errors"
	"fmt"

	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	chasmcommand "go.temporal.io/server/chasm/lib/workflow/command"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// ErrDuplicateRegistration is returned by a [CommandHandlerRegistry] when it detects duplicate registration.
var ErrDuplicateRegistration = errors.New("duplicate registration")

// CommandHandler is a function for handling a workflow command as part of processing a RespondWorkflowTaskCompleted
// worker request.
type CommandHandler func(
	context.Context,
	historyi.MutableState,
	chasmcommand.Validator,
	int64,
	*commandpb.Command,
) error

// CommandHandlerRegistry maintains a mapping of command type to [CommandHandler].
type CommandHandlerRegistry struct {
	handlers map[enumspb.CommandType]CommandHandler
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
