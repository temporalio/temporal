package workflowregistry

import (
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
)

// ErrDuplicateRegistration is returned by a [Registry] when it detects duplicate registration.
var ErrDuplicateRegistration = errors.New("duplicate registration")

// Registry maintains a mapping of command type to [Handler].
type Registry struct {
	commandHandlers map[enumspb.CommandType]Handler
	eventHandlers   map[enumspb.EventType]EventDefinition
}

// NewRegistry creates a new [Registry].
func NewRegistry() *Registry {
	return &Registry{
		commandHandlers: make(map[enumspb.CommandType]Handler),
		eventHandlers:   make(map[enumspb.EventType]EventDefinition),
	}
}

// RegisterCommandHandler registers a [Handler] for a given command type.
// Returns an [ErrDuplicateRegistration] if a handler for the given command is already registered.
// All registration is expected to happen in a single thread on process initialization.
func (r *Registry) RegisterCommandHandler(t enumspb.CommandType, handler Handler) error {
	if existing, ok := r.commandHandlers[t]; ok {
		return fmt.Errorf("%w: command handler for %v: %v", ErrDuplicateRegistration, t, existing)
	}
	r.commandHandlers[t] = handler
	return nil
}

// CommandHandler returns a [Handler] for a given command type and a boolean indicating whether it was found.
func (r *Registry) CommandHandler(t enumspb.CommandType) (handler Handler, ok bool) {
	handler, ok = r.commandHandlers[t]
	return
}

// RegisterEvent registers a [EventDefinition] for a given event type.
// Returns an [ErrDuplicateRegistration] if a handler for the given event is already registered.
// All registration is expected to happen in a single thread on process initialization.
func (r *Registry) RegisterEvent(t enumspb.EventType, handler EventDefinition) error {
	if existing, ok := r.eventHandlers[t]; ok {
		return fmt.Errorf("%w: event handler for %v: %v", ErrDuplicateRegistration, t, existing)
	}
	r.eventHandlers[t] = handler
	return nil
}

// EventHandler returns a [workflow.EventDefinition] for a given event type and a boolean indicating whether it was found.
func (r *Registry) EventHandler(t enumspb.EventType) (handler EventDefinition, ok bool) {
	handler, ok = r.eventHandlers[t]
	return
}
