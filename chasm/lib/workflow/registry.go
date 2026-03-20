package workflow

import (
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
)

// ErrDuplicateRegistration is returned by a [Registry] when it detects duplicate registration.
var ErrDuplicateRegistration = errors.New("duplicate registration")

// Registry maintains a the following mappings for a workflow:
// CommandType -> Handler
// EventType -> EventDefinition
type Registry struct {
	commandHandlers  map[enumspb.CommandType]CommandHandler
	eventDefinitions map[enumspb.EventType]EventDefinition
}

// NewRegistry creates a new [Registry].
func NewRegistry() *Registry {
	return &Registry{
		commandHandlers:  make(map[enumspb.CommandType]CommandHandler),
		eventDefinitions: make(map[enumspb.EventType]EventDefinition),
	}
}

// RegisterCommandHandler registers a [CommandHandler] for a given command type.
// Returns an [ErrDuplicateRegistration] if a handler for the given command is already registered.
// All registration is expected to happen in a single thread on process initialization.
func (r *Registry) RegisterCommandHandler(t enumspb.CommandType, handler CommandHandler) error {
	if existing, ok := r.commandHandlers[t]; ok {
		return fmt.Errorf("%w: command handler for %v: %v", ErrDuplicateRegistration, t, existing)
	}
	r.commandHandlers[t] = handler
	return nil
}

// CommandHandler returns a [CommandHandler] for a given command type and a boolean indicating whether it was found.
func (r *Registry) CommandHandler(t enumspb.CommandType) (handler CommandHandler, ok bool) {
	handler, ok = r.commandHandlers[t]
	return
}

// RegisterEventDefinition registers an [EventDefinition] for a given event type.
// Returns an [ErrDuplicateRegistration] if a handler for the given event is already registered.
// All registration is expected to happen in a single thread on process initialization.
func (r *Registry) RegisterEventDefinition(def EventDefinition) error {
	if existing, ok := r.eventDefinitions[def.Type()]; ok {
		return fmt.Errorf("%w: event handler for %v: %v", ErrDuplicateRegistration, def.Type(), existing)
	}
	r.eventDefinitions[def.Type()] = def
	return nil
}

// EventDefinition returns an [EventDefinition] for a given event type and a boolean indicating whether it was found.
func (r *Registry) EventDefinition(t enumspb.EventType) (EventDefinition, bool) {
	def, ok := r.eventDefinitions[t]
	return def, ok
}
