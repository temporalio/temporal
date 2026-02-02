package command

import (
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
)

// ErrDuplicateRegistration is returned by a [Registry] when it detects duplicate registration.
var ErrDuplicateRegistration = errors.New("duplicate registration")

// Registry maintains a mapping of command type to [Handler].
type Registry struct {
	handlers map[enumspb.CommandType]Handler
}

// NewRegistry creates a new [Registry].
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[enumspb.CommandType]Handler),
	}
}

// Register registers a [Handler] for a given command type.
// Returns an [ErrDuplicateRegistration] if a handler for the given command is already registered.
// All registration is expected to happen in a single thread on process initialization.
func (r *Registry) Register(t enumspb.CommandType, handler Handler) error {
	if existing, ok := r.handlers[t]; ok {
		return fmt.Errorf("%w: command handler for %v: %v", ErrDuplicateRegistration, t, existing)
	}
	r.handlers[t] = handler
	return nil
}

// Handler returns a [Handler] for a given type and a boolean indicating whether it was found.
func (r *Registry) Handler(t enumspb.CommandType) (handler Handler, ok bool) {
	handler, ok = r.handlers[t]
	return
}
