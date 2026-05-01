package workflow

import (
	"errors"
	"fmt"
	"reflect"

	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
)

// ErrDuplicateRegistration is returned by a [Registry] when it detects duplicate registration.
var ErrDuplicateRegistration = errors.New("duplicate registration")

// Library is an interface for registering command handlers and event definitions with a [Registry].
type Library interface {
	CommandHandlers() map[enumspb.CommandType]CommandHandler
	EventDefinitions() []EventDefinition
}

// Registry maintains a the following mappings for a workflow:
// CommandType -> Handler
// EventType -> EventDefinition
type Registry struct {
	commandHandlers          map[enumspb.CommandType]CommandHandler
	eventDefinitions         map[enumspb.EventType]EventDefinition
	eventDefinitionsByGoType map[reflect.Type]EventDefinition
}

// NewRegistry creates a new [Registry].
func NewRegistry() *Registry {
	return &Registry{
		commandHandlers:          make(map[enumspb.CommandType]CommandHandler),
		eventDefinitions:         make(map[enumspb.EventType]EventDefinition),
		eventDefinitionsByGoType: make(map[reflect.Type]EventDefinition),
	}
}

// Register registers all command handlers and event definitions from a [Library].
// Returns an [ErrDuplicateRegistration] if a handler or definition is already registered.
// All registration is expected to happen in a single thread on process initialization.
func (r *Registry) Register(lib Library) error {
	for t, handler := range lib.CommandHandlers() {
		if existing, ok := r.commandHandlers[t]; ok {
			return fmt.Errorf("%w: command handler for %v: %v", ErrDuplicateRegistration, t, existing)
		}
		r.commandHandlers[t] = handler
	}
	for _, def := range lib.EventDefinitions() {
		if existing, ok := r.eventDefinitions[def.Type()]; ok {
			return fmt.Errorf("%w: event handler for %v: %v", ErrDuplicateRegistration, def.Type(), existing)
		}
		goType := reflect.TypeOf(def)
		for goType.Kind() == reflect.Pointer {
			goType = goType.Elem()
		}
		if existing, ok := r.eventDefinitionsByGoType[goType]; ok {
			return fmt.Errorf("%w: event definition for Go type %v: %v", ErrDuplicateRegistration, goType, existing)
		}
		r.eventDefinitions[def.Type()] = def
		r.eventDefinitionsByGoType[goType] = def
	}
	return nil
}

// CommandHandler returns a [CommandHandler] for a given command type and a boolean indicating whether it was found.
func (r *Registry) CommandHandler(t enumspb.CommandType) (handler CommandHandler, ok bool) {
	handler, ok = r.commandHandlers[t]
	return
}

// EventDefinitionByEventType returns an [EventDefinition] for a given event type and a boolean indicating whether it was found.
func (r *Registry) EventDefinitionByEventType(t enumspb.EventType) (EventDefinition, bool) {
	def, ok := r.eventDefinitions[t]
	return def, ok
}

// eventDefinitionByGoType returns an [EventDefinition] for a given Go type and a boolean indicating whether it was found.
// Registration by Go type allows easy go-to-definition navigation in call sites.
func eventDefinitionByGoType[D EventDefinition](r *Registry) (D, bool) {
	var zero D
	goType := reflect.TypeFor[D]()
	for goType.Kind() == reflect.Pointer {
		goType = goType.Elem()
	}
	def, ok := r.eventDefinitionsByGoType[goType]
	if !ok {
		return zero, false
	}
	d, ok := def.(D)
	if !ok {
		// D is a struct but def was registered as a pointer; dereference.
		d, ok = reflect.ValueOf(def).Elem().Interface().(D)
	}
	return d, ok
}

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
