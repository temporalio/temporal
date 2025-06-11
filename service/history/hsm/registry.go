package hsm

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	enumspb "go.temporal.io/api/enums/v1"
)

// ErrDuplicateRegistration is returned by a [Registry] when it detects duplicate registration.
var ErrDuplicateRegistration = errors.New("duplicate registration")

// ErrNotRegistered is returned by a [Registry] when trying to get a type that is not registered.
var ErrNotRegistered error = notRegisteredError{"not registered"}

var ErrSerializationFailed = errors.New("serialization failed")

// notRegisteredError is returned by a [Registry] when trying to get a type that is not registered.
type notRegisteredError struct {
	Message string
}

func (e notRegisteredError) Error() string {
	return e.Message
}

func (notRegisteredError) IsTerminalTaskError() bool {
	return true
}

// Registry maintains a mapping from state machine type to a [StateMachineDefinition] and task type to [TaskSerializer].
// Registry methods are **not** protected by a lock and all registration is expected to happen in a single thread on
// startup for performance reasons.
type Registry struct {
	machines map[string]StateMachineDefinition
	tasks    map[string]TaskSerializer
	// The executor maps are mapped to any because of Go's limited generics support.
	// The actual value is ImmediateExecutor[T].
	immediateExecutors map[string]any
	// The actual value is TimerExecutor[T].
	timerExecutors  map[string]any
	remoteExecutors map[string]remoteMethodDefinition
	events          map[enumspb.EventType]EventDefinition
}

// NewRegistry creates a new [Registry].
func NewRegistry() *Registry {
	return &Registry{
		machines:           make(map[string]StateMachineDefinition),
		tasks:              make(map[string]TaskSerializer),
		immediateExecutors: make(map[string]any),
		timerExecutors:     make(map[string]any),
		remoteExecutors:    make(map[string]remoteMethodDefinition),
		events:             make(map[enumspb.EventType]EventDefinition),
	}
}

// RegisterMachine registers a [StateMachineDefinition] by its type.
// Returns an [ErrDuplicateRegistration] if the state machine type has already been registered.
func (r *Registry) RegisterMachine(sm StateMachineDefinition) error {
	t := sm.Type()
	if existing, ok := r.machines[t]; ok {
		return fmt.Errorf("%w: state machine already registered for %v - %v", ErrDuplicateRegistration, sm.Type(), existing.Type())
	}
	r.machines[t] = sm
	return nil
}

// Machine returns a [StateMachineDefinition] for a given type and a boolean indicating whether it was found.
func (r *Registry) Machine(t string) (def StateMachineDefinition, ok bool) {
	def, ok = r.machines[t]
	return
}

// RegisterTaskSerializer registers a [TaskSerializer] for a given type.
// Returns an [ErrDuplicateRegistration] if a serializer for this task type has already been registered.
func (r *Registry) RegisterTaskSerializer(t string, def TaskSerializer) error {
	if exising, ok := r.tasks[t]; ok {
		return fmt.Errorf("%w: task already registered for %v: %v", ErrDuplicateRegistration, t, exising)
	}
	r.tasks[t] = def
	return nil
}

// TaskSerializer returns a [TaskSerializer] for a given type and a boolean indicating whether it was found.
func (r *Registry) TaskSerializer(t string) (d TaskSerializer, ok bool) {
	d, ok = r.tasks[t]
	return
}

// RegisterImmediateExecutor registers an [ImmediateExecutor] for the given task type.
// Returns an [ErrDuplicateRegistration] if an executor for the type has already been registered.
func RegisterImmediateExecutor[T Task](r *Registry, executor ImmediateExecutor[T]) error {
	var task T
	taskType := task.Type()
	// The executors are registered in pairs, so only need to check in one map.
	if existing, ok := r.immediateExecutors[taskType]; ok {
		return fmt.Errorf(
			"%w: executor already registered for task type %v: %v",
			ErrDuplicateRegistration,
			taskType,
			existing,
		)
	}
	r.immediateExecutors[taskType] = executor
	return nil
}

// RegisterRemoteMethod registers an [RemoteExecutor] for the given remote method definition.
// Returns an [ErrDuplicateRegistration] if an executor for the type has already been registered.
func RegisterRemoteMethod(r *Registry, method RemoteMethod, executor RemoteExecutor) error {
	methodName := method.Name()
	if existing, ok := r.remoteExecutors[methodName]; ok {
		return fmt.Errorf(
			"%w: executor already registered for method %v: %v",
			ErrDuplicateRegistration,
			methodName,
			existing,
		)
	}

	r.remoteExecutors[methodName] = remoteMethodDefinition{
		method:   method,
		executor: executor,
	}
	return nil
}

// RegisterTimerExecutor registers a [TimerExecutor] for the given task type.
// Returns an [ErrDuplicateRegistration] if an executor for the type has already been registered.
func RegisterTimerExecutor[T Task](r *Registry, executor TimerExecutor[T]) error {
	var task T
	taskType := task.Type()
	// The executors are registered in pairs, so only need to check in one map.
	if existing, ok := r.timerExecutors[taskType]; ok {
		return fmt.Errorf(
			"%w: executor already registered for task type %v: %v",
			ErrDuplicateRegistration,
			taskType,
			existing,
		)
	}
	r.timerExecutors[taskType] = executor
	return nil
}

// ExecuteImmediateTask gets an [ImmediateExecutor] from the registry and invokes it.
// Returns [ErrNotRegistered] if an executor is not registered for the given task's type.
func (r *Registry) ExecuteImmediateTask(
	ctx context.Context,
	env Environment,
	ref Ref,
	task Task,
) error {
	executor, ok := r.immediateExecutors[task.Type()]
	if !ok {
		return fmt.Errorf("%w: executor for task type %v", ErrNotRegistered, task.Type())
	}
	return r.execute(ctx, executor, env, ref, task)
}

// execute invokes an [ImmediateExecutor].
func (r *Registry) execute(
	ctx context.Context,
	executor any,
	env Environment,
	ref Ref,
	task Task,
) error {
	if executor == nil {
		return nil
	}
	fn := reflect.ValueOf(executor)
	values := fn.Call(
		[]reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(env),
			reflect.ValueOf(ref),
			reflect.ValueOf(task),
		},
	)
	if !values[0].IsNil() {
		//nolint:revive // type cast result is unchecked
		return values[0].Interface().(error)
	}
	return nil
}

// ExecuteRemoteMethod gets an [RemoteExecutor] from the registry and invokes it.
// Returns [ErrNotRegistered] if an executor is not registered for the given task's type.
func (r *Registry) ExecuteRemoteMethod(
	ctx context.Context,
	env Environment,
	ref Ref,
	methodName string,
	serializedInput []byte,
) ([]byte, error) {
	defn, ok := r.remoteExecutors[methodName]
	if !ok {
		return nil, fmt.Errorf("executor for remote method %v: %w", methodName, ErrNotRegistered)
	}

	input, err := defn.method.DeserializeInput(serializedInput)
	if err != nil {
		return nil, fmt.Errorf("executor for remote method %v failed to deserialize input: %w", methodName, err)
	}

	output, err := defn.executor(ctx, env, ref, input)

	if err != nil {
		return nil, err
	}

	serializedOutput, err := defn.method.SerializeOutput(output)
	if err != nil {
		return nil, fmt.Errorf("executor for remote method %v failed to serialize output: %w", methodName, err)
	}
	return serializedOutput, nil
}

// ExecuteTimerTask gets a [TimerExecutor] from the registry and invokes it.
// Returns [ErrNotRegistered] if an executor is not registered for the given task's type.
func (r *Registry) ExecuteTimerTask(
	env Environment,
	node *Node,
	task Task,
) error {
	executor, ok := r.timerExecutors[task.Type()]
	if !ok {
		return fmt.Errorf("%w: executor for task type %v", ErrNotRegistered, task.Type())
	}
	return r.executeTimer(executor, env, node, task)
}

// executeTimer invokes a [TimerExecutor].
func (r *Registry) executeTimer(
	executor any,
	env Environment,
	node *Node,
	task Task,
) error {
	if executor == nil {
		return nil
	}
	fn := reflect.ValueOf(executor)
	values := fn.Call(
		[]reflect.Value{
			reflect.ValueOf(env),
			reflect.ValueOf(node),
			reflect.ValueOf(task),
		},
	)
	if !values[0].IsNil() {
		//nolint:revive // type cast result is unchecked
		return values[0].Interface().(error)
	}
	return nil
}

// RegisterEventDefinition registers an [EventDefinition] for the given event type.
// Returns an [ErrDuplicateRegistration] if a definition for the type has already been registered.
func (r *Registry) RegisterEventDefinition(def EventDefinition) error {
	t := def.Type()
	prev, ok := r.events[t]
	if ok {
		return fmt.Errorf("%w: event definition for event type %v: %v", ErrDuplicateRegistration, t, prev)
	}
	r.events[t] = def
	return nil
}

// EventDefinition returns an [EventDefinition] for a given type and a boolean indicating whether it was found.
func (r *Registry) EventDefinition(t enumspb.EventType) (def EventDefinition, ok bool) {
	def, ok = r.events[t]
	return
}
