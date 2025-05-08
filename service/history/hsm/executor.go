package hsm

import (
	"context"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
)

// Ref is a reference to a statemachine on a specific workflow.
// It contains the workflow key and the key of the statemachine in the state machine [Environment] as well as the
// information to perform staleness checks for itself or the state that it is referencing.
type Ref struct {
	WorkflowKey     definition.WorkflowKey
	StateMachineRef *persistencespb.StateMachineRef
	// If non-zero, this field represents the ID of the task this Ref came from. Used for stale task detection and
	// serves as an indicator whether this Ref can reference stale state. This should be set during task processing
	// where we can validate the task that embeds this reference against shard clock.
	TaskID int64

	// An optional function to validate the ref is not stale.
	// For tasks, this is copied from the task's Validate() implementation.
	Validate func(ref *persistencespb.StateMachineRef, node *Node) error
}

// StateMachinePath gets the state machine path for from this reference.
func (r Ref) StateMachinePath() []Key {
	path := make([]Key, len(r.StateMachineRef.Path))
	for i, k := range r.StateMachineRef.Path {
		path[i] = Key{Type: k.Type, ID: k.Id}
	}
	return path
}

// AccessType is a specifier for storage access.
type AccessType int

const (
	// AccessRead specifies read access.
	AccessRead AccessType = iota
	// AccessWrite specifies write access.
	AccessWrite AccessType = iota
)

// Executor environment.
type Environment interface {
	// Wall clock. Backed by a the shard's time source.
	Now() time.Time
	// Access a state machine Node for the given ref.
	//
	// When using AccessRead, the accessor must guarantee not to mutate any state, accessor errors will not cause
	// mutable state unload.
	Access(ctx context.Context, ref Ref, accessType AccessType, accessor func(*Node) error) error
}

// ImmediateExecutor is responsible for executing immediate tasks (e.g: transfer, outbound).
// Implementations should be registered via [RegisterImmediateExecutors] to handle specific task types.
type ImmediateExecutor[T Task] func(ctx context.Context, env Environment, ref Ref, task T) error

// TimerExecutor is responsible for executing timer tasks.
// Implementations should be registered via [RegisterTimerExecutors] to handle specific task types.
// Timers tasks are collapsed into a single task which will execute all timers that have hit their deadline while
// holding a lock on the workflow.
type TimerExecutor[T Task] func(env Environment, node *Node, task T) error

// RemoteExecutor is responsible for executing remote methods.
// // Implementations should be registered via [RegisterRemoteExecutors] to handle specific methods.
type RemoteExecutor func(ctx context.Context, env Environment, ref Ref, input any) (any, error)
