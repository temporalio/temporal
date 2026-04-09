//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination task_mock.go

package chasm

import (
	"context"
	"errors"
	"time"
)

// ErrTaskDiscarded is the error returned by the default [SideEffectTaskHandlerBase] Discard implementation,
// indicating that a side-effect task on a standby cluster has been pending past the discard delay.
var ErrTaskDiscarded = errors.New("standby task pending for too long")

type (
	// TaskAttributes specifies scheduling metadata for a task.
	TaskAttributes struct {
		// ScheduledTime is when the task should fire. Use [TaskScheduledTimeImmediate] (zero value)
		// for tasks that should execute as soon as possible.
		ScheduledTime time.Time
		// Destination is an optional routing key for outbound tasks (e.g., a URL host for HTTP
		// callbacks). When non-empty, the task is categorized as outbound; when empty, it is
		// categorized as a transfer task. Destination must only be set on immediate tasks.
		Destination string
	}

	// SideEffectTaskHandler handles side effect tasks that run outside of the state lock and have access to a Go
	// context to perform I/O and access chasm engine methods such as [UpdateComponent]. Implementations must embed
	// [SideEffectTaskHandlerBase].
	SideEffectTaskHandler[C any, T any] interface {
		TaskValidator[C, T]
		Execute(context.Context, ComponentRef, TaskAttributes, T) error
		// Discard implements custom discard behavior on standby clusters. When a side-effect task has been
		// pending on standby past the discard delay, the framework calls Discard instead of silently dropping
		// the task. For example, the activity dispatch handler implements this to spill tasks to matching.
		// The ctx carries engine access, but implementations must avoid mutating component state on standby
		// clusters.
		Discard(context.Context, ComponentRef, TaskAttributes, T) error
		sideEffectTaskHandler()
	}

	// PureTaskHandler handles pure tasks that run while holding execution state write lock and should not do I/O.
	// Implementations must embed [PureTaskHandlerBase].
	PureTaskHandler[C any, T any] interface {
		TaskValidator[C, T]
		Execute(MutableContext, C, TaskAttributes, T) error
		pureTaskHandler()
	}

	// TaskValidator is implemented by both [SideEffectTaskHandler] and [PureTaskHandler] to gate
	// whether a task should proceed with execution.
	TaskValidator[C any, T any] interface {
		// Validate determines whether a task should proceed with execution based on the current context, component
		// state, task attributes, and task data.
		//
		// This function serves as a gate to prevent unnecessary task execution in several scenarios:
		// 1. Standby cluster deduplication: When state is replicated to standby clusters, tasks are also replicated.
		//    Validate allows standby clusters to check if a task was already completed on the active cluster and
		//    skip execution if so (e.g., checking if an activity already transitioned from scheduled to started state).
		// 2. Task obsolescence: Tasks can become irrelevant when state changes invalidate them (e.g., when a scheduler
		//    is updated to run at a different time, making the previously scheduled task invalid for the new state).
		//    For pure tasks that can run in a single transaction, Validate is called before execution to avoid
		//    unnecessary work.
		//
		// The framework automatically calls Validate at key points, such as after closing transactions, to check all
		// generated tasks before they execute.
		//
		// Returns:
		// - (true, nil) if the task is valid and should be executed
		// - (false, nil) if the task should be silently dropped (it's no longer relevant)
		// - (anything, error) if validation fails with an error
		Validate(Context, C, TaskAttributes, T) (bool, error)
	}
)

// TaskScheduledTimeImmediate is the zero time value used to indicate that a task should execute immediately.
var TaskScheduledTimeImmediate = time.Time{}

// IsImmediate reports whether the task is scheduled for immediate execution (zero or unset scheduled time).
func (a *TaskAttributes) IsImmediate() bool {
	return a.ScheduledTime.IsZero() ||
		a.ScheduledTime.Equal(TaskScheduledTimeImmediate)
}

// IsValid reports whether the task attributes are well-formed. A Destination may only be set on
// immediate tasks; deferred tasks with a Destination are invalid.
func (a *TaskAttributes) IsValid() bool {
	return a.Destination == "" || a.IsImmediate()
}
