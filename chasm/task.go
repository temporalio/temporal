//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination task_mock.go

package chasm

import (
	"context"
	"time"
)

type (
	TaskAttributes struct {
		ScheduledTime time.Time
		Destination   string
	}

	SideEffectTaskExecutor[C any, T any] interface {
		Execute(context.Context, ComponentRef, TaskAttributes, T) error
	}

	PureTaskExecutor[C any, T any] interface {
		Execute(MutableContext, C, TaskAttributes, T) error
	}

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

var TaskScheduledTimeImmediate = time.Time{}

func (a *TaskAttributes) IsImmediate() bool {
	return a.ScheduledTime.IsZero() ||
		a.ScheduledTime.Equal(TaskScheduledTimeImmediate)
}

func (a *TaskAttributes) IsValid() bool {
	return a.Destination == "" || a.IsImmediate()
}
