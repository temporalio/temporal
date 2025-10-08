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
