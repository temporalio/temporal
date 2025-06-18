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
		Execute(context.Context, ComponentRef, T) error
	}

	PureTaskExecutor[C any, T any] interface {
		Execute(MutableContext, C, T) error
	}

	TaskValidator[C any, T any] interface {
		Validate(Context, C, T) (bool, error)
	}
)

var TaskScheduledTimeImmediate = time.Time{}
