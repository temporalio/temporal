package chasm

import (
	"context"
	"time"
)

type TaskAttributes struct {
	ScheduledTime time.Time
	Destination   string
}

type TaskHandler[C any, T any] interface {
	Validate(Context, C, T) error
	Execute(context.Context, ComponentRef, T) error
}
