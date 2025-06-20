package future

import (
	"context"
)

type (
	ReadyFutureImpl[T any] struct {
		value T
		err   error
	}
)

func NewReadyFuture[T any](
	value T,
	err error,
) *ReadyFutureImpl[T] {
	return &ReadyFutureImpl[T]{
		value: value,
		err:   err,
	}
}

func (f *ReadyFutureImpl[T]) Get(
	_ context.Context,
) (T, error) {
	return f.value, f.err
}

func (f *ReadyFutureImpl[T]) Ready() bool {
	return true
}
