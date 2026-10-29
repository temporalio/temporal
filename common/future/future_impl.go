package future

import (
	"context"
	"errors"
	"sync/atomic"
)

var errorFutureNotReady = errors.New("future is not ready yet")

const (
	// pending status indicates future is not ready
	// setting status indicates future is in transition to be ready, used to prevent data race
	// ready   status indicates future is ready

	pending int32 = iota
	setting
	ready
)

type (
	FutureImpl[T any] struct {
		status  int32
		readyCh chan struct{}

		value T
		err   error
	}
)

func NewFuture[T any]() *FutureImpl[T] {
	var value T
	return &FutureImpl[T]{
		status:  pending,
		readyCh: make(chan struct{}),

		value: value,
		err:   nil,
	}
}

func (f *FutureImpl[T]) Get(
	ctx context.Context,
) (T, error) {
	if f.Ready() {
		return f.value, f.err
	}

	select {
	case <-f.readyCh:
		return f.value, f.err
	case <-ctx.Done():
		var value T
		return value, ctx.Err()
	}
}

func (f *FutureImpl[T]) GetIfReady() (T, error) {
	if f.Ready() {
		return f.value, f.err
	}
	var value T
	return value, errorFutureNotReady
}

func (f *FutureImpl[T]) Set(
	value T,
	err error,
) {
	// cannot directly set status to `ready`, to prevent data race in case multiple `Get` occurs
	// instead set status to `setting` to prevent concurrent completion of this future
	if !atomic.CompareAndSwapInt32(
		&f.status,
		pending,
		setting,
	) {
		panic("future has already been completed")
	}

	f.value = value
	f.err = err
	atomic.CompareAndSwapInt32(&f.status, setting, ready)
	close(f.readyCh)
}

// Sets the value of the future, if it has not been set already. Returns true if this call set the value.
func (f *FutureImpl[T]) SetIfNotReady(
	value T,
	err error,
) bool {
	if !atomic.CompareAndSwapInt32(
		&f.status,
		pending,
		setting,
	) {
		return false
	}

	f.value = value
	f.err = err
	atomic.CompareAndSwapInt32(&f.status, setting, ready)
	close(f.readyCh)
	return true
}

func (f *FutureImpl[T]) Ready() bool {
	return atomic.LoadInt32(&f.status) == ready
}
