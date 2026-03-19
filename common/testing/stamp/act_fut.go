package stamp

import (
	"context"
	"fmt"

	"go.temporal.io/server/common/future"
)

type (
	Future[T any] struct {
		testEnv testEnv
		ctx     context.Context
		f       *future.FutureImpl[T]
		cancel  context.CancelFunc
	}
)

func newFuture[T any](
	env testEnv,
	fn func() (T, error),
) Future[T] {
	// TODO: configurable timeout
	ctx, cancel := context.WithCancel(env.Context(defaultWaiterTimeout))
	fut := Future[T]{
		testEnv: env,
		ctx:     ctx,
		f:       future.NewFuture[T](),
		cancel:  cancel,
	}

	var zero T
	go func() {
		select {
		case <-ctx.Done():
			fut.f.Set(zero, ctx.Err())
			return
		default:
			res, err := fn()
			select {
			case <-ctx.Done():
				// ignore result if cancelled before
				fut.f.Set(zero, ctx.Err())
				return
			default:
				fut.f.Set(res, err)
			}
		}
	}()

	return fut
}

// TODO: replace with top-level function
func (f *Future[T]) Await() T {
	// TODO: configurable timeout
	res, err := f.f.Get(f.ctx)
	if err != nil {
		panic(fmt.Sprintf("Future[T] Await() failed: %v", err))
	}
	return res
}

func (f *Future[T]) TryAwait() (T, error) {
	// TODO: configurable timeout
	return f.f.Get(f.ctx)
}

func (f *Future[T]) Cancel() {
	if f.cancel != nil {
		f.cancel()
	}
}
