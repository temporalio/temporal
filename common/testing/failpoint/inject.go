//go:build test_dep

package failpoint

import (
	"context"
	"time"
)

func InjectReturn[T any](
	ctx context.Context,
	key Key,
	ret T,
	recur Recurrence,
) (context.Context, *Failpoint[T]) {
	fp := NewFailpoint[T](key)
	fp.Return(ret, recur)
	return context.WithValue(ctx, ctxKey{key}, fp), fp
}

func InjectPanic[T any](
	ctx context.Context,
	key Key,
	v any,
	recur Recurrence,
) (context.Context, *Failpoint[T]) {
	fp := NewFailpoint[T](key)
	fp.Panic(v, recur)
	return context.WithValue(ctx, ctxKey{key}, fp), fp
}

func InjectBlock[T any](
	ctx context.Context,
	key Key,
	recur Recurrence,
) (context.Context, *Failpoint[T]) {
	fp := NewFailpoint[T](key)
	fp.Block(recur)
	return context.WithValue(ctx, ctxKey{key}, fp), fp
}

func InjectCall[T any](
	ctx context.Context,
	key Key,
	fn func() T,
	recur Recurrence,
) (context.Context, *Failpoint[T]) {
	fp := NewFailpoint[T](key)
	fp.Call(fn, recur)
	return context.WithValue(ctx, ctxKey{key}, fp), fp
}

func InjectPause[T any](
	ctx context.Context,
	key Key,
	dur time.Duration,
	recur Recurrence,
) (context.Context, *Failpoint[T]) {
	fp := NewFailpoint[T](key)
	fp.Pause(dur, recur)
	return context.WithValue(ctx, ctxKey{key}, fp), fp
}
