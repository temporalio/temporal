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

// === Failpoint2 ===

func InjectReturn2[T1, T2 any](
	ctx context.Context,
	key Key,
	ret1 T1,
	ret2 T2,
	recur Recurrence,
) (context.Context, *Failpoint2[T1, T2]) {
	fp := NewFailpoint2[T1, T2](key)
	fp.Return(ret1, ret2, recur)
	return context.WithValue(ctx, ctxKey{key}, fp), fp
}

func InjectPanic2[T1, T2 any](
	ctx context.Context,
	key Key,
	v any,
	recur Recurrence,
) (context.Context, *Failpoint2[T1, T2]) {
	fp := NewFailpoint2[T1, T2](key)
	fp.Panic(v, recur)
	return context.WithValue(ctx, ctxKey{key}, fp), fp
}

func InjectBlock2[T1, T2 any](
	ctx context.Context,
	key Key,
	recur Recurrence,
) (context.Context, *Failpoint2[T1, T2]) {
	fp := NewFailpoint2[T1, T2](key)
	fp.Block(recur)
	return context.WithValue(ctx, ctxKey{key}, fp), fp
}

func InjectCall2[T1, T2 any](
	ctx context.Context,
	key Key,
	fn func() (T1, T2),
	recur Recurrence,
) (context.Context, *Failpoint2[T1, T2]) {
	fp := NewFailpoint2[T1, T2](key)
	fp.Call(fn, recur)
	return context.WithValue(ctx, ctxKey{key}, fp), fp
}

func InjectPause2[T1, T2 any](
	ctx context.Context,
	key Key,
	dur time.Duration,
	recur Recurrence,
) (context.Context, *Failpoint2[T1, T2]) {
	fp := NewFailpoint2[T1, T2](key)
	fp.Pause(dur, recur)
	return context.WithValue(ctx, ctxKey{key}, fp), fp
}
