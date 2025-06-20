package clock

import (
	"context"
	"sync"
	"time"
)

type ctxWithDeadline struct {
	context.Context
	deadline time.Time
	timer    Timer
	once     sync.Once
	done     chan struct{}
	err      error
}

func (ctx *ctxWithDeadline) Deadline() (deadline time.Time, ok bool) {
	return ctx.deadline, true
}

func (ctx *ctxWithDeadline) Done() <-chan struct{} {
	return ctx.done
}

func (ctx *ctxWithDeadline) Err() error {
	select {
	case <-ctx.done:
		return ctx.err
	default:
		return nil
	}
}

func (ctx *ctxWithDeadline) deadlineExceeded() {
	ctx.once.Do(func() {
		ctx.err = context.DeadlineExceeded
		close(ctx.done)
	})
}

func (ctx *ctxWithDeadline) cancel() {
	ctx.once.Do(func() {
		// We'd like to call ctx.timer.Stop() here, but we can't: the time source may call
		// deadlineExceeded while holding its lock, which acquires the once mutex. Here we have
		// the once mutex and want to cancel a timer, which would create a potential lock
		// cycle. So just leave the timer as a no-op.
		ctx.err = context.Canceled
		close(ctx.done)
	})
}

func ContextWithDeadline(
	ctx context.Context,
	deadline time.Time,
	timeSource TimeSource,
) (context.Context, context.CancelFunc) {
	ctxd := &ctxWithDeadline{
		Context:  ctx,
		deadline: deadline,
		done:     make(chan struct{}),
	}
	timer := timeSource.AfterFunc(deadline.Sub(timeSource.Now()), ctxd.deadlineExceeded)
	ctxd.timer = timer
	return ctxd, ctxd.cancel
}

func ContextWithTimeout(
	ctx context.Context,
	timeout time.Duration,
	timeSource TimeSource,
) (context.Context, context.CancelFunc) {
	return ContextWithDeadline(ctx, timeSource.Now().Add(timeout), timeSource)
}
