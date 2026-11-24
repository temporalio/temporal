package queues

import (
	"context"
	"time"

	"go.temporal.io/server/common/clock"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
)

// MemoryImmediateQueue submits volatile tasks without blocking and skips tasks whose queue timeout has elapsed.
type MemoryImmediateQueue struct {
	scheduler  ctasks.RunnableScheduler
	ctx        context.Context
	cancel     context.CancelFunc
	timeSource clock.TimeSource
}

func NewMemoryImmediateQueue(scheduler ctasks.RunnableScheduler, timeSource clock.TimeSource) *MemoryImmediateQueue {
	ctx, cancel := context.WithCancel(context.Background())
	return &MemoryImmediateQueue{scheduler: scheduler, ctx: ctx, cancel: cancel, timeSource: timeSource}
}

func (q *MemoryImmediateQueue) Start()                   {}
func (q *MemoryImmediateQueue) Stop()                    { q.cancel() }
func (q *MemoryImmediateQueue) Category() tasks.Category { return tasks.CategoryMemoryImmediate }
func (q *MemoryImmediateQueue) FailoverNamespace(string) {}

func (q *MemoryImmediateQueue) NotifyNewTasks(ts []tasks.Task) {
	for _, task := range ts {
		immediate, ok := task.(*tasks.MemoryImmediateTask)
		if !ok {
			continue
		}
		var deadline time.Time
		if immediate.QueueTimeout > 0 {
			deadline = q.timeSource.Now().Add(immediate.QueueTimeout)
		}
		if q.ctx.Err() != nil || !q.scheduler.TrySubmit(memoryImmediateRunnable{
			Runnable: immediate, queueCtx: q.ctx, timeSource: q.timeSource, deadline: deadline,
		}) {
			immediate.Abort()
		}
	}
}

type memoryImmediateRunnable struct {
	ctasks.Runnable
	queueCtx   context.Context
	timeSource clock.TimeSource
	deadline   time.Time
}

func (r memoryImmediateRunnable) Run(ctx context.Context) {
	if !r.deadline.IsZero() && !r.timeSource.Now().Before(r.deadline) {
		r.Abort()
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.queueCtx, cancel)
	defer stop()
	defer cancel()
	if r.queueCtx.Err() != nil || ctx.Err() != nil {
		r.Abort()
		return
	}
	r.Runnable.Run(ctx)
}
