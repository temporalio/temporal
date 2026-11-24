package queues

import (
	"context"

	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
)

// MemoryImmediateQueue submits volatile tasks without blocking or rescheduling when workers are busy.
type MemoryImmediateQueue struct {
	scheduler ctasks.RunnableScheduler
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewMemoryImmediateQueue(scheduler ctasks.RunnableScheduler) *MemoryImmediateQueue {
	ctx, cancel := context.WithCancel(context.Background())
	return &MemoryImmediateQueue{scheduler: scheduler, ctx: ctx, cancel: cancel}
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
		if q.ctx.Err() != nil || !q.scheduler.TrySubmit(memoryImmediateRunnable{Runnable: immediate, queueCtx: q.ctx}) {
			immediate.Abort()
		}
	}
}

type memoryImmediateRunnable struct {
	ctasks.Runnable
	queueCtx context.Context
}

func (r memoryImmediateRunnable) Run(ctx context.Context) {
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
