package persistence

import (
	"context"
	"sync"
	"time"
)

type WorkflowExecutionUpdateReceipt interface {
	Wait(context.Context) error
	Release()
}

type executionUpdateReceipt struct {
	WorkflowExecutionUpdateReceipt
	ctx      context.Context
	complete func(context.Context, error)
	once     sync.Once
}

func (r *executionUpdateReceipt) Wait(ctx context.Context) error {
	err := r.WorkflowExecutionUpdateReceipt.Wait(ctx)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	r.once.Do(func() {
		cleanupCtx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
		defer cancel()
		r.complete(cleanupCtx, err)
	})
	return err
}

func (r *executionUpdateReceipt) Release() {
	_ = r.Wait(context.Background())
	r.WorkflowExecutionUpdateReceipt.Release()
}
