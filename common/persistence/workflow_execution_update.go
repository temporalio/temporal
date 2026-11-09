package persistence

import (
	"context"
	"sync"
)

type (
	// WorkflowExecutionUpdateReceipt reports the outcome of a submitted workflow execution update.
	WorkflowExecutionUpdateReceipt interface {
		// Wait blocks until the update is durable or has failed, or until ctx is done. A write
		// error has the same meaning as an UpdateWorkflowExecution error. Wait may be called
		// more than once.
		Wait(ctx context.Context) error
	}

	// CompletedWorkflowExecutionUpdateReceipt is the receipt for an update whose outcome was
	// known when it was submitted.
	CompletedWorkflowExecutionUpdateReceipt struct {
		Err error
	}

	observedWorkflowExecutionUpdateReceipt struct {
		receipt WorkflowExecutionUpdateReceipt
		observe func(ctx context.Context, err error)
		once    sync.Once
	}
)

func (r CompletedWorkflowExecutionUpdateReceipt) Wait(context.Context) error {
	return r.Err
}

// ObserveWorkflowExecutionUpdate returns a receipt that runs observe once, with the waiter's
// ctx and the first result of Wait. That result is ctx.Err() if the first waiter gave up
// before the outcome was known.
func ObserveWorkflowExecutionUpdate(
	receipt WorkflowExecutionUpdateReceipt,
	observe func(ctx context.Context, err error),
) WorkflowExecutionUpdateReceipt {
	return &observedWorkflowExecutionUpdateReceipt{receipt: receipt, observe: observe}
}

func (r *observedWorkflowExecutionUpdateReceipt) Wait(ctx context.Context) error {
	err := r.receipt.Wait(ctx)
	r.once.Do(func() { r.observe(ctx, err) })
	return err
}
