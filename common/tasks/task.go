package tasks

import (
	"context"

	"go.temporal.io/server/common/backoff"
)

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination task_mock.go
type (
	Runnable interface {
		// Run and handle errors, abort on context error.
		Run(context.Context)
		// Abort marks the task as aborted, usually means task scheduler shutdown.
		Abort()
	}

	// Task is the interface for tasks which should be executed sequentially
	Task interface {
		// Execute process this task
		Execute() error
		// HandleErr handle the error returned by Execute
		HandleErr(err error) error
		// IsRetryableError check whether to retry after HandleErr(Execute())
		IsRetryableError(err error) bool
		// RetryPolicy returns the retry policy for task processing
		RetryPolicy() backoff.RetryPolicy
		// Abort marks the task as aborted, usually means task executor shutdown
		Abort()
		// Cancel marks the task as cancelled, usually by the task submitter
		Cancel()
		// Ack marks the task as successful completed
		Ack()
		// Nack marks the task as unsuccessful completed
		Nack(err error)
		// Reschedule marks the task for retry
		Reschedule()
		// State returns the current task state
		State() State
	}
)
