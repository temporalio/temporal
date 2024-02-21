// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tasks

import (
	"context"

	"go.temporal.io/server/common/backoff"
)

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination task_mock.go
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
