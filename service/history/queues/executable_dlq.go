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

package queues

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
)

type (
	// ExecutableDLQ is an Executable decorator that will enqueue the task to a DLQ if the underlying Executable fails.
	// The first call to Execute for which the underlying Executable returns a terminal error will return an
	// ErrTerminalTaskFailure error and change the behavior of this task to just try to send itself to the DLQ.
	// Specifically, all subsequent calls to Execute will attempt to send the task to the DLQ. If this fails, the error
	// will be returned (with the expectation that clients will retry). If it succeeds, no error will be returned and
	// the task can be acked. When the executable is in the failed state, all calls to HandleErr will just return the
	// error passed in.
	// TODO: wrap all executables with this
	// TODO: add metrics and logging to this
	ExecutableDLQ struct {
		Executable
		dlq        DLQ
		timeSource clock.TimeSource
		// dlqCause is the original error which caused this task to be sent to the DLQ. It is only set once.
		dlqCause    error
		clusterName string
	}

	// DLQ is a dead letter queue that can be used to enqueue tasks that fail to be processed.
	DLQ interface {
		EnqueueTask(
			ctx context.Context,
			request *persistence.EnqueueTaskRequest,
		) (*persistence.EnqueueTaskResponse, error)
	}
)

const (
	sendToDLQTimeout = 3 * time.Second
)

var (
	_                      Executable = new(ExecutableDLQ)
	ErrTerminalTaskFailure            = errors.New("original task failed and this task is now to send the original to the DLQ")
	ErrSendTaskToDLQ                  = errors.New("failed to send task to DLQ")
)

// NewExecutableDLQ wraps an Executable to ensure that it is sent to the DLQ if it fails terminally.
func NewExecutableDLQ(executable Executable, dlq DLQ, timeSource clock.TimeSource, clusterName string) *ExecutableDLQ {
	return &ExecutableDLQ{
		Executable:  executable,
		dlq:         dlq,
		timeSource:  timeSource,
		clusterName: clusterName,
	}
}

// Execute is not thread-safe.
func (d *ExecutableDLQ) Execute() error {
	if d.dlqCause == nil {
		// This task has not experienced a terminal failure yet, so we should execute it.
		err := d.Executable.Execute()
		// TODO: expand on the errors that should be considered terminal
		if !errors.As(err, new(*serialization.DeserializationError)) &&
			!errors.As(err, new(*serialization.UnknownEncodingTypeError)) {
			return err
		}
		d.dlqCause = err
		return fmt.Errorf("%w: %v", ErrTerminalTaskFailure, err)
	}
	// This task experienced a terminal failure, so we should try to send it to the DLQ.
	ctx := headers.SetCallerInfo(context.Background(), headers.SystemPreemptableCallerInfo)
	ctx, cancel := clock.ContextWithTimeout(ctx, sendToDLQTimeout, d.timeSource)
	defer cancel()
	_, err := d.dlq.EnqueueTask(ctx, &persistence.EnqueueTaskRequest{
		QueueType:     persistence.QueueTypeHistoryDLQ,
		SourceCluster: d.clusterName,
		TargetCluster: d.clusterName,
		Task:          d.GetTask(),
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSendTaskToDLQ, err)
	}
	return nil
}
