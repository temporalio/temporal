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
	ExecutableDLQ struct {
		Executable
		dlqWriter  *DLQWriter
		timeSource clock.TimeSource
		// This is the original error which caused this task to be sent to the DLQ. It is only set once.
		terminalFailure error
		clusterName     string
	}
)

const (
	// This has to be a relatively short timeout because we don't want to block the task queue processor for too long.
	sendToDLQTimeout = 3 * time.Second
)

var (
	_                      Executable = new(ExecutableDLQ)
	ErrTerminalTaskFailure            = errors.New("original task failed and this task is now to send the original to the DLQ")
	ErrSendTaskToDLQ                  = errors.New("failed to send task to DLQ")
	ErrCreateDLQ                      = errors.New("failed to create DLQ")
)

// NewExecutableDLQ wraps an Executable to ensure that it is sent to the DLQ if it fails terminally.
func NewExecutableDLQ(
	executable Executable,
	dlq *DLQWriter,
	timeSource clock.TimeSource,
	clusterName string,
) *ExecutableDLQ {
	return &ExecutableDLQ{
		Executable:  executable,
		dlqWriter:   dlq,
		timeSource:  timeSource,
		clusterName: clusterName,
	}
}

// Execute is not thread-safe.
func (d *ExecutableDLQ) Execute() error {
	if d.terminalFailure == nil {
		// This task has not experienced a terminal failure yet, so we should execute it.
		return d.executeNormally()
	}
	// This task experienced a terminal failure, so we should try to send it to the DLQ.
	return d.sendToDLQ(context.Background())
}

func (d *ExecutableDLQ) executeNormally() error {
	err := d.Executable.Execute()
	if !d.isTerminalFailure(err) {
		return err
	}
	d.terminalFailure = err
	return fmt.Errorf("%w: %v", ErrTerminalTaskFailure, err)
}

func (d *ExecutableDLQ) isTerminalFailure(err error) bool {
	// TODO: expand on the errors that should be considered terminal
	return errors.As(err, new(*serialization.DeserializationError)) ||
		errors.As(err, new(*serialization.UnknownEncodingTypeError))
}

func (d *ExecutableDLQ) sendToDLQ(ctx context.Context) error {
	// TODO: use clock.ContextWithTimeout here instead
	ctx = headers.SetCallerInfo(ctx, headers.SystemPreemptableCallerInfo)
	ctx, cancel := context.WithTimeout(ctx, sendToDLQTimeout)
	defer cancel()
	return d.dlqWriter.WriteTaskToDLQ(ctx, d.clusterName, d.clusterName, d.GetTask())
}
