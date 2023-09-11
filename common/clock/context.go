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
		ctx.timer.Stop()
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
