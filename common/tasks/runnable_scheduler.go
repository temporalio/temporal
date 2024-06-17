// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	"time"

	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
)

// RunnableScheduler is scheduler for [Runnable] tasks.
type RunnableScheduler interface {
	// InitiateShutdown signals the scheduler to stop without waiting for shutdown to complete.
	InitiateShutdown()
	// WaitShutdown waits for the scheduler to complete shutdown. Must be called after InitiateShutdown().
	WaitShutdown()
	// Submit a Runnable for scheduling, if the scheduler is already stopped, the runnable will be aborted.
	// Returns a boolean indicating whether the task was accepted.
	TrySubmit(Runnable) bool
}

// RunnableTask turns a [Task] into a [Runnable]. Does **not** retry tasks.
type RunnableTask struct {
	Task
}

// Run the embedded task, handling errors and aborting on context errors.
func (a RunnableTask) Run(ctx context.Context) {
	if err := a.HandleErr(a.Execute()); err != nil {
		if ctx.Err() != nil {
			a.Abort()
		} else {
			a.Nack(err)
		}
	} else {
		a.Ack()
	}
}

// RateLimitedTaskRunnable wraps a [Runnable] with a rate limiter.
type RateLimitedTaskRunnable struct {
	Runnable
	Limiter quotas.RateLimiter

	metricsHandler metrics.Handler
}

// NewRateLimitedTaskRunnableFromTask creates a [NewRateLimitedTaskRunnable] from a [Task] and a [rate.Limiter].
func NewRateLimitedTaskRunnableFromTask(
	task Task,
	limiter quotas.RateLimiter,
	metricsHandler metrics.Handler,
) RateLimitedTaskRunnable {
	return RateLimitedTaskRunnable{
		Runnable: RunnableTask{task},
		Limiter:  limiter,

		metricsHandler: metricsHandler,
	}
}

// Run the embedded [Runnable], applying the rate limiter.
func (r RateLimitedTaskRunnable) Run(ctx context.Context) {
	t0 := time.Now()
	if err := r.Limiter.Wait(ctx); err != nil {
		r.Abort()
		return
	}

	metrics.RateLimitedTaskRunnableWaitTime.With(r.metricsHandler).Record(time.Since(t0))
	r.Runnable.Run(ctx)
}
