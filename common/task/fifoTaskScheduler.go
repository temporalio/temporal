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

package task

import (
	"sync/atomic"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
)

type (
	// FIFOTaskSchedulerOptions configs FIFO task scheduler
	FIFOTaskSchedulerOptions struct {
		QueueSize   int
		WorkerCount int
		RetryPolicy backoff.RetryPolicy
	}

	fifoTaskSchedulerImpl struct {
		status       int32
		logger       log.Logger
		metricsScope metrics.Scope

		processor Processor
		// TODO: for non-blocking submit,
		// the scheduler should have its own queue and dispatcher
	}
)

// NewFIFOTaskScheduler creats a new FIFO task scheduler
func NewFIFOTaskScheduler(
	logger log.Logger,
	metricsScope metrics.Scope,
	options *FIFOTaskSchedulerOptions,
) Scheduler {
	return &fifoTaskSchedulerImpl{
		status:       common.DaemonStatusInitialized,
		logger:       logger,
		metricsScope: metricsScope,
		processor: NewParallelTaskProcessor(
			logger,
			metricsScope,
			&ParallelTaskProcessorOptions{
				QueueSize:   options.QueueSize,
				WorkerCount: options.WorkerCount,
				RetryPolicy: options.RetryPolicy,
			},
		),
	}
}

func (f *fifoTaskSchedulerImpl) Start() {
	if !atomic.CompareAndSwapInt32(&f.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	f.processor.Start()
}

func (f *fifoTaskSchedulerImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&f.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	f.processor.Stop()
}

func (f *fifoTaskSchedulerImpl) Submit(task PriorityTask) error {
	f.metricsScope.IncCounter(metrics.ParallelTaskSubmitRequest)
	sw := f.metricsScope.StartTimer(metrics.ParallelTaskSubmitLatency)
	defer sw.Stop()

	return f.processor.Submit(task)
}
