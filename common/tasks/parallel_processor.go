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
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type (
	// ParallelProcessorOptions is the configs for ParallelProcessor
	ParallelProcessorOptions struct {
		QueueSize   int
		WorkerCount int
	}

	ParallelProcessor struct {
		status  int32
		options *ParallelProcessorOptions

		metricsScope metrics.Scope
		logger       log.Logger

		tasksChan    chan Task
		shutdownChan chan struct{}
		workerWG     sync.WaitGroup
	}
)

// NewParallelProcessor creates a new ParallelProcessor
func NewParallelProcessor(
	options *ParallelProcessorOptions,
	metricsClient metrics.Client,
	logger log.Logger,
) *ParallelProcessor {
	return &ParallelProcessor{
		status:  common.DaemonStatusInitialized,
		options: options,

		logger:       logger,
		metricsScope: metricsClient.Scope(metrics.ParallelTaskProcessingScope),

		tasksChan:    make(chan Task, options.QueueSize),
		shutdownChan: make(chan struct{}),
	}
}

func (p *ParallelProcessor) Start() {
	if !atomic.CompareAndSwapInt32(
		&p.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	p.workerWG.Add(p.options.WorkerCount)
	for i := 0; i < p.options.WorkerCount; i++ {
		go p.processTask()
	}

	p.logger.Info("Parallel task processor started")
}

func (p *ParallelProcessor) Stop() {
	if !atomic.CompareAndSwapInt32(
		&p.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	close(p.shutdownChan)
	// must be called after the close of the shutdownChan
	p.drainTasks()

	go func() {
		if success := common.AwaitWaitGroup(&p.workerWG, time.Minute); !success {
			p.logger.Warn("parallel processor timed out waiting for workers")
		}
	}()
	p.logger.Info("parallel processor stopped")
}

func (p *ParallelProcessor) Submit(task Task) {

	p.tasksChan <- task
	if p.isStopped() {
		p.drainTasks()
	}
}

func (p *ParallelProcessor) processTask() {
	defer p.workerWG.Done()

	for {
		if p.isStopped() {
			return
		}

		select {
		case task := <-p.tasksChan:
			p.executeTask(task)

		case <-p.shutdownChan:
			return
		}
	}
}

func (p *ParallelProcessor) executeTask(
	task Task,
) {
	operation := func() error {
		if err := task.Execute(); err != nil {
			return task.HandleErr(err)
		}
		return nil
	}

	isRetryable := func(err error) bool {
		return !p.isStopped() && task.IsRetryableError(err)
	}

	if err := backoff.Retry(operation, task.RetryPolicy(), isRetryable); err != nil {
		if p.isStopped() {
			task.Reschedule()
			return
		}

		task.Nack()
		return
	}

	task.Ack()
}

func (p *ParallelProcessor) drainTasks() {
LoopDrain:
	for {
		select {
		case task := <-p.tasksChan:
			task.Reschedule()
		default:
			break LoopDrain
		}
	}
}

func (p *ParallelProcessor) isStopped() bool {
	return atomic.LoadInt32(&p.status) == common.DaemonStatusStopped
}
