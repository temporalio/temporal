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
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
)

type (
	// ParallelTaskProcessorOptions configs PriorityTaskProcessor
	ParallelTaskProcessorOptions struct {
		QueueSize   int
		WorkerCount int
		RetryPolicy backoff.RetryPolicy
	}

	parallelTaskProcessorImpl struct {
		status       int32
		tasksCh      chan Task
		shutdownCh   chan struct{}
		workerWG     sync.WaitGroup
		logger       log.Logger
		metricsScope metrics.Scope
		options      *ParallelTaskProcessorOptions
	}
)

var (
	// ErrTaskProcessorClosed is the error returned when submiting task to a stopped processor
	ErrTaskProcessorClosed = errors.New("task processor has already shutdown")
)

// NewParallelTaskProcessor creates a new PriorityTaskProcessor
func NewParallelTaskProcessor(
	logger log.Logger,
	metricsScope metrics.Scope,
	options *ParallelTaskProcessorOptions,
) Processor {
	return &parallelTaskProcessorImpl{
		status:       common.DaemonStatusInitialized,
		tasksCh:      make(chan Task, options.QueueSize),
		shutdownCh:   make(chan struct{}),
		logger:       logger,
		metricsScope: metricsScope,
		options:      options,
	}
}

func (p *parallelTaskProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	p.workerWG.Add(p.options.WorkerCount)
	for i := 0; i < p.options.WorkerCount; i++ {
		go p.taskWorker()
	}
	p.logger.Info("Parallel task processor started.")
}

func (p *parallelTaskProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(p.shutdownCh)
	if success := common.AwaitWaitGroup(&p.workerWG, time.Minute); !success {
		p.logger.Warn("Parallel task processor timedout on shutdown.")
	}
	p.logger.Info("Parallel task processor shutdown.")
}

func (p *parallelTaskProcessorImpl) Submit(task Task) error {
	p.metricsScope.IncCounter(metrics.ParallelTaskSubmitRequest)
	sw := p.metricsScope.StartTimer(metrics.ParallelTaskSubmitLatency)
	defer sw.Stop()

	select {
	case p.tasksCh <- task:
		return nil
	case <-p.shutdownCh:
		return ErrTaskProcessorClosed
	}
}

func (p *parallelTaskProcessorImpl) taskWorker() {
	defer p.workerWG.Done()

	for {
		select {
		case <-p.shutdownCh:
			return
		case task := <-p.tasksCh:
			p.executeTask(task)
		}
	}
}

func (p *parallelTaskProcessorImpl) executeTask(task Task) {
	sw := p.metricsScope.StartTimer(metrics.ParallelTaskTaskProcessingLatency)
	defer sw.Stop()

	op := func() error {
		if err := task.Execute(); err != nil {
			return task.HandleErr(err)
		}
		return nil
	}

	isRetryable := func(err error) bool {
		if p.isStopped() {
			return false
		}
		return task.RetryErr(err)
	}

	if err := backoff.Retry(op, p.options.RetryPolicy, isRetryable); err != nil {
		if p.isStopped() {
			// neither ack or nack here
			return
		}

		// non-retryable error or exhausted all retries
		task.Nack()
		return
	}

	// no error
	task.Ack()
}

func (p *parallelTaskProcessorImpl) isStopped() bool {
	return atomic.LoadInt32(&p.status) == common.DaemonStatusStopped
}
