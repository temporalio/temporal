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
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

const (
	defaultMonitorTickerDuration = time.Minute
	defaultMonitorTickerJitter   = 0.15
)

type (
	// ParallelProcessorOptions is the configs for ParallelProcessor
	ParallelProcessorOptions struct {
		QueueSize   int
		WorkerCount dynamicconfig.IntPropertyFn
	}

	ParallelProcessor struct {
		status  int32
		options *ParallelProcessorOptions

		metricsProvider metrics.MetricsHandler
		logger          log.Logger

		tasksChan        chan Task
		shutdownChan     chan struct{}
		shutdownWG       sync.WaitGroup
		workerShutdownCh []chan struct{}
	}
)

// NewParallelProcessor creates a new ParallelProcessor
func NewParallelProcessor(
	options *ParallelProcessorOptions,
	metricsProvider metrics.MetricsHandler,
	logger log.Logger,
) *ParallelProcessor {
	return &ParallelProcessor{
		status:  common.DaemonStatusInitialized,
		options: options,

		logger:          logger,
		metricsProvider: metricsProvider.WithTags(metrics.OperationTag(OperationParallelTaskProcessing)),

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

	p.startWorkers(p.options.WorkerCount())

	p.shutdownWG.Add(1)
	go p.workerMonitor()

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
		if success := common.AwaitWaitGroup(&p.shutdownWG, time.Minute); !success {
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

func (p *ParallelProcessor) workerMonitor() {
	defer p.shutdownWG.Done()

	timer := time.NewTimer(backoff.JitDuration(defaultMonitorTickerDuration, defaultMonitorTickerJitter))
	defer timer.Stop()

	for {
		select {
		case <-p.shutdownChan:
			p.stopWorkers(len(p.workerShutdownCh))
			return
		case <-timer.C:
			timer.Reset(backoff.JitDuration(defaultMonitorTickerDuration, defaultMonitorTickerJitter))

			targetWorkerNum := p.options.WorkerCount()
			currentWorkerNum := len(p.workerShutdownCh)

			if targetWorkerNum == currentWorkerNum {
				continue
			}

			if targetWorkerNum > currentWorkerNum {
				p.startWorkers(targetWorkerNum - currentWorkerNum)
			} else {
				p.stopWorkers(currentWorkerNum - targetWorkerNum)
			}
			p.logger.Info("Update worker pool size", tag.Key("worker-pool-size"), tag.Value(targetWorkerNum))
		}
	}
}

func (p *ParallelProcessor) startWorkers(
	count int,
) {
	for i := 0; i < count; i++ {
		shutdownCh := make(chan struct{})
		p.workerShutdownCh = append(p.workerShutdownCh, shutdownCh)

		p.shutdownWG.Add(1)
		go p.processTask(shutdownCh)
	}
}

func (p *ParallelProcessor) stopWorkers(
	count int,
) {
	shutdownChToClose := p.workerShutdownCh[:count]
	p.workerShutdownCh = p.workerShutdownCh[count:]

	for _, shutdownCh := range shutdownChToClose {
		close(shutdownCh)
	}
}

func (p *ParallelProcessor) processTask(
	shutdownCh chan struct{},
) {
	defer p.shutdownWG.Done()

	for {
		if p.isStopped() {
			return
		}

		select {
		case task := <-p.tasksChan:
			p.executeTask(task)

		case <-shutdownCh:
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

	if err := backoff.ThrottleRetry(operation, task.RetryPolicy(), isRetryable); err != nil {
		if p.isStopped() {
			task.Reschedule()
			return
		}

		task.Nack(err)
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
