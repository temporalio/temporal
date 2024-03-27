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
)

const (
	defaultMonitorTickerDuration = time.Minute
	defaultMonitorTickerJitter   = 0.15
)

var _ Scheduler[Task] = (*FIFOScheduler[Task])(nil)

type (
	// FIFOSchedulerOptions is the configs for FIFOScheduler
	FIFOSchedulerOptions struct {
		QueueSize   int
		WorkerCount dynamicconfig.IntPropertyFn
	}

	FIFOScheduler[T Task] struct {
		status  int32
		options *FIFOSchedulerOptions

		logger log.Logger

		tasksChan        chan T
		shutdownChan     chan struct{}
		shutdownWG       sync.WaitGroup
		workerShutdownCh []chan struct{}
	}
)

// NewFIFOScheduler creates a new FIFOScheduler
func NewFIFOScheduler[T Task](
	options *FIFOSchedulerOptions,
	logger log.Logger,
) *FIFOScheduler[T] {
	return &FIFOScheduler[T]{
		status:  common.DaemonStatusInitialized,
		options: options,

		logger: logger,

		tasksChan:    make(chan T, options.QueueSize),
		shutdownChan: make(chan struct{}),
	}
}

func (f *FIFOScheduler[T]) Start() {
	if !atomic.CompareAndSwapInt32(
		&f.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	f.startWorkers(f.options.WorkerCount())

	f.shutdownWG.Add(1)
	go f.workerMonitor()

	f.logger.Info("fifo scheduler started")
}

func (f *FIFOScheduler[T]) Stop() {
	if !atomic.CompareAndSwapInt32(
		&f.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	close(f.shutdownChan)
	// must be called after the close of the shutdownChan
	f.drainTasks()

	go func() {
		if success := common.AwaitWaitGroup(&f.shutdownWG, time.Minute); !success {
			f.logger.Warn("fifo scheduler timed out waiting for workers")
		}
	}()
	f.logger.Info("fifo scheduler stopped")
}

func (f *FIFOScheduler[T]) Submit(task T) {
	f.tasksChan <- task
	if f.isStopped() {
		f.drainTasks()
	}
}

func (f *FIFOScheduler[T]) TrySubmit(task T) bool {
	select {
	case f.tasksChan <- task:
		if f.isStopped() {
			f.drainTasks()
		}
		return true
	default:
		return false
	}
}

func (f *FIFOScheduler[T]) workerMonitor() {
	defer f.shutdownWG.Done()

	timer := time.NewTimer(backoff.Jitter(defaultMonitorTickerDuration, defaultMonitorTickerJitter))
	defer timer.Stop()

	for {
		select {
		case <-f.shutdownChan:
			f.stopWorkers(len(f.workerShutdownCh))
			return
		case <-timer.C:
			timer.Reset(backoff.Jitter(defaultMonitorTickerDuration, defaultMonitorTickerJitter))

			targetWorkerNum := f.options.WorkerCount()
			if targetWorkerNum < 0 {
				f.logger.Error("Target worker pool size is negative. Please fix the dynamic config.", tag.Key("worker-pool-size"), tag.Value(targetWorkerNum))
				continue
			}
			currentWorkerNum := len(f.workerShutdownCh)

			if targetWorkerNum == currentWorkerNum {
				continue
			}

			if targetWorkerNum > currentWorkerNum {
				f.startWorkers(targetWorkerNum - currentWorkerNum)
			} else {
				f.stopWorkers(currentWorkerNum - targetWorkerNum)
			}
			f.logger.Info("Update worker pool size", tag.Key("worker-pool-size"), tag.Value(targetWorkerNum))
		}
	}
}

func (f *FIFOScheduler[T]) startWorkers(
	count int,
) {
	for i := 0; i < count; i++ {
		shutdownCh := make(chan struct{})
		f.workerShutdownCh = append(f.workerShutdownCh, shutdownCh)

		f.shutdownWG.Add(1)
		go f.processTask(shutdownCh)
	}
}

func (f *FIFOScheduler[T]) stopWorkers(
	count int,
) {
	shutdownChToClose := f.workerShutdownCh[:count]
	f.workerShutdownCh = f.workerShutdownCh[count:]

	for _, shutdownCh := range shutdownChToClose {
		close(shutdownCh)
	}
}

func (f *FIFOScheduler[T]) processTask(
	shutdownCh chan struct{},
) {
	defer f.shutdownWG.Done()

	for {
		if f.isStopped() {
			return
		}

		select {
		case <-shutdownCh:
			return
		default:
		}

		select {
		case task := <-f.tasksChan:
			f.executeTask(task)

		case <-shutdownCh:
			return
		}
	}
}

func (f *FIFOScheduler[T]) executeTask(
	task T,
) {
	operation := func() error {
		if err := task.Execute(); err != nil {
			return task.HandleErr(err)
		}
		return nil
	}

	isRetryable := func(err error) bool {
		return !f.isStopped() && task.IsRetryableError(err)
	}

	if err := backoff.ThrottleRetry(operation, task.RetryPolicy(), isRetryable); err != nil {
		if f.isStopped() {
			task.Abort()
			return
		}

		task.Nack(err)
		return
	}

	task.Ack()
}

func (f *FIFOScheduler[T]) drainTasks() {
LoopDrain:
	for {
		select {
		case task := <-f.tasksChan:
			task.Abort()
		default:
			break LoopDrain
		}
	}
}

func (f *FIFOScheduler[T]) isStopped() bool {
	return atomic.LoadInt32(&f.status) == common.DaemonStatusStopped
}
