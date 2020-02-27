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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	// WeightedRoundRobinTaskSchedulerOptions configs WRR task scheduler
	WeightedRoundRobinTaskSchedulerOptions struct {
		Weights     map[int]dynamicconfig.IntPropertyFn
		QueueSize   int
		WorkerCount int
		RetryPolicy backoff.RetryPolicy
	}

	weightedRoundRobinTaskSchedulerImpl struct {
		sync.RWMutex

		status       int32
		taskChs      map[int]chan PriorityTask
		shutdownCh   chan struct{}
		notifyCh     chan struct{}
		dispatcherWG sync.WaitGroup
		logger       log.Logger
		metricsScope metrics.Scope
		options      *WeightedRoundRobinTaskSchedulerOptions

		processor Processor
	}
)

const (
	wRRTaskProcessorQueueSize = 1
)

var (
	// ErrTaskSchedulerClosed is the error returned when submitting task to a stopped scheduler
	ErrTaskSchedulerClosed = errors.New("task scheduler has already shutdown")
)

// NewWeightedRoundRobinTaskScheduler creates a new WRR task scheduler
func NewWeightedRoundRobinTaskScheduler(
	logger log.Logger,
	metricsScope metrics.Scope,
	options *WeightedRoundRobinTaskSchedulerOptions,
) (Scheduler, error) {
	if len(options.Weights) == 0 {
		return nil, errors.New("weight is not specified in the scheduler option")
	}

	return &weightedRoundRobinTaskSchedulerImpl{
		status:       common.DaemonStatusInitialized,
		taskChs:      make(map[int]chan PriorityTask),
		shutdownCh:   make(chan struct{}),
		notifyCh:     make(chan struct{}, 1),
		logger:       logger,
		metricsScope: metricsScope,
		options:      options,
		processor: NewParallelTaskProcessor(
			logger,
			metricsScope,
			&ParallelTaskProcessorOptions{
				QueueSize:   wRRTaskProcessorQueueSize,
				WorkerCount: options.WorkerCount,
				RetryPolicy: options.RetryPolicy,
			},
		),
	}, nil
}

func (w *weightedRoundRobinTaskSchedulerImpl) Start() {
	if !atomic.CompareAndSwapInt32(&w.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	w.processor.Start()

	w.dispatcherWG.Add(1)
	go w.dispatcher()

	w.logger.Info("Weighted round robin task scheduler started.")
}

func (w *weightedRoundRobinTaskSchedulerImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&w.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(w.shutdownCh)

	w.processor.Stop()

	if success := common.AwaitWaitGroup(&w.dispatcherWG, time.Minute); !success {
		w.logger.Warn("Weighted round robin task scheduler timedout on shutdown.")
	}

	w.logger.Info("Weighted round robin task scheduler shutdown.")
}

func (w *weightedRoundRobinTaskSchedulerImpl) Submit(task PriorityTask) error {
	w.metricsScope.IncCounter(metrics.PriorityTaskSubmitRequest)
	sw := w.metricsScope.StartTimer(metrics.PriorityTaskSubmitLatency)
	defer sw.Stop()

	taskCh, err := w.getOrCreateTaskChan(task.Priority())
	if err != nil {
		return err
	}

	select {
	case taskCh <- task:
		w.notifyDispatcher()
		return nil
	case <-w.shutdownCh:
		return ErrTaskSchedulerClosed
	}
}

func (w *weightedRoundRobinTaskSchedulerImpl) TrySubmit(
	task PriorityTask,
) (bool, error) {
	taskCh, err := w.getOrCreateTaskChan(task.Priority())
	if err != nil {
		return false, err
	}

	select {
	case taskCh <- task:
		w.metricsScope.IncCounter(metrics.PriorityTaskSubmitRequest)
		w.notifyDispatcher()
		return true, nil
	case <-w.shutdownCh:
		return false, ErrTaskSchedulerClosed
	default:
		return false, nil
	}
}

func (w *weightedRoundRobinTaskSchedulerImpl) dispatcher() {
	defer w.dispatcherWG.Done()

	outstandingTasks := false
	taskChs := make(map[int]chan PriorityTask)

	for {
		if !outstandingTasks {
			// if no task is dispatched in the last round,
			// wait for a notification
			select {
			case <-w.notifyCh:
				// block until there's a new task
			case <-w.shutdownCh:
				return
			}
		}

		outstandingTasks = false
		w.updateTaskChs(taskChs)
		for priority, taskCh := range taskChs {
			for i := 0; i < w.options.Weights[priority](); i++ {
				select {
				case task := <-taskCh:
					// dispatched at least one task in this round
					outstandingTasks = true

					if err := w.processor.Submit(task); err != nil {
						w.logger.Error("fail to submit task to processor", tag.Error(err))
						task.Nack()
					}
				case <-w.shutdownCh:
					return
				default:
					// if no task, don't block. Skip to next priority
					break
				}
			}
		}
	}
}

func (w *weightedRoundRobinTaskSchedulerImpl) getOrCreateTaskChan(
	priority int,
) (chan PriorityTask, error) {
	if _, ok := w.options.Weights[priority]; !ok {
		return nil, fmt.Errorf("unknown task priority: %v", priority)
	}

	w.RLock()
	if taskCh, ok := w.taskChs[priority]; ok {
		w.RUnlock()
		return taskCh, nil
	}
	w.RUnlock()

	w.Lock()
	defer w.Unlock()
	if taskCh, ok := w.taskChs[priority]; ok {
		return taskCh, nil
	}
	taskCh := make(chan PriorityTask, w.options.QueueSize)
	w.taskChs[priority] = taskCh
	return taskCh, nil
}

func (w *weightedRoundRobinTaskSchedulerImpl) updateTaskChs(taskChs map[int]chan PriorityTask) {
	w.RLock()
	defer w.RUnlock()

	for priority, taskCh := range w.taskChs {
		if _, ok := taskChs[priority]; !ok {
			taskChs[priority] = taskCh
		}
	}
}

func (w *weightedRoundRobinTaskSchedulerImpl) notifyDispatcher() {
	select {
	case w.notifyCh <- struct{}{}:
		// sent a notification to the dispatcher
	default:
		// do not block if there's already a notification
	}
}
