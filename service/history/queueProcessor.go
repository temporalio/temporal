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

package history

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/shard"
)

type (
	// QueueProcessorOptions is options passed to queue processor implementation
	QueueProcessorOptions struct {
		BatchSize                           dynamicconfig.IntPropertyFn
		WorkerCount                         dynamicconfig.IntPropertyFn
		MaxPollRPS                          dynamicconfig.IntPropertyFn
		MaxPollInterval                     dynamicconfig.DurationPropertyFn
		MaxPollIntervalJitterCoefficient    dynamicconfig.FloatPropertyFn
		UpdateAckInterval                   dynamicconfig.DurationPropertyFn
		UpdateAckIntervalJitterCoefficient  dynamicconfig.FloatPropertyFn
		MaxRetryCount                       dynamicconfig.IntPropertyFn
		RedispatchInterval                  dynamicconfig.DurationPropertyFn
		RedispatchIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
		MaxRedispatchQueueSize              dynamicconfig.IntPropertyFn
		EnablePriorityTaskProcessor         dynamicconfig.BoolPropertyFn
		MetricScope                         int
	}

	queueTaskInitializer func(queueTaskInfo) queueTask

	queueProcessorBase struct {
		clusterName          string
		shard                shard.Context
		timeSource           clock.TimeSource
		options              *QueueProcessorOptions
		processor            processor
		logger               log.Logger
		metricsScope         metrics.Scope
		rateLimiter          quotas.RateLimiter // Read rate limiter
		ackMgr               queueAckMgr
		taskProcessor        *taskProcessor // TODO: deprecate task processor, in favor of queueTaskProcessor
		queueTaskProcessor   queueTaskProcessor
		redispatchQueue      collection.Queue
		queueTaskInitializer queueTaskInitializer

		lastPollTime time.Time

		notifyCh   chan struct{}
		status     int32
		shutdownWG sync.WaitGroup
		shutdownCh chan struct{}
	}
)

var (
	errUnexpectedQueueTask = errors.New("unexpected queue task")

	loadQueueTaskThrottleRetryDelay = 5 * time.Second
)

func newQueueProcessorBase(
	clusterName string,
	shard shard.Context,
	options *QueueProcessorOptions,
	processor processor,
	queueTaskProcessor queueTaskProcessor,
	queueAckMgr queueAckMgr,
	redispatchQueue collection.Queue,
	historyCache *historyCache,
	queueTaskInitializer queueTaskInitializer,
	logger log.Logger,
	metricsScope metrics.Scope,
) *queueProcessorBase {

	var taskProcessor *taskProcessor
	if !options.EnablePriorityTaskProcessor() {
		taskProcessorOptions := taskProcessorOptions{
			queueSize:   options.BatchSize(),
			workerCount: options.WorkerCount(),
		}
		taskProcessor = newTaskProcessor(taskProcessorOptions, shard, historyCache, logger)
	}

	p := &queueProcessorBase{
		clusterName: clusterName,
		shard:       shard,
		timeSource:  shard.GetTimeSource(),
		options:     options,
		processor:   processor,
		rateLimiter: quotas.NewDefaultOutgoingDynamicRateLimiter(
			func() float64 { return float64(options.MaxPollRPS()) },
		),
		status:               common.DaemonStatusInitialized,
		notifyCh:             make(chan struct{}, 1),
		shutdownCh:           make(chan struct{}),
		logger:               logger,
		metricsScope:         metricsScope,
		ackMgr:               queueAckMgr,
		lastPollTime:         time.Time{},
		taskProcessor:        taskProcessor,
		queueTaskProcessor:   queueTaskProcessor,
		redispatchQueue:      redispatchQueue,
		queueTaskInitializer: queueTaskInitializer,
	}

	return p
}

func (p *queueProcessorBase) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	p.logger.Info("", tag.LifeCycleStarting, tag.ComponentTransferQueue)
	defer p.logger.Info("", tag.LifeCycleStarted, tag.ComponentTransferQueue)

	if p.taskProcessor != nil {
		p.taskProcessor.start()
	}
	p.shutdownWG.Add(1)
	p.notifyNewTask()
	go p.processorPump()
}

func (p *queueProcessorBase) Stop() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	p.logger.Info("", tag.LifeCycleStopping, tag.ComponentTransferQueue)
	defer p.logger.Info("", tag.LifeCycleStopped, tag.ComponentTransferQueue)

	close(p.shutdownCh)
	p.retryTasks()

	if success := common.AwaitWaitGroup(&p.shutdownWG, time.Minute); !success {
		p.logger.Warn("", tag.LifeCycleStopTimedout, tag.ComponentTransferQueue)
	}

	if p.taskProcessor != nil {
		p.taskProcessor.stop()
	}
}

func (p *queueProcessorBase) notifyNewTask() {
	var event struct{}
	select {
	case p.notifyCh <- event:
	default: // channel already has an event, don't block
	}
}

func (p *queueProcessorBase) processorPump() {
	defer p.shutdownWG.Done()

	pollTimer := time.NewTimer(backoff.JitDuration(
		p.options.MaxPollInterval(),
		p.options.MaxPollIntervalJitterCoefficient(),
	))
	defer pollTimer.Stop()

	updateAckTimer := time.NewTimer(backoff.JitDuration(
		p.options.UpdateAckInterval(),
		p.options.UpdateAckIntervalJitterCoefficient(),
	))
	defer updateAckTimer.Stop()

	redispatchTimer := time.NewTimer(backoff.JitDuration(
		p.options.RedispatchInterval(),
		p.options.RedispatchIntervalJitterCoefficient(),
	))
	defer redispatchTimer.Stop()

processorPumpLoop:
	for {
		select {
		case <-p.shutdownCh:
			break processorPumpLoop
		case <-p.ackMgr.getFinishedChan():
			// use a separate gorouting since the caller hold the shutdownWG
			go p.Stop()
		case <-p.notifyCh:
			if !p.isPriorityTaskProcessorEnabled() || p.redispatchQueue.Len() <= p.options.MaxRedispatchQueueSize() {
				p.processBatch()
				continue
			}

			// has too many pending tasks in re-dispatch queue, block loading tasks from persistence
			p.redispatchTasks()
			// re-enqueue the event to see if we need keep re-dispatching or load new tasks from persistence
			p.notifyNewTask()
		case <-pollTimer.C:
			pollTimer.Reset(backoff.JitDuration(
				p.options.MaxPollInterval(),
				p.options.MaxPollIntervalJitterCoefficient(),
			))
			if p.lastPollTime.Add(p.options.MaxPollInterval()).Before(p.timeSource.Now()) {
				p.processBatch()
			}
		case <-updateAckTimer.C:
			updateAckTimer.Reset(backoff.JitDuration(
				p.options.UpdateAckInterval(),
				p.options.UpdateAckIntervalJitterCoefficient(),
			))
			if err := p.ackMgr.updateQueueAckLevel(); err == shard.ErrShardClosed {
				// shard is no longer owned by this instance, bail out
				go p.Stop()
				break processorPumpLoop
			}
		}
	}

	p.logger.Info("Queue processor pump shut down.")
}

func (p *queueProcessorBase) processBatch() {

	ctx, cancel := context.WithTimeout(context.Background(), loadQueueTaskThrottleRetryDelay)
	if err := p.rateLimiter.Wait(ctx); err != nil {
		cancel()
		p.notifyNewTask() // re-enqueue the event
		return
	}
	cancel()

	p.lastPollTime = p.timeSource.Now()
	tasks, more, err := p.ackMgr.readQueueTasks()

	if err != nil {
		p.logger.Warn("Processor unable to retrieve tasks", tag.Error(err))
		p.notifyNewTask() // re-enqueue the event
		return
	}

	if len(tasks) == 0 {
		return
	}

	for _, task := range tasks {
		if submitted := p.submitTask(task); !submitted {
			// submitted since processor has been shutdown
			return
		}
		select {
		case <-p.shutdownCh:
			return
		default:
		}
	}

	if more {
		// There might be more task
		// We return now to yield, but enqueue an event to poll later
		p.notifyNewTask()
	}

	return
}

func (p *queueProcessorBase) submitTask(
	taskInfo queueTaskInfo,
) bool {
	if !p.isPriorityTaskProcessorEnabled() {
		return p.taskProcessor.addTask(
			newTaskInfo(
				p.processor,
				taskInfo,
				initializeLoggerForTask(p.shard.GetShardID(), taskInfo, p.logger),
			),
		)
	}

	queueTask := p.queueTaskInitializer(taskInfo)
	submitted, err := p.queueTaskProcessor.TrySubmit(queueTask)
	if err != nil {
		return false
	}
	if !submitted {
		p.redispatchQueue.Add(queueTask)
	}

	return true
}

func (p *queueProcessorBase) redispatchTasks() {
	if !p.isPriorityTaskProcessorEnabled() {
		return
	}

	redispatchQueueTasks(
		p.redispatchQueue,
		p.queueTaskProcessor,
		p.logger,
		p.metricsScope,
		p.shutdownCh,
	)
}

func (p *queueProcessorBase) retryTasks() {
	if p.taskProcessor != nil {
		p.taskProcessor.retryTasks()
	}
}

func (p *queueProcessorBase) complete(
	task queueTaskInfo,
) {
	p.ackMgr.completeQueueTask(task.GetTaskId())
}

func (p *queueProcessorBase) isPriorityTaskProcessorEnabled() bool {
	return p.taskProcessor == nil
}

func redispatchQueueTasks(
	redispatchQueue collection.Queue,
	queueTaskProcessor queueTaskProcessor,
	logger log.Logger,
	metricsScope metrics.Scope,
	shutdownCh <-chan struct{},
) {
	queueLength := redispatchQueue.Len()
	metricsScope.RecordDistribution(metrics.TaskRedispatchQueuePendingTasksTimer, queueLength)
	for i := 0; i != queueLength; i++ {
		queueTask := redispatchQueue.Remove().(queueTask)
		submitted, err := queueTaskProcessor.TrySubmit(queueTask)
		if err != nil {
			// the only reason error will be returned here is because
			// task processor has already shutdown. Just return in this case.
			logger.Error("failed to redispatch task", tag.Error(err))
			return
		}
		if !submitted {
			// failed to submit, enqueue again
			redispatchQueue.Add(queueTask)
		}

		select {
		case <-shutdownCh:
			return
		default:
		}
	}
}
