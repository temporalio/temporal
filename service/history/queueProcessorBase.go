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
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type (
	// QueueProcessorOptions is options passed to queue processor implementation
	QueueProcessorOptions struct {
		BatchSize                          dynamicconfig.IntPropertyFn
		MaxPollInterval                    dynamicconfig.DurationPropertyFn
		MaxPollIntervalJitterCoefficient   dynamicconfig.FloatPropertyFn
		UpdateAckInterval                  dynamicconfig.DurationPropertyFn
		UpdateAckIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
		MaxReschdulerSize                  dynamicconfig.IntPropertyFn
		PollBackoffInterval                dynamicconfig.DurationPropertyFn
		MetricScope                        int
	}

	queueProcessorBase struct {
		clusterName    string
		shard          shard.Context
		timeSource     clock.TimeSource
		options        *QueueProcessorOptions
		queueProcessor common.Daemon
		logger         log.Logger
		rateLimiter    quotas.RateLimiter // Read rate limiter
		ackMgr         queueAckMgr
		scheduler      queues.Scheduler
		rescheduler    queues.Rescheduler

		lastPollTime     time.Time
		backoffTimerLock sync.Mutex
		backoffTimer     *time.Timer
		readTaskRetrier  backoff.Retrier

		notifyCh   chan struct{}
		status     int32
		shutdownWG sync.WaitGroup
		shutdownCh chan struct{}
	}
)

var (
	loadQueueTaskThrottleRetryDelay = 3 * time.Second
)

func newQueueProcessorBase(
	clusterName string,
	shard shard.Context,
	options *QueueProcessorOptions,
	queueProcessor common.Daemon,
	queueAckMgr queueAckMgr,
	historyCache workflow.Cache,
	scheduler queues.Scheduler,
	rescheduler queues.Rescheduler,
	rateLimiter quotas.RateLimiter,
	logger log.Logger,
) *queueProcessorBase {

	p := &queueProcessorBase{
		clusterName:    clusterName,
		shard:          shard,
		timeSource:     shard.GetTimeSource(),
		options:        options,
		queueProcessor: queueProcessor,
		rateLimiter:    rateLimiter,
		status:         common.DaemonStatusInitialized,
		notifyCh:       make(chan struct{}, 1),
		shutdownCh:     make(chan struct{}),
		logger:         logger,
		ackMgr:         queueAckMgr,
		lastPollTime:   time.Time{},
		readTaskRetrier: backoff.NewRetrier(
			common.CreateReadTaskRetryPolicy(),
			backoff.SystemClock,
		),
		scheduler:   scheduler,
		rescheduler: rescheduler,
	}

	return p
}

func (p *queueProcessorBase) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	p.logger.Info("", tag.LifeCycleStarting, tag.ComponentTransferQueue)
	defer p.logger.Info("", tag.LifeCycleStarted, tag.ComponentTransferQueue)

	p.rescheduler.Start()

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

	p.rescheduler.Stop()

	close(p.shutdownCh)

	if success := common.AwaitWaitGroup(&p.shutdownWG, time.Minute); !success {
		p.logger.Warn("", tag.LifeCycleStopTimedout, tag.ComponentTransferQueue)
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

eventLoop:
	for {
		// prioritize shutdown
		select {
		case <-p.shutdownCh:
			break eventLoop
		default:
			// noop
		}

		select {
		case <-p.shutdownCh:
			break eventLoop
		case <-p.ackMgr.getFinishedChan():
			// use a separate gorouting since the caller hold the shutdownWG
			// stop the entire queue processor, not just processor base.
			go p.queueProcessor.Stop()
		case <-p.notifyCh:
			p.processBatch()
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
			if err := p.ackMgr.updateQueueAckLevel(); shard.IsShardOwnershipLostError(err) {
				// shard is no longer owned by this instance, bail out
				// stop the entire queue processor, not just processor base.
				go p.queueProcessor.Stop()
				break eventLoop
			}
		}
	}

	p.logger.Info("Queue processor pump shut down.")
}

func (p *queueProcessorBase) processBatch() {
	ctx, cancel := context.WithTimeout(context.Background(), loadQueueTaskThrottleRetryDelay)
	if err := p.rateLimiter.Wait(ctx); err != nil {
		deadline, _ := ctx.Deadline()
		p.throttle(deadline.Sub(p.timeSource.Now()))
		cancel()
		return
	}
	cancel()

	if !p.verifyReschedulerSize() {
		return
	}

	p.lastPollTime = p.timeSource.Now()
	tasks, more, err := p.ackMgr.readQueueTasks()
	if err != nil {
		p.logger.Error("Processor unable to retrieve tasks", tag.Error(err))
		if common.IsResourceExhausted(err) {
			p.throttle(loadQueueTaskThrottleRetryDelay)
		} else {
			p.throttle(p.readTaskRetrier.NextBackOff())
		}
		return
	}
	p.readTaskRetrier.Reset()

	if len(tasks) == 0 {
		return
	}

	for _, task := range tasks {
		p.submitTask(task)
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
}

func (p *queueProcessorBase) verifyReschedulerSize() bool {
	passed := p.rescheduler.Len() < p.options.MaxReschdulerSize()
	if !passed {
		p.throttle(p.options.PollBackoffInterval())
	}
	return passed
}

func (p *queueProcessorBase) throttle(duration time.Duration) {
	p.backoffTimerLock.Lock()
	defer p.backoffTimerLock.Unlock()

	if p.backoffTimer == nil {
		p.backoffTimer = time.AfterFunc(duration, func() {
			p.backoffTimerLock.Lock()
			defer p.backoffTimerLock.Unlock()

			p.notifyNewTask() // re-enqueue the event
			p.backoffTimer = nil
		})
	}
}

func (p *queueProcessorBase) submitTask(
	executable queues.Executable,
) {
	if !p.scheduler.TrySubmit(executable) {
		executable.Reschedule()
	}
}
