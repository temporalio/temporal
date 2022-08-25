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

	"go.temporal.io/server/common/timer"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
)

var (
	emptyTime = time.Time{}

	loadTimerTaskThrottleRetryDelay = 3 * time.Second
)

type (
	timerQueueProcessorBase struct {
		scope            int
		shard            shard.Context
		cache            workflow.Cache
		executionManager persistence.ExecutionManager
		status           int32
		shutdownWG       sync.WaitGroup
		shutdownCh       chan struct{}
		config           *configs.Config
		logger           log.Logger
		metricsClient    metrics.Client
		timerProcessor   common.Daemon
		timerQueueAckMgr timerQueueAckMgr
		timerGate        timer.Gate
		timeSource       clock.TimeSource
		rateLimiter      quotas.RateLimiter
		lastPollTime     time.Time
		readTaskRetrier  backoff.Retrier
		scheduler        queues.Scheduler
		rescheduler      queues.Rescheduler

		// timer notification
		newTimerCh  chan struct{}
		newTimeLock sync.Mutex
		newTime     time.Time
	}
)

func newTimerQueueProcessorBase(
	scope int,
	shard shard.Context,
	workflowCache workflow.Cache,
	timerProcessor common.Daemon,
	timerQueueAckMgr timerQueueAckMgr,
	timerGate timer.Gate,
	scheduler queues.Scheduler,
	rescheduler queues.Rescheduler,
	rateLimiter quotas.RateLimiter,
	logger log.Logger,
) *timerQueueProcessorBase {
	logger = log.With(logger, tag.ComponentTimerQueue)
	config := shard.GetConfig()

	base := &timerQueueProcessorBase{
		scope:            scope,
		shard:            shard,
		timerProcessor:   timerProcessor,
		cache:            workflowCache,
		executionManager: shard.GetExecutionManager(),
		status:           common.DaemonStatusInitialized,
		shutdownCh:       make(chan struct{}),
		config:           config,
		logger:           logger,
		metricsClient:    shard.GetMetricsClient(),
		timerQueueAckMgr: timerQueueAckMgr,
		timerGate:        timerGate,
		timeSource:       shard.GetTimeSource(),
		newTimerCh:       make(chan struct{}, 1),
		lastPollTime:     time.Time{},
		scheduler:        scheduler,
		rescheduler:      rescheduler,
		rateLimiter:      rateLimiter,
		readTaskRetrier: backoff.NewRetrier(
			common.CreateReadTaskRetryPolicy(),
			backoff.SystemClock,
		),
	}

	return base
}

func (t *timerQueueProcessorBase) Start() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	t.rescheduler.Start()

	t.shutdownWG.Add(1)
	// notify a initial scan
	t.notifyNewTimer(time.Time{})
	go t.processorPump()

	t.logger.Info("Timer queue processor started.")
}

func (t *timerQueueProcessorBase) Stop() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	t.rescheduler.Stop()

	t.timerGate.Close()
	close(t.shutdownCh)

	if success := common.AwaitWaitGroup(&t.shutdownWG, time.Minute); !success {
		t.logger.Warn("Timer queue processor timedout on shutdown.")
	}

	t.logger.Info("Timer queue processor stopped.")
}

func (t *timerQueueProcessorBase) processorPump() {
	defer t.shutdownWG.Done()

RetryProcessor:
	for {
		select {
		case <-t.shutdownCh:
			break RetryProcessor
		default:
			err := t.internalProcessor()
			if err != nil {
				t.logger.Error("processor pump failed with error", tag.Error(err))
			}
		}
	}

	t.logger.Info("Timer queue processor pump shutting down.")
	t.logger.Info("Timer processor exiting.")
}

// NotifyNewTimers - Notify the processor about the new timer events arrival.
// This should be called each time new timer events arrives, otherwise timers maybe fired unexpected.
func (t *timerQueueProcessorBase) notifyNewTimers(
	timerTasks []tasks.Task,
) {
	if len(timerTasks) == 0 {
		return
	}

	newTime := timerTasks[0].GetVisibilityTime()
	for _, task := range timerTasks {
		ts := task.GetVisibilityTime()
		if ts.Before(newTime) {
			newTime = ts
		}
	}

	t.notifyNewTimer(newTime)
}

func (t *timerQueueProcessorBase) notifyNewTimer(
	newTime time.Time,
) {
	t.newTimeLock.Lock()
	defer t.newTimeLock.Unlock()
	if t.newTime.IsZero() || newTime.Before(t.newTime) {
		t.newTime = newTime
		select {
		case t.newTimerCh <- struct{}{}:
			// Notified about new time.
		default:
			// Channel "full" -> drop and move on, this will happen only if service is in high load.
		}
	}
}

func (t *timerQueueProcessorBase) internalProcessor() error {
	pollTimer := time.NewTimer(backoff.JitDuration(
		t.config.TimerProcessorMaxPollInterval(),
		t.config.TimerProcessorMaxPollIntervalJitterCoefficient(),
	))
	defer pollTimer.Stop()

	updateAckTimer := time.NewTimer(backoff.JitDuration(
		t.config.TimerProcessorUpdateAckInterval(),
		t.config.TimerProcessorUpdateAckIntervalJitterCoefficient(),
	))
	defer updateAckTimer.Stop()

eventLoop:
	for {
		// prioritize shutdown
		select {
		case <-t.shutdownCh:
			break eventLoop
		default:
			// noop
		}

		// Wait until one of four things occurs:
		// 1. we get notified of a new message
		// 2. the timer gate fires (message scheduled to be delivered)
		// 3. shutdown was triggered.
		// 4. updating ack level
		//
		select {
		case <-t.shutdownCh:
			break eventLoop
		case <-t.timerQueueAckMgr.getFinishedChan():
			// timer queue ack manager indicate that all task scanned
			// are finished and no more tasks
			// use a separate goroutine since the caller hold the shutdownWG
			// stop the entire timer queue processor, not just processor base.
			go t.timerProcessor.Stop()
			return nil
		case <-t.timerGate.FireChan():
			nextFireTime, err := t.readAndFanoutTimerTasks()
			if err != nil {
				return err
			}
			if nextFireTime != nil {
				t.timerGate.Update(*nextFireTime)
			}
		case <-pollTimer.C:
			pollTimer.Reset(backoff.JitDuration(
				t.config.TimerProcessorMaxPollInterval(),
				t.config.TimerProcessorMaxPollIntervalJitterCoefficient(),
			))
			if t.lastPollTime.Add(t.config.TimerProcessorMaxPollInterval()).Before(t.timeSource.Now()) {
				nextFireTime, err := t.readAndFanoutTimerTasks()
				if err != nil {
					return err
				}
				if nextFireTime != nil {
					t.timerGate.Update(*nextFireTime)
				}
			}
		case <-updateAckTimer.C:
			updateAckTimer.Reset(backoff.JitDuration(
				t.config.TimerProcessorUpdateAckInterval(),
				t.config.TimerProcessorUpdateAckIntervalJitterCoefficient(),
			))
			if err := t.timerQueueAckMgr.updateAckLevel(); shard.IsShardOwnershipLostError(err) {
				// shard is closed, shutdown timerQProcessor and bail out
				// stop the entire timer queue processor, not just processor base.
				go t.timerProcessor.Stop()
				return err
			}
		case <-t.newTimerCh:
			t.newTimeLock.Lock()
			newTime := t.newTime
			t.newTime = emptyTime
			t.newTimeLock.Unlock()
			// New Timer has arrived.
			t.metricsClient.IncCounter(t.scope, metrics.NewTimerNotifyCounter)
			t.timerGate.Update(newTime)
		}
	}

	return nil
}

func (t *timerQueueProcessorBase) readAndFanoutTimerTasks() (*time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), loadTimerTaskThrottleRetryDelay)
	if err := t.rateLimiter.Wait(ctx); err != nil {
		deadline, _ := ctx.Deadline()
		t.notifyNewTimer(deadline) // re-enqueue the event
		cancel()
		return nil, nil
	}
	cancel()

	if !t.verifyReschedulerSize() {
		return nil, nil
	}

	t.lastPollTime = t.timeSource.Now()
	timerTasks, nextFireTime, moreTasks, err := t.timerQueueAckMgr.readTimerTasks()
	if err != nil {
		if common.IsResourceExhausted(err) {
			t.notifyNewTimer(t.timeSource.Now().Add(loadTimerTaskThrottleRetryDelay))
		} else {
			t.notifyNewTimer(t.timeSource.Now().Add(t.readTaskRetrier.NextBackOff()))
		}
		return nil, err
	}
	t.readTaskRetrier.Reset()

	for _, task := range timerTasks {
		t.submitTask(task)
		select {
		case <-t.shutdownCh:
			return nil, nil
		default:
		}
	}

	if !moreTasks {
		if nextFireTime == nil {
			return nil, nil
		}
		return nextFireTime, nil
	}

	t.notifyNewTimer(time.Time{}) // re-enqueue the event
	return nil, nil
}

func (t *timerQueueProcessorBase) verifyReschedulerSize() bool {
	passed := t.rescheduler.Len() < t.config.TimerProcessorMaxReschedulerSize()
	if !passed {
		// set backoff timer
		t.notifyNewTimer(t.timeSource.Now().Add(t.config.TimerProcessorPollBackoffInterval()))
	}

	return passed
}

func (t *timerQueueProcessorBase) submitTask(
	executable queues.Executable,
) {
	if !t.scheduler.TrySubmit(executable) {
		executable.Reschedule()
	}
}

func newTimerTaskShardScheduler(
	shard shard.Context,
	logger log.Logger,
) queues.Scheduler {
	config := shard.GetConfig()
	return queues.NewFIFOScheduler(
		queues.FIFOSchedulerOptions{
			WorkerCount: config.TimerTaskWorkerCount,
			QueueSize:   config.TimerTaskWorkerCount() * config.TimerTaskBatchSize(),
		},
		logger,
	)
}
