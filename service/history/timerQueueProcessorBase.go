// Copyright (c) 2017 Uber Technologies, Inc.
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
	ctx "context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/quotas"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/queue"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/history/task"
)

var (
	emptyTime = time.Time{}

	loadDomainEntryForTimerTaskRetryDelay = 100 * time.Millisecond
	loadTimerTaskThrottleRetryDelay       = 5 * time.Second
)

type (
	timerQueueProcessorBase struct {
		scope                int
		shard                shard.Context
		historyService       *historyEngineImpl
		cache                *execution.Cache
		executionManager     persistence.ExecutionManager
		status               int32
		shutdownWG           sync.WaitGroup
		shutdownCh           chan struct{}
		config               *config.Config
		logger               log.Logger
		metricsClient        metrics.Client
		metricsScope         metrics.Scope
		timerFiredCount      uint64
		timerProcessor       timerProcessor
		timerQueueAckMgr     timerQueueAckMgr
		timerGate            TimerGate
		timeSource           clock.TimeSource
		rateLimiter          quotas.Limiter
		retryPolicy          backoff.RetryPolicy
		lastPollTime         time.Time
		taskProcessor        *taskProcessor // TODO: deprecate task processor, in favor of queueTaskProcessor
		queueTaskProcessor   task.Processor
		redispatchQueue      collection.Queue
		queueTaskInitializer task.Initializer

		// timer notification
		newTimerCh  chan struct{}
		newTimeLock sync.Mutex
		newTime     time.Time
	}
)

func newTimerQueueProcessorBase(
	scope int,
	shard shard.Context,
	historyService *historyEngineImpl,
	timerProcessor timerProcessor,
	queueTaskProcessor task.Processor,
	timerQueueAckMgr timerQueueAckMgr,
	redispatchQueue collection.Queue,
	queueTaskInitializer task.Initializer,
	timerGate TimerGate,
	maxPollRPS dynamicconfig.IntPropertyFn,
	logger log.Logger,
	metricsScope metrics.Scope,
) *timerQueueProcessorBase {

	logger = logger.WithTags(tag.ComponentTimerQueue)
	config := shard.GetConfig()

	var taskProcessor *taskProcessor
	if !config.TimerProcessorEnablePriorityTaskProcessor() {
		options := taskProcessorOptions{
			workerCount: config.TimerTaskWorkerCount(),
			queueSize:   config.TimerTaskWorkerCount() * config.TimerTaskBatchSize(),
		}
		taskProcessor = newTaskProcessor(options, shard, historyService.executionCache, logger)
	}

	base := &timerQueueProcessorBase{
		scope:                scope,
		shard:                shard,
		historyService:       historyService,
		timerProcessor:       timerProcessor,
		cache:                historyService.executionCache,
		executionManager:     shard.GetExecutionManager(),
		status:               common.DaemonStatusInitialized,
		shutdownCh:           make(chan struct{}),
		config:               config,
		logger:               logger,
		metricsClient:        historyService.metricsClient,
		metricsScope:         metricsScope,
		timerQueueAckMgr:     timerQueueAckMgr,
		timerGate:            timerGate,
		timeSource:           shard.GetTimeSource(),
		newTimerCh:           make(chan struct{}, 1),
		lastPollTime:         time.Time{},
		taskProcessor:        taskProcessor,
		queueTaskProcessor:   queueTaskProcessor,
		redispatchQueue:      redispatchQueue,
		queueTaskInitializer: queueTaskInitializer,
		rateLimiter: quotas.NewDynamicRateLimiter(
			func() float64 {
				return float64(maxPollRPS())
			},
		),
		retryPolicy: common.CreatePersistanceRetryPolicy(),
	}

	return base
}

func (t *timerQueueProcessorBase) Start() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	if t.taskProcessor != nil {
		t.taskProcessor.start()
	}
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

	t.timerGate.Close()
	close(t.shutdownCh)
	t.retryTasks()

	if success := common.AwaitWaitGroup(&t.shutdownWG, time.Minute); !success {
		t.logger.Warn("Timer queue processor timedout on shutdown.")
	}

	if t.taskProcessor != nil {
		t.taskProcessor.stop()
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
	timerTasks []persistence.Task,
) {

	if len(timerTasks) == 0 {
		return
	}

	isActive := t.scope == metrics.TimerActiveQueueProcessorScope

	newTime := timerTasks[0].GetVisibilityTimestamp()
	for _, timerTask := range timerTasks {
		ts := timerTask.GetVisibilityTimestamp()
		if ts.Before(newTime) {
			newTime = ts
		}

		scopeIdx := task.GetTimerTaskMetricScope(timerTask.GetType(), isActive)
		t.metricsClient.IncCounter(scopeIdx, metrics.NewTimerCounter)
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

	redispatchTimer := time.NewTimer(backoff.JitDuration(
		t.config.TimerProcessorRedispatchInterval(),
		t.config.TimerProcessorRedispatchIntervalJitterCoefficient(),
	))
	defer redispatchTimer.Stop()

	for {
		// Wait until one of four things occurs:
		// 1. we get notified of a new message
		// 2. the timer gate fires (message scheduled to be delivered)
		// 3. shutdown was triggered.
		// 4. updating ack level
		//
		select {
		case <-t.shutdownCh:
			t.logger.Debug("Timer queue processor pump shutting down.")
			return nil
		case <-t.timerQueueAckMgr.getFinishedChan():
			// timer queue ack manager indicate that all task scanned
			// are finished and no more tasks
			// use a separate goroutine since the caller hold the shutdownWG
			go t.Stop()
			return nil
		case <-t.timerGate.FireChan():
			if !t.isPriorityTaskProcessorEnabled() || t.redispatchQueue.Len() <= t.config.TimerProcessorMaxRedispatchQueueSize() {
				lookAheadTimer, err := t.readAndFanoutTimerTasks()
				if err != nil {
					return err
				}
				if lookAheadTimer != nil {
					t.timerGate.Update(lookAheadTimer.VisibilityTimestamp)
				}
				continue
			}

			// has too many pending tasks in re-dispatch queue, block loading tasks from persistence
			t.redispatchTasks()
			// re-enqueue the event to see if we need keep re-dispatching or load new tasks from persistence
			t.notifyNewTimer(time.Time{})
		case <-pollTimer.C:
			pollTimer.Reset(backoff.JitDuration(
				t.config.TimerProcessorMaxPollInterval(),
				t.config.TimerProcessorMaxPollIntervalJitterCoefficient(),
			))
			if t.lastPollTime.Add(t.config.TimerProcessorMaxPollInterval()).Before(t.timeSource.Now()) {
				lookAheadTimer, err := t.readAndFanoutTimerTasks()
				if err != nil {
					return err
				}
				if lookAheadTimer != nil {
					t.timerGate.Update(lookAheadTimer.VisibilityTimestamp)
				}
			}
		case <-updateAckTimer.C:
			updateAckTimer.Reset(backoff.JitDuration(
				t.config.TimerProcessorUpdateAckInterval(),
				t.config.TimerProcessorUpdateAckIntervalJitterCoefficient(),
			))
			if err := t.timerQueueAckMgr.updateAckLevel(); err == shard.ErrShardClosed {
				// shard is closed, shutdown timerQProcessor and bail out
				go t.Stop()
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
		case <-redispatchTimer.C:
			redispatchTimer.Reset(backoff.JitDuration(
				t.config.TimerProcessorRedispatchInterval(),
				t.config.TimerProcessorRedispatchIntervalJitterCoefficient(),
			))
			t.redispatchTasks()
		}
	}
}

func (t *timerQueueProcessorBase) readAndFanoutTimerTasks() (*persistence.TimerTaskInfo, error) {
	ctx, cancel := ctx.WithTimeout(ctx.Background(), loadTimerTaskThrottleRetryDelay)
	if err := t.rateLimiter.Wait(ctx); err != nil {
		cancel()
		t.notifyNewTimer(time.Time{}) // re-enqueue the event
		return nil, nil
	}
	cancel()

	t.lastPollTime = t.timeSource.Now()
	timerTasks, lookAheadTask, moreTasks, err := t.timerQueueAckMgr.readTimerTasks()
	if err != nil {
		t.notifyNewTimer(time.Time{}) // re-enqueue the event
		return nil, err
	}

	for _, task := range timerTasks {
		if submitted := t.submitTask(task); !submitted {
			return nil, nil
		}
		select {
		case <-t.shutdownCh:
			return nil, nil
		default:
		}
	}

	if !moreTasks {
		return lookAheadTask, nil
	}

	t.notifyNewTimer(time.Time{}) // re-enqueue the event
	return nil, nil
}

func (t *timerQueueProcessorBase) submitTask(
	taskInfo task.Info,
) bool {
	if !t.isPriorityTaskProcessorEnabled() {
		return t.taskProcessor.addTask(
			newTaskInfo(
				t.timerProcessor,
				taskInfo,
				task.InitializeLoggerForTask(t.shard.GetShardID(), taskInfo, t.logger),
			),
		)
	}

	timeQueueTask := t.queueTaskInitializer(taskInfo)
	submitted, err := t.queueTaskProcessor.TrySubmit(timeQueueTask)
	if err != nil {
		return false
	}
	if !submitted {
		t.redispatchQueue.Add(timeQueueTask)
	}

	return true
}

func (t *timerQueueProcessorBase) redispatchTasks() {
	if !t.isPriorityTaskProcessorEnabled() {
		return
	}

	queue.RedispatchTasks(
		t.redispatchQueue,
		t.queueTaskProcessor,
		t.logger,
		t.metricsScope,
		t.shutdownCh,
	)
}

func (t *timerQueueProcessorBase) retryTasks() {
	if t.taskProcessor != nil {
		t.taskProcessor.retryTasks()
	}
}

func (t *timerQueueProcessorBase) complete(
	timerTask *persistence.TimerTaskInfo,
) {
	t.timerQueueAckMgr.completeTimerTask(timerTask)
	atomic.AddUint64(&t.timerFiredCount, 1)
}

func (t *timerQueueProcessorBase) isPriorityTaskProcessorEnabled() bool {
	return t.taskProcessor == nil
}

func (t *timerQueueProcessorBase) getTimerFiredCount() uint64 {
	return atomic.LoadUint64(&t.timerFiredCount)
}

//nolint:unused
func (t *timerQueueProcessorBase) getTimerTaskType(
	taskType int,
) string {

	switch taskType {
	case persistence.TaskTypeUserTimer:
		return "UserTimer"
	case persistence.TaskTypeActivityTimeout:
		return "ActivityTimeout"
	case persistence.TaskTypeDecisionTimeout:
		return "DecisionTimeout"
	case persistence.TaskTypeWorkflowTimeout:
		return "WorkflowTimeout"
	case persistence.TaskTypeDeleteHistoryEvent:
		return "DeleteHistoryEvent"
	case persistence.TaskTypeActivityRetryTimer:
		return "ActivityRetryTimerTask"
	case persistence.TaskTypeWorkflowBackoffTimer:
		return "WorkflowBackoffTimerTask"
	}
	return "UnKnown"
}
