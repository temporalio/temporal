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
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

var (
	errTimerTaskNotFound          = errors.New("Timer task not found")
	errFailedToAddTimeoutEvent    = errors.New("Failed to add timeout event")
	errFailedToAddTimerFiredEvent = errors.New("Failed to add timer fired event")
	emptyTime                     = time.Time{}
	maxTimestamp                  = time.Unix(0, math.MaxInt64)
)

type (
	timerQueueProcessorBase struct {
		scope            int
		shard            ShardContext
		historyService   *historyEngineImpl
		cache            *historyCache
		executionManager persistence.ExecutionManager
		status           int32
		shutdownWG       sync.WaitGroup
		shutdownCh       chan struct{}
		tasksCh          chan *persistence.TimerTaskInfo
		config           *Config
		logger           bark.Logger
		metricsClient    metrics.Client
		timerFiredCount  uint64
		timerProcessor   timerProcessor
		timerQueueAckMgr timerQueueAckMgr
		rateLimiter      common.TokenBucket
		startDelay       dynamicconfig.DurationPropertyFn
		retryPolicy      backoff.RetryPolicy

		// worker coroutines notification
		workerNotificationChans []chan struct{}
		// duplicate numOfWorker from config.TimerTaskWorkerCount for dynamic config works correctly
		numOfWorker int

		lastPollTime time.Time

		// timer notification
		newTimerCh  chan struct{}
		newTimeLock sync.Mutex
		newTime     time.Time
	}
)

func newTimerQueueProcessorBase(scope int, shard ShardContext, historyService *historyEngineImpl,
	timerQueueAckMgr timerQueueAckMgr, maxPollRPS dynamicconfig.IntPropertyFn,
	startDelay dynamicconfig.DurationPropertyFn, logger bark.Logger) *timerQueueProcessorBase {
	log := logger.WithFields(bark.Fields{
		logging.TagWorkflowComponent: logging.TagValueTimerQueueComponent,
	})

	workerNotificationChans := []chan struct{}{}
	numOfWorker := shard.GetConfig().TimerTaskWorkerCount()
	for index := 0; index < numOfWorker; index++ {
		workerNotificationChans = append(workerNotificationChans, make(chan struct{}, 1))
	}

	base := &timerQueueProcessorBase{
		scope:                   scope,
		shard:                   shard,
		historyService:          historyService,
		cache:                   historyService.historyCache,
		executionManager:        shard.GetExecutionManager(),
		status:                  common.DaemonStatusInitialized,
		shutdownCh:              make(chan struct{}),
		tasksCh:                 make(chan *persistence.TimerTaskInfo, 10*shard.GetConfig().TimerTaskBatchSize()),
		config:                  shard.GetConfig(),
		logger:                  log,
		metricsClient:           historyService.metricsClient,
		timerQueueAckMgr:        timerQueueAckMgr,
		numOfWorker:             numOfWorker,
		workerNotificationChans: workerNotificationChans,
		newTimerCh:              make(chan struct{}, 1),
		lastPollTime:            time.Time{},
		rateLimiter:             common.NewTokenBucket(maxPollRPS(), common.NewRealTimeSource()),
		startDelay:              startDelay,
		retryPolicy:             common.CreatePersistanceRetryPolicy(),
	}

	return base
}

func (t *timerQueueProcessorBase) Start() {
	if !atomic.CompareAndSwapInt32(&t.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
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

	close(t.shutdownCh)
	t.retryTasks()

	if success := common.AwaitWaitGroup(&t.shutdownWG, time.Minute); !success {
		t.logger.Warn("Timer queue processor timedout on shutdown.")
	}

	t.logger.Info("Timer queue processor stopped.")
}

func (t *timerQueueProcessorBase) processorPump() {
	<-time.NewTimer(backoff.NewJitter().JitDuration(t.startDelay(), 0.99)).C

	defer t.shutdownWG.Done()

	var workerWG sync.WaitGroup
	for i := 0; i < t.numOfWorker; i++ {
		workerWG.Add(1)
		notificationChan := t.workerNotificationChans[i]
		go t.taskWorker(&workerWG, notificationChan)
	}

RetryProcessor:
	for {
		select {
		case <-t.shutdownCh:
			break RetryProcessor
		default:
			err := t.internalProcessor()
			if err != nil {
				t.logger.Error("processor pump failed with error: ", err)
			}
		}
	}

	t.logger.Info("Timer queue processor pump shutting down.")
	// This is the only pump which writes to tasksCh, so it is safe to close channel here
	close(t.tasksCh)
	if success := common.AwaitWaitGroup(&workerWG, 10*time.Second); !success {
		t.logger.Warn("Timer queue processor timedout on worker shutdown.")
	}
	t.logger.Info("Timer processor exiting.")
}

func (t *timerQueueProcessorBase) taskWorker(workerWG *sync.WaitGroup, notificationChan chan struct{}) {
	defer workerWG.Done()

	for {
		select {
		case <-t.shutdownCh:
			return
		case task, ok := <-t.tasksCh:
			if !ok {
				return
			}
			t.processTaskAndAck(notificationChan, task)
		}
	}
}

// NotifyNewTimers - Notify the processor about the new timer events arrival.
// This should be called each time new timer events arrives, otherwise timers maybe fired unexpected.
func (t *timerQueueProcessorBase) notifyNewTimers(timerTasks []persistence.Task) {
	if len(timerTasks) == 0 {
		return
	}

	isActive := t.scope == metrics.TimerActiveQueueProcessorScope

	newTime := timerTasks[0].GetVisibilityTimestamp()
	for _, task := range timerTasks {
		ts := task.GetVisibilityTimestamp()
		if ts.Before(newTime) {
			newTime = ts
		}

		switch task.GetType() {
		case persistence.TaskTypeDecisionTimeout:
			if isActive {
				t.metricsClient.IncCounter(metrics.TimerActiveTaskDecisionTimeoutScope, metrics.NewTimerCounter)
			} else {
				t.metricsClient.IncCounter(metrics.TimerStandbyTaskDecisionTimeoutScope, metrics.NewTimerCounter)
			}
		case persistence.TaskTypeActivityTimeout:
			if isActive {
				t.metricsClient.IncCounter(metrics.TimerActiveTaskActivityTimeoutScope, metrics.NewTimerCounter)
			} else {
				t.metricsClient.IncCounter(metrics.TimerStandbyTaskActivityTimeoutScope, metrics.NewTimerCounter)
			}
		case persistence.TaskTypeUserTimer:
			if isActive {
				t.metricsClient.IncCounter(metrics.TimerActiveTaskUserTimerScope, metrics.NewTimerCounter)
			} else {
				t.metricsClient.IncCounter(metrics.TimerStandbyTaskUserTimerScope, metrics.NewTimerCounter)
			}
		case persistence.TaskTypeWorkflowTimeout:
			if isActive {
				t.metricsClient.IncCounter(metrics.TimerActiveTaskWorkflowTimeoutScope, metrics.NewTimerCounter)
			} else {
				t.metricsClient.IncCounter(metrics.TimerStandbyTaskWorkflowTimeoutScope, metrics.NewTimerCounter)
			}
		case persistence.TaskTypeDeleteHistoryEvent:
			if isActive {
				t.metricsClient.IncCounter(metrics.TimerActiveTaskDeleteHistoryEventScope, metrics.NewTimerCounter)
			} else {
				t.metricsClient.IncCounter(metrics.TimerStandbyTaskDeleteHistoryEventScope, metrics.NewTimerCounter)
			}
		case persistence.TaskTypeActivityRetryTimer:
			if isActive {
				t.metricsClient.IncCounter(metrics.TimerActiveTaskActivityRetryTimerScope, metrics.NewTimerCounter)
			} else {
				t.metricsClient.IncCounter(metrics.TimerStandbyTaskActivityRetryTimerScope, metrics.NewTimerCounter)
			}
		case persistence.TaskTypeWorkflowRetryTimer:
			if isActive {
				t.metricsClient.IncCounter(metrics.TimerActiveTaskWorkflowRetryTimerScope, metrics.NewTimerCounter)
			} else {
				t.metricsClient.IncCounter(metrics.TimerStandbyTaskWorkflowRetryTimerScope, metrics.NewTimerCounter)
			}
			// TODO add default
		}
	}

	t.notifyNewTimer(newTime)
}

func (t *timerQueueProcessorBase) notifyNewTimer(newTime time.Time) {
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
	timerGate := t.timerProcessor.getTimerGate()
	jitter := backoff.NewJitter()
	pollTimer := time.NewTimer(jitter.JitDuration(
		t.config.TimerProcessorMaxPollInterval(),
		t.config.TimerProcessorMaxPollIntervalJitterCoefficient(),
	))
	defer pollTimer.Stop()

	updateAckTimer := time.NewTimer(jitter.JitDuration(
		t.config.TimerProcessorUpdateAckInterval(),
		t.config.TimerProcessorUpdateAckIntervalJitterCoefficient(),
	))
	defer updateAckTimer.Stop()

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
			// use a separate gorouting since the caller hold the shutdownWG
			go t.Stop()
			return nil
		case <-timerGate.FireChan():
			lookAheadTimer, err := t.readAndFanoutTimerTasks()
			if err != nil {
				return err
			}
			if lookAheadTimer != nil {
				timerGate.Update(lookAheadTimer.VisibilityTimestamp)
			}
		case <-pollTimer.C:
			pollTimer.Reset(jitter.JitDuration(
				t.config.TimerProcessorMaxPollInterval(),
				t.config.TimerProcessorMaxPollIntervalJitterCoefficient(),
			))
			if t.lastPollTime.Add(t.config.TimerProcessorMaxPollInterval()).Before(time.Now()) {
				lookAheadTimer, err := t.readAndFanoutTimerTasks()
				if err != nil {
					return err
				}
				if lookAheadTimer != nil {
					timerGate.Update(lookAheadTimer.VisibilityTimestamp)
				}
			}
		case <-updateAckTimer.C:
			updateAckTimer.Reset(jitter.JitDuration(
				t.config.TimerProcessorUpdateAckInterval(),
				t.config.TimerProcessorUpdateAckIntervalJitterCoefficient(),
			))
			t.timerQueueAckMgr.updateAckLevel()
		case <-t.newTimerCh:
			t.newTimeLock.Lock()
			newTime := t.newTime
			t.newTime = emptyTime
			t.newTimeLock.Unlock()
			// New Timer has arrived.
			t.metricsClient.IncCounter(t.scope, metrics.NewTimerNotifyCounter)
			timerGate.Update(newTime)
		}
	}
}

func (t *timerQueueProcessorBase) readAndFanoutTimerTasks() (*persistence.TimerTaskInfo, error) {
	if !t.rateLimiter.Consume(1, t.shard.GetConfig().TimerProcessorMaxPollInterval()) {
		t.notifyNewTimer(time.Time{}) // re-enqueue the event
		return nil, nil
	}

	t.lastPollTime = time.Now()
	timerTasks, lookAheadTask, moreTasks, err := t.timerQueueAckMgr.readTimerTasks()
	if err != nil {
		t.notifyNewTimer(time.Time{}) // re-enqueue the event
		return nil, err
	}

	for _, task := range timerTasks {
		// We have a timer to fire.
		select {
		case t.tasksCh <- task:
		case <-t.shutdownCh:
			return nil, nil
		}
	}

	if !moreTasks {
		return lookAheadTask, nil
	}

	t.notifyNewTimer(time.Time{}) // re-enqueue the event
	return nil, nil
}

func (t *timerQueueProcessorBase) retryTasks() {
	for _, workerNotificationChan := range t.workerNotificationChans {
		select {
		case workerNotificationChan <- struct{}{}:
		default:
		}
	}
}

func (t *timerQueueProcessorBase) processTaskAndAck(notificationChan <-chan struct{}, task *persistence.TimerTaskInfo) {

	var scope int
	var err error
	startTime := time.Now()
	logger := t.initializeLoggerForTask(task)
	attempt := 0
	op := func() error {
		scope, err = t.processTaskOnce(notificationChan, task, logger)
		return t.handleTaskError(scope, startTime, notificationChan, err, logger)
	}
	retryCondition := func(err error) bool { return true }

	for {
		select {
		case <-t.shutdownCh:
			// this must return without ack
			return
		default:
			err = backoff.Retry(op, t.retryPolicy, retryCondition)
			if err == nil {
				t.metricsClient.RecordTimer(scope, metrics.TaskAttemptTimer, time.Duration(attempt))
				t.ackTaskOnce(task, scope)
				return
			}

			attempt++

			if attempt >= t.config.TimerTaskMaxRetryCount() {
				t.metricsClient.RecordTimer(scope, metrics.TaskAttemptTimer, time.Duration(attempt))
				logging.LogCriticalErrorEvent(logger, "Critical error processing timer task, retrying.", err)
			}
		}
	}
}

func (t *timerQueueProcessorBase) processTaskOnce(notificationChan <-chan struct{}, task *persistence.TimerTaskInfo, logger bark.Logger) (int, error) {

	select {
	case <-notificationChan:
	default:
	}

	startTime := time.Now()
	scope, err := t.timerProcessor.process(task)
	t.metricsClient.IncCounter(scope, metrics.TaskRequests)
	t.metricsClient.RecordTimer(scope, metrics.TaskProcessingLatency, time.Since(startTime))

	return scope, err
}

func (t *timerQueueProcessorBase) handleTaskError(scope int, startTime time.Time,
	notificationChan <-chan struct{}, err error, logger bark.Logger) error {

	if err == nil {
		return nil
	}

	if _, ok := err.(*workflow.EntityNotExistsError); ok {
		return nil
	}

	// this is a transient error
	if err == ErrTaskRetry {
		t.metricsClient.IncCounter(scope, metrics.TaskStandbyRetryCounter)
		<-notificationChan
		return err
	}

	if err == ErrTaskDiscarded {
		t.metricsClient.IncCounter(scope, metrics.TaskDiscarded)
		err = nil
	}

	// this is a transient error
	if _, ok := err.(*workflow.DomainNotActiveError); ok {
		if time.Now().Sub(startTime) > cache.DomainCacheRefreshInterval {
			t.metricsClient.IncCounter(scope, metrics.TaskNotActiveCounter)
			return nil
		}

		return err
	}

	t.metricsClient.IncCounter(scope, metrics.TaskFailures)

	if _, ok := err.(*persistence.CurrentWorkflowConditionFailedError); ok {
		logging.LogTaskProcessingFailedEvent(logger, "More than 2 workflow are running.", err)
		return nil
	}

	if _, ok := err.(*workflow.LimitExceededError); ok {
		t.metricsClient.IncCounter(scope, metrics.TaskLimitExceededCounter)
		logging.LogTaskProcessingFailedEvent(logger, "Task encounter limit exceeded error.", err)
		return err
	}

	logging.LogTaskProcessingFailedEvent(logger, "Fail to process task", err)
	return err
}

func (t *timerQueueProcessorBase) ackTaskOnce(task *persistence.TimerTaskInfo, scope int) {
	t.timerQueueAckMgr.completeTimerTask(task)
	t.metricsClient.RecordTimer(
		scope,
		metrics.TaskQueueLatency,
		time.Since(task.GetVisibilityTimestamp()),
	)
	atomic.AddUint64(&t.timerFiredCount, 1)
}

func (t *timerQueueProcessorBase) initializeLoggerForTask(task *persistence.TimerTaskInfo) bark.Logger {
	logger := t.logger.WithFields(bark.Fields{
		logging.TagHistoryShardID:      t.shard.GetShardID(),
		logging.TagTaskID:              task.GetTaskID(),
		logging.TagTaskType:            task.GetTaskType(),
		logging.TagVersion:             task.GetVersion(),
		logging.TagTimeoutType:         task.TimeoutType,
		logging.TagDomainID:            task.DomainID,
		logging.TagWorkflowExecutionID: task.WorkflowID,
		logging.TagWorkflowRunID:       task.RunID,
	})
	logger.Debugf("Processing timer task: %v, type: %v", task.GetTaskID(), task.GetTaskType())
	return logger
}

func (t *timerQueueProcessorBase) getTimerFiredCount() uint64 {
	return atomic.LoadUint64(&t.timerFiredCount)
}

func (t *timerQueueProcessorBase) getDomainIDAndWorkflowExecution(task *persistence.TimerTaskInfo) (string, workflow.WorkflowExecution) {
	return task.DomainID, workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}
}

func (t *timerQueueProcessorBase) processDeleteHistoryEvent(task *persistence.TimerTaskInfo) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecution(t.getDomainIDAndWorkflowExecution(task))
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	msBuilder, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	} else if msBuilder == nil || msBuilder.IsWorkflowExecutionRunning() {
		// this can happen if workflow is reset.
		return nil
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, msBuilder.GetLastWriteVersion(), task.Version, task)
	if err != nil {
		return err
	} else if !ok {
		return nil
	}

	op := func() error {
		return t.executionManager.DeleteWorkflowExecution(&persistence.DeleteWorkflowExecutionRequest{
			DomainID:   task.DomainID,
			WorkflowID: task.WorkflowID,
			RunID:      task.RunID,
		})
	}

	err = backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return err
	}

	domainID, workflowExecution := t.getDomainIDAndWorkflowExecution(task)
	op = func() error {
		return t.historyService.historyMgr.DeleteWorkflowExecutionHistory(
			&persistence.DeleteWorkflowExecutionHistoryRequest{
				DomainID:  domainID,
				Execution: workflowExecution,
			})
	}

	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (t *timerQueueProcessorBase) getTimerTaskType(taskType int) string {
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
	case persistence.TaskTypeWorkflowRetryTimer:
		return "WorkflowRetryTimerTask"
	}
	return "UnKnown"
}
