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
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
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
		shard            ShardContext
		historyService   *historyEngineImpl
		executionManager persistence.ExecutionManager
		isStarted        int32
		isStopped        int32
		shutdownWG       sync.WaitGroup
		shutdownCh       chan struct{}
		tasksCh          chan *persistence.TimerTaskInfo
		config           *Config
		logger           bark.Logger
		metricsClient    metrics.Client
		now              timeNow
		timerFiredCount  uint64
		timerProcessor   timerProcessor
		timerQueueAckMgr timerQueueAckMgr

		// worker coroutines notification
		workerNotificationChans []chan struct{}

		// timer notification
		newTimerCh  chan struct{}
		newTimeLock sync.Mutex
		newTime     time.Time
	}
)

func newTimerQueueProcessorBase(shard ShardContext, historyService *historyEngineImpl, timerQueueAckMgr timerQueueAckMgr, timeNow timeNow, logger bark.Logger) *timerQueueProcessorBase {
	log := logger.WithFields(bark.Fields{
		logging.TagWorkflowComponent: logging.TagValueTimerQueueComponent,
	})

	workerNotificationChans := []chan struct{}{}
	for index := 0; index < shard.GetConfig().TimerProcessorTaskWorkerCount; index++ {
		workerNotificationChans = append(workerNotificationChans, make(chan struct{}, 1))
	}

	base := &timerQueueProcessorBase{
		shard:                   shard,
		historyService:          historyService,
		executionManager:        shard.GetExecutionManager(),
		shutdownCh:              make(chan struct{}),
		tasksCh:                 make(chan *persistence.TimerTaskInfo, 10*shard.GetConfig().TimerTaskBatchSize),
		config:                  shard.GetConfig(),
		logger:                  log,
		metricsClient:           historyService.metricsClient,
		now:                     timeNow,
		timerQueueAckMgr:        timerQueueAckMgr,
		workerNotificationChans: workerNotificationChans,
		newTimerCh:              make(chan struct{}, 1),
	}

	return base
}

func (t *timerQueueProcessorBase) Start() {
	if !atomic.CompareAndSwapInt32(&t.isStarted, 0, 1) {
		return
	}

	t.shutdownWG.Add(1)
	go t.processorPump()

	t.logger.Info("Timer queue processor started.")
}

func (t *timerQueueProcessorBase) Stop() {
	if !atomic.CompareAndSwapInt32(&t.isStopped, 0, 1) {
		return
	}

	if atomic.LoadInt32(&t.isStarted) == 1 {
		close(t.shutdownCh)
	}

	if success := common.AwaitWaitGroup(&t.shutdownWG, time.Minute); !success {
		t.logger.Warn("Timer queue processor timed out on shutdown.")
	}

	t.logger.Info("Timer queue processor stopped.")
}

func (t *timerQueueProcessorBase) processorPump() {
	defer t.shutdownWG.Done()

	// Workers to process timer tasks that are expired.

	var workerWG sync.WaitGroup
	for i := 0; i < t.config.TimerProcessorTaskWorkerCount; i++ {
		workerWG.Add(1)
		notificationChan := t.workerNotificationChans[i]
		go t.processTaskWorker(&workerWG, notificationChan)
	}

RetryProcessor:
	for {
		select {
		case <-t.shutdownCh:
			t.logger.Info("Timer queue processor pump shutting down.")
			close(t.tasksCh)
			if success := common.AwaitWaitGroup(&workerWG, 10*time.Second); !success {
				t.logger.Warn("Timer queue processor timed out on worker shutdown.")
			}
			break RetryProcessor
		default:
			err := t.internalProcessor()
			if err != nil {
				t.logger.Error("processor pump failed with error: ", err)
			}
		}
	}
	t.logger.Info("Timer processor exiting.")
}

func (t *timerQueueProcessorBase) processTaskWorker(workerWG *sync.WaitGroup, notificationChan chan struct{}) {
	defer workerWG.Done()
	for {
		select {
		case task, ok := <-t.tasksCh:
			if !ok {
				return
			}

		UpdateFailureLoop:
			for attempt := 1; attempt <= t.config.TimerProcessorUpdateFailureRetryCount; {

				// clear the existing notification
				select {
				case <-notificationChan:
				default:
				}

				err := t.timerProcessor.process(task)
				if err != nil {
					if err == ErrTaskRetry {
						<-notificationChan
					} else {
						// We will retry until we don't find the timer task any more.
						t.logger.Infof("Failed to process timer: %v; %v.", task, err)
						backoff := time.Duration(attempt * 100)
						time.Sleep(backoff * time.Millisecond)
						attempt++
					}
				} else {
					atomic.AddUint64(&t.timerFiredCount, 1)
					break UpdateFailureLoop
				}
			}
		}
	}
}

// NotifyNewTimers - Notify the processor about the new timer events arrival.
// This should be called each time new timer events arrives, otherwise timers maybe fired unexpected.
func (t *timerQueueProcessorBase) notifyNewTimers(timerTasks []persistence.Task, counterType int) {
	if len(timerTasks) == 0 {
		return
	}

	t.metricsClient.AddCounter(metrics.TimerQueueProcessorScope, counterType, int64(len(timerTasks)))
	newTime := persistence.GetVisibilityTSFrom(timerTasks[0])
	for _, task := range timerTasks {
		ts := persistence.GetVisibilityTSFrom(task)
		if ts.Before(newTime) {
			newTime = ts
		}

		switch task.GetType() {
		case persistence.TaskTypeDecisionTimeout:
			t.metricsClient.IncCounter(metrics.TimerTaskDecisionTimeoutScope, counterType)
		case persistence.TaskTypeActivityTimeout:
			t.metricsClient.IncCounter(metrics.TimerTaskActivityTimeoutScope, counterType)
		case persistence.TaskTypeUserTimer:
			t.metricsClient.IncCounter(metrics.TimerTaskUserTimerScope, counterType)
		case persistence.TaskTypeWorkflowTimeout:
			t.metricsClient.IncCounter(metrics.TimerTaskWorkflowTimeoutScope, counterType)
		case persistence.TaskTypeDeleteHistoryEvent:
			t.metricsClient.IncCounter(metrics.TimerTaskDeleteHistoryEvent, counterType)
		case persistence.TaskTypeRetryTimer:
			t.metricsClient.IncCounter(metrics.TimerTaskRetryTimerScope, counterType)
			// TODO add default
		}
	}

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
	pollTimer := time.NewTimer(t.config.TimerProcessorMaxPollInterval)
	defer pollTimer.Stop()

	updateAckChan := time.NewTicker(t.shard.GetConfig().TimerProcessorUpdateAckInterval).C
	var nextKeyTask *persistence.TimerTaskInfo

continueProcessor:
	for {
		now := t.now()
		if nextKeyTask == nil || timerGate.FireAfter(now) {
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
				t.Stop()
				return nil

			case <-timerGate.FireChan():
				// Timer Fired.

			case <-pollTimer.C:
				// forced timer scan
				pollTimer.Reset(t.config.TimerProcessorMaxPollInterval)

			case <-updateAckChan:
				t.timerQueueAckMgr.updateAckLevel()
				continue continueProcessor

			case <-t.newTimerCh:
				t.newTimeLock.Lock()
				newTime := t.newTime
				t.newTime = emptyTime
				t.newTimeLock.Unlock()
				// New Timer has arrived.
				t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.NewTimerNotifyCounter)
				t.logger.Debugf("Woke up by the timer")

				if timerGate.Update(newTime) {
					// this means timer is updated, to the new time provided
					// reset the nextKeyTask as the new timer is expected to fire before previously read nextKeyTask
					nextKeyTask = nil
				}

				now = t.now()
				t.logger.Debugf("%v: Next key after woke up by timer: %v", now.UTC(), newTime.UTC())
				if timerGate.FireAfter(now) {
					continue continueProcessor
				}
			}
		}

		var err error
		nextKeyTask, err = t.readAndFanoutTimerTasks()
		if err != nil {
			return err
		}

		if nextKeyTask != nil {
			nextKey := TimerSequenceID{VisibilityTimestamp: nextKeyTask.VisibilityTimestamp, TaskID: nextKeyTask.TaskID}
			t.logger.Debugf("%s: GetNextKey: %s", time.Now(), nextKey)

			timerGate.Update(nextKey.VisibilityTimestamp)
		}
	}
}

func (t *timerQueueProcessorBase) readAndFanoutTimerTasks() (*persistence.TimerTaskInfo, error) {
	for {
		// Get next set of timer tasks.
		timerTasks, lookAheadTask, moreTasks, err := t.timerQueueAckMgr.readTimerTasks()
		if err != nil {
			return nil, err
		}

		for _, task := range timerTasks {
			// We have a timer to fire.
			t.tasksCh <- task
		}

		if !moreTasks {
			return lookAheadTask, nil
		}
	}
}

func (t *timerQueueProcessorBase) retryTasks() {
	for _, workerNotificationChan := range t.workerNotificationChans {
		select {
		case workerNotificationChan <- struct{}{}:
		default:
		}
	}
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

func (t *timerQueueProcessorBase) processDeleteHistoryEvent(task *persistence.TimerTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TimerTaskDeleteHistoryEvent, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerTaskDeleteHistoryEvent, metrics.TaskLatency)
	defer sw.Stop()

	op := func() error {
		return t.executionManager.DeleteWorkflowExecution(&persistence.DeleteWorkflowExecutionRequest{
			DomainID:   task.DomainID,
			WorkflowID: task.WorkflowID,
			RunID:      task.RunID,
		})
	}

	err := backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
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
	case persistence.TaskTypeRetryTimer:
		return "RetryTimerTask"
	}
	return "UnKnown"
}
