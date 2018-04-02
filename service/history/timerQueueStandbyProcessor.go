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
	"fmt"
	"sync"
	"time"

	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	timerQueueStandbyProcessorImpl struct {
		shard                   ShardContext
		historyService          *historyEngineImpl
		cache                   *historyCache
		logger                  bark.Logger
		metricsClient           metrics.Client
		newTimerCh              chan struct{}
		clusterName             string
		timerGate               RemoteTimerGate
		timerQueueProcessorBase *timerQueueProcessorBase
		timerQueueAckMgr        timerQueueAckMgr

		timeLock sync.Mutex
		newTime  time.Time // used to notify the earlist time of next batch of timers to be processed
	}
)

func newTimerQueueStandbyProcessor(shard ShardContext, historyService *historyEngineImpl, executionManager persistence.ExecutionManager, clusterName string, logger bark.Logger) *timerQueueStandbyProcessorImpl {
	log := logger.WithFields(bark.Fields{
		logging.TagWorkflowComponent: logging.TagValueTimerQueueComponent,
	})
	timerQueueAckMgr := newTimerQueueAckMgr(shard, historyService.metricsClient, executionManager, clusterName, log)
	processor := &timerQueueStandbyProcessorImpl{
		shard:                   shard,
		historyService:          historyService,
		cache:                   historyService.historyCache,
		logger:                  log,
		metricsClient:           historyService.metricsClient,
		newTimerCh:              make(chan struct{}, 1),
		clusterName:             clusterName,
		timerGate:               NewRemoteTimerGate(),
		timerQueueProcessorBase: newTimerQueueProcessorBase(shard, historyService, executionManager, timerQueueAckMgr, clusterName, logger),
		timerQueueAckMgr:        timerQueueAckMgr,
	}
	processor.timerQueueProcessorBase.timerProcessor = processor
	return processor
}

func (t *timerQueueStandbyProcessorImpl) Start() {
	t.timerQueueProcessorBase.Start()
}

func (t *timerQueueStandbyProcessorImpl) Stop() {
	t.timerQueueProcessorBase.Stop()
}

func (t *timerQueueStandbyProcessorImpl) getTimerFiredCount() uint64 {
	return t.timerQueueProcessorBase.getTimerFiredCount()
}

func (t *timerQueueStandbyProcessorImpl) getTimerGate() TimerGate {
	return t.timerGate
}

func (t *timerQueueStandbyProcessorImpl) setCurrentTime(currentTime time.Time) {
	t.timerGate.SetCurrentTime(currentTime)
	t.timerQueueProcessorBase.retryTimerTasks()
}

// NotifyNewTimers - Notify the processor about the new standby timer events arrival.
// This should be called each time new timer events arrives, otherwise timers maybe fired unexpected.
func (t *timerQueueStandbyProcessorImpl) notifyNewTimers(timerTasks []persistence.Task) {
	t.timerQueueProcessorBase.notifyNewTimers(timerTasks, metrics.NewStandbyTimerCounter)
}

func (t *timerQueueStandbyProcessorImpl) process(timerTask *persistence.TimerTaskInfo) error {
	taskID := TimerSequenceID{VisibilityTimestamp: timerTask.VisibilityTimestamp, TaskID: timerTask.TaskID}
	t.logger.Debugf("Processing timer: (%s), for WorkflowID: %v, RunID: %v, Type: %v, TimeoutType: %v, EventID: %v",
		taskID, timerTask.WorkflowID, timerTask.RunID, t.timerQueueProcessorBase.getTimerTaskType(timerTask.TaskType),
		workflow.TimeoutType(timerTask.TimeoutType).String(), timerTask.EventID)

	var err error
	scope := metrics.TimerQueueProcessorScope
	switch timerTask.TaskType {
	case persistence.TaskTypeUserTimer:
		scope = metrics.TimerTaskUserTimerScope
		err = t.processExpiredUserTimer(timerTask)

	case persistence.TaskTypeActivityTimeout:
		scope = metrics.TimerTaskActivityTimeoutScope
		err = t.processActivityTimeout(timerTask)

	case persistence.TaskTypeDecisionTimeout:
		scope = metrics.TimerTaskDecisionTimeoutScope
		err = t.processDecisionTimeout(timerTask)

	case persistence.TaskTypeWorkflowTimeout:
		scope = metrics.TimerTaskWorkflowTimeoutScope
		err = t.processWorkflowTimeout(timerTask)

	case persistence.TaskTypeDeleteHistoryEvent:
		scope = metrics.TimerTaskDeleteHistoryEvent
		err = t.timerQueueProcessorBase.processDeleteHistoryEvent(timerTask)
	}

	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// Timer could fire after the execution is deleted.
			// In which case just ignore the error so we can complete the timer task.
			t.timerQueueAckMgr.completeTimerTask(timerTask)
			err = nil
		}
		if err != nil {
			t.metricsClient.IncCounter(scope, metrics.TaskFailures)
		}
	}

	return err
}

func (t *timerQueueStandbyProcessorImpl) processExpiredUserTimer(timerTask *persistence.TimerTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TimerTaskUserTimerScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerTaskUserTimerScope, metrics.TaskLatency)
	defer sw.Stop()

	return t.processTimer(timerTask, func(msBuilder *mutableStateBuilder) error {
		tBuilder := t.historyService.getTimerBuilder(&workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(msBuilder.executionInfo.WorkflowID),
			RunId:      common.StringPtr(msBuilder.executionInfo.RunID),
		})

		for _, td := range tBuilder.GetUserTimers(msBuilder) {
			hasTimer, _ := tBuilder.GetUserTimer(td.TimerID)
			if !hasTimer {
				t.logger.Debugf("Failed to find in memory user timer: %s", td.TimerID)
				return fmt.Errorf("Failed to find in memory user timer: %s", td.TimerID)
			}

			if isExpired := tBuilder.IsTimerExpired(td, timerTask.VisibilityTimestamp); isExpired {
				// active cluster will add an timer fired event and schedule a decision if necessary
				// standby cluster should just call ack manager to retry this task
				// since we are stilling waiting for the fired event to be replicated
				//
				// we do not need to notity new timer to base, since if there is no new event being replicated
				// checking again if the timer can be completed is meaningless
				t.timerQueueAckMgr.retryTimerTask(timerTask)
				return nil
			}
		}
		// if there is no user timer expired, then we are good
		t.timerQueueAckMgr.completeTimerTask(timerTask)
		return nil
	})
}

func (t *timerQueueStandbyProcessorImpl) processActivityTimeout(timerTask *persistence.TimerTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TimerTaskActivityTimeoutScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerTaskActivityTimeoutScope, metrics.TaskLatency)
	defer sw.Stop()

	return t.processTimer(timerTask, func(msBuilder *mutableStateBuilder) error {
		tBuilder := t.historyService.getTimerBuilder(&workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(msBuilder.executionInfo.WorkflowID),
			RunId:      common.StringPtr(msBuilder.executionInfo.RunID),
		})

	ExpireActivityTimers:
		for _, td := range tBuilder.GetActivityTimers(msBuilder) {
			_, isRunning := msBuilder.GetActivityInfo(td.ActivityID)
			if !isRunning {
				//  We might have time out this activity already.
				continue ExpireActivityTimers
			}

			if isExpired := tBuilder.IsTimerExpired(td, timerTask.VisibilityTimestamp); isExpired {
				// active cluster will add an activity timeout event and schedule a decision if necessary
				// standby cluster should just call ack manager to retry this task
				// since we are stilling waiting for the activity timeout event to be replicated
				//
				// we do not need to notity new timer to base, since if there is no new event being replicated
				// checking again if the timer can be completed is meaningless
				t.timerQueueAckMgr.retryTimerTask(timerTask)
				return nil
			}
		}
		// if there is no user timer expired, then we are good
		t.timerQueueAckMgr.completeTimerTask(timerTask)
		return nil
	})
}

func (t *timerQueueStandbyProcessorImpl) processDecisionTimeout(timerTask *persistence.TimerTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TimerTaskDecisionTimeoutScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerTaskDecisionTimeoutScope, metrics.TaskLatency)
	defer sw.Stop()

	return t.processTimer(timerTask, func(msBuilder *mutableStateBuilder) error {
		_, isPending := msBuilder.GetPendingDecision(timerTask.EventID)
		if isPending {
			// active cluster will add an decision timeout event and schedule a decision
			// standby cluster should just call ack manager to retry this task
			// since we are stilling waiting for the decision timeout event / decision completion to be replicated
			//
			// we do not need to notity new timer to base, since if there is no new event being replicated
			// checking again if the timer can be completed is meaningless
			t.timerQueueAckMgr.retryTimerTask(timerTask)
			return nil
		}
		t.timerQueueAckMgr.completeTimerTask(timerTask)
		return nil
	})
}

func (t *timerQueueStandbyProcessorImpl) processWorkflowTimeout(timerTask *persistence.TimerTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TimerTaskWorkflowTimeoutScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerTaskWorkflowTimeoutScope, metrics.TaskLatency)
	defer sw.Stop()

	return t.processTimer(timerTask, func(msBuilder *mutableStateBuilder) error {
		// we do not need to notity new timer to base, since if there is no new event being replicated
		// checking again if the timer can be completed is meaningless
		t.timerQueueAckMgr.retryTimerTask(timerTask)
		return nil
	})
}

func (t *timerQueueStandbyProcessorImpl) processTimer(timerTask *persistence.TimerTaskInfo, fn func(*mutableStateBuilder) error) error {
	context, release, err := t.cache.getOrCreateWorkflowExecution(t.timerQueueProcessorBase.getDomainIDAndWorkflowExecution(timerTask))
	if err != nil {
		return err
	}
	defer release()

Process_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err := context.loadWorkflowExecution()
		if err != nil {
			return err
		}

		// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
		// some extreme cassandra failure cases.
		if timerTask.EventID >= msBuilder.GetNextEventID() {
			t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.StaleMutableStateCounter)
			t.logger.Debugf("processExpiredUserTimer: timer event ID: %v >= MS NextEventID: %v.", timerTask.EventID, msBuilder.GetNextEventID())
			// Reload workflow execution history
			context.clear()
			continue Process_Loop
		}

		if !msBuilder.isWorkflowExecutionRunning() {
			// workflow already finished, no need to process the timer
			t.timerQueueAckMgr.completeTimerTask(timerTask)
			return nil
		}

		return fn(msBuilder)
	}
	return ErrMaxAttemptsExceeded
}
