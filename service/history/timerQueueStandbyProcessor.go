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
		timerTaskFilter         timerTaskFilter
		logger                  bark.Logger
		metricsClient           metrics.Client
		clusterName             string
		timerGate               RemoteTimerGate
		timerQueueProcessorBase *timerQueueProcessorBase
		timerQueueAckMgr        timerQueueAckMgr
	}
)

func newTimerQueueStandbyProcessor(shard ShardContext, historyService *historyEngineImpl, clusterName string, logger bark.Logger) *timerQueueStandbyProcessorImpl {
	timeNow := func() time.Time {
		return shard.GetCurrentTime(clusterName)
	}
	logger = logger.WithFields(bark.Fields{
		logging.TagWorkflowCluster: clusterName,
	})
	timerTaskFilter := func(timer *persistence.TimerTaskInfo) (bool, error) {
		domainEntry, err := shard.GetDomainCache().GetDomainByID(timer.DomainID)
		if err != nil {
			return false, err
		}
		if !domainEntry.IsGlobalDomain() {
			// non global domain, timer task does not belong here
			return false, nil
		} else if domainEntry.IsGlobalDomain() && domainEntry.GetReplicationConfig().ActiveClusterName != clusterName {
			// timer task does not belong here
			return false, nil
		}
		return true, nil
	}

	timerGate := NewRemoteTimerGate()
	// this will trigger a timer gate fire event immediately
	timerGate.Update(time.Time{})
	timerGate.SetCurrentTime(shard.GetCurrentTime(clusterName))
	timerQueueAckMgr := newTimerQueueAckMgr(shard, historyService.metricsClient, clusterName, logger)
	processor := &timerQueueStandbyProcessorImpl{
		shard:                   shard,
		historyService:          historyService,
		cache:                   historyService.historyCache,
		timerTaskFilter:         timerTaskFilter,
		logger:                  logger,
		metricsClient:           historyService.metricsClient,
		clusterName:             clusterName,
		timerGate:               NewRemoteTimerGate(),
		timerQueueProcessorBase: newTimerQueueProcessorBase(shard, historyService, timerQueueAckMgr, timeNow, logger),
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
}

func (t *timerQueueStandbyProcessorImpl) retryTasks() {
	t.timerQueueProcessorBase.retryTasks()
}

// NotifyNewTimers - Notify the processor about the new standby timer events arrival.
// This should be called each time new timer events arrives, otherwise timers maybe fired unexpected.
func (t *timerQueueStandbyProcessorImpl) notifyNewTimers(timerTasks []persistence.Task) {
	t.timerQueueProcessorBase.notifyNewTimers(timerTasks, metrics.NewStandbyTimerCounter)
}

func (t *timerQueueStandbyProcessorImpl) process(timerTask *persistence.TimerTaskInfo) error {
	ok, err := t.timerTaskFilter(timerTask)
	if err != nil {
		return err
	} else if !ok {
		t.timerQueueAckMgr.completeTimerTask(timerTask)
		return nil
	}

	taskID := TimerSequenceID{VisibilityTimestamp: timerTask.VisibilityTimestamp, TaskID: timerTask.TaskID}
	t.logger.Debugf("Processing timer: (%s), for WorkflowID: %v, RunID: %v, Type: %v, TimeoutType: %v, EventID: %v",
		taskID, timerTask.WorkflowID, timerTask.RunID, t.timerQueueProcessorBase.getTimerTaskType(timerTask.TaskType),
		workflow.TimeoutType(timerTask.TimeoutType).String(), timerTask.EventID)

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

	case persistence.TaskTypeRetryTimer:
		scope = metrics.TimerTaskRetryTimerScope
		err = nil // retry backoff timer should not get created on passive cluster

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
	} else {
		t.timerQueueAckMgr.completeTimerTask(timerTask)
	}

	return err
}

func (t *timerQueueStandbyProcessorImpl) processExpiredUserTimer(timerTask *persistence.TimerTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TimerTaskUserTimerScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerTaskUserTimerScope, metrics.TaskLatency)
	defer sw.Stop()

	return t.processTimer(timerTask, func(msBuilder mutableState) error {
		executionInfo := msBuilder.GetExecutionInfo()
		tBuilder := t.historyService.getTimerBuilder(&workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(executionInfo.WorkflowID),
			RunId:      common.StringPtr(executionInfo.RunID),
		})

	ExpireUserTimers:
		for _, td := range tBuilder.GetUserTimers(msBuilder) {
			hasTimer, _ := tBuilder.GetUserTimer(td.TimerID)
			if !hasTimer {
				t.logger.Debugf("Failed to find in memory user timer: %s", td.TimerID)
				return fmt.Errorf("Failed to find in memory user timer: %s", td.TimerID)
			}
			if !td.TaskCreated {
				break ExpireUserTimers
			}

			if isExpired := tBuilder.IsTimerExpired(td, timerTask.VisibilityTimestamp); isExpired {
				// active cluster will add an timer fired event and schedule a decision if necessary
				// standby cluster should just call ack manager to retry this task
				// since we are stilling waiting for the fired event to be replicated
				//
				// we do not need to notity new timer to base, since if there is no new event being replicated
				// checking again if the timer can be completed is meaningless
				return ErrTaskRetry
			}
			// since the user timer are already sorted, so if there is one timer which will not expired
			// all user timer after this timer will not expired
			break ExpireUserTimers
		}
		// if there is no user timer expired, then we are good
		return nil
	})
}

func (t *timerQueueStandbyProcessorImpl) processActivityTimeout(timerTask *persistence.TimerTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TimerTaskActivityTimeoutScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerTaskActivityTimeoutScope, metrics.TaskLatency)
	defer sw.Stop()

	return t.processTimer(timerTask, func(msBuilder mutableState) error {
		executionInfo := msBuilder.GetExecutionInfo()
		tBuilder := t.historyService.getTimerBuilder(&workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(executionInfo.WorkflowID),
			RunId:      common.StringPtr(executionInfo.RunID),
		})

	ExpireActivityTimers:
		for _, td := range tBuilder.GetActivityTimers(msBuilder) {
			_, isRunning := msBuilder.GetActivityInfo(td.ActivityID)
			if !isRunning {
				//  We might have time out this activity already.
				continue ExpireActivityTimers
			}
			if !td.TaskCreated {
				break ExpireActivityTimers
			}

			if isExpired := tBuilder.IsTimerExpired(td, timerTask.VisibilityTimestamp); isExpired {
				// active cluster will add an activity timeout event and schedule a decision if necessary
				// standby cluster should just call ack manager to retry this task
				// since we are stilling waiting for the activity timeout event to be replicated
				//
				// we do not need to notity new timer to base, since if there is no new event being replicated
				// checking again if the timer can be completed is meaningless
				return ErrTaskRetry
			}
			// since the activity timer are already sorted, so if there is one timer which will not expired
			// all activity timer after this timer will not expired
			break ExpireActivityTimers
		}
		// if there is no user timer expired, then we are good
		return nil
	})
}

func (t *timerQueueStandbyProcessorImpl) processDecisionTimeout(timerTask *persistence.TimerTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TimerTaskDecisionTimeoutScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerTaskDecisionTimeoutScope, metrics.TaskLatency)
	defer sw.Stop()

	return t.processTimer(timerTask, func(msBuilder mutableState) error {
		di, isPending := msBuilder.GetPendingDecision(timerTask.EventID)

		if !isPending {
			return nil
		}

		ok, err := verifyTimerTaskVersion(t.shard, timerTask.DomainID, di.Version, timerTask)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		// active cluster will add an decision timeout event and schedule a decision
		// standby cluster should just call ack manager to retry this task
		// since we are stilling waiting for the decision timeout event / decision completion to be replicated
		//
		// we do not need to notity new timer to base, since if there is no new event being replicated
		// checking again if the timer can be completed is meaningless
		return ErrTaskRetry
	})
}

func (t *timerQueueStandbyProcessorImpl) processWorkflowTimeout(timerTask *persistence.TimerTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TimerTaskWorkflowTimeoutScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerTaskWorkflowTimeoutScope, metrics.TaskLatency)
	defer sw.Stop()

	return t.processTimer(timerTask, func(msBuilder mutableState) error {
		// we do not need to notity new timer to base, since if there is no new event being replicated
		// checking again if the timer can be completed is meaningless

		ok, err := verifyTimerTaskVersion(t.shard, timerTask.DomainID, msBuilder.GetStartVersion(), timerTask)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		return ErrTaskRetry
	})
}

func (t *timerQueueStandbyProcessorImpl) processTimer(timerTask *persistence.TimerTaskInfo, fn func(mutableState) error) (retError error) {
	context, release, err := t.cache.getOrCreateWorkflowExecution(t.timerQueueProcessorBase.getDomainIDAndWorkflowExecution(timerTask))
	if err != nil {
		return err
	}
	defer func() {
		if retError == ErrTaskRetry {
			release(nil)
		} else {
			release(retError)
		}
	}()

	msBuilder, err := loadMutableStateForTimerTask(context, timerTask, t.metricsClient, t.logger)
	if err != nil {
		return err
	} else if msBuilder == nil {
		return nil
	}

	if !msBuilder.IsWorkflowExecutionRunning() {
		// workflow already finished, no need to process the timer
		return nil
	}

	return fn(msBuilder)

}
