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
	m "github.com/uber/cadence/.gen/go/matching"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	timerQueueActiveProcessorImpl struct {
		shard                   ShardContext
		historyService          *historyEngineImpl
		cache                   *historyCache
		timerTaskFilter         timerTaskFilter
		logger                  bark.Logger
		metricsClient           metrics.Client
		currentClusterName      string
		matchingClient          matching.Client
		timerGate               LocalTimerGate
		timerQueueProcessorBase *timerQueueProcessorBase
		timerQueueAckMgr        timerQueueAckMgr
	}
)

func newTimerQueueActiveProcessor(shard ShardContext, historyService *historyEngineImpl, matchingClient matching.Client, logger bark.Logger) *timerQueueActiveProcessorImpl {
	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	timeNow := func() time.Time {
		return shard.GetCurrentTime(currentClusterName)
	}
	logger = logger.WithFields(bark.Fields{
		logging.TagWorkflowCluster: currentClusterName,
	})
	timerTaskFilter := func(timer *persistence.TimerTaskInfo) (bool, error) {
		return verifyActiveTask(shard, logger, timer.DomainID, timer)
	}

	timerGate := NewLocalTimerGate()
	// this will trigger a timer gate fire event immediately
	timerGate.Update(time.Time{})
	timerQueueAckMgr := newTimerQueueAckMgr(metrics.TimerActiveQueueProcessorScope, shard, historyService.metricsClient, currentClusterName, logger)
	retryableMatchingClient := matching.NewRetryableClient(matchingClient, common.CreateMatchingRetryPolicy(),
		common.IsMatchingServiceTransientError)
	processor := &timerQueueActiveProcessorImpl{
		shard:                   shard,
		historyService:          historyService,
		cache:                   historyService.historyCache,
		timerTaskFilter:         timerTaskFilter,
		logger:                  logger,
		matchingClient:          retryableMatchingClient,
		metricsClient:           historyService.metricsClient,
		currentClusterName:      currentClusterName,
		timerGate:               timerGate,
		timerQueueProcessorBase: newTimerQueueProcessorBase(metrics.TimerActiveQueueProcessorScope, shard, historyService, timerQueueAckMgr, timeNow, logger),
		timerQueueAckMgr:        timerQueueAckMgr,
	}
	processor.timerQueueProcessorBase.timerProcessor = processor
	return processor
}

func newTimerQueueFailoverProcessor(shard ShardContext, historyService *historyEngineImpl, domainID string, standbyClusterName string,
	minLevel time.Time, maxLevel time.Time, matchingClient matching.Client, logger bark.Logger) *timerQueueActiveProcessorImpl {
	clusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	timeNow := func() time.Time {
		// should use current cluster's time when doing domain failover
		return shard.GetCurrentTime(clusterName)
	}
	logger = logger.WithFields(bark.Fields{
		logging.TagWorkflowCluster: clusterName,
		logging.TagDomainID:        domainID,
		logging.TagFailover:        "from: " + standbyClusterName,
	})
	timerTaskFilter := func(timer *persistence.TimerTaskInfo) (bool, error) {
		return verifyFailoverActiveTask(logger, domainID, timer.DomainID, timer)
	}

	timerQueueAckMgr := newTimerQueueFailoverAckMgr(shard, historyService.metricsClient, standbyClusterName, minLevel,
		maxLevel, logger)
	retryableMatchingClient := matching.NewRetryableClient(matchingClient, common.CreateMatchingRetryPolicy(),
		common.IsMatchingServiceTransientError)
	processor := &timerQueueActiveProcessorImpl{
		shard:                   shard,
		historyService:          historyService,
		cache:                   historyService.historyCache,
		timerTaskFilter:         timerTaskFilter,
		logger:                  logger,
		metricsClient:           historyService.metricsClient,
		matchingClient:          retryableMatchingClient,
		timerGate:               NewLocalTimerGate(),
		timerQueueProcessorBase: newTimerQueueProcessorBase(metrics.TimerActiveQueueProcessorScope, shard, historyService, timerQueueAckMgr, timeNow, logger),
		timerQueueAckMgr:        timerQueueAckMgr,
	}
	processor.timerQueueProcessorBase.timerProcessor = processor
	return processor
}

func (t *timerQueueActiveProcessorImpl) Start() {
	t.timerQueueProcessorBase.Start()
}

func (t *timerQueueActiveProcessorImpl) Stop() {
	t.timerGate.Close()
	t.timerQueueProcessorBase.Stop()
}

func (t *timerQueueActiveProcessorImpl) getTimerFiredCount() uint64 {
	return t.timerQueueProcessorBase.getTimerFiredCount()
}

func (t *timerQueueActiveProcessorImpl) getTimerGate() TimerGate {
	return t.timerGate
}

// NotifyNewTimers - Notify the processor about the new active timer events arrival.
// This should be called each time new timer events arrives, otherwise timers maybe fired unexpected.
func (t *timerQueueActiveProcessorImpl) notifyNewTimers(timerTasks []persistence.Task) {
	t.timerQueueProcessorBase.notifyNewTimers(timerTasks)
}

func (t *timerQueueActiveProcessorImpl) process(timerTask *persistence.TimerTaskInfo) error {
	ok, err := t.timerTaskFilter(timerTask)
	if err != nil {
		return err
	} else if !ok {
		t.timerQueueAckMgr.completeTimerTask(timerTask)
		t.logger.Debugf("Discarding timer: (%v, %v), for WorkflowID: %v, RunID: %v, Type: %v, EventID: %v, Error: %v",
			timerTask.TaskID, timerTask.VisibilityTimestamp, timerTask.WorkflowID, timerTask.RunID, timerTask.TaskType, timerTask.EventID, err)
		return nil
	}

	scope := metrics.TimerActiveQueueProcessorScope
	switch timerTask.TaskType {
	case persistence.TaskTypeUserTimer:
		scope = metrics.TimerActiveTaskUserTimerScope
		err = t.processExpiredUserTimer(timerTask)

	case persistence.TaskTypeActivityTimeout:
		scope = metrics.TimerActiveTaskActivityTimeoutScope
		err = t.processActivityTimeout(timerTask)

	case persistence.TaskTypeDecisionTimeout:
		scope = metrics.TimerActiveTaskDecisionTimeoutScope
		err = t.processDecisionTimeout(timerTask)

	case persistence.TaskTypeWorkflowTimeout:
		scope = metrics.TimerActiveTaskWorkflowTimeoutScope
		err = t.processWorkflowTimeout(timerTask)

	case persistence.TaskTypeActivityRetryTimer:
		scope = metrics.TimerActiveTaskActivityRetryTimerScope
		err = t.processActivityRetryTimer(timerTask)

	case persistence.TaskTypeWorkflowRetryTimer:
		scope = metrics.TimerActiveTaskWorkflowRetryTimerScope
		err = t.processWorkflowRetryTimer(timerTask)

	case persistence.TaskTypeDeleteHistoryEvent:
		scope = metrics.TimerActiveTaskDeleteHistoryEvent
		err = t.timerQueueProcessorBase.processDeleteHistoryEvent(timerTask)
	}

	t.logger.Debugf("Processing timer: (%v, %v), for WorkflowID: %v, RunID: %v, Type: %v, EventID: %v, Error: %v",
		timerTask.TaskID, timerTask.VisibilityTimestamp, timerTask.WorkflowID, timerTask.RunID, timerTask.TaskType, timerTask.EventID, err)

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

func (t *timerQueueActiveProcessorImpl) processExpiredUserTimer(task *persistence.TimerTaskInfo) (retError error) {
	t.metricsClient.IncCounter(metrics.TimerActiveTaskUserTimerScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerActiveTaskUserTimerScope, metrics.TaskLatency)
	defer sw.Stop()

	context, release, err0 := t.cache.getOrCreateWorkflowExecution(t.timerQueueProcessorBase.getDomainIDAndWorkflowExecution(task))
	if err0 != nil {
		return err0
	}
	defer func() { release(retError) }()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
		if err != nil {
			return err
		} else if msBuilder == nil || !msBuilder.IsWorkflowExecutionRunning() {
			return nil
		}
		tBuilder := t.historyService.getTimerBuilder(&context.workflowExecution)

		var timerTasks []persistence.Task
		scheduleNewDecision := false

	ExpireUserTimers:
		for _, td := range tBuilder.GetUserTimers(msBuilder) {
			hasTimer, ti := tBuilder.GetUserTimer(td.TimerID)
			if !hasTimer {
				t.logger.Debugf("Failed to find in memory user timer: %s", td.TimerID)
				return fmt.Errorf("Failed to find in memory user timer: %s", td.TimerID)
			}

			if isExpired := tBuilder.IsTimerExpired(td, task.VisibilityTimestamp); isExpired {
				// Add TimerFired event to history.
				if msBuilder.AddTimerFiredEvent(ti.StartedID, ti.TimerID) == nil {
					return errFailedToAddTimerFiredEvent
				}

				scheduleNewDecision = !msBuilder.HasPendingDecisionTask()
			} else {
				// See if we have next timer in list to be created.
				if !td.TaskCreated {
					nextTask := tBuilder.createNewTask(td)
					timerTasks = []persistence.Task{nextTask}

					// Update the task ID tracking the corresponding timer task.
					ti.TaskID = TimerTaskStatusCreated
					msBuilder.UpdateUserTimer(ti.TimerID, ti)
					defer t.notifyNewTimers(timerTasks)
				}

				// Done!
				break ExpireUserTimers
			}
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		err = t.updateWorkflowExecution(context, msBuilder, scheduleNewDecision, false, timerTasks, nil)
		if err != nil {
			if err == ErrConflict {
				continue Update_History_Loop
			}
		}
		return err
	}
	return ErrMaxAttemptsExceeded
}

func (t *timerQueueActiveProcessorImpl) processActivityTimeout(timerTask *persistence.TimerTaskInfo) (retError error) {
	t.metricsClient.IncCounter(metrics.TimerActiveTaskActivityTimeoutScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerActiveTaskActivityTimeoutScope, metrics.TaskLatency)
	defer sw.Stop()

	context, release, err0 := t.cache.getOrCreateWorkflowExecution(t.timerQueueProcessorBase.getDomainIDAndWorkflowExecution(timerTask))
	if err0 != nil {
		return err0
	}
	defer func() { release(retError) }()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err := loadMutableStateForTimerTask(context, timerTask, t.metricsClient, t.logger)
		if err != nil {
			return err
		} else if msBuilder == nil || !msBuilder.IsWorkflowExecutionRunning() {
			return nil
		}
		tBuilder := t.historyService.getTimerBuilder(&context.workflowExecution)

		ai, running := msBuilder.GetActivityInfo(timerTask.EventID)
		if running {
			// If current one is HB task then we may need to create the next heartbeat timer.  Clear the create flag for this
			// heartbeat timer so we can create it again if needed.
			// NOTE: When record activity HB comes in we only update last heartbeat timestamp, this is the place
			// where we create next timer task based on that new updated timestamp.
			isHeartBeatTask := timerTask.TimeoutType == int(workflow.TimeoutTypeHeartbeat)
			if isHeartBeatTask {
				ai.TimerTaskStatus = ai.TimerTaskStatus &^ TimerTaskStatusCreatedHeartbeat
				msBuilder.UpdateActivity(ai)
			}

			// No need to check for attempt on the timer task.  ExpireActivityTimer logic below already checks if the
			// activity should be timedout and it will not let the timer expire for earlier attempts.  And creation of
			// duplicate timer task is protected by Created flag.
		}

		var timerTasks []persistence.Task
		updateHistory := false
		createNewTimer := false

	ExpireActivityTimers:
		for _, td := range tBuilder.GetActivityTimers(msBuilder) {
			ai, isRunning := msBuilder.GetActivityInfo(td.ActivityID)
			if !isRunning {
				//  We might have time out this activity already.
				continue ExpireActivityTimers
			}

			if isExpired := tBuilder.IsTimerExpired(td, timerTask.VisibilityTimestamp); isExpired {
				timeoutType := td.TimeoutType
				t.logger.Debugf("Activity TimeoutType: %v, scheduledID: %v, startedId: %v. \n",
					timeoutType, ai.ScheduleID, ai.StartedID)

				if td.Attempt < ai.Attempt && timeoutType != workflow.TimeoutTypeScheduleToClose {
					// retry could update ai.Attempt, and we should ignore further timeouts for previous attempt
					continue
				}

				if timeoutType != workflow.TimeoutTypeScheduleToStart {
					// ScheduleToStart (queue timeout) is not retriable. Instead of retry, customer should set larger
					// ScheduleToStart timeout.
					retryTask := msBuilder.CreateRetryTimer(ai, getTimeoutErrorReason(timeoutType))
					if retryTask != nil {
						timerTasks = append(timerTasks, retryTask)
						createNewTimer = true

						t.logger.Debugf("Ignore ActivityTimeout (%v) as retry is needed. New attempt: %v, retry backoff duration: %v.",
							timeoutType, ai.Attempt, retryTask.(*persistence.ActivityRetryTimerTask).VisibilityTimestamp.Sub(time.Now()))

						continue
					}
				}

				switch timeoutType {
				case workflow.TimeoutTypeScheduleToClose:
					{
						t.metricsClient.IncCounter(metrics.TimerActiveTaskActivityTimeoutScope, metrics.ScheduleToCloseTimeoutCounter)
						if msBuilder.AddActivityTaskTimedOutEvent(ai.ScheduleID, ai.StartedID, timeoutType, nil) == nil {
							return errFailedToAddTimeoutEvent
						}
						updateHistory = true
					}

				case workflow.TimeoutTypeStartToClose:
					{
						t.metricsClient.IncCounter(metrics.TimerActiveTaskActivityTimeoutScope, metrics.StartToCloseTimeoutCounter)
						if ai.StartedID != common.EmptyEventID {
							if msBuilder.AddActivityTaskTimedOutEvent(ai.ScheduleID, ai.StartedID, timeoutType, nil) == nil {
								return errFailedToAddTimeoutEvent
							}
							updateHistory = true
						}
					}

				case workflow.TimeoutTypeHeartbeat:
					{
						t.metricsClient.IncCounter(metrics.TimerActiveTaskActivityTimeoutScope, metrics.HeartbeatTimeoutCounter)
						if msBuilder.AddActivityTaskTimedOutEvent(ai.ScheduleID, ai.StartedID, timeoutType, ai.Details) == nil {
							return errFailedToAddTimeoutEvent
						}
						updateHistory = true
					}

				case workflow.TimeoutTypeScheduleToStart:
					{
						t.metricsClient.IncCounter(metrics.TimerActiveTaskActivityTimeoutScope, metrics.ScheduleToStartTimeoutCounter)
						if ai.StartedID == common.EmptyEventID {
							if msBuilder.AddActivityTaskTimedOutEvent(ai.ScheduleID, ai.StartedID, timeoutType, nil) == nil {
								return errFailedToAddTimeoutEvent
							}
							updateHistory = true
						}
					}
				}
			} else {
				// See if we have next timer in list to be created.
				// Create next timer task if we don't have one
				if !td.TaskCreated {
					nextTask := tBuilder.createNewTask(td)
					timerTasks = append(timerTasks, nextTask)
					at := nextTask.(*persistence.ActivityTimeoutTask)

					ai.TimerTaskStatus = ai.TimerTaskStatus | getActivityTimerStatus(workflow.TimeoutType(at.TimeoutType))
					msBuilder.UpdateActivity(ai)
					createNewTimer = true

					t.logger.Debugf("%s: Adding Activity Timeout: with timeout: %v sec, ExpiryTime: %s, TimeoutType: %v, EventID: %v",
						time.Now(), td.TimeoutSec, at.VisibilityTimestamp, td.TimeoutType.String(), at.EventID)
				}

				// Done!
				break ExpireActivityTimers
			}
		}

		if updateHistory || createNewTimer {
			// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
			// the history and try the operation again.
			scheduleNewDecision := updateHistory && !msBuilder.HasPendingDecisionTask()
			err := t.updateWorkflowExecution(context, msBuilder, scheduleNewDecision, false, timerTasks, nil)
			if err != nil {
				if err == ErrConflict {
					continue Update_History_Loop
				}
			}

			t.notifyNewTimers(timerTasks)
			return nil
		}

		return nil
	}
	return ErrMaxAttemptsExceeded
}

func (t *timerQueueActiveProcessorImpl) processDecisionTimeout(task *persistence.TimerTaskInfo) (retError error) {
	t.metricsClient.IncCounter(metrics.TimerActiveTaskDecisionTimeoutScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerActiveTaskDecisionTimeoutScope, metrics.TaskLatency)
	defer sw.Stop()

	context, release, err0 := t.cache.getOrCreateWorkflowExecution(t.timerQueueProcessorBase.getDomainIDAndWorkflowExecution(task))
	if err0 != nil {
		return err0
	}
	defer func() { release(retError) }()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
		if err != nil {
			return err
		} else if msBuilder == nil || !msBuilder.IsWorkflowExecutionRunning() {
			return nil
		}

		scheduleID := task.EventID
		di, found := msBuilder.GetPendingDecision(scheduleID)
		if !found {
			logging.LogDuplicateTransferTaskEvent(t.logger, persistence.TaskTypeDecisionTimeout, task.TaskID, scheduleID)
			return nil
		}
		ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, di.Version, task.Version, task)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		scheduleNewDecision := false
		switch task.TimeoutType {
		case int(workflow.TimeoutTypeStartToClose):
			t.metricsClient.IncCounter(metrics.TimerActiveTaskDecisionTimeoutScope, metrics.StartToCloseTimeoutCounter)
			if di.Attempt == task.ScheduleAttempt {
				// Add a decision task timeout event.
				msBuilder.AddDecisionTaskTimedOutEvent(scheduleID, di.StartedID)
				scheduleNewDecision = true
			}
		case int(workflow.TimeoutTypeScheduleToStart):
			t.metricsClient.IncCounter(metrics.TimerActiveTaskDecisionTimeoutScope, metrics.ScheduleToStartTimeoutCounter)
			// decision schedule to start timeout only apply to sticky decision
			// check if scheduled decision still pending and not started yet
			if di.Attempt == task.ScheduleAttempt && di.StartedID == common.EmptyEventID && msBuilder.IsStickyTaskListEnabled() {
				timeoutEvent := msBuilder.AddDecisionTaskScheduleToStartTimeoutEvent(scheduleID)
				if timeoutEvent == nil {
					// Unable to add DecisionTaskTimedout event to history
					return &workflow.InternalServiceError{Message: "Unable to add DecisionTaskScheduleToStartTimeout event to history."}
				}

				// reschedule decision, which will be on its original task list
				scheduleNewDecision = true
			}
		}

		if scheduleNewDecision {
			// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
			// the history and try the operation again.
			err := t.updateWorkflowExecution(context, msBuilder, scheduleNewDecision, false, nil, nil)
			if err != nil {
				if err == ErrConflict {
					continue Update_History_Loop
				}
			}
			return err
		}

		return nil

	}
	return ErrMaxAttemptsExceeded
}

func (t *timerQueueActiveProcessorImpl) processWorkflowRetryTimer(task *persistence.TimerTaskInfo) (retError error) {
	t.metricsClient.IncCounter(metrics.TimerActiveTaskWorkflowRetryTimerScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerActiveTaskWorkflowRetryTimerScope, metrics.TaskLatency)
	defer sw.Stop()

	context, release, err0 := t.cache.getOrCreateWorkflowExecution(t.timerQueueProcessorBase.getDomainIDAndWorkflowExecution(task))
	if err0 != nil {
		return err0
	}
	defer func() { release(retError) }()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
		if err != nil {
			return err
		} else if msBuilder == nil || !msBuilder.IsWorkflowExecutionRunning() {
			return nil
		}

		if msBuilder.HasPendingDecisionTask() {
			// already has decision task
			return nil
		}

		// schedule first decision task
		err = t.updateWorkflowExecution(context, msBuilder, true, false, nil, nil)
		if err != nil {
			if err == ErrConflict {
				continue Update_History_Loop
			}
		}
		return err
	}

	return ErrMaxAttemptsExceeded
}

func (t *timerQueueActiveProcessorImpl) processActivityRetryTimer(task *persistence.TimerTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TimerActiveTaskActivityRetryTimerScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerActiveTaskActivityRetryTimerScope, metrics.TaskLatency)
	defer sw.Stop()

	processFn := func() error {
		context, release, err0 := t.cache.getOrCreateWorkflowExecution(t.timerQueueProcessorBase.getDomainIDAndWorkflowExecution(task))
		defer release(nil)
		if err0 != nil {
			return err0
		}
		msBuilder, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
		if err != nil {
			return err
		} else if msBuilder == nil || !msBuilder.IsWorkflowExecutionRunning() {
			return nil
		}

		// generate activity task
		scheduledID := task.EventID
		ai, running := msBuilder.GetActivityInfo(scheduledID)
		if !running || task.ScheduleAttempt < int64(ai.Attempt) {
			return nil
		}
		ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, ai.Version, task.Version, task)
		if err != nil {
			return err
		} else if !ok {
			return nil
		}

		domainID := task.DomainID
		targetDomainID := domainID
		scheduledEvent, _ := msBuilder.GetActivityScheduledEvent(scheduledID)
		if scheduledEvent.ActivityTaskScheduledEventAttributes.Domain != nil {
			domainEntry, err := t.shard.GetDomainCache().GetDomain(scheduledEvent.ActivityTaskScheduledEventAttributes.GetDomain())
			if err != nil {
				return &workflow.InternalServiceError{Message: "Unable to re-schedule activity across domain."}
			}
			targetDomainID = domainEntry.GetInfo().ID
		}

		execution := workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(task.WorkflowID),
			RunId:      common.StringPtr(task.RunID)}
		taskList := &workflow.TaskList{
			Name: &ai.TaskList,
		}
		scheduleToStartTimeout := ai.ScheduleToStartTimeout

		release(nil) // release earlier as we don't need the lock anymore
		err = t.matchingClient.AddActivityTask(nil, &m.AddActivityTaskRequest{
			DomainUUID:                    common.StringPtr(targetDomainID),
			SourceDomainUUID:              common.StringPtr(domainID),
			Execution:                     &execution,
			TaskList:                      taskList,
			ScheduleId:                    &scheduledID,
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(scheduleToStartTimeout),
		})

		t.logger.Debugf("Adding ActivityTask for retry, WorkflowID: %v, RunID: %v, ScheduledID: %v, TaskList: %v, Attempt: %v, Err: %v",
			task.WorkflowID, task.RunID, scheduledID, taskList.GetName(), task.ScheduleAttempt, err)

		return err
	}

	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		if err := processFn(); err == nil {
			return nil
		}
	}

	return ErrMaxAttemptsExceeded
}

func (t *timerQueueActiveProcessorImpl) processWorkflowTimeout(task *persistence.TimerTaskInfo) (retError error) {
	t.metricsClient.IncCounter(metrics.TimerActiveTaskWorkflowTimeoutScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerActiveTaskWorkflowTimeoutScope, metrics.TaskLatency)
	defer sw.Stop()

	domainID, workflowExecution := t.timerQueueProcessorBase.getDomainIDAndWorkflowExecution(task)
	context, release, err0 := t.cache.getOrCreateWorkflowExecution(domainID, workflowExecution)
	if err0 != nil {
		return err0
	}
	defer func() { release(retError) }()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
		if err != nil {
			return err
		} else if msBuilder == nil || !msBuilder.IsWorkflowExecutionRunning() {
			return nil
		}

		// do version check for global domain task
		if msBuilder.GetReplicationState() != nil {
			ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, msBuilder.GetReplicationState().StartVersion, task.Version, task)
			if err != nil {
				return err
			} else if !ok {
				return nil
			}
		}

		retryBackoffInterval := msBuilder.GetRetryBackoffDuration(getTimeoutErrorReason(workflow.TimeoutTypeStartToClose))
		if retryBackoffInterval == common.NoRetryBackoff {
			if e := msBuilder.AddTimeoutWorkflowEvent(); e == nil {
				// If we failed to add the event that means the workflow is already completed.
				// we drop this timeout event.
				return nil
			}

			// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
			// the history and try the operation again.
			err = t.updateWorkflowExecution(context, msBuilder, false, true, nil, nil)
			if err != nil {
				if err == ErrConflict {
					continue Update_History_Loop
				}
			}
			return err
		}

		// workflow timeout, but a retry is needed, so we do continue as new to retry
		startEvent, err := getWorkflowStartedEvent(t.historyService.historyMgr, domainID, workflowExecution.GetWorkflowId(), workflowExecution.GetRunId())
		if err != nil {
			return err
		}

		startAttributes := startEvent.WorkflowExecutionStartedEventAttributes
		continueAsnewAttributes := &workflow.ContinueAsNewWorkflowExecutionDecisionAttributes{
			WorkflowType: startAttributes.WorkflowType,
			TaskList:     startAttributes.TaskList,
			RetryPolicy:  startAttributes.RetryPolicy,
			Input:        startAttributes.Input,
			ExecutionStartToCloseTimeoutSeconds: startAttributes.ExecutionStartToCloseTimeoutSeconds,
			TaskStartToCloseTimeoutSeconds:      startAttributes.TaskStartToCloseTimeoutSeconds,
			BackoffStartIntervalInSeconds:       common.Int32Ptr(int32(retryBackoffInterval.Seconds())),
		}
		domainEntry, err := getActiveDomainEntryFromShard(t.shard, &domainID)
		if err != nil {
			return err
		}
		_, continueAsNewBuilder, err := msBuilder.AddContinueAsNewEvent(common.EmptyEventID, domainEntry, startAttributes.GetParentWorkflowDomain(), continueAsnewAttributes)
		if err != nil {
			return err
		}

		tBuilder := t.historyService.getTimerBuilder(&context.workflowExecution)
		var transferTasks, timerTasks []persistence.Task
		tranT, timerT, err := getDeleteWorkflowTasksFromShard(t.shard, domainID, tBuilder)
		if err != nil {
			return err
		}
		transferTasks = append(transferTasks, tranT)
		timerTasks = append(timerTasks, timerT)

		// Generate a transaction ID for appending events to history
		transactionID, err3 := t.shard.GetNextTransferTaskID()
		if err3 != nil {
			return err3
		}

		timersToNotify := append(timerTasks, msBuilder.GetContinueAsNew().TimerTasks...)
		err = context.continueAsNewWorkflowExecution(nil, continueAsNewBuilder, transferTasks, timerTasks, transactionID)
		t.notifyNewTimers(timersToNotify)

		if err != nil {
			if err == ErrConflict {
				continue Update_History_Loop
			}
		}
		return err
	}
	return ErrMaxAttemptsExceeded
}

func (t *timerQueueActiveProcessorImpl) updateWorkflowExecution(
	context *workflowExecutionContext,
	msBuilder mutableState,
	scheduleNewDecision bool,
	createDeletionTask bool,
	timerTasks []persistence.Task,
	clearTimerTask persistence.Task,
) error {
	executionInfo := msBuilder.GetExecutionInfo()
	var transferTasks []persistence.Task
	if scheduleNewDecision {
		// Schedule a new decision.
		di := msBuilder.AddDecisionTaskScheduledEvent()
		transferTasks = []persistence.Task{&persistence.DecisionTask{
			DomainID:   executionInfo.DomainID,
			TaskList:   di.TaskList,
			ScheduleID: di.ScheduleID,
		}}
		if msBuilder.IsStickyTaskListEnabled() {
			tBuilder := t.historyService.getTimerBuilder(&context.workflowExecution)
			stickyTaskTimeoutTimer := tBuilder.AddScheduleToStartDecisionTimoutTask(di.ScheduleID, di.Attempt,
				executionInfo.StickyScheduleToStartTimeout)
			timerTasks = append(timerTasks, stickyTaskTimeoutTimer)
		}
	}

	if createDeletionTask {
		tBuilder := t.historyService.getTimerBuilder(&context.workflowExecution)
		tranT, timerT, err := t.historyService.getDeleteWorkflowTasks(executionInfo.DomainID, tBuilder)
		if err != nil {
			return nil
		}
		transferTasks = append(transferTasks, tranT)
		timerTasks = append(timerTasks, timerT)
	}

	// Generate a transaction ID for appending events to history
	transactionID, err1 := t.historyService.shard.GetNextTransferTaskID()
	if err1 != nil {
		return err1
	}

	err := context.updateWorkflowExecutionWithDeleteTask(transferTasks, timerTasks, clearTimerTask, transactionID)
	if err != nil {
		if isShardOwnershiptLostError(err) {
			// Shard is stolen.  Stop timer processing to reduce duplicates
			t.timerQueueProcessorBase.Stop()
		}
	}
	t.notifyNewTimers(timerTasks)
	return err
}
