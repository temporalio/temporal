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
	"fmt"
	"math"
	"sort"
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
	timerQueueProcessorImpl struct {
		historyService   *historyEngineImpl
		cache            *historyCache
		executionManager persistence.ExecutionManager
		isStarted        int32
		isStopped        int32
		shutdownWG       sync.WaitGroup
		shutdownCh       chan struct{}
		newTimerCh       chan struct{}
		config           *Config
		logger           bark.Logger
		metricsClient    metrics.Client
		timerFiredCount  uint64
		ackMgr           *timerAckMgr

		newTimeLock sync.Mutex
		newTime     time.Time
	}

	timerAckMgr struct {
		sync.RWMutex
		processor        *timerQueueProcessorImpl
		shard            ShardContext
		executionMgr     persistence.ExecutionManager
		logger           bark.Logger
		outstandingTasks map[SequenceID]bool
		readLevel        SequenceID
		ackLevel         time.Time
		metricsClient    metrics.Client
		lastUpdated      time.Time
		config           *Config
	}
)

func newTimerQueueProcessor(shard ShardContext, historyService *historyEngineImpl, executionManager persistence.ExecutionManager,
	logger bark.Logger) timerQueueProcessor {
	l := logger.WithFields(bark.Fields{
		logging.TagWorkflowComponent: logging.TagValueTimerQueueComponent,
	})
	tp := &timerQueueProcessorImpl{
		historyService:   historyService,
		cache:            historyService.historyCache,
		executionManager: executionManager,
		shutdownCh:       make(chan struct{}),
		newTimerCh:       make(chan struct{}, 1),
		config:           shard.GetConfig(),
		logger:           l,
		metricsClient:    historyService.metricsClient,
	}
	tp.ackMgr = newTimerAckMgr(tp, shard, executionManager, l)
	return tp
}

func (t *timerQueueProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&t.isStarted, 0, 1) {
		return
	}

	t.shutdownWG.Add(1)
	go t.processorPump(t.config.ProcessTimerTaskWorkerCount)

	t.logger.Info("Timer queue processor started.")
}

func (t *timerQueueProcessorImpl) Stop() {
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

// NotifyNewTimer - Notify the processor about the new timer arrival.
// This should be called each time new timer created, otherwise timer maybe fired unexpected.
func (t *timerQueueProcessorImpl) NotifyNewTimers(timerTasks []persistence.Task) {
	if len(timerTasks) == 0 {
		return
	}
	t.metricsClient.AddCounter(metrics.TimerQueueProcessorScope, metrics.NewTimerCounter, int64(len(timerTasks)))

	newTime := persistence.GetVisibilityTSFrom(timerTasks[0])
	for _, task := range timerTasks {
		ts := persistence.GetVisibilityTSFrom(task)
		if ts.Before(newTime) {
			newTime = ts
		}

		switch task.GetType() {
		case persistence.TaskTypeDecisionTimeout:
			t.metricsClient.IncCounter(metrics.TimerTaskDecisionTimeoutScope, metrics.NewTimerCounter)
		case persistence.TaskTypeActivityTimeout:
			t.metricsClient.IncCounter(metrics.TimerTaskActivityTimeoutScope, metrics.NewTimerCounter)
		case persistence.TaskTypeUserTimer:
			t.metricsClient.IncCounter(metrics.TimerTaskUserTimerScope, metrics.NewTimerCounter)
		case persistence.TaskTypeWorkflowTimeout:
			t.metricsClient.IncCounter(metrics.TimerTaskWorkflowTimeoutScope, metrics.NewTimerCounter)
		case persistence.TaskTypeDeleteHistoryEvent:
			t.metricsClient.IncCounter(metrics.TimerTaskDeleteHistoryEvent, metrics.NewTimerCounter)
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

func (t *timerQueueProcessorImpl) processorPump(taskWorkerCount int) {
	defer t.shutdownWG.Done()

	// Workers to process timer tasks that are expired.
	tasksCh := make(chan *persistence.TimerTaskInfo, 10*t.config.TimerTaskBatchSize)
	var workerWG sync.WaitGroup
	for i := 0; i < taskWorkerCount; i++ {
		workerWG.Add(1)
		go t.processTaskWorker(tasksCh, &workerWG)
	}

RetryProcessor:
	for {
		select {
		case <-t.shutdownCh:
			t.logger.Info("Timer queue processor pump shutting down.")
			close(tasksCh)
			if success := common.AwaitWaitGroup(&workerWG, 10*time.Second); !success {
				t.logger.Warn("Timer queue processor timed out on worker shutdown.")
			}
			break RetryProcessor
		default:
			err := t.internalProcessor(tasksCh)
			if err != nil {
				t.logger.Error("processor pump failed with error: ", err)
			}
		}
	}
	t.logger.Info("Timer processor exiting.")
}

func (t *timerQueueProcessorImpl) internalProcessor(tasksCh chan<- *persistence.TimerTaskInfo) error {
	timerGate := NewTimerGate()
	defer timerGate.Close()

	updateAckChan := time.NewTicker(t.config.TimerProcessorUpdateAckInterval).C
	var nextKeyTask *persistence.TimerTaskInfo

continueProcessor:
	for {
		if nextKeyTask == nil || timerGate.FireAfter(time.Now()) {
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

			case <-timerGate.FireChan():
				// Timer Fired.

			case <-updateAckChan:
				t.ackMgr.updateAckLevel()
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

				t.logger.Debugf("%v: Next key after woke up by timer: %v", time.Now().UTC(), newTime.UTC())

				if timerGate.FireAfter(time.Now()) {
					continue continueProcessor
				}
			}
		}

		// Either we have new timer (or) we are gated on timer to query for it.
	ProcessPendingTimers:
		for {
			// Get next set of timer tasks.
			timerTasks, lookAheadTask, moreTasks, err := t.getTasksAndNextKey()
			if err != nil {
				return err
			}

			for _, task := range timerTasks {
				// We have a timer to fire.
				tasksCh <- task
			}

			if !moreTasks {
				// We have processed all the tasks.
				nextKeyTask = lookAheadTask
				break ProcessPendingTimers
			}
		}

		if nextKeyTask != nil {
			nextKey := SequenceID{VisibilityTimestamp: nextKeyTask.VisibilityTimestamp, TaskID: nextKeyTask.TaskID}
			t.logger.Debugf("%s: GetNextKey: %s", time.Now().UTC(), nextKey)

			timerGate.Update(nextKey.VisibilityTimestamp)
		}
	}
}

func (t *timerQueueProcessorImpl) isProcessNow(expiryTime time.Time) bool {
	return !expiryTime.IsZero() && expiryTime.UnixNano() <= time.Now().UnixNano()
}

func (t *timerQueueProcessorImpl) getTasksAndNextKey() ([]*persistence.TimerTaskInfo, *persistence.TimerTaskInfo, bool,
	error) {
	tasks, lookAheadTask, moreTasks, err := t.ackMgr.readTimerTasks()
	if err != nil {
		return nil, nil, false, err
	}
	return tasks, lookAheadTask, moreTasks, nil
}

func (t *timerQueueProcessorImpl) getTimerTasks(
	minTimestamp time.Time,
	maxTimestamp time.Time,
	batchSize int) ([]*persistence.TimerTaskInfo, error) {
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: minTimestamp,
		MaxTimestamp: maxTimestamp,
		BatchSize:    batchSize}

	for attempt := 1; attempt <= t.config.TimerProcessorGetFailureRetryCount; attempt++ {
		response, err := t.executionManager.GetTimerIndexTasks(request)
		if err == nil {
			return response.Timers, nil
		}
		backoff := time.Duration(attempt * 100)
		time.Sleep(backoff * time.Millisecond)
	}
	return nil, ErrMaxAttemptsExceeded
}

func (t *timerQueueProcessorImpl) processTaskWorker(tasksCh <-chan *persistence.TimerTaskInfo, workerWG *sync.WaitGroup) {
	defer workerWG.Done()
	for {
		select {
		case task, ok := <-tasksCh:
			if !ok {
				return
			}

			var err error

		UpdateFailureLoop:
			for attempt := 1; attempt <= t.config.TimerProcessorUpdateFailureRetryCount; attempt++ {
				taskID := SequenceID{VisibilityTimestamp: task.VisibilityTimestamp, TaskID: task.TaskID}
				err = t.processTimerTask(task)
				if err != nil && err != errTimerTaskNotFound {
					// We will retry until we don't find the timer task any more.
					t.logger.Infof("Failed to process timer with SequenceID: %s with error: %v",
						taskID, err)
					backoff := time.Duration(attempt * 100)
					time.Sleep(backoff * time.Millisecond)
				} else {
					// Completed processing the timer task.
					t.ackMgr.completeTimerTask(taskID)
					break UpdateFailureLoop
				}
			}
		}
	}
}

func (t *timerQueueProcessorImpl) processTimerTask(timerTask *persistence.TimerTaskInfo) error {
	taskID := SequenceID{VisibilityTimestamp: timerTask.VisibilityTimestamp, TaskID: timerTask.TaskID}
	t.logger.Debugf("Processing timer: (%s), for WorkflowID: %v, RunID: %v, Type: %v, TimeoutType: %v, EventID: %v",
		taskID, timerTask.WorkflowID, timerTask.RunID, t.getTimerTaskType(timerTask.TaskType),
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
		err = t.processDeleteHistoryEvent(timerTask)
	}

	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// Timer could fire after the execution is deleted.
			// In which case just ignore the error so we can complete the timer task.
			err = nil
		}
		if err != nil {
			t.metricsClient.IncCounter(scope, metrics.TaskFailures)
		}
	}

	if err == nil {
		// Tracking only successful ones.
		atomic.AddUint64(&t.timerFiredCount, 1)
		err := t.executionManager.CompleteTimerTask(&persistence.CompleteTimerTaskRequest{
			VisibilityTimestamp: timerTask.VisibilityTimestamp,
			TaskID:              timerTask.TaskID})
		if err != nil {
			t.logger.Warnf("Processor unable to complete timer task '%v': %v", timerTask.TaskID, err)
		}
		return nil
	}

	return err
}

func (t *timerQueueProcessorImpl) processExpiredUserTimer(task *persistence.TimerTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TimerTaskUserTimerScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerTaskUserTimerScope, metrics.TaskLatency)
	defer sw.Stop()

	context, release, err0 := t.cache.getOrCreateWorkflowExecution(getDomainIDAndWorkflowExecution(task))
	if err0 != nil {
		return err0
	}
	defer release()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return err1
		}
		tBuilder := t.historyService.getTimerBuilder(&context.workflowExecution)

		if !msBuilder.isWorkflowExecutionRunning() {
			// Workflow is completed.
			return nil
		}

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
					ti.TaskID = nextTask.GetTaskID()
					msBuilder.UpdateUserTimer(ti.TimerID, ti)
					defer t.NotifyNewTimers(timerTasks)
				}

				// Done!
				break ExpireUserTimers
			}
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		err := t.updateWorkflowExecution(context, msBuilder, scheduleNewDecision, false, timerTasks, nil)
		if err != nil {
			if err == ErrConflict {
				continue Update_History_Loop
			}
		}
		return err
	}
	return ErrMaxAttemptsExceeded
}

func getDomainIDAndWorkflowExecution(task *persistence.TimerTaskInfo) (string, workflow.WorkflowExecution) {
	return task.DomainID, workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}
}

func (t *timerQueueProcessorImpl) processActivityTimeout(timerTask *persistence.TimerTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TimerTaskActivityTimeoutScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerTaskActivityTimeoutScope, metrics.TaskLatency)
	defer sw.Stop()

	context, release, err0 := t.cache.getOrCreateWorkflowExecution(getDomainIDAndWorkflowExecution(timerTask))
	if err0 != nil {
		return err0
	}
	defer release()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return err1
		}
		tBuilder := t.historyService.getTimerBuilder(&context.workflowExecution)

		scheduleID := timerTask.EventID
		// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
		// some extreme cassandra failure cases.
		if scheduleID >= msBuilder.GetNextEventID() {
			t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.StaleMutableStateCounter)
			t.logger.Debugf("processActivityTimeout: scheduleID mismatch. MS NextEventID: %v, scheduleID: %v",
				msBuilder.GetNextEventID(), scheduleID)
			// Reload workflow execution history
			context.clear()
			continue Update_History_Loop
		}

		if !msBuilder.isWorkflowExecutionRunning() {
			// Workflow is completed.
			return nil
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

				switch timeoutType {
				case workflow.TimeoutTypeScheduleToClose:
					{
						t.metricsClient.IncCounter(metrics.TimerTaskActivityTimeoutScope, metrics.ScheduleToCloseTimeoutCounter)
						if msBuilder.AddActivityTaskTimedOutEvent(ai.ScheduleID, ai.StartedID, timeoutType, nil) == nil {
							return errFailedToAddTimeoutEvent
						}
						updateHistory = true
					}

				case workflow.TimeoutTypeStartToClose:
					{
						t.metricsClient.IncCounter(metrics.TimerTaskActivityTimeoutScope, metrics.StartToCloseTimeoutCounter)
						if ai.StartedID != emptyEventID {
							if msBuilder.AddActivityTaskTimedOutEvent(ai.ScheduleID, ai.StartedID, timeoutType, nil) == nil {
								return errFailedToAddTimeoutEvent
							}
							updateHistory = true
						}
					}

				case workflow.TimeoutTypeHeartbeat:
					{
						t.metricsClient.IncCounter(metrics.TimerTaskActivityTimeoutScope, metrics.HeartbeatTimeoutCounter)
						t.logger.Debugf("Activity Heartbeat expired: %+v", *ai)

						if msBuilder.AddActivityTaskTimedOutEvent(ai.ScheduleID, ai.StartedID, timeoutType, ai.Details) == nil {
							return errFailedToAddTimeoutEvent
						}
						updateHistory = true
					}

				case workflow.TimeoutTypeScheduleToStart:
					{
						t.metricsClient.IncCounter(metrics.TimerTaskActivityTimeoutScope, metrics.ScheduleToStartTimeoutCounter)
						if ai.StartedID == emptyEventID {
							if msBuilder.AddActivityTaskTimedOutEvent(ai.ScheduleID, ai.StartedID, timeoutType, nil) == nil {
								return errFailedToAddTimeoutEvent
							}
							updateHistory = true
						}
					}
				}
			} else {
				// See if we have next timer in list to be created.
				isHeartBeatTask := timerTask.TimeoutType == int(workflow.TimeoutTypeHeartbeat)

				// Create next timer task if we don't have one (or)
				// if current one is HB task and we need to create next HB task for the same.
				// NOTE: When record activity HB comes in we only update last heartbeat timestamp, this is the place
				// where we create next timer task based on that new updated timestamp.
				if !td.TaskCreated || (isHeartBeatTask && td.EventID == scheduleID) {
					nextTask := tBuilder.createNewTask(td)
					timerTasks = []persistence.Task{nextTask}
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

			t.NotifyNewTimers(timerTasks)
			return nil
		}

		return nil
	}
	return ErrMaxAttemptsExceeded
}

func (t *timerQueueProcessorImpl) processDeleteHistoryEvent(task *persistence.TimerTaskInfo) error {
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

	domainID, workflowExecution := getDomainIDAndWorkflowExecution(task)
	op = func() error {
		return t.historyService.historyMgr.DeleteWorkflowExecutionHistory(
			&persistence.DeleteWorkflowExecutionHistoryRequest{
				DomainID:  domainID,
				Execution: workflowExecution,
			})
	}

	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (t *timerQueueProcessorImpl) processDecisionTimeout(task *persistence.TimerTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TimerTaskDecisionTimeoutScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerTaskDecisionTimeoutScope, metrics.TaskLatency)
	defer sw.Stop()

	context, release, err0 := t.cache.getOrCreateWorkflowExecution(getDomainIDAndWorkflowExecution(task))
	if err0 != nil {
		return err0
	}
	defer release()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return err1
		}

		scheduleID := task.EventID
		di, isPending := msBuilder.GetPendingDecision(scheduleID)

		// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
		// some extreme cassandra failure cases.
		if !isPending && scheduleID >= msBuilder.GetNextEventID() {
			t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.StaleMutableStateCounter)
			// Reload workflow execution history
			context.clear()
			continue Update_History_Loop
		}

		scheduleNewDecision := false
		switch task.TimeoutType {
		case int(workflow.TimeoutTypeStartToClose):
			t.metricsClient.IncCounter(metrics.TimerTaskDecisionTimeoutScope, metrics.StartToCloseTimeoutCounter)
			if isPending && di.Attempt == task.ScheduleAttempt && msBuilder.isWorkflowExecutionRunning() {
				// Add a decision task timeout event.
				msBuilder.AddDecisionTaskTimedOutEvent(scheduleID, di.StartedID)
				scheduleNewDecision = true
			}
		case int(workflow.TimeoutTypeScheduleToStart):
			t.metricsClient.IncCounter(metrics.TimerTaskDecisionTimeoutScope, metrics.ScheduleToStartTimeoutCounter)
			// decision schedule to start timeout only apply to sticky decision
			// check if scheduled decision still pending and not started yet
			if isPending && di.Attempt == task.ScheduleAttempt && msBuilder.isWorkflowExecutionRunning() &&
				di.StartedID == emptyEventID && msBuilder.isStickyTaskListEnabled() {
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

func (t *timerQueueProcessorImpl) processWorkflowTimeout(task *persistence.TimerTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TimerTaskWorkflowTimeoutScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerTaskWorkflowTimeoutScope, metrics.TaskLatency)
	defer sw.Stop()

	context, release, err0 := t.cache.getOrCreateWorkflowExecution(getDomainIDAndWorkflowExecution(task))
	if err0 != nil {
		return err0
	}
	defer release()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return err1
		}

		if !msBuilder.isWorkflowExecutionRunning() {
			return nil
		}

		if e := msBuilder.AddTimeoutWorkflowEvent(); e == nil {
			// If we failed to add the event that means the workflow is already completed.
			// we drop this timeout event.
			return nil
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		err := t.updateWorkflowExecution(context, msBuilder, false, true, nil, nil)
		if err != nil {
			if err == ErrConflict {
				continue Update_History_Loop
			}
		}
		return err
	}
	return ErrMaxAttemptsExceeded
}

func (t *timerQueueProcessorImpl) updateWorkflowExecution(
	context *workflowExecutionContext,
	msBuilder *mutableStateBuilder,
	scheduleNewDecision bool,
	createDeletionTask bool,
	timerTasks []persistence.Task,
	clearTimerTask persistence.Task,
) error {
	var transferTasks []persistence.Task
	if scheduleNewDecision {
		// Schedule a new decision.
		di := msBuilder.AddDecisionTaskScheduledEvent()
		transferTasks = []persistence.Task{&persistence.DecisionTask{
			DomainID:   msBuilder.executionInfo.DomainID,
			TaskList:   di.Tasklist,
			ScheduleID: di.ScheduleID,
		}}
		if msBuilder.isStickyTaskListEnabled() {
			tBuilder := t.historyService.getTimerBuilder(&context.workflowExecution)
			stickyTaskTimeoutTimer := tBuilder.AddScheduleToStartDecisionTimoutTask(di.ScheduleID, di.Attempt,
				msBuilder.executionInfo.StickyScheduleToStartTimeout)
			timerTasks = append(timerTasks, stickyTaskTimeoutTimer)
		}
	}

	if createDeletionTask {
		tBuilder := t.historyService.getTimerBuilder(&context.workflowExecution)
		tranT, timerT, err := t.historyService.getDeleteWorkflowTasks(msBuilder.executionInfo.DomainID, tBuilder)
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
			t.Stop()
		}
	}
	t.NotifyNewTimers(timerTasks)
	return err
}

func (t *timerQueueProcessorImpl) getTimerTaskType(taskType int) string {
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
	}
	return "UnKnown"
}

type timerTaskIDs []SequenceID

// Len implements sort.Interace
func (t timerTaskIDs) Len() int {
	return len(t)
}

// Swap implements sort.Interface.
func (t timerTaskIDs) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// Less implements sort.Interface
func (t timerTaskIDs) Less(i, j int) bool {
	return compareTimerIDLess(&t[i], &t[j])
}

func newTimerAckMgr(processor *timerQueueProcessorImpl, shard ShardContext, executionMgr persistence.ExecutionManager,
	logger bark.Logger) *timerAckMgr {
	ackLevel := shard.GetTimerAckLevel()
	config := shard.GetConfig()
	return &timerAckMgr{
		processor:        processor,
		shard:            shard,
		executionMgr:     executionMgr,
		outstandingTasks: make(map[SequenceID]bool),
		readLevel:        SequenceID{VisibilityTimestamp: ackLevel},
		ackLevel:         ackLevel,
		metricsClient:    processor.metricsClient,
		logger:           logger,
		lastUpdated:      time.Now(),
		config:           config,
	}
}

func (t *timerAckMgr) readTimerTasks() ([]*persistence.TimerTaskInfo, *persistence.TimerTaskInfo, bool, error) {
	t.RLock()
	rLevel := t.readLevel
	t.RUnlock()

	tasks, err := t.processor.getTimerTasks(rLevel.VisibilityTimestamp, maxTimestamp, t.processor.config.TimerTaskBatchSize)
	if err != nil {
		return nil, nil, false, err
	}

	taskCount := len(tasks)
	t.logger.Debugf("readTimerTasks: ReadLevel: (%s) count: %v", rLevel, taskCount)

	// We filter tasks so read only moves to desired timer tasks.
	// We also get a look ahead task but it doesn't move the read level, this is for timer
	// to wait on it instead of doing queries.

	var lookAheadTask *persistence.TimerTaskInfo
	filteredTasks := []*persistence.TimerTaskInfo{}

	t.Lock()
	for _, task := range tasks {
		taskSeq := SequenceID{VisibilityTimestamp: task.VisibilityTimestamp, TaskID: task.TaskID}
		if _, ok := t.outstandingTasks[taskSeq]; ok {
			t.logger.Infof("Skipping task: %v.  WorkflowID: %v, RunID: %v, Type: %v", taskSeq.String(), task.WorkflowID,
				task.RunID, task.TaskType)
			continue
		}
		if task.VisibilityTimestamp.Before(t.readLevel.VisibilityTimestamp) {
			t.logger.Fatalf(
				"Next timer task time stamp is less than current timer task read level. timer task: (%s), ReadLevel: (%s)",
				taskSeq, t.readLevel)
		}

		if !t.processor.isProcessNow(task.VisibilityTimestamp) {
			lookAheadTask = task
			break
		}

		t.logger.Debugf("Moving timer read level: (%s)", taskSeq)
		t.readLevel = taskSeq
		t.outstandingTasks[t.readLevel] = false
		filteredTasks = append(filteredTasks, task)
	}
	t.Unlock()

	// We may have large number of timers which need to be fired immediately.  Return true in such case so the pump
	// can call back immediately to retrieve more tasks
	moreTasks := lookAheadTask == nil && taskCount == t.processor.config.TimerTaskBatchSize

	return filteredTasks, lookAheadTask, moreTasks, nil
}

func (t *timerAckMgr) completeTimerTask(taskID SequenceID) {
	t.Lock()
	if _, ok := t.outstandingTasks[taskID]; ok {
		t.outstandingTasks[taskID] = true
	}
	t.Unlock()
}

func (t *timerAckMgr) updateAckLevel() {
	t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.AckLevelUpdateCounter)
	initialAckLevel := t.ackLevel
	updatedAckLevel := t.ackLevel
	t.Lock()

	// Timer IDs can have holes in the middle. So we sort the map to get the order to
	// check. TODO: we can maintain a sorted slice as well.
	var taskIDs timerTaskIDs
	for k := range t.outstandingTasks {
		taskIDs = append(taskIDs, k)
	}
	sort.Sort(taskIDs)

MoveAckLevelLoop:
	for _, current := range taskIDs {
		if acked, ok := t.outstandingTasks[current]; ok {
			if acked {
				t.ackLevel = current.VisibilityTimestamp
				updatedAckLevel = current.VisibilityTimestamp
				delete(t.outstandingTasks, current)
			} else {
				break MoveAckLevelLoop
			}
		}
	}
	t.Unlock()

	// Do not update Acklevel if nothing changed upto force update interval
	if initialAckLevel == updatedAckLevel && time.Since(t.lastUpdated) < t.config.TimerProcessorForceUpdateInterval {
		return
	}

	t.logger.Debugf("Updating timer ack level: %v", updatedAckLevel)

	// Always update ackLevel to detect if the shared is stolen
	if err := t.shard.UpdateTimerAckLevel(updatedAckLevel); err != nil {
		t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.AckLevelUpdateFailedCounter)
		t.logger.Errorf("Error updating timer ack level for shard: %v", err)
	} else {
		t.lastUpdated = time.Now()
	}
}
