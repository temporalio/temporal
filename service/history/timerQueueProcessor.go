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
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

const (
	timerTaskBatchSize              = 100
	processTimerTaskWorkerCount     = 30
	updateFailureRetryCount         = 5
	getFailureRetryCount            = 5
	timerProcessorUpdateAckInterval = 10 * time.Second
)

var (
	errTimerTaskNotFound          = errors.New("Timer task not found")
	errFailedToAddTimeoutEvent    = errors.New("Failed to add timeout event")
	errFailedToAddTimerFiredEvent = errors.New("Failed to add timer fired event")
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
		logger           bark.Logger
		metricsClient    metrics.Client
		timerFiredCount  uint64
		lock             sync.Mutex // Used to synchronize pending timers.
		ackMgr           *timerAckMgr
		minPendingTimer  time.Time // Track the minimum timer ID in memory.
	}

	timeGate struct {
		tNext, tNow, tEnd int64       // time (in 'UnixNano' units) for next, (last) now and end
		timer             *time.Timer // timer used to wake us up when the next message is ready to deliver
		gateC             chan struct{}
		closeC            chan struct{}
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
	}
)

func newTimeGate() *timeGate {
	tNow := time.Now()

	// setup timeGate with timer set to fire at the 'end of time'
	t := &timeGate{
		tNow:   tNow.UnixNano(),
		tEnd:   math.MaxInt64,
		gateC:  make(chan struct{}),
		closeC: make(chan struct{}),
		timer:  time.NewTimer(time.Unix(0, math.MaxInt64).Sub(tNow)),
	}

	// "Cast" chan Time to chan struct{}.
	// Unfortunately go doesn't have a common channel supertype.
	go func() {
		defer close(t.gateC)
		defer t.timer.Stop()
	loop:
		for {
			select {
			case <-t.timer.C:
				// re-transmit on gateC
				t.gateC <- struct{}{}

			case <-t.closeC:
				// closed; cleanup and quit
				break loop
			}
		}
	}()

	return t
}

func (t *timeGate) setEoxReached() {
	t.tNext = t.tEnd
}

func (t *timeGate) beforeSleep() <-chan struct{} {
	if t.engaged() && t.tNext != t.tEnd {
		// reset timer to fire when the next message should be made 'visible'
		tNow := time.Now()
		t.tNow = tNow.UnixNano()
		t.timer.Reset(time.Unix(0, t.tNext).Sub(tNow))
	}
	return t.gateC
}

func (t *timeGate) engaged() bool {
	t.tNow = time.Now().UnixNano()
	return t.tNext > t.tNow
}

func (t *timeGate) setNext(next time.Time) {
	t.tNext = next.UnixNano()
}

func (t *timeGate) close() {
	close(t.closeC)
}

func (t *timeGate) String() string {
	return fmt.Sprintf("timeGate [engaged=%v eox=%v tNext=%x tNow=%x]", t.engaged(), t.tNext == t.tEnd, t.tNext, t.tNow)
}

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
	go t.processorPump(processTimerTaskWorkerCount)

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
func (t *timerQueueProcessorImpl) NotifyNewTimer(timerTasks []persistence.Task) {
	t.metricsClient.AddCounter(metrics.TimerQueueProcessorScope, metrics.NewTimerCounter, int64(len(timerTasks)))

	updatedMinTimer := false
	t.lock.Lock()
	for _, task := range timerTasks {
		ts := persistence.GetVisibilityTSFrom(task)
		if t.minPendingTimer.IsZero() || ts.Before(t.minPendingTimer) {
			t.minPendingTimer = ts
			updatedMinTimer = true
		}

		switch task.GetType() {
		case persistence.TaskTypeDecisionTimeout:
			t.metricsClient.IncCounter(metrics.TimerTaskDecisionTimeoutScope, metrics.NewTimerCounter)
		case persistence.TaskTypeActivityTimeout:
			t.metricsClient.IncCounter(metrics.TimerTaskActivityTimeoutScope, metrics.NewTimerCounter)
		case persistence.TaskTypeUserTimer:
			t.metricsClient.IncCounter(metrics.TimerTaskUserTimerScope, metrics.NewTimerCounter)
		}
	}
	t.lock.Unlock()

	if updatedMinTimer {
		select {
		case t.newTimerCh <- struct{}{}:
			// Notified about new timer.

		default:
			// Channel "full" -> drop and move on, since we are using it as an event.
		}
	}
}

func (t *timerQueueProcessorImpl) processorPump(taskWorkerCount int) {
	defer t.shutdownWG.Done()

	// Workers to process timer tasks that are expired.
	tasksCh := make(chan *persistence.TimerTaskInfo, 10*timerTaskBatchSize)
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
	gate := newTimeGate()
	defer gate.close()

	updateAckChan := time.NewTicker(timerProcessorUpdateAckInterval).C
	var nextKeyTask *persistence.TimerTaskInfo

	for {
		isWokeByNewTimer := false

		if nextKeyTask == nil || gate.engaged() {
			gateC := gate.beforeSleep()

			// Wait until one of four things occurs:
			// 1. we get notified of a new message
			// 2. the timer fires (message scheduled to be delivered)
			// 3. shutdown was triggered.
			// 4. updating ack level
			//
			select {

			case <-t.shutdownCh:
				t.logger.Debug("Timer queue processor pump shutting down.")
				return nil

			case <-gateC:
				// Timer Fired.

			case <-t.newTimerCh:
				// New Timer has arrived.
				isWokeByNewTimer = true

			case <-updateAckChan:
				t.ackMgr.updateAckLevel()
			}
		}

		if isWokeByNewTimer {
			t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.NewTimerNotifyCounter)
			t.logger.Debugf("Woke up by the timer")

			t.lock.Lock()
			newMinTimestamp := t.minPendingTimer
			if !gate.engaged() || newMinTimestamp.UnixNano() < gate.tNext {
				nextKeyTask = nil
				gate.setNext(newMinTimestamp)
			}
			t.minPendingTimer = time.Time{}
			t.lock.Unlock()

			t.logger.Debugf("%v: Next key after woke up by timer: %v",
				time.Now().UTC(), newMinTimestamp.UTC())

			if !t.isProcessNow(time.Unix(0, gate.tNext)) {
				continue
			}
		}

		// Either we have new timer (or) we are gated on timer to query for it.
		for {
			// Get next set of timer tasks.
			timerTasks, lookAheadTask, err := t.getTasksAndNextKey()
			if err != nil {
				return err
			}

			for _, task := range timerTasks {
				// We have a timer to fire.
				tasksCh <- task
			}

			if lookAheadTask != nil || len(timerTasks) < timerTaskBatchSize {
				// We have processed all the tasks.
				nextKeyTask = lookAheadTask
				break
			}
		}

		if nextKeyTask != nil {
			nextKey := SequenceID{VisibilityTimestamp: nextKeyTask.VisibilityTimestamp, TaskID: nextKeyTask.TaskID}
			t.logger.Debugf("%s: GetNextKey: %s", time.Now().UTC(), nextKey)

			gate.setNext(nextKey.VisibilityTimestamp)
		}
	}
}

func (t *timerQueueProcessorImpl) isProcessNow(expiryTime time.Time) bool {
	return !expiryTime.IsZero() && expiryTime.UnixNano() <= time.Now().UnixNano()
}

func (t *timerQueueProcessorImpl) getTasksAndNextKey() ([]*persistence.TimerTaskInfo, *persistence.TimerTaskInfo, error) {
	tasks, lookAheadTask, err := t.ackMgr.readTimerTasks()
	if err != nil {
		return nil, nil, err
	}
	return tasks, lookAheadTask, nil
}

func (t *timerQueueProcessorImpl) getTimerTasks(
	minTimestamp time.Time,
	maxTimestamp time.Time,
	batchSize int) ([]*persistence.TimerTaskInfo, error) {
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: minTimestamp,
		MaxTimestamp: maxTimestamp,
		BatchSize:    batchSize}

	for attempt := 1; attempt <= getFailureRetryCount; attempt++ {
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
			for attempt := 1; attempt <= updateFailureRetryCount; attempt++ {
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
	t.logger.Debugf("Processing timer: (%s), for WorkflowID: %v, RunID: %v, Type: %v, TimeoutTupe: %v, EventID: %v",
		taskID, timerTask.WorkflowID, timerTask.RunID, t.getTimerTaskType(timerTask.TaskType),
		workflow.TimeoutType(timerTask.TimeoutType).String(), timerTask.EventID)

	domainID := timerTask.DomainID
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(timerTask.WorkflowID),
		RunId:      common.StringPtr(timerTask.RunID),
	}

	context, release, err0 := t.cache.getOrCreateWorkflowExecution(domainID, workflowExecution)
	if err0 != nil {
		return err0
	}
	defer release()

	var err error
	scope := metrics.TimerQueueProcessorScope
	switch timerTask.TaskType {
	case persistence.TaskTypeUserTimer:
		scope = metrics.TimerTaskUserTimerScope
		err = t.processExpiredUserTimer(context, timerTask)
	case persistence.TaskTypeActivityTimeout:
		scope = metrics.TimerTaskActivityTimeoutScope
		err = t.processActivityTimeout(context, timerTask)
	case persistence.TaskTypeDecisionTimeout:
		scope = metrics.TimerTaskDecisionTimeoutScope
		err = t.processDecisionTimeout(context, timerTask)
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

func (t *timerQueueProcessorImpl) processExpiredUserTimer(
	context *workflowExecutionContext, task *persistence.TimerTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TimerTaskUserTimerScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerTaskUserTimerScope, metrics.TaskLatency)
	defer sw.Stop()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return err1
		}

		if !msBuilder.isWorkflowExecutionRunning() {
			// Workflow is completed.
			return nil
		}

		context.tBuilder.LoadUserTimers(msBuilder)

		var timerTasks []persistence.Task

		scheduleNewDecision := false

	ExpireUserTimers:
		for _, td := range context.tBuilder.AllTimers() {
			hasTimer, ti := context.tBuilder.UserTimer(td.SequenceID)
			if !hasTimer {
				t.logger.Debugf("Failed to find in memory user timer for: %s", td.SequenceID)
				return fmt.Errorf("failed to find user timer")
			}

			if isExpired := context.tBuilder.IsTimerExpired(td, task.VisibilityTimestamp); isExpired {
				// Add TimerFired event to history.
				if msBuilder.AddTimerFiredEvent(ti.StartedID, ti.TimerID) == nil {
					return errFailedToAddTimerFiredEvent
				}

				scheduleNewDecision = !msBuilder.HasPendingDecisionTask()
			} else {
				// See if we have next timer in list to be created.
				if !td.TaskCreated {
					nextTask := context.tBuilder.createNewTask(td)
					timerTasks = []persistence.Task{nextTask}

					// Update the task ID tracking the corresponding timer task.
					ti.TaskID = nextTask.GetTaskID()
					msBuilder.UpdateUserTimer(ti.TimerID, ti)
					defer t.NotifyNewTimer(timerTasks)
				}

				// Done!
				break ExpireUserTimers
			}
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		err := t.updateWorkflowExecution(context, msBuilder, scheduleNewDecision, timerTasks, nil)
		if err != nil {
			if err == ErrConflict {
				continue Update_History_Loop
			}
		}
		return err
	}
	return ErrMaxAttemptsExceeded
}

func (t *timerQueueProcessorImpl) processActivityTimeout(
	context *workflowExecutionContext, timerTask *persistence.TimerTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TimerTaskActivityTimeoutScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerTaskActivityTimeoutScope, metrics.TaskLatency)
	defer sw.Stop()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return err1
		}

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

		var timerTasks []persistence.Task
		scheduleNewDecision := false
		updateHistory := false

		if ai, isRunning := msBuilder.GetActivityInfo(scheduleID); isRunning && msBuilder.isWorkflowExecutionRunning() {
			timeoutType := workflow.TimeoutType(timerTask.TimeoutType)
			t.logger.Debugf("Activity TimeoutType: %v, scheduledID: %v, startedId: %v. \n",
				timeoutType, scheduleID, ai.StartedID)

			switch timeoutType {
			case workflow.TimeoutType_SCHEDULE_TO_CLOSE:
				{
					t.metricsClient.IncCounter(metrics.TimerTaskActivityTimeoutScope, metrics.ScheduleToCloseTimeoutCounter)
					if msBuilder.AddActivityTaskTimedOutEvent(scheduleID, ai.StartedID, timeoutType, nil) == nil {
						return errFailedToAddTimeoutEvent
					}

					updateHistory = true
					scheduleNewDecision = !msBuilder.HasPendingDecisionTask()
				}

			case workflow.TimeoutType_START_TO_CLOSE:
				{
					t.metricsClient.IncCounter(metrics.TimerTaskActivityTimeoutScope, metrics.StartToCloseTimeoutCounter)
					if ai.StartedID != emptyEventID {
						if msBuilder.AddActivityTaskTimedOutEvent(scheduleID, ai.StartedID, timeoutType, nil) == nil {
							return errFailedToAddTimeoutEvent
						}

						updateHistory = true
						scheduleNewDecision = !msBuilder.HasPendingDecisionTask()
					}
				}

			case workflow.TimeoutType_HEARTBEAT:
				{
					t.metricsClient.IncCounter(metrics.TimerTaskActivityTimeoutScope, metrics.HeartbeatTimeoutCounter)
					lastHeartbeat := ai.LastHeartBeatUpdatedTime.Add(
						time.Duration(ai.HeartbeatTimeout) * time.Second)

					if timerTask.VisibilityTimestamp.After(lastHeartbeat) {
						t.logger.Debugf("Activity Heartbeat expired: %+v", *ai)
						if msBuilder.AddActivityTaskTimedOutEvent(scheduleID, ai.StartedID, timeoutType, nil) == nil {
							return errFailedToAddTimeoutEvent
						}

						updateHistory = true
						scheduleNewDecision = !msBuilder.HasPendingDecisionTask()
					} else {
						// Re-Schedule next heartbeat.
						hbTimeoutTask, err := context.tBuilder.AddHeartBeatActivityTimeout(ai)
						if err != nil {
							return err
						}
						if hbTimeoutTask != nil {
							timerTasks = append(timerTasks, hbTimeoutTask)
						}
					}
				}

			case workflow.TimeoutType_SCHEDULE_TO_START:
				{
					t.metricsClient.IncCounter(metrics.TimerTaskActivityTimeoutScope, metrics.ScheduleToStartTimeoutCounter)
					if ai.StartedID == emptyEventID {
						if msBuilder.AddActivityTaskTimedOutEvent(scheduleID, ai.StartedID, timeoutType, nil) == nil {
							return errFailedToAddTimeoutEvent
						}

						updateHistory = true
						scheduleNewDecision = !msBuilder.HasPendingDecisionTask()
					}
				}
			}
		}

		if updateHistory {
			// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
			// the history and try the operation again.
			defer t.NotifyNewTimer(timerTasks)
			err := t.updateWorkflowExecution(context, msBuilder, scheduleNewDecision, timerTasks, nil)
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

func (t *timerQueueProcessorImpl) processDecisionTimeout(
	context *workflowExecutionContext, task *persistence.TimerTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TimerTaskDecisionTimeoutScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TimerTaskDecisionTimeoutScope, metrics.TaskLatency)
	defer sw.Stop()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return err1
		}

		scheduleID := task.EventID

		// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
		// some extreme cassandra failure cases.
		if scheduleID >= msBuilder.GetNextEventID() {
			t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.StaleMutableStateCounter)
			// Reload workflow execution history
			context.clear()
			continue Update_History_Loop
		}

		scheduleNewDecision := false
		di, isRunning := msBuilder.GetPendingDecision(scheduleID)
		if isRunning && msBuilder.isWorkflowExecutionRunning() {
			// Add a decision task timeout event.
			timeoutEvent := msBuilder.AddDecisionTaskTimedOutEvent(scheduleID, di.StartedID)
			if timeoutEvent == nil {
				// Unable to add DecisionTaskTimedout event to history
				return &workflow.InternalServiceError{Message: "Unable to add DecisionTaskTimedout event to history."}
			}

			scheduleNewDecision = true
		}

		if scheduleNewDecision {
			// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
			// the history and try the operation again.
			err := t.updateWorkflowExecution(context, msBuilder, scheduleNewDecision, nil, nil)
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

func (t *timerQueueProcessorImpl) updateWorkflowExecution(context *workflowExecutionContext,
	msBuilder *mutableStateBuilder, scheduleNewDecision bool, timerTasks []persistence.Task,
	clearTimerTask persistence.Task) error {
	var transferTasks []persistence.Task
	if scheduleNewDecision {
		// Schedule a new decision.
		newDecisionEvent, _ := msBuilder.AddDecisionTaskScheduledEvent()
		transferTasks = []persistence.Task{&persistence.DecisionTask{
			DomainID:   msBuilder.executionInfo.DomainID,
			TaskList:   newDecisionEvent.GetDecisionTaskScheduledEventAttributes().GetTaskList().GetName(),
			ScheduleID: newDecisionEvent.GetEventId(),
		}}
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
	return &timerAckMgr{
		processor:        processor,
		shard:            shard,
		executionMgr:     executionMgr,
		outstandingTasks: make(map[SequenceID]bool),
		readLevel:        SequenceID{VisibilityTimestamp: ackLevel},
		ackLevel:         ackLevel,
		metricsClient:    processor.metricsClient,
		logger:           logger,
	}
}

func (t *timerAckMgr) readTimerTasks() ([]*persistence.TimerTaskInfo, *persistence.TimerTaskInfo, error) {
	t.RLock()
	rLevel := t.readLevel
	t.RUnlock()

	tasks, err := t.processor.getTimerTasks(rLevel.VisibilityTimestamp, maxTimestamp, timerTaskBatchSize)
	if err != nil {
		return nil, nil, err
	}

	t.logger.Debugf("readTimerTasks: ReadLevel: (%s) count: %v", rLevel, len(tasks))

	// We filter tasks so read only moves to desired timer tasks.
	// We also get a look ahead task but it doesn't move the read level, this is for timer
	// to wait on it instead of doing queries.

	var lookAheadTask *persistence.TimerTaskInfo
	filteredTasks := []*persistence.TimerTaskInfo{}

	t.Lock()
	for _, task := range tasks {
		taskSeq := SequenceID{VisibilityTimestamp: task.VisibilityTimestamp, TaskID: task.TaskID}
		if _, ok := t.outstandingTasks[taskSeq]; ok {
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

	return filteredTasks, lookAheadTask, nil
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

	t.logger.Debugf("Updating timer ack level: %v", updatedAckLevel)

	// Always update ackLevel to detect if the shared is stolen
	if err := t.shard.UpdateTimerAckLevel(updatedAckLevel); err != nil {
		t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.AckLevelUpdateFailedCounter)
		t.logger.Errorf("Error updating timer ack level for shard: %v", err)
	}
}
