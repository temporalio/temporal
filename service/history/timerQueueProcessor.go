package history

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/persistence"

	"github.com/uber-common/bark"
)

type (
	timerQueueProcessorImpl struct {
		historyService   *historyEngineImpl
		executionManager persistence.ExecutionManager
		isStarted        int32
		isStopped        int32
		shutdownWG       sync.WaitGroup
		shutdownCh       chan struct{}
		newTimerCh       chan struct{}
		logger           bark.Logger
		timerFiredCount  uint64
	}

	timeGate struct {
		tNext, tNow, tEnd int64       // time (in 'UnixNano' units) for next, (last) now and end
		timer             *time.Timer // timer used to wake us up when the next message is ready to deliver
		gateC             chan struct{}
		closeC            chan struct{}
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

func (t *timeGate) setNext(nextKey SequenceID) {
	expiryTime, _ := DeconstructTimerKey(nextKey)
	t.tNext = expiryTime
}

func (t *timeGate) close() {
	close(t.closeC)
}

func (t *timeGate) String() string {
	return fmt.Sprintf("timeGate [engaged=%v eox=%v tNext=%x tNow=%x]", t.engaged(), t.tNext == t.tEnd, t.tNext, t.tNow)
}

func newTimerQueueProcessor(historyService *historyEngineImpl, executionManager persistence.ExecutionManager, logger bark.Logger) timerQueueProcessor {
	return &timerQueueProcessorImpl{
		historyService:   historyService,
		executionManager: executionManager,
		shutdownCh:       make(chan struct{}),
		newTimerCh:       make(chan struct{}, 1),
		logger:           logger.WithFields(bark.Fields{"component": "TimerProcessor"}),
	}
}

func (t *timerQueueProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&t.isStarted, 0, 1) {
		return
	}

	t.shutdownWG.Add(1)
	go t.processorPump()

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
func (t *timerQueueProcessorImpl) NotifyNewTimer() {
	select {
	case t.newTimerCh <- struct{}{}:
		// Notified about new timer.

	default:
		// Channel "full" -> drop and move on, since we are using it as an event.
	}
}

func (t *timerQueueProcessorImpl) processorPump() {
	defer t.shutdownWG.Done()

	err := t.internalProcessor()
	if err != nil {
		t.logger.Error("processor pump failed with error: ", err)
	}
	t.logger.Info("Timer processor exiting.")
}

func (t *timerQueueProcessorImpl) internalProcessor() error {
	nextKey, err := t.getInitialSeed()
	if err != nil {
		return err
	}

	gate := newTimeGate()
	defer gate.close()

	if nextKey != MaxTimerKey {
		gate.setNext(nextKey)
	}

	t.logger.Debugf("InitialSeed Key: %s", nextKey)

	for {
		isWokeByNewTimer := false

		if nextKey == MaxTimerKey || gate.engaged() {
			gateC := gate.beforeSleep()

			// Wait until one of four things occurs:
			// 1. we get notified of a new message
			// 2. the timer fires (message scheduled to be delivered)
			// 3. shutdown was triggered.
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

			}
		}

		// Either we have timer to be fired (or) we have a new timer.

		if isWokeByNewTimer {
			t.logger.Debugf("Woke up by the timer")
			// We have a new timer msg, see if it is earlier than what we know.
			earlyTimeKey := SequenceID(time.Now().UnixNano() - int64(time.Second))
			tempKey, err := t.getNextKey(earlyTimeKey, nextKey)
			if err != nil {
				return err
			}
			if tempKey != MaxTimerKey {
				nextKey = tempKey
			}
		}

		if nextKey != MaxTimerKey && t.isProcessNow(nextKey) {

			// We have a timer to fire.
			err = t.processTimerTask(nextKey)
			if err != nil {
				return err
			}

			// Get next key.
			nextKey, err = t.getNextKey(nextKey, MaxTimerKey)
			if err != nil {
				return err
			}

			t.logger.Debugf("GetNextKey: %s", nextKey)

			if nextKey != MaxTimerKey {
				gate.setNext(nextKey)
			}
		}
	}
}

func (t *timerQueueProcessorImpl) getInitialSeed() (SequenceID, error) {
	return t.getNextKey(MinTimerKey, MaxTimerKey)
}

func (t *timerQueueProcessorImpl) isProcessNow(key SequenceID) bool {
	expiryTime, _ := DeconstructTimerKey(key)
	return expiryTime <= time.Now().UnixNano()
}

func (t *timerQueueProcessorImpl) getNextKey(minKey SequenceID, maxKey SequenceID) (SequenceID, error) {
	tasks, err := t.getTimerTasks(minKey, maxKey, 1)
	if err != nil {
		return MaxTimerKey, err
	}
	if len(tasks) > 0 {
		return SequenceID(tasks[0].TaskID), nil
	}
	return MaxTimerKey, nil
}

func (t *timerQueueProcessorImpl) getTimerTasks(minKey SequenceID, maxKey SequenceID, batchSize int) ([]*persistence.TimerTaskInfo, error) {
	request := &persistence.GetTimerIndexTasksRequest{
		MinKey:    int64(minKey),
		MaxKey:    int64(maxKey),
		BatchSize: batchSize}
	response, err := t.executionManager.GetTimerIndexTasks(request)
	if err != nil {
		return nil, err
	}
	return response.Timers, nil
}

func (t *timerQueueProcessorImpl) processTimerTask(key SequenceID) error {
	t.logger.Debugf("Processing timer with SequenceID: %s", key)

	tasks, err := t.getTimerTasks(key, key+1, 1)
	if err != nil {
		return err
	}

	if len(tasks) != 1 {
		return fmt.Errorf("Unable to find exact task for - SequenceID: %d, found task count: %d", key, len(tasks))
	}

	timerTask := tasks[0]

	if timerTask.TaskID != int64(key) {
		return fmt.Errorf("The key didn't match - SequenceID: %d, found task: %v", key, timerTask)
	}

	t.logger.Debugf("Processing found timer: %s, timer: %+v", SequenceID(timerTask.TaskID), timerTask)

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(timerTask.WorkflowID),
		RunId:      common.StringPtr(timerTask.RunID),
	}

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		// Load the workflow execution information.
		context := newWorkflowExecutionContext(t.historyService, workflowExecution)
		builder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return err1
		}

		msBuilder, err1 := context.loadWorkflowMutableState()
		if err1 != nil {
			return err1
		}

		var transferTasks []persistence.Task
		var timerTasks []persistence.Task
		var clearTimerTask persistence.Task
		scheduleNewDecision := false

		switch timerTask.TaskType {

		case persistence.TaskTypeUserTimer:
			referenceExpiryTime, _ := DeconstructTimerKey(SequenceID(timerTask.TaskID))
			context.tBuilder.LoadUserTimers(msBuilder)

		ExpireUserTimers:
			for _, td := range context.tBuilder.AllTimers() {
				hasTimer, ti := context.tBuilder.UserTimer(td.SequenceID)
				if !hasTimer {
					t.logger.Debugf("Failed to find in memory user timer for: %s", td.SequenceID)
					return fmt.Errorf("failed to find user timer")
				}

				if isExpired := context.tBuilder.IsTimerExpired(td, referenceExpiryTime); isExpired {
					// Add TimerFired event to history.
					_, err = builder.AddTimerFiredEvent(ti.StartedID, ti.TimerID)
					if err != nil {
						return err
					}

					// Remove timer from mutable state.
					msBuilder.DeletePendingTimer(ti.TimerID)
					scheduleNewDecision = !builder.hasPendingDecisionTask()
				} else {
					// See if we have next timer in list to be created.
					if !td.TaskCreated {
						nextTask := context.tBuilder.createNewTask(td)
						timerTasks = []persistence.Task{nextTask}

						// Update the task ID tracking the corresponding timer task.
						ti.TaskID = nextTask.GetTaskID()
						msBuilder.UpdatePendingTimers(ti.TimerID, ti)
					}

					// Done!
					break ExpireUserTimers
				}
			}

			clearTimerTask = &persistence.UserTimerTask{TaskID: timerTask.TaskID}

		case persistence.TaskTypeActivityTimeout:
			clearTimerTask = &persistence.ActivityTimeoutTask{TaskID: timerTask.TaskID}

			scheduleID := timerTask.EventID

			if isRunning, startedID := builder.isActivityTaskRunning(scheduleID); isRunning {
				timeoutType := workflow.TimeoutType(timerTask.TimeoutType)
				t.logger.Debugf("Activity TimeoutType: %v, scheduledID: %v, startedId: %v. \n",
					timeoutType, scheduleID, startedID)

				switch timeoutType {
				case workflow.TimeoutType_SCHEDULE_TO_CLOSE:
					builder.AddActivityTaskTimedOutEvent(scheduleID, startedID, timeoutType, nil)
					scheduleNewDecision = !builder.hasPendingDecisionTask()

				case workflow.TimeoutType_START_TO_CLOSE:
					if startedID != emptyEventID {
						builder.AddActivityTaskTimedOutEvent(scheduleID, startedID, timeoutType, nil)
						scheduleNewDecision = !builder.hasPendingDecisionTask()
					}

				case workflow.TimeoutType_HEARTBEAT:
					if startedID != emptyEventID {
						isTimerRunning, ai := msBuilder.isActivityHeartBeatRunning(scheduleID)
						if isTimerRunning {
							t.logger.Debugf("Activity Heartbeat expired: %+v", *ai)
							// The current heart beat expired.
							builder.AddActivityTaskTimedOutEvent(scheduleID, startedID, timeoutType, ai.Details)
							msBuilder.DeletePendingActivity(scheduleID)
							scheduleNewDecision = !builder.hasPendingDecisionTask()
						}
					}

				case workflow.TimeoutType_SCHEDULE_TO_START:
					if startedID == emptyEventID {
						builder.AddActivityTaskTimedOutEvent(scheduleID, startedID, timeoutType, nil)
						scheduleNewDecision = !builder.hasPendingDecisionTask()
					}
				}
			}

		case persistence.TaskTypeDecisionTimeout:
			clearTimerTask = &persistence.DecisionTimeoutTask{TaskID: timerTask.TaskID}

			scheduleID := timerTask.EventID
			isRunning, startedID := builder.isDecisionTaskRunning(scheduleID)
			if isRunning && startedID != emptyEventID {
				// Add a decision task timeout event.
				builder.AddDecisionTaskTimedOutEvent(scheduleID, startedID)
				scheduleNewDecision = true
			}
		}

		if scheduleNewDecision {
			// Schedule a new decision.
			id := t.historyService.tracker.getNextTaskID()
			defer t.historyService.tracker.completeTask(id)
			newDecisionEvent := builder.ScheduleDecisionTask()
			transferTasks = []persistence.Task{&persistence.DecisionTask{
				TaskID:     id,
				TaskList:   newDecisionEvent.GetDecisionTaskScheduledEventAttributes().GetTaskList().GetName(),
				ScheduleID: newDecisionEvent.GetEventId(),
			}}
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		if err := context.updateWorkflowExecutionWithDeleteTask(transferTasks, timerTasks, clearTimerTask); err != nil {
			if err == ErrConflict {
				continue Update_History_Loop
			}
			return err
		}

		atomic.AddUint64(&t.timerFiredCount, 1)
		return nil
	}

	return ErrMaxAttemptsExceeded
}
