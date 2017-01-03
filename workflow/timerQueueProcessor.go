package workflow

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/util"
	"code.uber.internal/devexp/minions/persistence"

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
		logger:           logger,
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

	if success := util.AwaitWaitGroup(&t.shutdownWG, time.Minute); !success {
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
	t.logger.Info("timer processor exiting.")
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

	t.logger.Infof("InitialSeed Key: %s", nextKey)

	for i := 0; i < 4; i++ {
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
				t.logger.Info("Timer queue processor pump shutting down.")
				break

			case <-gateC:
				// Timer Fired.

			case <-t.newTimerCh:
				// New Timer has arrived.
				isWokeByNewTimer = true

			}
		}

		// Either we have timer to be fired (or) we have a new timer.

		if isWokeByNewTimer {
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

			t.logger.Infof("GetNextKey: %s", nextKey)

			if nextKey != MaxTimerKey {
				gate.setNext(nextKey)
			}
		}
	}
	return nil
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

func (t *timerQueueProcessorImpl) getTimerTasks(minKey SequenceID, maxKey SequenceID, batchSize int) ([]*persistence.TimerInfo, error) {
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
	t.logger.Infof("Processing timer with SequenceID: %s", key)

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

		var timerTasks []persistence.Task
		var clearTimerTask persistence.Task

		switch timerTask.TaskType {

		case persistence.TaskTypeUserTimer:
			// Add TimerFired event to history.
			isRunning, timerID := builder.isTimerTaskRunning(timerTask.EventID)
			if !isRunning {
				return fmt.Errorf("The timer task is not running - TimerID: %s, SequenceID: %s, StartedID: %d",
					timerID, SequenceID(timerTask.TaskID), timerTask.EventID)
			}
			_, err = builder.AddTimerFiredEvent(timerTask.EventID, timerID, timerTask.TaskID)
			if err != nil {
				return err
			}

			// See if we have next timer in list to be created.
			nextTask, err := context.tBuilder.RemoveTimer(&persistence.UserTimerTask{TaskID: timerTask.TaskID})
			if err != nil {
				return err
			}
			if nextTask != nil {
				timerTasks = []persistence.Task{nextTask}
			}
			clearTimerTask = &persistence.UserTimerTask{TaskID: timerTask.TaskID}

		case persistence.TaskTypeActivityTimeout:
			clearTimerTask = &persistence.ActivityTimeoutTask{TaskID: timerTask.TaskID}

		case persistence.TaskTypeDecisionTimeout:
			clearTimerTask = &persistence.DecisionTimeoutTask{TaskID: timerTask.TaskID}
		}

		var transferTasks []persistence.Task
		if !builder.hasPendingDecisionTask() {
			startWorkflowExecutionEvent := builder.GetEvent(firstEventID)
			startAttributes := startWorkflowExecutionEvent.GetWorkflowExecutionStartedEventAttributes()
			newDecisionEvent := builder.AddDecisionTaskScheduledEvent(startAttributes.GetTaskList().GetName(),
				startAttributes.GetTaskStartToCloseTimeoutSeconds())
			transferTasks = []persistence.Task{&persistence.DecisionTask{
				TaskList:   startAttributes.GetTaskList().GetName(),
				ScheduleID: newDecisionEvent.GetEventId(),
			}}
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		if err := context.updateWorkflowExecutionWithDeleteTask(transferTasks, timerTasks, clearTimerTask); err != nil {
			if err == errConflict {
				continue Update_History_Loop
			}
			return err
		}

		atomic.AddUint64(&t.timerFiredCount, 1)
		return nil
	}

	return errMaxAttemptsExceeded
}
