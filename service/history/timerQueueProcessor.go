package history

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"

	"github.com/uber-common/bark"
)

const (
	timerTaskBatchSize          = 10
	processTimerTaskWorkerCount = 5
	updateFailureRetryCount     = 5
)

var (
	errTimerTaskNotFound          = errors.New("Timer task not found")
	errFailedToAddTimeoutEvent    = errors.New("Failed to add timeout event")
	errFailedToAddTimerFiredEvent = errors.New("Failed to add timer fired event")
)

type (
	timerQueueProcessorImpl struct {
		historyService    *historyEngineImpl
		cache             *historyCache
		executionManager  persistence.ExecutionManager
		isStarted         int32
		isStopped         int32
		shutdownWG        sync.WaitGroup
		shutdownCh        chan struct{}
		newTimerCh        chan struct{}
		logger            bark.Logger
		timerFiredCount   uint64
		lock              sync.Mutex // Used to synchronize pending timers.
		minPendingTimerID SequenceID // Track the minimum timer ID in memory.
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

func newTimerQueueProcessor(historyService *historyEngineImpl, executionManager persistence.ExecutionManager,
	logger bark.Logger) timerQueueProcessor {
	return &timerQueueProcessorImpl{
		historyService:    historyService,
		cache:             historyService.historyCache,
		executionManager:  executionManager,
		shutdownCh:        make(chan struct{}),
		newTimerCh:        make(chan struct{}, 1),
		minPendingTimerID: MaxTimerKey,
		logger: logger.WithFields(bark.Fields{
			tagWorkflowComponent: tagValueTimerQueueComponent,
		}),
	}
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
func (t *timerQueueProcessorImpl) NotifyNewTimer(taskID int64) {

	t.lock.Lock()
	taskSeqID := SequenceID(taskID)
	if taskSeqID < t.minPendingTimerID {
		t.minPendingTimerID = taskSeqID
	}
	t.lock.Unlock()

	select {
	case t.newTimerCh <- struct{}{}:
	// Notified about new timer.

	default:
		// Channel "full" -> drop and move on, since we are using it as an event.
	}
}

func (t *timerQueueProcessorImpl) processorPump(taskWorkerCount int) {
	defer t.shutdownWG.Done()

	// Workers to process timer tasks that are expired.
	tasksCh := make(chan SequenceID, timerTaskBatchSize)
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

func (t *timerQueueProcessorImpl) internalProcessor(tasksCh chan<- SequenceID) error {
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
			tempKey := MaxTimerKey
			t.lock.Lock()
			tempKey, t.minPendingTimerID = t.minPendingTimerID, tempKey
			t.lock.Unlock()

			if tempKey != MaxTimerKey && tempKey < nextKey {
				nextKey = tempKey
			}
			t.logger.Debugf("Next key after woke up by timer: %v, tempKey: %v", nextKey, tempKey)
		}

		pendingNextKeysList := []SequenceID{}
		for nextKey != MaxTimerKey && t.isProcessNow(nextKey) {
			// We have a timer to fire.
			tasksCh <- nextKey

			// Get next key.
			if len(pendingNextKeysList) == 0 {
				pendingNextKeysList, err = t.getNextKey(nextKey+1, MaxTimerKey)
				if err != nil {
					return err
				}
			}
			nextKey = pendingNextKeysList[0]
			pendingNextKeysList = pendingNextKeysList[1:]
		}

		if nextKey != MaxTimerKey {
			t.logger.Debugf("GetNextKey: %s", nextKey)

			if nextKey != MaxTimerKey {
				gate.setNext(nextKey)
			}
		}
	}
}

func (t *timerQueueProcessorImpl) getInitialSeed() (SequenceID, error) {
	keys, err := t.getNextKey(MinTimerKey, MaxTimerKey)
	if err != nil {
		return MaxTimerKey, err
	}
	return keys[0], nil
}

func (t *timerQueueProcessorImpl) isProcessNow(key SequenceID) bool {
	expiryTime, _ := DeconstructTimerKey(key)
	return expiryTime <= time.Now().UnixNano()
}

func (t *timerQueueProcessorImpl) getNextKey(minKey SequenceID, maxKey SequenceID) ([]SequenceID, error) {
	tasks, err := t.getTimerTasks(minKey, maxKey, timerTaskBatchSize)
	if err != nil {
		return []SequenceID{MaxTimerKey}, err
	}
	keys := []SequenceID{}
	if len(tasks) > 0 {
		for _, ti := range tasks {
			keys = append(keys, SequenceID(ti.TaskID))
		}
		return keys, nil
	}
	return []SequenceID{MaxTimerKey}, nil
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

func (t *timerQueueProcessorImpl) processTaskWorker(tasksCh <-chan SequenceID, workerWG *sync.WaitGroup) {
	defer workerWG.Done()
	for {
		select {
		case key, ok := <-tasksCh:
			if !ok {
				return
			}

			var err error

		UpdateFailureLoop:
			for attempt := 1; attempt <= updateFailureRetryCount; attempt++ {
				err = t.processTimerTask(key)
				if err != nil && err != errTimerTaskNotFound {
					// We will retry until we don't find the timer task any more.
					t.logger.Infof("Failed to process timer with SequenceID: %s with error: %v", key, err)
					backoff := time.Duration(attempt * 100)
					time.Sleep(backoff * time.Millisecond)
				} else {
					// Completed processing the timer task.
					break UpdateFailureLoop
				}
			}

			if err != nil && err != errTimerTaskNotFound {
				// We need to retry for this timer task ID
				t.NotifyNewTimer(int64(key))
			}
		}
	}
}

func (t *timerQueueProcessorImpl) processTimerTask(key SequenceID) error {
	t.logger.Debugf("Processing timer with SequenceID: %s", key)

	tasks, err := t.getTimerTasks(key, key+1, 1)
	if err != nil {
		return err
	}

	if len(tasks) != 1 {
		t.logger.Infof("Unable to find exact task for - SequenceID: %d, found task count: %d", key, len(tasks))
		return errTimerTaskNotFound
	}

	timerTask := tasks[0]

	if timerTask.TaskID != int64(key) {
		t.logger.Infof("The key didn't match - SequenceID: %d, found task: %v", key, timerTask)
		return errTimerTaskNotFound
	}

	t.logger.Debugf("Processing found timer: %s, for WorkflowID: %v, RunID: %v, Type: %v, TimeoutTupe: %v, EventID: %v",
		SequenceID(timerTask.TaskID), timerTask.WorkflowID, timerTask.RunID, timerTask.TaskType,
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

	switch timerTask.TaskType {
	case persistence.TaskTypeUserTimer:
		err = t.processExpiredUserTimer(context, timerTask)
	case persistence.TaskTypeActivityTimeout:
		err = t.processActivityTimeout(context, timerTask)
	case persistence.TaskTypeDecisionTimeout:
		err = t.processDecisionTimeout(context, timerTask)
	}

	if err == nil {
		// Tracking only successful ones.
		atomic.AddUint64(&t.timerFiredCount, 1)
	}

	return err
}

func (t *timerQueueProcessorImpl) processExpiredUserTimer(
	context *workflowExecutionContext, task *persistence.TimerTaskInfo) error {
Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return err1
		}

		referenceExpiryTime, _ := DeconstructTimerKey(SequenceID(task.TaskID))
		context.tBuilder.LoadUserTimers(msBuilder)

		var timerTasks []persistence.Task
		var clearTimerTask persistence.Task
		scheduleNewDecision := false
	ExpireUserTimers:
		for _, td := range context.tBuilder.AllTimers() {
			hasTimer, ti := context.tBuilder.UserTimer(td.SequenceID)
			if !hasTimer {
				t.logger.Debugf("Failed to find in memory user timer for: %s", td.SequenceID)
				return fmt.Errorf("failed to find user timer")
			}

			if isExpired := context.tBuilder.IsTimerExpired(td, referenceExpiryTime); isExpired {
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
					defer t.NotifyNewTimer(ti.TaskID)
				}

				// Done!
				break ExpireUserTimers
			}
		}

		clearTimerTask = &persistence.UserTimerTask{TaskID: task.TaskID}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		err := t.updateWorkflowExecution(context, msBuilder, scheduleNewDecision, timerTasks, clearTimerTask)
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
			t.logger.Debugf("processActivityTimeout: scheduleID mismatch. MS NextEventID: %v, scheduleID: %v",
				msBuilder.GetNextEventID(), scheduleID)
			// Reload workflow execution history
			context.clear()
			continue Update_History_Loop
		}

		clearTimerTask := &persistence.ActivityTimeoutTask{TaskID: timerTask.TaskID}

		scheduleNewDecision := false
		updateHistory := false

		if ai, isRunning := msBuilder.GetActivityInfo(scheduleID); isRunning {
			timeoutType := workflow.TimeoutType(timerTask.TimeoutType)
			t.logger.Debugf("Activity TimeoutType: %v, scheduledID: %v, startedId: %v. \n",
				timeoutType, scheduleID, ai.StartedID)

			switch timeoutType {
			case workflow.TimeoutType_SCHEDULE_TO_CLOSE:
				{
					if msBuilder.AddActivityTaskTimedOutEvent(scheduleID, ai.StartedID, timeoutType, nil) == nil {
						return errFailedToAddTimeoutEvent
					}

					updateHistory = true
					scheduleNewDecision = !msBuilder.HasPendingDecisionTask()
				}

			case workflow.TimeoutType_START_TO_CLOSE:
				{
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
					if ai.StartedID != emptyEventID {
						if msBuilder.AddActivityTaskTimedOutEvent(scheduleID, ai.StartedID, timeoutType, nil) == nil {
							return errFailedToAddTimeoutEvent
						}

						updateHistory = true
						scheduleNewDecision = !msBuilder.HasPendingDecisionTask()
					}
				}

			case workflow.TimeoutType_SCHEDULE_TO_START:
				{
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
			err := t.updateWorkflowExecution(context, msBuilder, scheduleNewDecision, nil, clearTimerTask)
			if err != nil {
				if err == ErrConflict {
					continue Update_History_Loop
				}
			}
			return err
		}
		err := t.executionManager.CompleteTimerTask(&persistence.CompleteTimerTaskRequest{TaskID: timerTask.TaskID})
		if err != nil {
			t.logger.Warnf("Processor unable to complete timer task '%v': %v", timerTask.TaskID, err)
		}
		return nil

	}
	return ErrMaxAttemptsExceeded
}

func (t *timerQueueProcessorImpl) processDecisionTimeout(
	context *workflowExecutionContext, task *persistence.TimerTaskInfo) error {
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
			// Reload workflow execution history
			context.clear()
			continue Update_History_Loop
		}

		scheduleNewDecision := false
		clearTimerTask := &persistence.DecisionTimeoutTask{TaskID: task.TaskID}

		di, isRunning := msBuilder.GetPendingDecision(scheduleID)
		if isRunning {
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
			err := t.updateWorkflowExecution(context, msBuilder, scheduleNewDecision, nil, clearTimerTask)
			if err != nil {
				if err == ErrConflict {
					continue Update_History_Loop
				}
			}
			return err
		}
		err := t.executionManager.CompleteTimerTask(&persistence.CompleteTimerTaskRequest{TaskID: task.TaskID})
		if err != nil {
			t.logger.Warnf("Processor unable to complete timer task '%v': %v", task.TaskID, err)
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
