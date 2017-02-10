package matching

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/uber-common/bark"

	"math"
	"sync/atomic"

	h "code.uber.internal/devexp/minions/.gen/go/history"
	m "code.uber.internal/devexp/minions/.gen/go/matching"
	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/client/history"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/backoff"
	"code.uber.internal/devexp/minions/common/persistence"
)

const (
	defaultRangeSize  = 100000
	getTasksBatchSize = 100
	// To perform one db operation if there are no pollers
	taskBufferSize = getTasksBatchSize - 1

	done time.Duration = -1
)

// Implements matching.Engine
type matchingEngineImpl struct {
	taskManager                persistence.TaskManager
	historyService             history.Client
	tokenSerializer            common.TaskTokenSerializer
	rangeSize                  int64
	logger                     bark.Logger
	longPollExpirationInterval time.Duration
	taskListsLock              sync.RWMutex                    // locks mutation of taskLists
	taskLists                  map[taskListID]*taskListContext // Convert to LRU cache
}

type taskListID struct {
	taskListName string
	taskType     int
}

// Contains information needed for current task transition from Activity queue to Workflow execution history.
type taskContext struct {
	tlCtx             *taskListContext
	info              *persistence.TaskInfo
	syncResponseCh    chan<- *syncMatchResponse
	workflowExecution workflow.WorkflowExecution
}

// Used to convert out of order acks into ackLevel movement.
type ackManager struct {
	logger bark.Logger

	outstandingTasks map[int64]bool // key->TaskID, value->(true for acked, false->for non acked)
	readLevel        int64          // Maximum TaskID inserted into outstandingTasks
	ackLevel         int64          // Maximum TaskID below which all tasks are acked
}

// Single task list in memory state
type taskListContext struct {
	taskListID *taskListID
	logger     bark.Logger
	engine     *matchingEngineImpl
	taskBuffer chan *persistence.TaskInfo // tasks loaded from persistence
	// Sync channel used to perform sync matching.
	// It must to be unbuffered. addTask publishes to it asynchronously and expects publish to succeed
	// only if there is waiting poll that consumes from it.
	syncMatch  chan *getTaskResult
	shutdownCh chan struct{}
	stopped    int32

	sync.Mutex
	taskAckManager          ackManager // tracks ackLevel for delivered messages
	writeOffsetManager      ackManager // tracks maxReadLevel for out of order message puts
	rangeID                 int64      // Current range of the task list. Starts from 1.
	taskSequenceNumber      int64      // Sequence number of the next task. Starts from 1.
	nextRangeSequenceNumber int64      // Current range boundary
}

// getTaskResult contains task info and optional channel to notify createTask caller
// that task is successfully started and returned to a poller
type getTaskResult struct {
	task *persistence.TaskInfo
	C    chan *syncMatchResponse
}

// syncMatchResponse result of sync match delivered to a createTask caller
type syncMatchResponse struct {
	response *persistence.CreateTaskResponse
	err      error
}

const (
	defaultLongPollExpirationInterval = time.Minute
	emptyGetRetryInitialInterval      = 100 * time.Millisecond
	emptyGetRetryMaxInterval          = 1 * time.Second
)

var (
	// EmptyPollForDecisionTaskResponse is the response when there are no decision tasks to hand out
	emptyPollForDecisionTaskResponse = workflow.NewPollForDecisionTaskResponse()
	// EmptyPollForActivityTaskResponse is the response when there are no activity tasks to hand out
	emptyPollForActivityTaskResponse   = workflow.NewPollForActivityTaskResponse()
	persistenceOperationRetryPolicy    = common.CreatePersistanceRetryPolicy()
	historyServiceOperationRetryPolicy = common.CreateHistoryServiceRetryPolicy()
	emptyGetTasksRetryPolicy           = createEmptyGetTasksRetryPolicy()
	// ErrNoTasks is exported temporarily for integration test
	ErrNoTasks    = errors.New("No tasks")
	errPumpClosed = errors.New("Task list pump closed its channel")
)

var _ Engine = (*matchingEngineImpl)(nil) // Asserts that interface is indeed implemented

// NewEngine creates an instance of matching engine
func NewEngine(taskManager persistence.TaskManager, historyService history.Client, logger bark.Logger) Engine {
	return &matchingEngineImpl{
		taskManager:                taskManager,
		historyService:             historyService,
		tokenSerializer:            common.NewJSONTaskTokenSerializer(),
		taskLists:                  make(map[taskListID]*taskListContext),
		rangeSize:                  defaultRangeSize,
		longPollExpirationInterval: defaultLongPollExpirationInterval,
		logger: logger.WithFields(bark.Fields{
			tagWorkflowComponent: tagValueWorkflowEngineComponent,
		}),
	}
}

// Returns taskListContext for a task list. If not already cached gets new range from DB and if successful creates one.
func (e *matchingEngineImpl) getTaskListContext(taskList *taskListID) (*taskListContext, error) {
	e.taskListsLock.RLock()
	if result, ok := e.taskLists[*taskList]; ok {
		e.taskListsLock.RUnlock()
		return result, nil
	}
	e.taskListsLock.RUnlock()
	ctx := &taskListContext{
		engine:             e,
		taskBuffer:         make(chan *persistence.TaskInfo, taskBufferSize),
		shutdownCh:         make(chan struct{}),
		taskListID:         taskList,
		logger:             e.logger,
		taskAckManager:     newAckManager(e.logger),
		writeOffsetManager: newAckManager(e.logger),
		syncMatch:          make(chan *getTaskResult),
	}
	e.taskListsLock.Lock()
	if result, ok := e.taskLists[*taskList]; ok {
		e.taskListsLock.Unlock()
		return result, nil
	}
	e.taskLists[*taskList] = ctx
	e.taskListsLock.Unlock()

	err := ctx.updateRangeIfNeeded(e) // Grabs a new range and updates read and ackLevels
	if err != nil {
		return nil, err
	}
	ctx.Start()
	return ctx, nil
}

// AddDecisionTask either delivers task directly to waiting poller or save it into task list persistence.
func (e *matchingEngineImpl) AddDecisionTask(addRequest *m.AddDecisionTaskRequest) error {
	taskListName := addRequest.GetTaskList().GetName()
	e.logger.Debugf("Received AddDecisionTask for taskList=%v, WorkflowID=%v, RunID=%v",
		addRequest.TaskList.Name, addRequest.Execution.WorkflowId, addRequest.Execution.RunId)
	taskList := newTaskListID(taskListName, persistence.TaskListTypeDecision)
	t, err := e.getTaskListContext(taskList)
	if err != nil {
		return err
	}
	_, err = t.executeWithRetry(func(rangeID int64) (interface{}, error) {
		// TODO: Unify ActivityTask, DecisionTask, Task and potentially TaskInfo in a single structure
		taskInfo := &persistence.TaskInfo{
			RunID:      addRequest.GetExecution().GetRunId(),
			WorkflowID: addRequest.GetExecution().GetWorkflowId(),
			ScheduleID: addRequest.GetScheduleId(),
		}
		r, err := t.trySyncMatch(taskInfo)
		if err != nil || r != nil {
			return r, err
		}
		taskID, err := t.initiateTaskAppend(e)
		if err != nil {
			return nil, err
		}
		task := &persistence.DecisionTask{
			TaskList:   taskListName,
			ScheduleID: addRequest.GetScheduleId(),
			TaskID:     taskID,
		}
		r, err = e.taskManager.CreateTask(&persistence.CreateTaskRequest{
			Execution: *addRequest.GetExecution(),
			Data:      task,
			TaskID:    task.TaskID,
			RangeID:   rangeID,
		})
		t.completeTaskAppend(taskID)

		return r, err
	})
	return err
}

// AddActivityTask either delivers task directly to waiting poller or save it into task list persistence.
func (e *matchingEngineImpl) AddActivityTask(addRequest *m.AddActivityTaskRequest) error {
	taskListName := addRequest.GetTaskList().GetName()
	e.logger.Debugf("Received AddActivityTask for taskList=%v WorkflowID=%v, RunID=%v",
		taskListName, addRequest.Execution.WorkflowId, addRequest.Execution.RunId)
	taskList := newTaskListID(taskListName, persistence.TaskListTypeActivity)
	t, err := e.getTaskListContext(taskList)
	if err != nil {
		return err
	}
	_, err = t.executeWithRetry(func(rangeID int64) (interface{}, error) {
		// TODO: Unify ActivityTask, DecisionTask, Task and potentially TaskInfo in a single structure
		taskInfo := &persistence.TaskInfo{
			RunID:      addRequest.GetExecution().GetRunId(),
			WorkflowID: addRequest.GetExecution().GetWorkflowId(),
			ScheduleID: addRequest.GetScheduleId(),
		}
		r, err := t.trySyncMatch(taskInfo)
		if err != nil || r != nil {
			return r, err
		}
		taskID, err := t.initiateTaskAppend(e)
		if err != nil {
			return nil, err
		}
		task := &persistence.ActivityTask{
			TaskList:   taskListName,
			ScheduleID: addRequest.GetScheduleId(),
			TaskID:     taskID,
		}
		r, err = e.taskManager.CreateTask(&persistence.CreateTaskRequest{
			Execution: *addRequest.GetExecution(),
			Data:      task,
			TaskID:    task.TaskID,
			RangeID:   rangeID,
		})
		t.completeTaskAppend(taskID)

		return r, err
	})
	return err
}

// PollForDecisionTask tries to get the decision task using exponential backoff.
func (e *matchingEngineImpl) PollForDecisionTask(request *workflow.PollForDecisionTaskRequest) (
	*workflow.PollForDecisionTaskResponse, error) {
	taskListName := request.GetTaskList().GetName()
	e.logger.Debugf("Received PollForDecisionTask for taskList=%v", taskListName)
pollLoop:
	for {
		taskList := newTaskListID(taskListName, persistence.TaskListTypeDecision)
		tCtx, err := e.getTask(taskList)
		if err != nil {
			if err == ErrNoTasks {
				return emptyPollForDecisionTaskResponse, nil
			}
			return nil, err
		}
		resp, err := tCtx.RecordDecisionTaskStartedWithRetry(&h.RecordDecisionTaskStartedRequest{
			WorkflowExecution: &tCtx.workflowExecution,
			ScheduleId:        &tCtx.info.ScheduleID,
			TaskId:            &tCtx.info.TaskID,
			PollRequest:       request,
		})
		if err != nil {
			if _, ok := err.(*workflow.EntityNotExistsError); ok {
				e.logger.Debugf("Duplicated decision task taskList=%v, taskID=",
					taskListName, tCtx.info.TaskID)
				tCtx.completeTask(nil)
				continue pollLoop
			}
			// This essentially looses task when it was loaded from persistence.
			tCtx.completeTask(err)
			// TODO: Implement task retries on intermittent history service failures
			// TODO: Stop loosing tasks if history service is not available
			e.logger.Errorf("Lost decision task (workflowID=%v, runID=%v, scheduleID=%v, taskList=%v, taskID=%v) "+
				"due to error from historyService.RecordDecisionTaskStarted: %v",
				tCtx.workflowExecution.GetWorkflowId(),
				tCtx.workflowExecution.GetRunId(),
				tCtx.info.RunID,
				tCtx.info.ScheduleID,
				taskListName,
				err)
			continue pollLoop
		}
		tCtx.completeTask(nil)
		return e.createPollForDecisionTaskResponse(tCtx, resp), nil
	}
}

// pollForActivityTaskOperation takes one task from the task manager, update workflow execution history, mark task as
// completed and return it to user. If a task from task manager is already started, return an empty response, without
// error. Timeouts handled by the timer queue.
func (e *matchingEngineImpl) PollForActivityTask(request *workflow.PollForActivityTaskRequest) (
	*workflow.PollForActivityTaskResponse, error) {
	taskListName := request.GetTaskList().GetName()
	e.logger.Debugf("Received PollForActivityTask for taskList=%v", taskListName)
pollLoop:
	for {
		taskList := newTaskListID(taskListName, persistence.TaskListTypeActivity)
		tCtx, err := e.getTask(taskList)
		if err != nil {
			if err == ErrNoTasks {
				return emptyPollForActivityTaskResponse, nil
			}
			return nil, err
		}
		resp, err := tCtx.RecordActivityTaskStartedWithRetry(&h.RecordActivityTaskStartedRequest{
			WorkflowExecution: &tCtx.workflowExecution,
			ScheduleId:        &tCtx.info.ScheduleID,
			TaskId:            &tCtx.info.TaskID,
			PollRequest:       request,
		})
		if err != nil {
			if _, ok := err.(*workflow.EntityNotExistsError); ok {
				e.logger.Debugf("Duplicated activity task taskList=%v, taskID=%v",
					taskListName, tCtx.info.TaskID)
				tCtx.completeTask(nil)
				continue pollLoop // Duplicated or cancelled task
			}
			// This essentially looses task when it was loaded from persistence.
			tCtx.completeTask(err)
			// TODO: Implement task retries on intermittent history service failures
			// TODO: Stop loosing tasks if history service is not available
			e.logger.Errorf("Lost activity task (workflowID=%v, runID=%v, scheduleID=%v, taskList=%v, taskID=%v) "+
				"due to error from historyService.RecordActivityTaskStarted: %v",
				tCtx.workflowExecution.WorkflowId,
				tCtx.workflowExecution.RunId,
				tCtx.info.RunID,
				tCtx.info.ScheduleID,
				taskListName,
				err)
			continue pollLoop
		}
		tCtx.completeTask(nil)
		return e.createPollForActivityTaskResponse(tCtx, resp), nil
	}
}

// Loads a task from persistence and wraps it in a task context
func (e *matchingEngineImpl) getTask(taskList *taskListID) (*taskContext, error) {
	tlCtx, err := e.getTaskListContext(taskList)
	if err != nil {
		return nil, err
	}
	return tlCtx.getTaskContext()
}

func (e *matchingEngineImpl) unloadTaskList(id *taskListID) {
	e.taskListsLock.Lock()
	tlCtx, ok := e.taskLists[*id]
	if ok {
		delete(e.taskLists, *id)
	}
	e.taskListsLock.Unlock()
	if ok {
		tlCtx.Stop()
	}
}

// Populate the decision task response based on context and scheduled/started events.
func (e *matchingEngineImpl) createPollForDecisionTaskResponse(context *taskContext,
	historyResponse *h.RecordDecisionTaskStartedResponse) *workflow.PollForDecisionTaskResponse {
	task := context.info

	response := workflow.NewPollForDecisionTaskResponse()
	response.WorkflowExecution = workflowExecutionPtr(context.workflowExecution)
	token := &common.TaskToken{
		WorkflowID: task.WorkflowID,
		RunID:      task.RunID,
		ScheduleID: task.ScheduleID,
	}
	response.TaskToken, _ = e.tokenSerializer.Serialize(token)
	response.WorkflowType = historyResponse.GetWorkflowType()
	if historyResponse.GetPreviousStartedEventId() != common.EmptyEventID {
		response.PreviousStartedEventId = historyResponse.PreviousStartedEventId
	}
	response.StartedEventId = historyResponse.StartedEventId
	response.History = historyResponse.History

	return response
}

// Populate the activity task response based on context and scheduled/started events.
func (e *matchingEngineImpl) createPollForActivityTaskResponse(context *taskContext,
	historyResponse *h.RecordActivityTaskStartedResponse) *workflow.PollForActivityTaskResponse {
	task := context.info

	startedEvent := historyResponse.StartedEvent
	scheduledEvent := historyResponse.ScheduledEvent
	if !scheduledEvent.IsSetActivityTaskScheduledEventAttributes() {
		panic("GetActivityTaskScheduledEventAttributes is not set")
	}
	attributes := scheduledEvent.GetActivityTaskScheduledEventAttributes()
	if !attributes.IsSetActivityId() {
		panic("ActivityTaskScheduledEventAttributes.ActivityID is not set")
	}

	response := workflow.NewPollForActivityTaskResponse()
	response.ActivityId = attributes.ActivityId
	response.ActivityType = attributes.GetActivityType()
	response.Input = attributes.GetInput()
	response.StartedEventId = common.Int64Ptr(startedEvent.GetEventId())
	response.WorkflowExecution = workflowExecutionPtr(context.workflowExecution)

	token := &common.TaskToken{
		WorkflowID: task.WorkflowID,
		RunID:      task.RunID,
		ScheduleID: task.ScheduleID,
	}
	response.TaskToken, _ = e.tokenSerializer.Serialize(token)
	e.logger.Debugf("matchingEngineImpl.createPollForActivityTaskResponse=%v", token)
	return response
}

func (c *taskListContext) getRangeID() int64 {
	c.Lock()
	defer c.Unlock()
	return c.rangeID
}

// returns false if rangeID differs from the current range
func (c *taskListContext) isEqualRangeID(rangeID int64) bool {
	c.Lock()
	defer c.Unlock()
	return c.rangeID == rangeID
}

// Starts reading pump for the given task list.
// The pump fills up taskBuffer from persistence.
func (c *taskListContext) Start() {
	go func() {
		defer close(c.taskBuffer)
		retrier := backoff.NewRetrier(emptyGetTasksRetryPolicy, backoff.SystemClock)
	getTasksPumpLoop:
		for {
			rangeID := c.getRangeID()
			tasks, err := c.getTaskBatch()
			if err != nil {
				if _, ok := err.(*persistence.ConditionFailedError); ok { // range changed
					if !c.isEqualRangeID(rangeID) { // Could be still owning next range
						continue getTasksPumpLoop
					}
				}
				logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationGetTasks, err,
					fmt.Sprintf("{taskType: %v, taskList: %v}", c.taskListID.taskType, c.taskListID.taskListName))
				break getTasksPumpLoop
			}
			// Exponential sleep on empty poll
			if len(tasks) == 0 {
				var next time.Duration
				if next = retrier.NextBackOff(); next != done {
					time.Sleep(next)
				}
				continue getTasksPumpLoop
			}
			retrier.Reset()
			c.Lock()
			for _, t := range tasks {
				c.taskAckManager.addTask(t.TaskID)
			}
			c.Unlock()

			for _, t := range tasks {
				select {
				case c.taskBuffer <- t:
				case <-c.shutdownCh:
					break getTasksPumpLoop
				}
			}
		}
	}()
}

// Stops pump that fills up taskBuffer from persistence.
func (c *taskListContext) Stop() {
	if !atomic.CompareAndSwapInt32(&c.stopped, 0, 1) {
		return
	}
	close(c.shutdownCh)
}

// initiateTaskAppend returns taskID to use to persist the task
func (c *taskListContext) initiateTaskAppend(e *matchingEngineImpl) (taskID int64, err error) {
	c.Lock()
	defer c.Unlock()
	err = c.updateRangeIfNeededLocked(e)
	if err != nil {
		return -1, err
	}
	taskID = c.taskSequenceNumber
	c.taskSequenceNumber++
	c.writeOffsetManager.addTask(taskID)
	return
}

// completeTaskAppend should be called after task append is done even if append has failed.
// There is no correspondent initiateTaskAppend as append is initiated in getTaskID
func (c *taskListContext) completeTaskAppend(taskID int64) (ackLevel int64) {
	c.Lock()
	defer c.Unlock()
	return c.writeOffsetManager.completeTask(taskID)
}

// completeTaskPoll should be called after task poll is done even if append has failed.
// There is no correspondent initiateTaskAppend as append is initiated in getTaskID
func (c *taskListContext) completeTaskPoll(taskID int64) {
	c.Lock()
	defer c.Unlock()
	c.taskAckManager.completeTask(taskID)
}

// Loads a task from DB or from sync match and wraps it in a task context
func (c *taskListContext) getTaskContext() (*taskContext, error) {
	result, err := c.getTask()
	if err != nil {
		return nil, err
	}
	task := result.task
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}
	tCtx := &taskContext{
		info:              task,
		workflowExecution: workflowExecution,
		tlCtx:             c,
		syncResponseCh:    result.C, // nil if task is loaded from persistence
	}
	return tCtx, nil
}

// Loads task from taskBuffer (which is populated from persistence) or from sync match to add task call
func (c *taskListContext) getTask() (*getTaskResult, error) {
	timer := time.NewTimer(c.engine.longPollExpirationInterval)
	defer timer.Stop()
	select {
	case task, ok := <-c.taskBuffer:
		if !ok { // Task list getTasks pump is shutdown
			return nil, errPumpClosed
		}
		return &getTaskResult{task: task}, nil
	case resultFromSyncMatch := <-c.syncMatch:
		return resultFromSyncMatch, nil
	case <-timer.C:
		return nil, ErrNoTasks
	}
}

// Returns a batch of tasks from persistence starting form current read level.
func (c *taskListContext) getTaskBatch() ([]*persistence.TaskInfo, error) {
	response, err := c.executeWithRetry(func(rangeID int64) (interface{}, error) {
		c.Lock()
		request := &persistence.GetTasksRequest{
			TaskList:     c.taskListID.taskListName,
			TaskType:     c.taskListID.taskType,
			BatchSize:    getTasksBatchSize,
			RangeID:      c.rangeID,
			ReadLevel:    c.taskAckManager.getReadLevel(),
			MaxReadLevel: c.writeOffsetManager.getAckLevel(),
		}
		c.Unlock()
		return c.engine.taskManager.GetTasks(request)
	})
	if err != nil {
		return nil, err
	}
	return response.(*persistence.GetTasksResponse).Tasks, err
}

func (c *taskListContext) updateRangeIfNeeded(e *matchingEngineImpl) error {
	c.Lock()
	defer c.Unlock()
	return c.updateRangeIfNeededLocked(e)
}

// Check current sequence number and if it is on the range boundary performs conditional update on
// persistence to grab the next range. Then updates sequence number and read offset to match the new range.
func (c *taskListContext) updateRangeIfNeededLocked(e *matchingEngineImpl) error {
	if c.taskSequenceNumber < c.nextRangeSequenceNumber { // also works for initial values of 0
		return nil
	}
	var resp *persistence.LeaseTaskListResponse
	op := func() (err error) {
		resp, err = e.taskManager.LeaseTaskList(&persistence.LeaseTaskListRequest{
			TaskList: c.taskListID.taskListName,
			TaskType: c.taskListID.taskType,
		})
		return
	}
	err := backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)

	if err != nil {
		c.engine.unloadTaskList(c.taskListID)
		return err
	}

	tli := resp.TaskListInfo
	c.rangeID = tli.RangeID // Starts from 1
	c.taskAckManager.setAckLevel(tli.AckLevel)
	c.taskSequenceNumber = (tli.RangeID-1)*e.rangeSize + 1
	c.writeOffsetManager.setAckLevel(c.taskSequenceNumber - 1) // maxReadLevel is inclusive
	c.nextRangeSequenceNumber = (tli.RangeID)*e.rangeSize + 1
	c.logger.Debugf("updateRangeLocked c.taskSequenceNumber=%v", c.taskSequenceNumber)
	return nil
}

// Tries to match task to a poller that is already waiting on getTask.
// When this method returns non nil response without error it is guaranteed that the task is started
// and sent to a poller. So it not necessary to persist it.
// Returns (nil, nil) if there is no waiting poller which indicates that task has to be persisted.
func (c *taskListContext) trySyncMatch(task *persistence.TaskInfo) (*persistence.CreateTaskResponse, error) {
	// Request from the point of view of Add(Activity|Decision)Task operation.
	// But it is getTask result from the point of view of a poll operation.
	request := &getTaskResult{task: task, C: make(chan *syncMatchResponse, 1)}
	select {
	case c.syncMatch <- request: // poller goroutine picked up the task
		r := <-request.C
		return r.response, r.err
	default: // no poller waiting for tasks
		return nil, nil
	}
}

// Retry operation on transient error and on rangeID change.
func (c *taskListContext) executeWithRetry(operation func(rangeID int64) (interface{}, error)) (result interface{}, err error) {
	var rangeID int64
	op := func() error {
		rangeID = c.getRangeID()
		result, err = operation(rangeID)
		return err
	}

	err = backoff.Retry(op, persistenceOperationRetryPolicy, func(err error) bool {
		// Operation failed due to invalid range, but this task list has a different rangeID as well.
		// Retry as the failure could be due to a rangeID update by this task list instance.
		if _, ok := err.(*persistence.ConditionFailedError); ok && !c.isEqualRangeID(rangeID) {
			return true
		}
		return common.IsPersistenceTransientError(err)
	})
	return
}

func (c *taskContext) RecordDecisionTaskStartedWithRetry(
	request *h.RecordDecisionTaskStartedRequest) (resp *h.RecordDecisionTaskStartedResponse, err error) {
	op := func() error {
		var err error
		resp, err = c.tlCtx.engine.historyService.RecordDecisionTaskStarted(request)
		return err
	}
	err = backoff.Retry(op, historyServiceOperationRetryPolicy, func(err error) bool {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			return true
		}
		return false
	})
	return
}

func (c *taskContext) RecordActivityTaskStartedWithRetry(
	request *h.RecordActivityTaskStartedRequest) (resp *h.RecordActivityTaskStartedResponse, err error) {
	op := func() error {
		var err error
		resp, err = c.tlCtx.engine.historyService.RecordActivityTaskStarted(request)
		return err
	}
	err = backoff.Retry(op, historyServiceOperationRetryPolicy, func(err error) bool {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			return true
		}
		return false
	})
	return
}

// If poll received task from addTask directly the addTask goroutine is notified about start task result.
// If poll received task from persistence then task is deleted from it if no error was reported.
func (c *taskContext) completeTask(err error) {
	tlCtx := c.tlCtx
	c.tlCtx.logger.Debugf("completeTask task taskList=%v, taskID=%v, err=%v",
		tlCtx.taskListID.taskListName, c.info.TaskID, err)
	if c.syncResponseCh != nil {
		// It is OK to succeed task creation as it was already completed
		c.syncResponseCh <- &syncMatchResponse{
			response: &persistence.CreateTaskResponse{}, err: err}
	}

	if err != nil {
		return
	}
	ackLevel := tlCtx.completeTaskAppend(c.info.TaskID)

	_, err = tlCtx.executeWithRetry(func(rangeID int64) (interface{}, error) {
		return nil, tlCtx.engine.taskManager.CompleteTask(&persistence.CompleteTaskRequest{
			TaskList: &persistence.TaskListInfo{
				Name:     tlCtx.taskListID.taskListName,
				TaskType: tlCtx.taskListID.taskType,
				AckLevel: ackLevel,
				RangeID:  rangeID,
			},
			TaskID: c.info.TaskID,
		})
	})
	if err != nil {
		logPersistantStoreErrorEvent(tlCtx.logger, tagValueStoreOperationCompleteTask, err,
			fmt.Sprintf("{taskID: %v, taskType: %v, taskList: %v}",
				c.info.TaskID, tlCtx.taskListID.taskType, tlCtx.taskListID.taskListName))
	}
}

// Registers task as in-flight and moves read level to it. Tasks can be added in increasing order of taskID only.
func (m *ackManager) addTask(taskID int64) {
	if m.readLevel >= taskID {
		m.logger.Fatalf("Next task ID is less than current read level.  TaskID: %v, ReadLevel: %v", taskID,
			m.readLevel)
	}
	m.readLevel = taskID
	m.outstandingTasks[taskID] = false
}

func newAckManager(logger bark.Logger) ackManager {
	return ackManager{logger: logger, outstandingTasks: make(map[int64]bool), readLevel: -1, ackLevel: -1}
}

func (m *ackManager) getReadLevel() int64 {
	return m.readLevel
}

func (m *ackManager) getAckLevel() int64 {
	return m.ackLevel
}

// Moves ack level to the new level if it is higher than the current one.
// Also updates the read level it is lower than the ackLevel.
func (m *ackManager) setAckLevel(ackLevel int64) {
	if ackLevel > m.ackLevel {
		m.ackLevel = ackLevel
	}
	if ackLevel > m.readLevel {
		m.readLevel = ackLevel
	}
}

func (m *ackManager) completeTask(taskID int64) (ackLevel int64) {
	if _, ok := m.outstandingTasks[taskID]; ok {
		m.outstandingTasks[taskID] = true
	}
	// Update ackLevel
	for current := m.ackLevel + 1; current <= m.readLevel; current++ {
		if acked, ok := m.outstandingTasks[current]; ok {
			if acked {
				m.ackLevel = current
				delete(m.outstandingTasks, current)
			} else {
				return
			}
		}
	}
	return m.ackLevel
}

func newTaskListID(taskListName string,
	taskType int) *taskListID {
	return &taskListID{taskListName: taskListName, taskType: taskType}
}

func createEmptyGetTasksRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(emptyGetRetryInitialInterval)
	policy.SetMaximumInterval(emptyGetRetryMaxInterval)
	policy.SetExpirationInterval(time.Duration(math.MaxInt64)) // keep retrying forever

	return policy
}

func isLongPollRetryableError(err error) bool {
	if err == ErrNoTasks {
		return true
	}

	// Any errors from history service that can be retriable as well.
	switch err.(type) {
	case *workflow.EntityNotExistsError:
		return true
	}

	return false
}

func workflowExecutionPtr(execution workflow.WorkflowExecution) *workflow.WorkflowExecution {
	return &execution
}
