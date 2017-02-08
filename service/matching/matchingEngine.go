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
	taskListID        *taskListID
	info              *persistence.TaskInfo
	workflowExecution workflow.WorkflowExecution
	matchingEngine    *matchingEngineImpl
	logger            bark.Logger
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
	taskList   *taskListID
	logger     bark.Logger
	engine     *matchingEngineImpl
	taskBuffer chan *persistence.TaskInfo
	shutdownCh chan struct{}
	stopped    int32

	sync.Mutex
	taskAckManager          ackManager // tracks ackLevel for delivered messages
	writeOffsetManager      ackManager // tracks maxReadLevel for out of order message puts
	rangeID                 int64      // Current range of the task list. Starts from 1.
	taskSequenceNumber      int64      // Sequence number of the next task. Starts from 1.
	nextRangeSequenceNumber int64      // Current range boundary
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
	emptyPollForActivityTaskResponse = workflow.NewPollForActivityTaskResponse()
	persistenceOperationRetryPolicy  = common.CreatePersistanceRetryPolicy()
	emptyGetTasksRetryPolicy         = createEmptyGetTasksRetryPolicy()
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
		taskList:           taskList,
		logger:             e.logger,
		taskAckManager:     newAckManager(e.logger),
		writeOffsetManager: newAckManager(e.logger),
	}
	e.taskListsLock.Lock()
	if result, ok := e.taskLists[*taskList]; ok {
		e.taskListsLock.Unlock()
		return result, nil
	}
	e.taskLists[*taskList] = ctx
	e.taskListsLock.Unlock()

	ctx.Lock()
	err := ctx.updateRange(e) // Grabs a new range and updates read and ackLevels
	ctx.Unlock()
	if err != nil {
		return nil, err
	}
	ctx.Start()
	return ctx, nil
}

func (e *matchingEngineImpl) AddDecisionTask(addRequest *m.AddDecisionTaskRequest) error {
	e.logger.Debugf("Received AddDecisionTask for taskList=%v", addRequest.TaskList.Name)
	taskList := newTaskListID(addRequest.TaskList.GetName(), persistence.TaskListTypeDecision)
	t, err := e.getTaskListContext(taskList)
	if err != nil {
		return err
	}
	_, err = t.executeWithRetry(func(rangeID int64) (interface{}, error) {
		t.Lock()
		taskID, err := t.getTaskID(e)
		if err != nil {
			t.Unlock()
			return nil, err
		}
		t.writeOffsetManager.addTask(taskID)
		t.Unlock()

		task := &persistence.DecisionTask{
			TaskList:   addRequest.TaskList.GetName(),
			ScheduleID: addRequest.GetScheduleId(),
			TaskID:     taskID,
		}

		r, err := t.engine.taskManager.CreateTask(&persistence.CreateTaskRequest{
			Execution: *addRequest.Execution,
			Data:      task,
			TaskID:    task.TaskID,
			RangeID:   rangeID,
		})
		t.Lock()
		t.writeOffsetManager.completeTask(taskID)
		t.Unlock()

		return r, err
	})
	return err
}

func (e *matchingEngineImpl) AddActivityTask(addRequest *m.AddActivityTaskRequest) error {
	e.logger.Debugf("Received AddActivityTask for taskList=%v", addRequest.TaskList.Name)
	taskList := newTaskListID(addRequest.TaskList.GetName(), persistence.TaskListTypeActivity)
	t, err := e.getTaskListContext(taskList)
	if err != nil {
		return err
	}
	_, err = t.executeWithRetry(func(rangeID int64) (interface{}, error) {
		t.Lock()
		taskID, err := t.getTaskID(e)
		if err != nil {
			t.Unlock()
			return nil, err
		}
		t.writeOffsetManager.addTask(taskID)
		t.Unlock()

		task := &persistence.ActivityTask{
			TaskList:   addRequest.TaskList.GetName(),
			ScheduleID: addRequest.GetScheduleId(),
			TaskID:     taskID,
		}

		r, err := t.engine.taskManager.CreateTask(&persistence.CreateTaskRequest{
			Execution: *addRequest.Execution,
			Data:      task,
			TaskID:    task.TaskID,
			RangeID:   rangeID,
		})
		t.Lock()
		t.writeOffsetManager.completeTask(taskID)
		t.Unlock()

		return r, err
	})
	return err
}

// PollForDecisionTask tries to get the decision task using exponential backoff.
func (e *matchingEngineImpl) PollForDecisionTask(request *workflow.PollForDecisionTaskRequest) (
	*workflow.PollForDecisionTaskResponse, error) {
	for {
		taskList := newTaskListID(request.TaskList.GetName(), persistence.TaskListTypeDecision)
		tCtx, tlCtx, err := e.getTask(taskList)
		if err != nil {
			if err == ErrNoTasks {
				return emptyPollForDecisionTaskResponse, nil
			}
			return nil, err
		}
		resp, err := e.historyService.RecordDecisionTaskStarted(&h.RecordDecisionTaskStartedRequest{
			WorkflowExecution: &tCtx.workflowExecution,
			ScheduleId:        &tCtx.info.ScheduleID,
			TaskId:            &tCtx.info.TaskID,
			PollRequest:       request,
		})
		if err != nil {
			if _, ok := err.(*workflow.EntityNotExistsError); ok {
				e.logger.Debugf("Duplicated decision task taskList=%v, taskID=",
					request.TaskList.Name, tCtx.info.TaskID)
				tlCtx.completeTask(tCtx)
				continue // Duplicated or cancelled task
			}
			// This essentially looses task.
			// TODO: Implement task retries on intermittent history service failures
			// TODO: Stop loosing tasks if history service is not available
			tlCtx.completeTask(tCtx)
			e.logger.Errorf("Lost decision task (workflowID=%v, runID=%v, scheduleID=%v, taskList=%v, taskID=%v) "+
				"due to error from historyService.RecordDecisionTaskStarted: %v",
				tCtx.workflowExecution.WorkflowId,
				tCtx.workflowExecution.RunId,
				tCtx.info.RunID,
				tCtx.info.ScheduleID,
				taskList.taskListName,
				err)
			continue
		}
		tlCtx.completeTask(tCtx)
		return e.createPollForDecisionTaskResponse(tCtx, resp), nil
	}
}

// pollForActivityTaskOperation takes one task from the task manager, update workflow execution history, mark task as
// completed and return it to user. If a task from task manager is already started, return an empty response, without
// error. Timeouts handled by the timer queue.
func (e *matchingEngineImpl) PollForActivityTask(request *workflow.PollForActivityTaskRequest) (
	*workflow.PollForActivityTaskResponse, error) {
	e.logger.Debugf("Received PollForActivityTask for taskList=%v", request.TaskList.Name)
	for {
		taskList := newTaskListID(request.TaskList.GetName(), persistence.TaskListTypeActivity)
		tCtx, tlCtx, err := e.getTask(taskList)
		if err != nil {
			if err == ErrNoTasks {
				return emptyPollForActivityTaskResponse, nil
			}
			return nil, err
		}
		resp, err := e.historyService.RecordActivityTaskStarted(&h.RecordActivityTaskStartedRequest{
			WorkflowExecution: &tCtx.workflowExecution,
			ScheduleId:        &tCtx.info.ScheduleID,
			TaskId:            &tCtx.info.TaskID,
			PollRequest:       request,
		})
		if err != nil {
			if _, ok := err.(*workflow.EntityNotExistsError); ok {
				e.logger.Debugf("Duplicated activity task taskList=%v, taskID=%v",
					request.TaskList.Name, tCtx.info.TaskID)
				tlCtx.completeTask(tCtx)
				continue // Duplicated or cancelled task
			}
			// This essentially looses task.
			// TODO: Implement task retries on intermittent history service failures
			// TODO: Stop loosing tasks if history service is not available
			tlCtx.completeTask(tCtx)
			e.logger.Errorf("Lost activity task (workflowID=%v, runID=%v, scheduleID=%v, taskList=%v, taskID=%v) "+
				"due to error from historyService.RecordActivityTaskStarted: %v",
				tCtx.workflowExecution.WorkflowId,
				tCtx.workflowExecution.RunId,
				tCtx.info.RunID,
				tCtx.info.ScheduleID,
				taskList.taskListName,
				err)
			continue
		}
		tlCtx.completeTask(tCtx)
		return e.createPollForActivityTaskResponse(tCtx, resp), nil
	}
}

// Load a task from DB and wraps it in a task context
func (e *matchingEngineImpl) getTask(taskList *taskListID) (*taskContext, *taskListContext, error) {
	tlCtx, err := e.getTaskListContext(taskList)
	if err != nil {
		return nil, nil, err
	}

	task, err := tlCtx.getTask()

	if err != nil {
		return nil, nil, err
	}
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}

	tCtx := newTaskContext(e, task, tlCtx.taskList, workflowExecution, e.logger)
	return tCtx, tlCtx, nil
}

func (e *matchingEngineImpl) unloadTaskList(id *taskListID) {
	var tlCtx *taskListContext
	e.taskListsLock.Lock()
	tlCtx = e.taskLists[*id]
	delete(e.taskLists, *id)
	e.taskListsLock.Unlock()
	tlCtx.Stop()
}

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

func newTaskContext(matchingEngine *matchingEngineImpl, info *persistence.TaskInfo, taskListID *taskListID,
	execution workflow.WorkflowExecution, logger bark.Logger) *taskContext {
	return &taskContext{
		info:              info,
		matchingEngine:    matchingEngine,
		workflowExecution: execution,
		logger:            logger,
		taskListID:        taskListID,
	}
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

// Starts reading pump for the given task list
func (c *taskListContext) Start() {
	go func() {
		defer close(c.taskBuffer)
		retrier := backoff.NewRetrier(emptyGetTasksRetryPolicy, backoff.SystemClock)
	getTasksPumpLoop:
		for {
			rangeID := c.getRangeID()
			tasks, err := c.getTaskBatch()
			if err != nil {
				if _, ok := err.(*persistence.ConditionFailedError); ok {
					if !c.isEqualRangeID(rangeID) { // Could be still owning next lease
						continue
					}
				}
				logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationGetTasks, err,
					fmt.Sprintf("{taskType: %v, taskList: %v}", c.taskList.taskType, c.taskList.taskListName))
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

func (c *taskListContext) Stop() {
	if !atomic.CompareAndSwapInt32(&c.stopped, 0, 1) {
		return
	}
	close(c.shutdownCh)
}

func (c *taskListContext) getTask() (*persistence.TaskInfo, error) {
	timer := time.NewTimer(c.engine.longPollExpirationInterval)
	defer timer.Stop()
	select {
	case result, ok := <-c.taskBuffer:
		if !ok { // Task list getTasks pump is shutdown
			return nil, errPumpClosed
		}
		return result, nil
	case <-timer.C:
		return nil, ErrNoTasks
	}
}

func (c *taskListContext) getTaskBatch() ([]*persistence.TaskInfo, error) {
	response, err := c.executeWithRetry(func(rangeID int64) (interface{}, error) {
		c.Lock()
		request := &persistence.GetTasksRequest{
			TaskList:     c.taskList.taskListName,
			TaskType:     c.taskList.taskType,
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

func (c *taskListContext) getTaskID(e *matchingEngineImpl) (int64, error) {
	err := c.updateRange(e)
	if err != nil {
		return -1, err
	}
	result := c.taskSequenceNumber
	c.taskSequenceNumber++
	return result, nil
}

func (c *taskListContext) updateRange(e *matchingEngineImpl) error {
	if c.taskSequenceNumber < c.nextRangeSequenceNumber { // also works for initial values of 0
		return nil
	}
	var resp *persistence.LeaseTaskListResponse
	op := func() (err error) {
		resp, err = e.taskManager.LeaseTaskList(&persistence.LeaseTaskListRequest{
			TaskList: c.taskList.taskListName,
			TaskType: c.taskList.taskType,
		})
		return
	}
	err := backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)

	if err != nil {
		c.engine.unloadTaskList(c.taskList)
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

func (c *taskListContext) completeTask(taskContext *taskContext) error {
	taskContext.logger.Debugf("completeTask task taskList=%v, taskID=%v",
		taskContext.taskListID.taskListName, taskContext.info.TaskID)
	c.Lock()
	ackLevel := c.taskAckManager.completeTask(taskContext.info.TaskID)
	c.Unlock()
	_, err := c.executeWithRetry(func(rangeID int64) (interface{}, error) {
		return nil, c.engine.taskManager.CompleteTask(&persistence.CompleteTaskRequest{
			TaskList: &persistence.TaskListInfo{
				Name:     taskContext.taskListID.taskListName,
				TaskType: taskContext.taskListID.taskType,
				AckLevel: ackLevel,
				RangeID:  rangeID,
			},
			TaskID: taskContext.info.TaskID,
		})
	})
	if err != nil {
		logPersistantStoreErrorEvent(taskContext.logger, tagValueStoreOperationCompleteTask, err,
			fmt.Sprintf("{taskID: %v, taskType: %v, taskList: %v}",
				taskContext.info.TaskID, taskContext.taskListID.taskType, taskContext.taskListID.taskListName))
	}

	return err
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
