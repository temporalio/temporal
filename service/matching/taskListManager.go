package matching

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	h "github.com/uber/cadence/.gen/go/history"
	s "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/tchannel-go/thrift"
	"golang.org/x/net/context"
)

const (
	defaultRangeSize  = 100000
	getTasksBatchSize = 100
	// To perform one db operation if there are no pollers
	taskBufferSize    = getTasksBatchSize - 1
	updateAckInterval = 10 * time.Second

	done time.Duration = -1
)

type taskListManager interface {
	Start() error
	Stop()
	AddTask(execution *s.WorkflowExecution, taskInfo *persistence.TaskInfo) error
	GetTaskContext(ctx thrift.Context) (*taskContext, error)
	String() string
}

func newTaskListManager(e *matchingEngineImpl, taskList *taskListID) taskListManager {
	tlMgr := &taskListManagerImpl{
		engine:     e,
		taskBuffer: make(chan *persistence.TaskInfo, taskBufferSize),
		notifyCh:   make(chan struct{}, 1),
		shutdownCh: make(chan struct{}),
		taskListID: taskList,
		logger: e.logger.WithFields(bark.Fields{
			tagTaskListType: taskList.taskType,
			tagTaskListName: taskList.taskListName,
		}),
		taskAckManager: newAckManager(e.logger),
		syncMatch:      make(chan *getTaskResult),
	}
	tlMgr.taskWriter = newTaskWriter(tlMgr, tlMgr.shutdownCh)
	return tlMgr
}

// Contains information needed for current task transition from queue to Workflow execution history.
type taskContext struct {
	tlMgr             *taskListManagerImpl
	info              *persistence.TaskInfo
	syncResponseCh    chan<- *syncMatchResponse
	workflowExecution s.WorkflowExecution
}

// Single task list in memory state
type taskListManagerImpl struct {
	taskListID *taskListID
	logger     bark.Logger
	engine     *matchingEngineImpl
	taskWriter *taskWriter
	taskBuffer chan *persistence.TaskInfo // tasks loaded from persistence
	// Sync channel used to perform sync matching.
	// It must to be unbuffered. addTask publishes to it asynchronously and expects publish to succeed
	// only if there is waiting poll that consumes from it.
	syncMatch  chan *getTaskResult
	notifyCh   chan struct{} // Used as signal to notify pump of new tasks
	shutdownCh chan struct{} // Delivers stop to the pump that populates taskBuffer
	stopped    int32

	sync.Mutex
	taskAckManager          ackManager // tracks ackLevel for delivered messages
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
	response *persistence.CreateTasksResponse
	err      error
}

// Starts reading pump for the given task list.
// The pump fills up taskBuffer from persistence.
func (c *taskListManagerImpl) Start() error {
	err := c.updateRangeIfNeeded() // Grabs a new range and updates read and ackLevels
	if err != nil {
		return err
	}
	c.taskWriter.Start()
	c.signalNewTask()
	go c.getTasksPump()
	return nil
}

// Stops pump that fills up taskBuffer from persistence.
func (c *taskListManagerImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&c.stopped, 0, 1) {
		return
	}
	c.logger.Infof("Unloaded %v", c.taskListID)
	close(c.shutdownCh)
	c.engine.removeTaskListManager(c.taskListID)
}

func (c *taskListManagerImpl) AddTask(execution *s.WorkflowExecution, taskInfo *persistence.TaskInfo) error {
	_, err := c.executeWithRetry(func(rangeID int64) (interface{}, error) {
		r, err := c.trySyncMatch(taskInfo)
		if err != nil || r != nil {
			return r, err
		}

		r, err = c.taskWriter.appendTask(execution, taskInfo, rangeID)
		return r, err
	})
	if err == nil {
		c.signalNewTask()
	}
	return err
}

// Loads a task from DB or from sync match and wraps it in a task context
func (c *taskListManagerImpl) GetTaskContext(ctx thrift.Context) (*taskContext, error) {
	result, err := c.getTask(ctx)
	if err != nil {
		return nil, err
	}
	task := result.task
	workflowExecution := s.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}
	tCtx := &taskContext{
		info:              task,
		workflowExecution: workflowExecution,
		tlMgr:             c,
		syncResponseCh:    result.C, // nil if task is loaded from persistence
	}
	return tCtx, nil
}

func (c *taskListManagerImpl) getRangeID() int64 {
	c.Lock()
	defer c.Unlock()
	return c.rangeID
}

// returns false if rangeID differs from the current range
func (c *taskListManagerImpl) isEqualRangeID(rangeID int64) bool {
	c.Lock()
	defer c.Unlock()
	return c.rangeID == rangeID
}

// TODO: Update ackLevel on time based interval instead of on each GetTasks call
func (c *taskListManagerImpl) persistAckLevel() error {
	c.Lock()
	updateTaskListRequest := &persistence.UpdateTaskListRequest{
		TaskListInfo: &persistence.TaskListInfo{
			DomainID: c.taskListID.domainID,
			Name:     c.taskListID.taskListName,
			TaskType: c.taskListID.taskType,
			AckLevel: c.taskAckManager.getAckLevel(),
			RangeID:  c.rangeID,
		},
	}
	c.Unlock()
	_, err := c.engine.taskManager.UpdateTaskList(updateTaskListRequest)
	return err
}

// newTaskIDs taskID to use to persist the task
func (c *taskListManagerImpl) newTaskIDs(count int) (taskID []int64, err error) {
	c.Lock()
	defer c.Unlock()
	for i := 0; i < count; i++ {
		err = c.updateRangeIfNeededLocked(c.engine)
		if err != nil {
			return nil, err
		}
		taskID = append(taskID, c.taskSequenceNumber)
		c.taskSequenceNumber++
	}
	return
}

func (c *taskListManagerImpl) getTaskSequenceNumber() int64 {
	c.Lock()
	defer c.Unlock()
	return c.taskSequenceNumber
}

func (c *taskListManagerImpl) getAckLevel() (ackLevel int64) {
	c.Lock()
	defer c.Unlock()
	return c.taskAckManager.getAckLevel()
}

// completeTaskPoll should be called after task poll is done even if append has failed.
// There is no correspondent initiateTaskPoll as append is initiated in getTasksPump
func (c *taskListManagerImpl) completeTaskPoll(taskID int64) (ackLevel int64) {
	c.Lock()
	defer c.Unlock()
	ackLevel = c.taskAckManager.completeTask(taskID)
	return
}

// Loads task from taskBuffer (which is populated from persistence) or from sync match to add task call
func (c *taskListManagerImpl) getTask(ctx thrift.Context) (*getTaskResult, error) {
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
	case <-ctx.Done():
		err := ctx.Err()
		if err == context.DeadlineExceeded {
			err = ErrNoTasks
		}
		return nil, err
	}
}

// Returns a batch of tasks from persistence starting form current read level.
func (c *taskListManagerImpl) getTaskBatch() ([]*persistence.TaskInfo, error) {
	response, err := c.executeWithRetry(func(rangeID int64) (interface{}, error) {
		c.Lock()
		request := &persistence.GetTasksRequest{
			DomainID:     c.taskListID.domainID,
			TaskList:     c.taskListID.taskListName,
			TaskType:     c.taskListID.taskType,
			BatchSize:    getTasksBatchSize,
			RangeID:      rangeID,
			ReadLevel:    c.taskAckManager.getReadLevel(),
			MaxReadLevel: c.taskWriter.GetMaxReadLevel(),
		}
		c.Unlock()
		return c.engine.taskManager.GetTasks(request)
	})
	if err != nil {
		return nil, err
	}
	return response.(*persistence.GetTasksResponse).Tasks, err
}

func (c *taskListManagerImpl) updateRangeIfNeeded() error {
	c.Lock()
	defer c.Unlock()
	return c.updateRangeIfNeededLocked(c.engine)
}

// Check current sequence number and if it is on the range boundary performs conditional update on
// persistence to grab the next range. Then updates sequence number and read offset to match the new range.
func (c *taskListManagerImpl) updateRangeIfNeededLocked(e *matchingEngineImpl) error {
	if c.taskSequenceNumber < c.nextRangeSequenceNumber { // also works for initial values of 0
		return nil
	}
	var resp *persistence.LeaseTaskListResponse
	op := func() (err error) {
		resp, err = e.taskManager.LeaseTaskList(&persistence.LeaseTaskListRequest{
			DomainID: c.taskListID.domainID,
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

	c.nextRangeSequenceNumber = (tli.RangeID)*e.rangeSize + 1
	c.logger.Debugf("updateRangeLocked rangeID=%v, c.taskSequenceNumber=%v, c.nextRangeSequenceNumber=%v",
		c.rangeID, c.taskSequenceNumber, c.nextRangeSequenceNumber)
	return nil
}

func (c *taskListManagerImpl) String() string {
	c.Lock()
	defer c.Unlock()

	var r string
	if c.taskListID.taskType == persistence.TaskListTypeActivity {
		r += "Activity"
	} else {
		r += "Decision"
	}
	r += " task list " + c.taskListID.taskListName + "\n"
	r += fmt.Sprintf("RangeID=%v\n", c.rangeID)
	r += fmt.Sprintf("TaskSequenceNumber=%v\n", c.taskSequenceNumber)
	r += fmt.Sprintf("NextRangeSequenceNumber=%v\n", c.nextRangeSequenceNumber)
	r += fmt.Sprintf("AckLevel=%v\n", c.taskAckManager.ackLevel)
	r += fmt.Sprintf("MaxReadLevel=%v\n", c.taskAckManager.getReadLevel())

	return r
}

// Tries to match task to a poller that is already waiting on getTask.
// When this method returns non nil response without error it is guaranteed that the task is started
// and sent to a poller. So it not necessary to persist it.
// Returns (nil, nil) if there is no waiting poller which indicates that task has to be persisted.
func (c *taskListManagerImpl) trySyncMatch(task *persistence.TaskInfo) (*persistence.CreateTasksResponse, error) {
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

func (c *taskListManagerImpl) getTasksPump() {
	defer close(c.taskBuffer)

	updateAckTimer := time.NewTimer(updateAckInterval)
	defer updateAckTimer.Stop()

getTasksPumpLoop:
	for {
		select {
		case <-c.shutdownCh:
			break getTasksPumpLoop
		case <-c.notifyCh:
			{
				tasks, err := c.getTaskBatch()
				if err != nil {
					logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationGetTasks, err,
						fmt.Sprintf("{taskType: %v, taskList: %v}",
							c.taskListID.taskType, c.taskListID.taskListName))
					c.signalNewTask() // re-enqueue the event
					// TODO: Should we ever stop retrying on db errors?
					continue getTasksPumpLoop
				}
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
		case <-updateAckTimer.C:
			{
				err := c.persistAckLevel()
				//var err error
				if err != nil {
					logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationUpdateTaskList, err,
						fmt.Sprintf("{taskType: %v, taskList: %v}",
							c.taskListID.taskType, c.taskListID.taskListName))
					// keep going as saving ack is not critical
				}
				c.signalNewTask() // periodically signal pump to check persistence for tasks
			}
		}
	}
}

// Retry operation on transient error and on rangeID change. On rangeID update by another process calls c.Stop().
func (c *taskListManagerImpl) executeWithRetry(
	operation func(rangeID int64) (interface{}, error)) (result interface{}, err error) {

	var rangeID int64
	op := func() error {
		rangeID = c.getRangeID()
		result, err = operation(rangeID)
		return err
	}

	var retryCount int64
	err = backoff.Retry(op, persistenceOperationRetryPolicy, func(err error) bool {
		c.logger.Debugf("Retry executeWithRetry as task list range has changed. retryCount=%v, errType=%T", retryCount, err)

		// Operation failed due to invalid range, but this task list has a different rangeID as well.
		// Retry as the failure could be due to a rangeID update by this task list instance.
		if _, ok := err.(*persistence.ConditionFailedError); ok {
			if c.isEqualRangeID(rangeID) {
				c.logger.Debug("Retry range id didn't change. stopping task list")
				return false
			}
			// Our range has changed.
			// Could be still owning the next range, so keep retrying.
			c.logger.Debugf("Retry executeWithRetry as task list range has changed. retryCount=%v, errType=%T", retryCount, err)
			retryCount++
			return true
		}
		return common.IsPersistenceTransientError(err)
	})

	if _, ok := err.(*persistence.ConditionFailedError); ok {
		c.logger.Debugf("Stopping task list due to persistence condition failure. Err: %v", err)
		c.Stop()
	}
	return
}

func (c *taskListManagerImpl) signalNewTask() {
	var event struct{}
	select {
	case c.notifyCh <- event:
	default: // channel already has an event, don't block
	}
}

func (c *taskContext) RecordDecisionTaskStartedWithRetry(
	request *h.RecordDecisionTaskStartedRequest) (resp *h.RecordDecisionTaskStartedResponse, err error) {
	op := func() error {
		var err error
		resp, err = c.tlMgr.engine.historyService.RecordDecisionTaskStarted(nil, request)
		return err
	}
	err = backoff.Retry(op, historyServiceOperationRetryPolicy, func(err error) bool {
		if _, ok := err.(*s.EntityNotExistsError); !ok {
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
		resp, err = c.tlMgr.engine.historyService.RecordActivityTaskStarted(nil, request)
		return err
	}
	err = backoff.Retry(op, historyServiceOperationRetryPolicy, func(err error) bool {
		if _, ok := err.(*s.EntityNotExistsError); !ok {
			return true
		}
		return false
	})
	return
}

// If poll received task from addTask directly the addTask goroutine is notified about start task result.
// If poll received task from persistence then task is deleted from it if no error was reported.
func (c *taskContext) completeTask(err error) {
	tlMgr := c.tlMgr
	tlMgr.logger.Debugf("completeTask task taskList=%v, taskID=%v, err=%v",
		tlMgr.taskListID.taskListName, c.info.TaskID, err)
	if c.syncResponseCh != nil {
		// It is OK to succeed task creation as it was already completed
		c.syncResponseCh <- &syncMatchResponse{
			response: &persistence.CreateTasksResponse{}, err: err}
		return
	}

	if err != nil {
		// failed to start the task.
		// We cannot just remove it from persistence because then it will be lost,
		// which is criticial for decision tasks since there have no ScheduleToStart timeout.
		// We handle this by writing the task back to persistence with a higher taskID.
		// This will allow subsequent tasks to make progress, and hopefully by the time this task is picked-up
		// again the underlying reason for failing to start will be resolved.
		// Note that RecordTaskStarted only fails after retrying for a long time, so a single task will not be
		// re-written to persistence frequently.
		_, err = tlMgr.executeWithRetry(func(rangeID int64) (interface{}, error) {
			return tlMgr.taskWriter.appendTask(&c.workflowExecution, c.info, rangeID)
		})

		if err != nil {
			// OK, we also failed to write to persistence.
			// This should only happen in very extreme cases where persistence is completely down.
			// We still can't lose the old task so we just unload the entire task list
			logPersistantStoreErrorEvent(tlMgr.logger, tagValueStoreOperationStopTaskList, err,
				fmt.Sprintf("task writer failed to write task. Unloading TaskList{taskType: %v, taskList: %v}",
					tlMgr.taskListID.taskType, tlMgr.taskListID.taskListName))
			tlMgr.Stop()
			return
		}
		tlMgr.signalNewTask()
	}

	tlMgr.completeTaskPoll(c.info.TaskID)

	// TODO: use range deletes to complete all tasks below ack level instead of completing
	// tasks one by one.
	err2 := tlMgr.engine.taskManager.CompleteTask(&persistence.CompleteTaskRequest{
		TaskList: &persistence.TaskListInfo{
			DomainID: tlMgr.taskListID.domainID,
			Name:     tlMgr.taskListID.taskListName,
			TaskType: tlMgr.taskListID.taskType,
		},
		TaskID: c.info.TaskID,
	})

	if err2 != nil {
		logPersistantStoreErrorEvent(tlMgr.logger, tagValueStoreOperationCompleteTask, err2,
			fmt.Sprintf("{taskID: %v, taskType: %v, taskList: %v}",
				c.info.TaskID, tlMgr.taskListID.taskType, tlMgr.taskListID.taskListName))
	}
}

func createServiceBusyError() *s.ServiceBusyError {
	err := s.NewServiceBusyError()
	err.Message = "Too many outstanding appends to the TaskList"
	return err
}
