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

package matching

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	h "github.com/uber/cadence/.gen/go/history"
	m "github.com/uber/cadence/.gen/go/matching"
	s "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"

	"github.com/uber-common/bark"
	"github.com/uber/cadence/common/cache"
	"golang.org/x/time/rate"
)

const (
	done time.Duration = -1
)

// NOTE: Is this good enough for stress tests?
const (
	_defaultTaskDispatchRPS    = 100000.0
	_defaultTaskDispatchRPSTTL = 60 * time.Second
)

var errAddTasklistThrottled = errors.New("cannot add to tasklist, limit exceeded")

type taskListManager interface {
	Start() error
	Stop()
	AddTask(execution *s.WorkflowExecution, taskInfo *persistence.TaskInfo) error
	GetTaskContext(ctx context.Context, maxDispatchPerSecond *float64) (*taskContext, error)
	SyncMatchQueryTask(ctx context.Context, queryTask *queryTaskInfo) error
	CancelPoller(pollerID string)
	GetAllPollerInfo() []*pollerInfo
	String() string
}

type taskListConfig struct {
	EnableSyncMatch func() bool
	// Time to hold a poll request before returning an empty response if there are no tasks
	LongPollExpirationInterval func() time.Duration
	RangeSize                  int64
	GetTasksBatchSize          func() int
	UpdateAckInterval          func() time.Duration
	IdleTasklistCheckInterval  func() time.Duration
	MinTaskThrottlingBurstSize func() int
	// taskWriter configuration
	OutstandingTaskAppendsThreshold func() int
	MaxTaskBatchSize                func() int
}

func newTaskListConfig(id *taskListID, config *Config, domainCache cache.DomainCache) (*taskListConfig, error) {
	domainEntry, err := domainCache.GetDomainByID(id.domainID)
	if err != nil {
		return nil, err
	}

	domain := domainEntry.GetInfo().Name
	taskListName := id.taskListName
	taskType := id.taskType
	return &taskListConfig{
		RangeSize: config.RangeSize,
		GetTasksBatchSize: func() int {
			return config.GetTasksBatchSize(domain, taskListName, taskType)
		},
		UpdateAckInterval: func() time.Duration {
			return config.UpdateAckInterval(domain, taskListName, taskType)
		},
		IdleTasklistCheckInterval: func() time.Duration {
			return config.IdleTasklistCheckInterval(domain, taskListName, taskType)
		},
		MinTaskThrottlingBurstSize: func() int {
			return config.MinTaskThrottlingBurstSize(domain, taskListName, taskType)
		},
		EnableSyncMatch: func() bool {
			return config.EnableSyncMatch(domain, taskListName, taskType)
		},
		LongPollExpirationInterval: func() time.Duration {
			return config.LongPollExpirationInterval(domain, taskListName, taskType)
		},
		OutstandingTaskAppendsThreshold: func() int {
			return config.OutstandingTaskAppendsThreshold(domain, taskListName, taskType)
		},
		MaxTaskBatchSize: func() int {
			return config.MaxTaskBatchSize(domain, taskListName, taskType)
		},
	}, nil
}

type rateLimiter struct {
	sync.RWMutex
	maxDispatchPerSecond *float64
	globalLimiter        atomic.Value
	// TTL is used to determine whether to update the limit. Until TTL, pick
	// lower(existing TTL, input TTL). After TTL, pick input TTL if different from existing TTL
	ttlTimer *time.Timer
	ttl      time.Duration
	minBurst int
}

func newRateLimiter(maxDispatchPerSecond *float64, ttl time.Duration, minBurst int) rateLimiter {
	rl := rateLimiter{
		maxDispatchPerSecond: maxDispatchPerSecond,
		ttl:                  ttl,
		ttlTimer:             time.NewTimer(ttl),
		// Note: Potentially expose burst config to users in future
		minBurst: minBurst,
	}
	rl.storeLimiter(maxDispatchPerSecond)
	return rl
}

func (rl *rateLimiter) UpdateMaxDispatch(maxDispatchPerSecond *float64) {
	if rl.shouldUpdate(maxDispatchPerSecond) {
		rl.Lock()
		rl.maxDispatchPerSecond = maxDispatchPerSecond
		rl.storeLimiter(maxDispatchPerSecond)
		rl.Unlock()
	}
}

func (rl *rateLimiter) storeLimiter(maxDispatchPerSecond *float64) {
	burst := int(*maxDispatchPerSecond)
	// If throttling is zero, burst also has to be 0
	if *maxDispatchPerSecond != 0 && burst <= rl.minBurst {
		burst = rl.minBurst
	}
	limiter := rate.NewLimiter(rate.Limit(*maxDispatchPerSecond), burst)
	rl.globalLimiter.Store(limiter)
}

func (rl *rateLimiter) shouldUpdate(maxDispatchPerSecond *float64) bool {
	if maxDispatchPerSecond == nil {
		return false
	}
	select {
	case <-rl.ttlTimer.C:
		rl.ttlTimer.Reset(rl.ttl)
		rl.RLock()
		defer rl.RUnlock()
		return *maxDispatchPerSecond != *rl.maxDispatchPerSecond
	default:
		rl.RLock()
		defer rl.RUnlock()
		return *maxDispatchPerSecond < *rl.maxDispatchPerSecond
	}
}

func (rl *rateLimiter) Wait(ctx context.Context) error {
	limiter := rl.globalLimiter.Load().(*rate.Limiter)
	return limiter.Wait(ctx)
}

func (rl *rateLimiter) Reserve() *rate.Reservation {
	limiter := rl.globalLimiter.Load().(*rate.Limiter)
	return limiter.Reserve()
}

func newTaskListManager(
	e *matchingEngineImpl, taskList *taskListID, taskListKind *s.TaskListKind, config *Config,
) (taskListManager, error) {
	dPtr := _defaultTaskDispatchRPS
	taskListConfig, err := newTaskListConfig(taskList, config, e.domainCache)
	if err != nil {
		return nil, err
	}
	rl := newRateLimiter(
		&dPtr, _defaultTaskDispatchRPSTTL, taskListConfig.MinTaskThrottlingBurstSize(),
	)
	return newTaskListManagerWithRateLimiter(
		e, taskList, taskListKind, taskListConfig, rl,
	), nil
}

func newTaskListManagerWithRateLimiter(
	e *matchingEngineImpl, taskList *taskListID, taskListKind *s.TaskListKind, config *taskListConfig,
	rl rateLimiter,
) taskListManager {
	// To perform one db operation if there are no pollers
	taskBufferSize := config.GetTasksBatchSize() - 1
	ctx, cancel := context.WithCancel(context.Background())
	tlMgr := &taskListManagerImpl{
		engine:                  e,
		taskBuffer:              make(chan *persistence.TaskInfo, taskBufferSize),
		notifyCh:                make(chan struct{}, 1),
		shutdownCh:              make(chan struct{}),
		deliverBufferShutdownCh: make(chan struct{}),
		cancelCtx:               ctx,
		cancelFunc:              cancel,
		taskListID:              taskList,
		logger: e.logger.WithFields(bark.Fields{
			logging.TagTaskListType: taskList.taskType,
			logging.TagTaskListName: taskList.taskListName,
		}),
		metricsClient:       e.metricsClient,
		taskAckManager:      newAckManager(e.logger),
		tasksForPoll:        make(chan *getTaskResult),
		config:              config,
		pollerHistory:       newPollerHistory(),
		outstandingPollsMap: make(map[string]context.CancelFunc),
		rateLimiter:         rl,
		taskListKind:        taskListKind,
	}
	tlMgr.taskWriter = newTaskWriter(tlMgr)
	tlMgr.startWG.Add(1)
	return tlMgr
}

// Contains information needed for current task transition from queue to Workflow execution history.
type taskContext struct {
	tlMgr             *taskListManagerImpl
	info              *persistence.TaskInfo
	syncResponseCh    chan<- *syncMatchResponse
	workflowExecution s.WorkflowExecution
	queryTaskInfo     *queryTaskInfo
	backlogCountHint  int64
}

type queryTaskInfo struct {
	taskID       string
	queryRequest *m.QueryWorkflowRequest
}

// Single task list in memory state
type taskListManagerImpl struct {
	taskListID    *taskListID
	logger        bark.Logger
	metricsClient metrics.Client
	engine        *matchingEngineImpl
	config        *taskListConfig

	// pollerHistory stores poller which poll from this tasklist in last few minutes
	pollerHistory *pollerHistory

	// serializes all writes to persistence
	// This is needed because of a known Cassandra issue where concurrent LWT to the same partition
	// cause timeout errors.
	persistenceLock sync.Mutex
	taskWriter      *taskWriter
	taskBuffer      chan *persistence.TaskInfo // tasks loaded from persistence
	// tasksForPoll is used to deliver tasks to pollers.
	// It must to be unbuffered. addTask publishes to it asynchronously and expects publish to succeed
	// only if there is waiting poll that consumes from it. Tasks in taskBuffer will blocking-add to
	// this channel
	tasksForPoll chan *getTaskResult
	notifyCh     chan struct{} // Used as signal to notify pump of new tasks
	// Note: We need two shutdown channels so we can stop task pump independently of the deliverBuffer
	// loop in getTasksPump in unit tests
	shutdownCh              chan struct{}  // Delivers stop to the pump that populates taskBuffer
	deliverBufferShutdownCh chan struct{}  // Delivers stop to the pump that populates taskBuffer
	startWG                 sync.WaitGroup // ensures that background processes do not start until setup is ready
	stopped                 int32
	// The cancel objects are to cancel the ratelimiter Wait in deliverBufferTasksLoop. The ideal
	// approach is to use request-scoped contexts and use a unique one for each call to Wait. However
	// in order to cancel it on shutdown, we need a new goroutine for each call that would wait on
	// the shutdown channel. To optimize on efficiency, we instead create one and tag it on the struct
	// so the cancel can be called directly on shutdown.
	cancelCtx  context.Context
	cancelFunc context.CancelFunc

	sync.Mutex
	taskAckManager          ackManager // tracks ackLevel for delivered messages
	rangeID                 int64      // Current range of the task list. Starts from 1.
	taskSequenceNumber      int64      // Sequence number of the next task. Starts from 1.
	nextRangeSequenceNumber int64      // Current range boundary

	// outstandingPollsMap is needed to keep track of all outstanding pollers for a
	// particular tasklist.  PollerID generated by frontend is used as the key and
	// CancelFunc is the value.  This is used to cancel the context to unblock any
	// outstanding poller when the frontend detects client connection is closed to
	// prevent tasks being dispatched to zombie pollers.
	outstandingPollsLock sync.Mutex
	outstandingPollsMap  map[string]context.CancelFunc
	// Rate limiter for task dispatch
	rateLimiter rateLimiter

	taskListKind *s.TaskListKind // sticky taskList has different process in persistence
}

// getTaskResult contains task info and optional channel to notify createTask caller
// that task is successfully started and returned to a poller
type getTaskResult struct {
	task      *persistence.TaskInfo
	C         chan *syncMatchResponse
	queryTask *queryTaskInfo
	syncMatch bool
}

// syncMatchResponse result of sync match delivered to a createTask caller
type syncMatchResponse struct {
	response *persistence.CreateTasksResponse
	err      error
}

// Starts reading pump for the given task list.
// The pump fills up taskBuffer from persistence.
func (c *taskListManagerImpl) Start() error {
	defer c.startWG.Done()

	// Make sure to grab the range first before starting task writer, as it needs the range to initialize maxReadLevel
	err := c.updateRangeIfNeeded() // Grabs a new range and updates read and ackLevels
	if err != nil {
		c.Stop()
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
	close(c.deliverBufferShutdownCh)
	c.cancelFunc()
	close(c.shutdownCh)
	c.taskWriter.Stop()
	c.engine.removeTaskListManager(c.taskListID)
	logging.LogTaskListUnloadedEvent(c.logger)
}

func (c *taskListManagerImpl) AddTask(execution *s.WorkflowExecution, taskInfo *persistence.TaskInfo) error {
	c.startWG.Wait()
	_, err := c.executeWithRetry(func(rangeID int64) (interface{}, error) {
		r, err := c.trySyncMatch(taskInfo)
		if (err != nil && err != errAddTasklistThrottled) || r != nil {
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

func (c *taskListManagerImpl) SyncMatchQueryTask(ctx context.Context, queryTask *queryTaskInfo) error {
	c.startWG.Wait()

	domainID := queryTask.queryRequest.GetDomainUUID()
	we := queryTask.queryRequest.QueryRequest.Execution
	taskInfo := &persistence.TaskInfo{
		DomainID:   domainID,
		RunID:      we.GetRunId(),
		WorkflowID: we.GetWorkflowId(),
	}

	request := &getTaskResult{task: taskInfo, C: make(chan *syncMatchResponse, 1), queryTask: queryTask}
	select {
	case c.tasksForPoll <- request:
		<-request.C
		return nil
	case <-ctx.Done():
		return &s.QueryFailedError{Message: "timeout: no workflow worker polling for given tasklist"}
	}
}

// Loads a task from DB or from sync match and wraps it in a task context
func (c *taskListManagerImpl) GetTaskContext(
	ctx context.Context,
	maxDispatchPerSecond *float64,
) (*taskContext, error) {
	c.rateLimiter.UpdateMaxDispatch(maxDispatchPerSecond)
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
		syncResponseCh:    result.C,         // nil if task is loaded from persistence
		queryTaskInfo:     result.queryTask, // non-nil for query task
		backlogCountHint:  c.taskAckManager.getBacklogCountHint(),
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

func (c *taskListManagerImpl) persistAckLevel() error {
	c.Lock()
	updateTaskListRequest := &persistence.UpdateTaskListRequest{
		TaskListInfo: &persistence.TaskListInfo{
			DomainID: c.taskListID.domainID,
			Name:     c.taskListID.taskListName,
			TaskType: c.taskListID.taskType,
			AckLevel: c.taskAckManager.getAckLevel(),
			RangeID:  c.rangeID,
			Kind:     c.getTaskListKind(),
		},
	}
	c.Unlock()
	c.persistenceLock.Lock()
	defer c.persistenceLock.Unlock()
	_, err := c.engine.taskManager.UpdateTaskList(updateTaskListRequest)
	return err
}

// newTaskIDs taskID to use to persist the task
func (c *taskListManagerImpl) newTaskIDs(count int) (taskIDs []int64, err error) {
	c.Lock()
	defer c.Unlock()
	for i := 0; i < count; i++ {
		err = c.updateRangeIfNeededLocked(c.engine)
		if err != nil {
			return nil, err
		}
		taskIDs = append(taskIDs, c.taskSequenceNumber)
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

func (c *taskListManagerImpl) getTaskListKind() int {
	// there is no need to lock here,
	// since c.taskListKind is assigned when taskListManager been created and never changed.
	if c.taskListKind == nil {
		c.taskListKind = common.TaskListKindPtr(s.TaskListKindNormal)
	}
	return int(*c.taskListKind)
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
func (c *taskListManagerImpl) getTask(ctx context.Context) (*getTaskResult, error) {
	scope := metrics.MatchingTaskListMgrScope
	timer := time.NewTimer(c.config.LongPollExpirationInterval())
	defer timer.Stop()

	pollerID, ok := ctx.Value(pollerIDKey).(string)
	childCtx := ctx
	if ok && pollerID != "" {
		// Found pollerID on context, add it to the map to allow it to be canceled in
		// response to CancelPoller call
		var cancel context.CancelFunc
		childCtx, cancel = context.WithCancel(ctx)
		c.outstandingPollsLock.Lock()
		c.outstandingPollsMap[pollerID] = cancel
		c.outstandingPollsLock.Unlock()
		defer func() {
			c.outstandingPollsLock.Lock()
			delete(c.outstandingPollsMap, pollerID)
			c.outstandingPollsLock.Unlock()
			cancel()
		}()
	}

	identity, ok := ctx.Value(identityKey).(string)
	if ok && identity != "" {
		c.pollerHistory.updatePollerInfo(pollerIdentity{
			identity: identity,
		})
	}

	select {
	case result := <-c.tasksForPoll:
		if result.syncMatch {
			c.metricsClient.IncCounter(scope, metrics.PollSuccessWithSyncCounter)
		}
		c.metricsClient.IncCounter(scope, metrics.PollSuccessCounter)
		return result, nil
	case <-timer.C:
		c.metricsClient.IncCounter(scope, metrics.PollTimeoutCounter)
		return nil, ErrNoTasks
	case <-childCtx.Done():
		err := childCtx.Err()
		if err == context.DeadlineExceeded || err == context.Canceled {
			err = ErrNoTasks
		}
		c.metricsClient.IncCounter(scope, metrics.PollTimeoutCounter)
		return nil, err
	}
}

func (c *taskListManagerImpl) CancelPoller(pollerID string) {
	c.outstandingPollsLock.Lock()
	cancel, ok := c.outstandingPollsMap[pollerID]
	c.outstandingPollsLock.Unlock()

	if ok && cancel != nil {
		cancel()
	}
}

// Returns a batch of tasks from persistence starting form current read level.
// Also return a number that can be used to update readLevel
func (c *taskListManagerImpl) getTaskBatch() ([]*persistence.TaskInfo, int64, error) {
	var tasks []*persistence.TaskInfo
	readLevel := c.taskAckManager.getReadLevel()
	maxReadLevel := c.taskWriter.GetMaxReadLevel()
	for readLevel < maxReadLevel {
		upper := readLevel + c.config.RangeSize
		if upper > maxReadLevel {
			upper = maxReadLevel
		}
		tasks, err := c.getTaskBatchWithRange(readLevel, upper)
		if err != nil {
			return nil, readLevel, err
		}
		// return as long as it grabs any tasks
		if len(tasks) > 0 {
			return tasks, upper, nil
		}
		readLevel = upper
	}
	return tasks, readLevel, nil // caller will update readLevel when no task grabbed
}

func (c *taskListManagerImpl) getTaskBatchWithRange(readLevel int64, maxReadLevel int64) ([]*persistence.TaskInfo, error) {
	response, err := c.executeWithRetry(func(rangeID int64) (interface{}, error) {
		c.Lock()
		request := &persistence.GetTasksRequest{
			DomainID:     c.taskListID.domainID,
			TaskList:     c.taskListID.taskListName,
			TaskType:     c.taskListID.taskType,
			BatchSize:    c.config.GetTasksBatchSize(),
			RangeID:      rangeID,
			ReadLevel:    readLevel,    // exclusive
			MaxReadLevel: maxReadLevel, // inclusive
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
			DomainID:     c.taskListID.domainID,
			TaskList:     c.taskListID.taskListName,
			TaskType:     c.taskListID.taskType,
			TaskListKind: c.getTaskListKind(),
		})
		return
	}

	c.metricsClient.IncCounter(metrics.MatchingTaskListMgrScope, metrics.LeaseRequestCounter)
	err := backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)

	if err != nil {
		c.metricsClient.IncCounter(metrics.MatchingTaskListMgrScope, metrics.LeaseFailureCounter)
		c.engine.unloadTaskList(c.taskListID)
		return err
	}

	tli := resp.TaskListInfo
	c.rangeID = tli.RangeID // Starts from 1
	c.taskAckManager.setAckLevel(tli.AckLevel)
	c.taskSequenceNumber = (tli.RangeID-1)*c.config.RangeSize + 1
	c.nextRangeSequenceNumber = (tli.RangeID)*c.config.RangeSize + 1
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

// updatePollerInfo update the poller information for this tasklist
func (c *taskListManagerImpl) updatePollerInfo(id pollerIdentity) {
	c.pollerHistory.updatePollerInfo(id)
}

// getAllPollerInfo return poller which poll from this tasklist in last few minutes
func (c *taskListManagerImpl) GetAllPollerInfo() []*pollerInfo {
	return c.pollerHistory.getAllPollerInfo()
}

// Tries to match task to a poller that is already waiting on getTask.
// When this method returns non nil response without error it is guaranteed that the task is started
// and sent to a poller. So it not necessary to persist it.
// Returns (nil, nil) if there is no waiting poller which indicates that task has to be persisted.
func (c *taskListManagerImpl) trySyncMatch(task *persistence.TaskInfo) (*persistence.CreateTasksResponse, error) {
	if !c.config.EnableSyncMatch() {
		return nil, nil
	}
	// Request from the point of view of Add(Activity|Decision)Task operation.
	// But it is getTask result from the point of view of a poll operation.
	request := &getTaskResult{task: task, C: make(chan *syncMatchResponse, 1), syncMatch: true}

	rsv := c.rateLimiter.Reserve()
	// If we have to wait too long for reservation, better to store in task buffer and handle later.
	if !rsv.OK() || rsv.Delay() > time.Second {
		c.metricsClient.IncCounter(metrics.MatchingTaskListMgrScope, metrics.SyncThrottleCounter)
		return nil, errAddTasklistThrottled
	}
	time.Sleep(rsv.Delay())
	select {
	case c.tasksForPoll <- request: // poller goroutine picked up the task
		r := <-request.C
		return r.response, r.err
	default: // no poller waiting for tasks
		rsv.Cancel()
		return nil, nil
	}
}

func (c *taskListManagerImpl) deliverBufferTasksForPoll() {
deliverBufferTasksLoop:
	for {
		err := c.rateLimiter.Wait(c.cancelCtx)
		if err != nil {
			if err == context.Canceled {
				c.logger.Info("Tasklist manager context is cancelled, shutting down")
				break deliverBufferTasksLoop
			}
			c.logger.Debugf(
				"Unable to add buffer task, rate limit failed, domainId: %s, tasklist: %s, error: %s",
				c.taskListID.domainID, c.taskListID.taskListName, err.Error(),
			)
			c.metricsClient.IncCounter(metrics.MatchingTaskListMgrScope, metrics.BufferThrottleCounter)
			// This is to prevent busy looping when throttling is set to 0
			runtime.Gosched()
			continue
		}
		select {
		case task, ok := <-c.taskBuffer:
			if !ok { // Task list getTasks pump is shutdown
				break deliverBufferTasksLoop
			}
			c.tasksForPoll <- &getTaskResult{task: task}
		case <-c.deliverBufferShutdownCh:
			break deliverBufferTasksLoop
		}
	}
}

func (c *taskListManagerImpl) getTasksPump() {
	defer close(c.taskBuffer)
	c.startWG.Wait()

	go c.deliverBufferTasksForPoll()
	updateAckTimer := time.NewTimer(c.config.UpdateAckInterval())
	checkPollerTimer := time.NewTimer(c.config.IdleTasklistCheckInterval())
getTasksPumpLoop:
	for {
		select {
		case <-c.shutdownCh:
			break getTasksPumpLoop
		case <-c.notifyCh:
			{
				tasks, readLevel, err := c.getTaskBatch()
				if err != nil {
					c.signalNewTask() // re-enqueue the event
					// TODO: Should we ever stop retrying on db errors?
					continue getTasksPumpLoop
				}
				c.Lock()
				if len(tasks) == 0 {
					c.taskAckManager.setReadLevel(readLevel)
				} else {
					for _, t := range tasks {
						c.taskAckManager.addTask(t.TaskID)
					}
				}
				c.Unlock()
				for _, t := range tasks {
					select {
					case c.taskBuffer <- t:
					case <-c.shutdownCh:
						break getTasksPumpLoop
					}
				}

				if len(tasks) > 0 {
					// There maybe more tasks.
					// We yield now, but signal pump to check again later.
					c.signalNewTask()
				}
			}
		case <-updateAckTimer.C:
			{
				err := c.persistAckLevel()
				//var err error
				if err != nil {
					if _, ok := err.(*persistence.ConditionFailedError); ok {
						// This indicates the task list may have moved to another host.
						c.Stop()
					} else {
						logging.LogPersistantStoreErrorEvent(c.logger, logging.TagValueStoreOperationUpdateTaskList, err,
							"Persist AckLevel failed")
					}
					// keep going as saving ack is not critical
				}
				c.signalNewTask() // periodically signal pump to check persistence for tasks
				updateAckTimer = time.NewTimer(c.config.UpdateAckInterval())
			}
		case <-checkPollerTimer.C:
			{
				pollers := c.GetAllPollerInfo()
				if len(pollers) == 0 {
					c.Stop()
				}
				checkPollerTimer = time.NewTimer(c.config.IdleTasklistCheckInterval())
			}
		}
	}

	updateAckTimer.Stop()
	checkPollerTimer.Stop()
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
		c.metricsClient.IncCounter(metrics.MatchingTaskListMgrScope, metrics.ConditionFailedErrorCounter)
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
		switch err.(type) {
		case *s.EntityNotExistsError, *h.EventAlreadyStartedError:
			return false
		}
		return true
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
		switch err.(type) {
		case *s.EntityNotExistsError, *h.EventAlreadyStartedError:
			return false
		}
		return true
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
		// We cannot just remove it from persistence because then it will be lost.
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
			logging.LogPersistantStoreErrorEvent(tlMgr.logger, logging.TagValueStoreOperationStopTaskList, err,
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
		logging.LogPersistantStoreErrorEvent(tlMgr.logger, logging.TagValueStoreOperationCompleteTask, err2,
			fmt.Sprintf("{taskID: %v, taskType: %v, taskList: %v}",
				c.info.TaskID, tlMgr.taskListID.taskType, tlMgr.taskListID.taskListName))
	}
}

func createServiceBusyError(msg string) *s.ServiceBusyError {
	return &s.ServiceBusyError{Message: msg}
}
