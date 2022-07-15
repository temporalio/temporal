// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/util"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
)

const (
	// Time budget for empty task to propagate through the function stack and be returned to
	// pollForActivityTask or pollForWorkflowTask handler.
	returnEmptyTaskTimeBudget = time.Second

	// Fake Task ID to wrap a task for syncmatch
	syncMatchTaskId = -137
)

type (
	taskQueueManagerOpt func(*taskQueueManagerImpl)

	idBlockAllocator interface {
		RenewLease(context.Context) (taskQueueState, error)
		RangeID() int64
	}

	addTaskParams struct {
		execution     *commonpb.WorkflowExecution
		taskInfo      *persistencespb.TaskInfo
		source        enumsspb.TaskSource
		forwardedFrom string
	}

	taskQueueManager interface {
		Start()
		Stop()
		WaitUntilInitialized(context.Context) error
		// AddTask adds a task to the task queue. This method will first attempt a synchronous
		// match with a poller. When that fails, task will be written to database and later
		// asynchronously matched with a poller
		AddTask(ctx context.Context, params addTaskParams) (syncMatch bool, err error)
		// GetTask blocks waiting for a task Returns error when context deadline is exceeded
		// maxDispatchPerSecond is the max rate at which tasks are allowed to be dispatched
		// from this task queue to pollers
		GetTask(ctx context.Context, maxDispatchPerSecond *float64) (*internalTask, error)
		// DispatchTask dispatches a task to a poller. When there are no pollers to pick
		// up the task, this method will return error. Task will not be persisted to db
		DispatchTask(ctx context.Context, task *internalTask) error
		// DispatchQueryTask will dispatch query to local or remote poller. If forwarded then result or error is returned,
		// if dispatched to local poller then nil and nil is returned.
		DispatchQueryTask(ctx context.Context, taskID string, request *matchingservice.QueryWorkflowRequest) (*matchingservice.QueryWorkflowResponse, error)
		// GetVersioningData returns the versioning data for this task queue
		GetVersioningData(ctx context.Context) (*persistencespb.VersioningData, error)
		// MutateVersioningData allows callers to update versioning data for this task queue
		MutateVersioningData(ctx context.Context, mutator func(*persistencespb.VersioningData) error) error
		CancelPoller(pollerID string)
		GetAllPollerInfo() []*taskqueuepb.PollerInfo
		HasPollerAfter(accessTime time.Time) bool
		// DescribeTaskQueue returns information about the target task queue
		DescribeTaskQueue(includeTaskQueueStatus bool) *matchingservice.DescribeTaskQueueResponse
		String() string
		QueueID() *taskQueueID
		TaskQueueKind() enumspb.TaskQueueKind
	}

	// Single task queue in memory state
	taskQueueManagerImpl struct {
		status            int32
		taskQueueID       *taskQueueID
		taskQueueKind     enumspb.TaskQueueKind // sticky taskQueue has different process in persistence
		config            *taskQueueConfig
		db                *taskQueueDB
		taskWriter        *taskWriter
		taskReader        *taskReader // reads tasks from db and async matches it with poller
		liveness          *liveness
		taskGC            *taskGC
		taskAckManager    ackManager   // tracks ackLevel for delivered messages
		matcher           *TaskMatcher // for matching a task producer with a poller
		namespaceRegistry namespace.Registry
		logger            log.Logger
		metricsClient     metrics.Client
		namespace         namespace.Name
		metricScope       metrics.Scope // namespace/taskqueue tagged metric scope
		// pollerHistory stores poller which poll from this taskqueue in last few minutes
		pollerHistory *pollerHistory
		// outstandingPollsMap is needed to keep track of all outstanding pollers for a
		// particular taskqueue.  PollerID generated by frontend is used as the key and
		// CancelFunc is the value.  This is used to cancel the context to unblock any
		// outstanding poller when the frontend detects client connection is closed to
		// prevent tasks being dispatched to zombie pollers.
		outstandingPollsLock sync.Mutex
		outstandingPollsMap  map[string]context.CancelFunc
		signalFatalProblem   func(taskQueueManager)
		clusterMeta          cluster.Metadata
		initializedError     *future.FutureImpl[struct{}]
	}
)

var _ taskQueueManager = (*taskQueueManagerImpl)(nil)

var errRemoteSyncMatchFailed = serviceerror.NewCanceled("remote sync match failed")

func withIDBlockAllocator(ibl idBlockAllocator) taskQueueManagerOpt {
	return func(tqm *taskQueueManagerImpl) {
		tqm.taskWriter.idAlloc = ibl
	}
}

func newTaskQueueManager(
	e *matchingEngineImpl,
	taskQueue *taskQueueID,
	taskQueueKind enumspb.TaskQueueKind,
	config *Config,
	clusterMeta cluster.Metadata,
	opts ...taskQueueManagerOpt,
) (taskQueueManager, error) {
	namespaceEntry, err := e.namespaceRegistry.GetNamespaceByID(taskQueue.namespaceID)
	if err != nil {
		return nil, err
	}
	nsName := namespaceEntry.Name()

	taskQueueConfig := newTaskQueueConfig(taskQueue, config, nsName)

	db := newTaskQueueDB(e.taskManager, taskQueue.namespaceID, taskQueue.name, taskQueue.taskType, taskQueueKind, e.logger)
	logger := log.With(e.logger,
		tag.WorkflowTaskQueueName(taskQueue.name),
		tag.WorkflowTaskQueueType(taskQueue.taskType),
		tag.WorkflowNamespace(nsName.String()))
	metricsScope := metrics.GetPerTaskQueueScope(
		e.metricsClient.Scope(metrics.MatchingTaskQueueMgrScope),
		nsName.String(),
		taskQueue.name,
		taskQueueKind,
	).Tagged(metrics.TaskQueueTypeTag(taskQueue.taskType))
	tlMgr := &taskQueueManagerImpl{
		status:              common.DaemonStatusInitialized,
		namespaceRegistry:   e.namespaceRegistry,
		metricsClient:       e.metricsClient,
		taskQueueID:         taskQueue,
		taskQueueKind:       taskQueueKind,
		logger:              logger,
		db:                  db,
		taskAckManager:      newAckManager(e.logger),
		taskGC:              newTaskGC(db, taskQueueConfig),
		config:              taskQueueConfig,
		pollerHistory:       newPollerHistory(),
		outstandingPollsMap: make(map[string]context.CancelFunc),
		signalFatalProblem:  e.unloadTaskQueue,
		clusterMeta:         clusterMeta,
		namespace:           nsName,
		metricScope:         metricsScope,
		initializedError:    future.NewFuture[struct{}](),
	}

	tlMgr.liveness = newLiveness(
		clock.NewRealTimeSource(),
		taskQueueConfig.IdleTaskqueueCheckInterval(),
		func() { tlMgr.signalFatalProblem(tlMgr) },
	)
	tlMgr.taskWriter = newTaskWriter(tlMgr)
	tlMgr.taskReader = newTaskReader(tlMgr)

	var fwdr *Forwarder
	if tlMgr.isFowardingAllowed(taskQueue, taskQueueKind) {
		fwdr = newForwarder(&taskQueueConfig.forwarderConfig, taskQueue, taskQueueKind, e.matchingClient)
	}
	tlMgr.matcher = newTaskMatcher(taskQueueConfig, fwdr, tlMgr.metricScope)
	for _, opt := range opts {
		opt(tlMgr)
	}
	return tlMgr, nil
}

// signalIfFatal calls signalFatalProblem on this taskQueueManagerImpl instance
// if and only if the supplied error represents a fatal condition, e.g. the
// existence of another taskQueueManager newer lease. Returns true if the signal
// is emitted, false otherwise.
func (c *taskQueueManagerImpl) signalIfFatal(err error) bool {
	if err == nil {
		return false
	}
	var condfail *persistence.ConditionFailedError
	if errors.As(err, &condfail) {
		c.metricScope.IncCounter(metrics.ConditionFailedErrorPerTaskQueueCounter)
		c.signalFatalProblem(c)
		return true
	}
	return false
}

func (c *taskQueueManagerImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&c.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}
	c.liveness.Start()
	c.taskWriter.Start()
	c.taskReader.Start()
	c.logger.Info("", tag.LifeCycleStarted)
	c.metricScope.IncCounter(metrics.TaskQueueStartedCounter)
}

func (c *taskQueueManagerImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&c.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}
	// ackLevel in taskAckManager is initialized to -1 and then set to a real value (>= 0) once
	// we've successfully acquired a lease. If it's still -1, then we don't have current
	// metadata. UpdateState would fail on the lease check, but don't even bother calling it.
	ackLevel := c.taskAckManager.getAckLevel()
	if ackLevel >= 0 {
		c.db.UpdateState(context.TODO(), ackLevel)
		c.taskGC.RunNow(context.TODO(), ackLevel)
	}
	c.liveness.Stop()
	c.taskWriter.Stop()
	c.taskReader.Stop()
	c.logger.Info("", tag.LifeCycleStopped)
	c.metricScope.IncCounter(metrics.TaskQueueStoppedCounter)
}

func (c *taskQueueManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	_, err := c.initializedError.Get(ctx)
	return err
}

// AddTask adds a task to the task queue. This method will first attempt a synchronous
// match with a poller. When there are no pollers or if ratelimit is exceeded, task will
// be written to database and later asynchronously matched with a poller
func (c *taskQueueManagerImpl) AddTask(
	ctx context.Context,
	params addTaskParams,
) (bool, error) {
	if params.forwardedFrom == "" {
		// request sent by history service
		c.liveness.markAlive(time.Now())
	}

	var syncMatch bool
	err := executeWithRetry(ctx, func(_ context.Context) error {
		taskInfo := params.taskInfo

		namespaceEntry, err := c.namespaceRegistry.GetNamespaceByID(namespace.ID(taskInfo.GetNamespaceId()))
		if err != nil {
			return err
		}

		syncMatch = false
		if namespaceEntry.ActiveInCluster(c.clusterMeta.GetCurrentClusterName()) {
			syncMatch, err = c.trySyncMatch(ctx, params)
			if syncMatch {
				return err
			}
		}

		if params.forwardedFrom != "" {
			// forwarded from child partition - only do sync match
			// child partition will persist the task when sync match fails
			return errRemoteSyncMatchFailed
		}

		_, err = c.taskWriter.appendTask(params.execution, taskInfo)
		c.signalIfFatal(err)
		return err
	})
	if !syncMatch && err == nil {
		c.taskReader.Signal()
	}
	return syncMatch, err
}

// GetTask blocks waiting for a task.
// Returns error when context deadline is exceeded
// maxDispatchPerSecond is the max rate at which tasks are allowed
// to be dispatched from this task queue to pollers
func (c *taskQueueManagerImpl) GetTask(
	ctx context.Context,
	maxDispatchPerSecond *float64,
) (*internalTask, error) {
	c.liveness.markAlive(time.Now())

	// We need to set a shorter timeout than the original ctx; otherwise, by the time ctx deadline is
	// reached, instead of emptyTask, context timeout error is returned to the frontend by the rpc stack,
	// which counts against our SLO. By shortening the timeout by a very small amount, the emptyTask can be
	// returned to the handler before a context timeout error is generated.
	childCtx, cancel := c.newChildContext(ctx, c.config.LongPollExpirationInterval(), returnEmptyTaskTimeBudget)
	defer cancel()

	pollerID, ok := ctx.Value(pollerIDKey).(string)
	if ok && pollerID != "" {
		// Found pollerID on context, add it to the map to allow it to be canceled in
		// response to CancelPoller call
		c.outstandingPollsLock.Lock()
		c.outstandingPollsMap[pollerID] = cancel
		c.outstandingPollsLock.Unlock()
		defer func() {
			c.outstandingPollsLock.Lock()
			delete(c.outstandingPollsMap, pollerID)
			c.outstandingPollsLock.Unlock()
		}()
	}

	identity, ok := ctx.Value(identityKey).(string)
	if ok && identity != "" {
		c.pollerHistory.updatePollerInfo(pollerIdentity(identity), maxDispatchPerSecond)
		defer func() {
			// to update timestamp when long poll ends
			c.pollerHistory.updatePollerInfo(pollerIdentity(identity), maxDispatchPerSecond)
		}()
	}

	namespaceEntry, err := c.namespaceRegistry.GetNamespaceByID(c.taskQueueID.namespaceID)
	if err != nil {
		return nil, err
	}

	// the desired global rate limit for the task queue comes from the
	// poller, which lives inside the client side worker. There is
	// one rateLimiter for this entire task queue and as we get polls,
	// we update the ratelimiter rps if it has changed from the last
	// value. Last poller wins if different pollers provide different values
	c.matcher.UpdateRatelimit(maxDispatchPerSecond)

	if !namespaceEntry.ActiveInCluster(c.clusterMeta.GetCurrentClusterName()) {
		return c.matcher.PollForQuery(childCtx)
	}

	task, err := c.matcher.Poll(childCtx)
	if err != nil {
		return nil, err
	}

	task.namespace = c.namespace
	task.backlogCountHint = c.taskAckManager.getBacklogCountHint()
	return task, nil
}

// DispatchTask dispatches a task to a poller. When there are no pollers to pick
// up the task or if rate limit is exceeded, this method will return error. Task
// *will not* be persisted to db
func (c *taskQueueManagerImpl) DispatchTask(
	ctx context.Context,
	task *internalTask,
) error {
	return c.matcher.MustOffer(ctx, task)
}

// DispatchQueryTask will dispatch query to local or remote poller. If forwarded then result or error is returned,
// if dispatched to local poller then nil and nil is returned.
func (c *taskQueueManagerImpl) DispatchQueryTask(
	ctx context.Context,
	taskID string,
	request *matchingservice.QueryWorkflowRequest,
) (*matchingservice.QueryWorkflowResponse, error) {
	task := newInternalQueryTask(taskID, request)
	return c.matcher.OfferQuery(ctx, task)
}

func (c *taskQueueManagerImpl) GetVersioningData(ctx context.Context) (*persistencespb.VersioningData, error) {
	return c.db.GetVersioningData(ctx)
}

func (c *taskQueueManagerImpl) MutateVersioningData(ctx context.Context, mutator func(*persistencespb.VersioningData) error) error {
	err := c.db.MutateVersioningData(ctx, mutator)
	c.signalIfFatal(err)
	return err
}

// GetAllPollerInfo returns all pollers that polled from this taskqueue in last few minutes
func (c *taskQueueManagerImpl) GetAllPollerInfo() []*taskqueuepb.PollerInfo {
	return c.pollerHistory.getPollerInfo(time.Time{})
}

func (c *taskQueueManagerImpl) HasPollerAfter(accessTime time.Time) bool {
	inflightPollerCount := 0
	c.outstandingPollsLock.Lock()
	inflightPollerCount = len(c.outstandingPollsMap)
	c.outstandingPollsLock.Unlock()
	if inflightPollerCount > 0 {
		return true
	}
	recentPollers := c.pollerHistory.getPollerInfo(accessTime)
	return len(recentPollers) > 0
}

func (c *taskQueueManagerImpl) CancelPoller(pollerID string) {
	c.outstandingPollsLock.Lock()
	cancel, ok := c.outstandingPollsMap[pollerID]
	c.outstandingPollsLock.Unlock()

	if ok && cancel != nil {
		cancel()
	}
}

// DescribeTaskQueue returns information about the target taskqueue, right now this API returns the
// pollers which polled this taskqueue in last few minutes and status of taskqueue's ackManager
// (readLevel, ackLevel, backlogCountHint and taskIDBlock).
func (c *taskQueueManagerImpl) DescribeTaskQueue(includeTaskQueueStatus bool) *matchingservice.DescribeTaskQueueResponse {
	response := &matchingservice.DescribeTaskQueueResponse{Pollers: c.GetAllPollerInfo()}
	if !includeTaskQueueStatus {
		return response
	}

	taskIDBlock := rangeIDToTaskIDBlock(c.db.RangeID(), c.config.RangeSize)
	response.TaskQueueStatus = &taskqueuepb.TaskQueueStatus{
		ReadLevel:        c.taskAckManager.getReadLevel(),
		AckLevel:         c.taskAckManager.getAckLevel(),
		BacklogCountHint: c.taskAckManager.getBacklogCountHint(),
		RatePerSecond:    c.matcher.Rate(),
		TaskIdBlock: &taskqueuepb.TaskIdBlock{
			StartId: taskIDBlock.start,
			EndId:   taskIDBlock.end,
		},
	}

	return response
}

func (c *taskQueueManagerImpl) String() string {
	buf := new(bytes.Buffer)
	if c.taskQueueID.taskType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
		buf.WriteString("Activity")
	} else {
		buf.WriteString("Workflow")
	}
	rangeID := c.db.RangeID()
	_, _ = fmt.Fprintf(buf, " task queue %v\n", c.taskQueueID.name)
	_, _ = fmt.Fprintf(buf, "RangeID=%v\n", rangeID)
	_, _ = fmt.Fprintf(buf, "TaskIDBlock=%+v\n", rangeIDToTaskIDBlock(rangeID, c.config.RangeSize))
	_, _ = fmt.Fprintf(buf, "AckLevel=%v\n", c.taskAckManager.ackLevel)
	_, _ = fmt.Fprintf(buf, "MaxTaskID=%v\n", c.taskAckManager.getReadLevel())

	return buf.String()
}

// completeTask marks a task as processed. Only tasks created by taskReader (i.e. backlog from db) reach
// here. As part of completion:
//   - task is deleted from the database when err is nil
//   - new task is created and current task is deleted when err is not nil
func (c *taskQueueManagerImpl) completeTask(task *persistencespb.AllocatedTaskInfo, err error) {
	if err != nil {
		// failed to start the task.
		// We cannot just remove it from persistence because then it will be lost.
		// We handle this by writing the task back to persistence with a higher taskID.
		// This will allow subsequent tasks to make progress, and hopefully by the time this task is picked-up
		// again the underlying reason for failing to start will be resolved.
		// Note that RecordTaskStarted only fails after retrying for a long time, so a single task will not be
		// re-written to persistence frequently.
		err = executeWithRetry(context.Background(), func(_ context.Context) error {
			wf := &commonpb.WorkflowExecution{WorkflowId: task.Data.GetWorkflowId(), RunId: task.Data.GetRunId()}
			_, err := c.taskWriter.appendTask(wf, task.Data)
			return err
		})

		if err != nil {
			// OK, we also failed to write to persistence.
			// This should only happen in very extreme cases where persistence is completely down.
			// We still can't lose the old task so we just unload the entire task queue
			c.logger.Error("Persistent store operation failure",
				tag.StoreOperationStopTaskQueue,
				tag.Error(err),
				tag.WorkflowTaskQueueName(c.taskQueueID.name),
				tag.WorkflowTaskQueueType(c.taskQueueID.taskType))
			c.signalFatalProblem(c)
			return
		}
		c.taskReader.Signal()
	}

	ackLevel := c.taskAckManager.completeTask(task.GetTaskId())
	c.taskGC.Run(context.TODO(), ackLevel) // TODO: completeTaskFunc and task.finish() should take in a context
}

func rangeIDToTaskIDBlock(rangeID int64, rangeSize int64) taskIDBlock {
	return taskIDBlock{
		start: (rangeID-1)*rangeSize + 1,
		end:   rangeID * rangeSize,
	}
}

// Retry operation on transient error.
func executeWithRetry(
	ctx context.Context,
	operation func(context.Context) error,
) error {
	err := backoff.ThrottleRetryContext(ctx, operation, persistenceOperationRetryPolicy, func(err error) bool {
		if common.IsContextDeadlineExceededErr(err) || common.IsContextCanceledErr(err) {
			return false
		}
		if _, ok := err.(*persistence.ConditionFailedError); ok {
			return false
		}
		return common.IsPersistenceTransientError(err)
	})
	return err
}

func (c *taskQueueManagerImpl) trySyncMatch(ctx context.Context, params addTaskParams) (bool, error) {
	childCtx, cancel := c.newChildContext(ctx, c.config.SyncMatchWaitDuration(), time.Second)

	// Mocking out TaskId for syncmatch as it hasn't been allocated yet
	fakeTaskIdWrapper := &persistencespb.AllocatedTaskInfo{
		Data:   params.taskInfo,
		TaskId: syncMatchTaskId,
	}

	task := newInternalTask(fakeTaskIdWrapper, nil, params.source, params.forwardedFrom, true)
	matched, err := c.matcher.Offer(childCtx, task)
	cancel()
	return matched, err
}

// newChildContext creates a child context with desired timeout.
// if tailroom is non-zero, then child context timeout will be
// the minOf(parentCtx.Deadline()-tailroom, timeout). Use this
// method to create child context when childContext cannot use
// all of parent's deadline but instead there is a need to leave
// some time for parent to do some post-work
func (c *taskQueueManagerImpl) newChildContext(
	parent context.Context,
	timeout time.Duration,
	tailroom time.Duration,
) (context.Context, context.CancelFunc) {
	select {
	case <-parent.Done():
		return parent, func() {}
	default:
	}
	deadline, ok := parent.Deadline()
	if !ok {
		return context.WithTimeout(parent, timeout)
	}
	remaining := deadline.Sub(time.Now().UTC()) - tailroom
	if remaining < timeout {
		timeout = time.Duration(util.Max(0, int64(remaining)))
	}
	return context.WithTimeout(parent, timeout)
}

func (c *taskQueueManagerImpl) isFowardingAllowed(taskQueue *taskQueueID, kind enumspb.TaskQueueKind) bool {
	return !taskQueue.IsRoot() && kind != enumspb.TASK_QUEUE_KIND_STICKY
}

func (c *taskQueueManagerImpl) QueueID() *taskQueueID {
	return c.taskQueueID
}

func (c *taskQueueManagerImpl) TaskQueueKind() enumspb.TaskQueueKind {
	return c.taskQueueKind
}
