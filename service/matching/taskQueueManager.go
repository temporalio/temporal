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

	"github.com/jonboulle/clockwork"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/tqname"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/internal/goro"
)

const (
	// Time budget for empty task to propagate through the function stack and be returned to
	// pollForActivityTask or pollForWorkflowTask handler.
	returnEmptyTaskTimeBudget = time.Second

	// Fake Task ID to wrap a task for syncmatch
	syncMatchTaskId = -137

	ioTimeout = 5 * time.Second * debug.TimeoutMultiplier

	// Threshold for counting a AddTask call as a no recent poller call
	noPollerThreshold = time.Minute * 2
)

var (
	// this retry policy is currenly only used for matching persistence operations
	// that, if failed, the entire task queue needs to be reload
	persistenceOperationRetryPolicy = backoff.NewExponentialRetryPolicy(50 * time.Millisecond).
		WithMaximumInterval(1 * time.Second).
		WithExpirationInterval(30 * time.Second)
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

	UserDataUpdateFunc func(*persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, error)

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
		GetTask(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error)
		// DispatchTask dispatches a task to a poller. When there are no pollers to pick
		// up the task, this method will return error. Task will not be persisted to db
		DispatchTask(ctx context.Context, task *internalTask) error
		// DispatchQueryTask will dispatch query to local or remote poller. If forwarded then result or error is returned,
		// if dispatched to local poller then nil and nil is returned.
		DispatchQueryTask(ctx context.Context, taskID string, request *matchingservice.QueryWorkflowRequest) (*matchingservice.QueryWorkflowResponse, error)
		// GetUserData returns the verioned user data for this task queue
		GetUserData(ctx context.Context) (*persistencespb.VersionedTaskQueueUserData, chan struct{}, error)
		// UpdateUserData allows callers to update user data for this task queue
		// Extra care should be taken to avoid mutating the existing data in the update function.
		UpdateUserData(ctx context.Context, replicate bool, updateFn UserDataUpdateFunc) error
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
		status                    int32
		engine                    *matchingEngineImpl
		taskQueueID               *taskQueueID
		taskQueueKind             enumspb.TaskQueueKind // sticky taskQueue has different process in persistence
		config                    *taskQueueConfig
		db                        *taskQueueDB
		taskWriter                *taskWriter
		taskReader                *taskReader // reads tasks from db and async matches it with poller
		liveness                  *liveness
		taskGC                    *taskGC
		taskAckManager            ackManager   // tracks ackLevel for delivered messages
		matcher                   *TaskMatcher // for matching a task producer with a poller
		namespaceRegistry         namespace.Registry
		namespaceReplicationQueue persistence.NamespaceReplicationQueue
		logger                    log.Logger
		matchingClient            matchingservice.MatchingServiceClient
		metricsHandler            metrics.Handler
		namespace                 namespace.Name
		taggedMetricsHandler      metrics.Handler // namespace/taskqueue tagged metric scope
		// pollerHistory stores poller which poll from this taskqueue in last few minutes
		pollerHistory *pollerHistory
		// outstandingPollsMap is needed to keep track of all outstanding pollers for a
		// particular taskqueue.  PollerID generated by frontend is used as the key and
		// CancelFunc is the value.  This is used to cancel the context to unblock any
		// outstanding poller when the frontend detects client connection is closed to
		// prevent tasks being dispatched to zombie pollers.
		outstandingPollsLock sync.Mutex
		outstandingPollsMap  map[string]context.CancelFunc
		clusterMeta          cluster.Metadata
		goroGroup            goro.Group
		initializedError     *future.FutureImpl[struct{}]
		// userDataInitialFetch is fulfilled once versioning data is fetched from the root partition. If this TQ is
		// the root partition, it is fulfilled as soon as it is fetched from db.
		userDataInitialFetch *future.FutureImpl[struct{}]
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

	db := newTaskQueueDB(e.taskManager, taskQueue.namespaceID, taskQueue, taskQueueKind, e.logger)
	logger := log.With(e.logger,
		tag.WorkflowTaskQueueName(taskQueue.FullName()),
		tag.WorkflowTaskQueueType(taskQueue.taskType),
		tag.WorkflowNamespace(nsName.String()))
	taggedMetricsHandler := metrics.GetPerTaskQueueScope(
		e.metricsHandler.WithTags(metrics.OperationTag(metrics.MatchingTaskQueueMgrScope), metrics.TaskQueueTypeTag(taskQueue.taskType)),
		nsName.String(),
		taskQueue.FullName(),
		taskQueueKind,
	)
	tlMgr := &taskQueueManagerImpl{
		status:                    common.DaemonStatusInitialized,
		engine:                    e,
		namespaceRegistry:         e.namespaceRegistry,
		namespaceReplicationQueue: e.namespaceReplicationQueue,
		matchingClient:            e.matchingClient,
		metricsHandler:            e.metricsHandler,
		taskQueueID:               taskQueue,
		taskQueueKind:             taskQueueKind,
		logger:                    logger,
		db:                        db,
		taskAckManager:            newAckManager(e.logger),
		taskGC:                    newTaskGC(db, taskQueueConfig),
		config:                    taskQueueConfig,
		pollerHistory:             newPollerHistory(),
		outstandingPollsMap:       make(map[string]context.CancelFunc),
		clusterMeta:               clusterMeta,
		namespace:                 nsName,
		taggedMetricsHandler:      taggedMetricsHandler,
		initializedError:          future.NewFuture[struct{}](),
		userDataInitialFetch:      future.NewFuture[struct{}](),
	}

	tlMgr.liveness = newLiveness(
		clockwork.NewRealClock(),
		taskQueueConfig.MaxTaskQueueIdleTime,
		tlMgr.unloadFromEngine,
	)
	tlMgr.taskWriter = newTaskWriter(tlMgr)
	tlMgr.taskReader = newTaskReader(tlMgr)

	var fwdr *Forwarder
	if tlMgr.isFowardingAllowed(taskQueue, taskQueueKind) {
		// Forward without version set, the target will resolve the correct version set from
		// the build id itself. TODO: check if we still need this here after tqm refactoring
		forwardTaskQueue := newTaskQueueIDWithVersionSet(taskQueue, "")
		fwdr = newForwarder(&taskQueueConfig.forwarderConfig, forwardTaskQueue, taskQueueKind, e.matchingClient)
	}
	tlMgr.matcher = newTaskMatcher(taskQueueConfig, fwdr, tlMgr.taggedMetricsHandler)
	for _, opt := range opts {
		opt(tlMgr)
	}
	return tlMgr, nil
}

// unloadFromEngine asks the MatchingEngine to unload this task queue. It will cause Stop to be called.
func (c *taskQueueManagerImpl) unloadFromEngine() {
	c.engine.unloadTaskQueue(c)
}

// signalIfFatal calls unloadFromEngine on this taskQueueManagerImpl instance
// if and only if the supplied error represents a fatal condition, e.g. the
// existence of another taskQueueManager newer lease. Returns true if the signal
// is emitted, false otherwise.
func (c *taskQueueManagerImpl) signalIfFatal(err error) bool {
	if err == nil {
		return false
	}
	var condfail *persistence.ConditionFailedError
	if errors.As(err, &condfail) {
		c.taggedMetricsHandler.Counter(metrics.ConditionFailedErrorPerTaskQueueCounter.GetMetricName()).Record(1)
		c.unloadFromEngine()
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
	c.goroGroup.Go(c.fetchUserDataLoop)
	c.logger.Info("", tag.LifeCycleStarted)
	c.taggedMetricsHandler.Counter(metrics.TaskQueueStartedCounter.GetMetricName()).Record(1)
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
		ctx, cancel := c.newIOContext()
		defer cancel()

		if err := c.db.UpdateState(ctx, ackLevel); err != nil {
			c.logger.Error("Failed to update task queue state", tag.Error(err))
		}
		c.taskGC.RunNow(ctx, ackLevel)
	}
	c.liveness.Stop()
	c.taskWriter.Stop()
	c.taskReader.Stop()
	c.goroGroup.Cancel()
	c.logger.Info("", tag.LifeCycleStopped)
	c.taggedMetricsHandler.Counter(metrics.TaskQueueStoppedCounter.GetMetricName()).Record(1)
	// This may call Stop again, but the status check above makes that a no-op.
	c.unloadFromEngine()
}

func (c *taskQueueManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	_, err := c.initializedError.Get(ctx)
	if err != nil {
		return err
	}
	_, err = c.userDataInitialFetch.Get(ctx)
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
		c.liveness.markAlive()
	}

	if c.QueueID().IsRoot() && !c.HasPollerAfter(time.Now().Add(-noPollerThreshold)) {
		// Only checks recent pollers in the root partition
		c.taggedMetricsHandler.Counter(metrics.NoRecentPollerTasksPerTaskQueueCounter.GetMetricName()).Record(1)
	}

	taskInfo := params.taskInfo

	namespaceEntry, err := c.namespaceRegistry.GetNamespaceByID(namespace.ID(taskInfo.GetNamespaceId()))
	if err != nil {
		return false, err
	}

	if namespaceEntry.ActiveInCluster(c.clusterMeta.GetCurrentClusterName()) {
		syncMatch, err := c.trySyncMatch(ctx, params)
		if syncMatch {
			return syncMatch, err
		}
	}

	if params.forwardedFrom != "" {
		// forwarded from child partition - only do sync match
		// child partition will persist the task when sync match fails
		return false, errRemoteSyncMatchFailed
	}

	_, err = c.taskWriter.appendTask(params.execution, taskInfo)
	c.signalIfFatal(err)
	if err == nil {
		c.taskReader.Signal()
	}
	return false, err
}

// GetTask blocks waiting for a task.
// Returns error when context deadline is exceeded
// maxDispatchPerSecond is the max rate at which tasks are allowed
// to be dispatched from this task queue to pollers
func (c *taskQueueManagerImpl) GetTask(
	ctx context.Context,
	pollMetadata *pollMetadata,
) (*internalTask, error) {
	c.liveness.markAlive()

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
		c.pollerHistory.updatePollerInfo(pollerIdentity(identity), pollMetadata)
		defer func() {
			// to update timestamp when long poll ends
			c.pollerHistory.updatePollerInfo(pollerIdentity(identity), pollMetadata)
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
	c.matcher.UpdateRatelimit(pollMetadata.ratePerSecond)

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

// GetUserData returns the user data for the task queue if any.
func (c *taskQueueManagerImpl) GetUserData(ctx context.Context) (*persistencespb.VersionedTaskQueueUserData, chan struct{}, error) {
	return c.db.GetUserData(ctx)
}

//nolint:revive // control coupling
func (c *taskQueueManagerImpl) UpdateUserData(ctx context.Context, replicate bool, updateFn UserDataUpdateFunc) error {
	newData, err := c.db.UpdateUserData(ctx, updateFn)
	c.signalIfFatal(err)
	if err != nil {
		return err
	}
	if replicate && c.namespaceReplicationQueue != nil {
		err = c.namespaceReplicationQueue.Publish(ctx, &replicationspb.ReplicationTask{
			TaskType: enumsspb.REPLICATION_TASK_TYPE_TASK_QUEUE_USER_DATA,
			Attributes: &replicationspb.ReplicationTask_TaskQueueUserDataAttributes{
				TaskQueueUserDataAttributes: &replicationspb.TaskQueueUserDataAttributes{
					NamespaceId:   c.taskQueueID.namespaceID.String(),
					TaskQueueName: c.taskQueueID.BaseNameString(),
					UserData:      newData.GetData(),
				},
			},
		})
		if err != nil {
			c.logger.Error("Failed to publish a replication task after updating task queue user data", tag.Error(err))
			return serviceerror.NewUnavailable("storing task queue user data succeeded but publishing to the namespace replication queue failed, please try again")
		}
	}
	return nil
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
	_, _ = fmt.Fprintf(buf, " task queue %v\n", c.taskQueueID.FullName())
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
				tag.WorkflowTaskQueueName(c.taskQueueID.FullName()),
				tag.WorkflowTaskQueueType(c.taskQueueID.taskType))
			c.unloadFromEngine()
			return
		}
		c.taskReader.Signal()
	}

	ackLevel := c.taskAckManager.completeTask(task.GetTaskId())

	// TODO: completeTaskFunc and task.finish() should take in a context
	ctx, cancel := c.newIOContext()
	defer cancel()
	c.taskGC.Run(ctx, ackLevel)
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
	return backoff.ThrottleRetryContext(ctx, operation, persistenceOperationRetryPolicy, func(err error) bool {
		if common.IsContextDeadlineExceededErr(err) || common.IsContextCanceledErr(err) {
			return false
		}
		if _, ok := err.(*persistence.ConditionFailedError); ok {
			return false
		}
		return common.IsPersistenceTransientError(err)
	})
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

func (c *taskQueueManagerImpl) callerInfoContext(ctx context.Context) context.Context {
	namespace, _ := c.namespaceRegistry.GetNamespaceName(c.taskQueueID.namespaceID)
	return headers.SetCallerInfo(ctx, headers.NewBackgroundCallerInfo(namespace.String()))
}

func (c *taskQueueManagerImpl) newIOContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), ioTimeout)
	return c.callerInfoContext(ctx), cancel
}

func (c *taskQueueManagerImpl) fetchUserDataLoop(ctx context.Context) error {
	ctx = c.callerInfoContext(ctx)

	// root workflow partition reads data from db
	if c.taskQueueID.OwnsUserData() {
		return nil
	}

	degree := c.config.ForwarderMaxChildrenPerNode()
	var target string
	if parent, err := c.taskQueueID.Parent(degree); err == nil {
		target = parent.FullName()
	} else if err == tqname.ErrNoParent {
		// we're the root activity task queue, ask the root workflow task queue
		target = c.taskQueueID.FullName()
	} else {
		// invalid degree
		return err
	}

	policy := backoff.NewExponentialRetryPolicy(1 * time.Second).WithMaximumInterval(5 * time.Minute)

	const (
		// TODO: dynamic config
		callTimeout    = 5 * time.Minute
		maxMinWaitTime = callTimeout / 2
		startWaitTime  = 1 * time.Second
	)

	minWaitTime := startWaitTime

	op := func(ctx context.Context) error {
		knownUserData, _, err := c.db.GetUserData(ctx)
		if err != nil && !errors.Is(err, errUserDataNotPresentOnPartition) {
			return err
		}

		callCtx, cancel := context.WithTimeout(ctx, callTimeout)
		defer cancel()

		res, err := c.matchingClient.GetTaskQueueUserData(callCtx, &matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              c.taskQueueID.namespaceID.String(),
			TaskQueue:                target,
			LastKnownUserDataVersion: knownUserData.GetVersion(),
			WaitNewData:              true,
		})
		if err != nil {
			return err
		}
		// If the root partition returns nil here, then that means our data matched, and we don't need to update.
		// If it's nil because it never existed, then we'd never have any data.
		// It can't be nil due to removing versions, as that would result in a non-nil container with
		// nil inner fields.
		if res.GetUserData() != nil {
			c.db.setUserDataForNonOwningPartition(res.GetUserData())
		}
		return nil
	}

	for ctx.Err() == nil {
		start := time.Now()
		backoff.ThrottleRetryContext(ctx, op, policy, nil)
		elapsed := time.Since(start)

		// In general we want to start a new call immediately on completion of the previous
		// one. But if the remote is broken and returns success immediately, we might end up
		// spinning. So enforce a minimum wait time that increases as long as we keep getting
		// very fast replies.
		if elapsed < minWaitTime {
			select {
			case <-ctx.Done():
			case <-time.After(minWaitTime - elapsed):
			}
			minWaitTime = util.Min(minWaitTime*2, maxMinWaitTime)
		} else {
			minWaitTime = startWaitTime
		}
	}

	return ctx.Err()
}
