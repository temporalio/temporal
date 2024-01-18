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
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/clock"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
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
	// this retry policy is currently only used for matching persistence operations
	// that, if failed, the entire task queue needs to be reloaded
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

	stickyInfo struct {
		kind       enumspb.TaskQueueKind // sticky taskQueue has different process in persistence
		normalName string                // if kind is sticky, name of normal queue
	}

	UserDataUpdateOptions struct {
		TaskQueueLimitPerBuildId int
		// Only perform the update if current version equals to supplied version.
		// 0 is unset.
		KnownVersion int64
	}
	// UserDataUpdateFunc accepts the current user data for a task queue and returns the updated user data, a boolean
	// indicating whether this data should be replicated, and an error.
	// Extra care should be taken to avoid mutating the current user data to avoid keeping uncommitted data in memory.
	UserDataUpdateFunc func(*persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error)

	taskQueueManager interface {
		Start()
		Stop()
		WaitUntilInitialized(context.Context) error
		// AddTask adds a task to the task queue. This method will first attempt a synchronous
		// match with a poller. When that fails, task will be written to database and later
		// asynchronously matched with a poller
		AddTask(ctx context.Context, params addTaskParams) (syncMatch bool, err error)
		// PollTask blocks waiting for a task Returns error when context deadline is exceeded
		// maxDispatchPerSecond is the max rate at which tasks are allowed to be dispatched
		// from this task queue to pollers
		PollTask(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error)
		// MarkAlive updates the liveness timer to keep this taskQueueManager alive.
		MarkAlive()
		// SpoolTask spools a task to persistence to be matched asynchronously when a poller is available.
		SpoolTask(params addTaskParams) error
		// DispatchSpooledTask dispatches a task to a poller. When there are no pollers to pick
		// up the task, this method will return error. Task will not be persisted to db
		DispatchSpooledTask(ctx context.Context, task *internalTask, userDataChanged <-chan struct{}) error
		// DispatchQueryTask will dispatch query to local or remote poller. If forwarded then result or error is returned,
		// if dispatched to local poller then nil and nil is returned.
		DispatchQueryTask(ctx context.Context, taskID string, request *matchingservice.QueryWorkflowRequest) (*matchingservice.QueryWorkflowResponse, error)
		UpdatePollerInfo(pollerIdentity, *pollMetadata)
		GetAllPollerInfo() []*taskqueuepb.PollerInfo
		HasPollerAfter(accessTime time.Time) bool
		// DescribeTaskQueue returns information about the target task queue
		DescribeTaskQueue(includeTaskQueueStatus bool) *matchingservice.DescribeTaskQueueResponse
		String() string
		QueueID() *taskQueueID
	}

	// Single task queue in memory state
	taskQueueManagerImpl struct {
		status               int32
		partitionMgr         *taskQueuePartitionManagerImpl
		taskQueueID          *taskQueueID
		config               *taskQueueConfig
		db                   *taskQueueDB
		taskWriter           *taskWriter
		taskReader           *taskReader // reads tasks from db and async matches it with poller
		liveness             *liveness
		taskGC               *taskGC
		taskAckManager       ackManager   // tracks ackLevel for delivered messages
		matcher              *TaskMatcher // for matching a task producer with a poller
		namespaceRegistry    namespace.Registry
		logger               log.Logger
		throttledLogger      log.ThrottledLogger
		matchingClient       matchingservice.MatchingServiceClient
		metricsHandler       metrics.Handler
		clusterMeta          cluster.Metadata
		taggedMetricsHandler metrics.Handler // namespace/taskqueue tagged metric scope
		// pollerHistory stores poller which poll from this taskqueue in last few minutes
		pollerHistory    *pollerHistory
		currentPolls     atomic.Int64
		initializedError *future.FutureImpl[struct{}]
		// skipFinalUpdate controls behavior on Stop: if it's false, we try to write one final
		// update before unloading
		skipFinalUpdate atomic.Bool
	}
)

var _ taskQueueManager = (*taskQueueManagerImpl)(nil)

var (
	errRemoteSyncMatchFailed  = serviceerror.NewCanceled("remote sync match failed")
	errMissingNormalQueueName = errors.New("missing normal queue name")

	normalStickyInfo = stickyInfo{kind: enumspb.TASK_QUEUE_KIND_NORMAL}
)

func withIDBlockAllocator(ibl idBlockAllocator) taskQueueManagerOpt {
	return func(tqm *taskQueueManagerImpl) {
		tqm.taskWriter.idAlloc = ibl
	}
}

func stickyInfoFromTaskQueue(tq *taskqueuepb.TaskQueue) stickyInfo {
	return stickyInfo{
		kind:       tq.GetKind(),
		normalName: tq.GetNormalName(),
	}
}

func newTaskQueueManager(
	partitionMgr *taskQueuePartitionManagerImpl,
	taskQueue *taskQueueID,
	opts ...taskQueueManagerOpt,
) (*taskQueueManagerImpl, error) {
	e := partitionMgr.engine
	config := partitionMgr.config
	db := newTaskQueueDB(e.taskManager, e.matchingRawClient, taskQueue.namespaceID, taskQueue, partitionMgr.kind, e.logger)
	logger := log.With(partitionMgr.logger, tag.WorkerBuildId(taskQueue.VersionSet()))
	throttledLogger := log.With(partitionMgr.throttledLogger, tag.WorkerBuildId(taskQueue.VersionSet()))
	taggedMetricsHandler := partitionMgr.taggedMetricsHandler.WithTags(
		metrics.OperationTag(metrics.MatchingTaskQueueMgrScope),
		metrics.WorkerBuildIdTag(taskQueue.VersionSet()))
	tlMgr := &taskQueueManagerImpl{
		status:               common.DaemonStatusInitialized,
		partitionMgr:         partitionMgr,
		namespaceRegistry:    e.namespaceRegistry,
		matchingClient:       e.matchingRawClient,
		metricsHandler:       e.metricsHandler,
		clusterMeta:          e.clusterMeta,
		taskQueueID:          taskQueue,
		logger:               logger,
		throttledLogger:      throttledLogger,
		db:                   db,
		taskAckManager:       newAckManager(e.logger),
		taskGC:               newTaskGC(db, config),
		config:               config,
		taggedMetricsHandler: taggedMetricsHandler,
		initializedError:     future.NewFuture[struct{}](),
	}
	// TODO: move this to partition manager
	// poller history is only kept for the base task queue manager
	if !tlMgr.managesSpecificVersionSet() {
		tlMgr.pollerHistory = newPollerHistory()
	}

	tlMgr.liveness = newLiveness(
		clock.NewRealTimeSource(),
		config.MaxTaskQueueIdleTime,
		tlMgr.unloadFromPartitionManager,
	)
	tlMgr.taskWriter = newTaskWriter(tlMgr)
	tlMgr.taskReader = newTaskReader(tlMgr)

	var fwdr *Forwarder
	if tlMgr.isFowardingAllowed(taskQueue, partitionMgr.kind) {
		// Forward without version set, the target will resolve the correct version set from
		// the build id itself. TODO: move forwarder to partition manager, no need to have one per db queue
		forwardTaskQueue := newTaskQueueIDWithVersionSet(taskQueue, "")
		fwdr = newForwarder(&config.forwarderConfig, forwardTaskQueue, partitionMgr.kind, e.matchingRawClient)
	}
	tlMgr.matcher = newTaskMatcher(config, fwdr, tlMgr.taggedMetricsHandler)
	for _, opt := range opts {
		opt(tlMgr)
	}
	return tlMgr, nil
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
		c.taggedMetricsHandler.Counter(metrics.ConditionFailedErrorPerTaskQueueCounter.Name()).Record(1)
		c.skipFinalUpdate.Store(true)
		c.unloadFromPartitionManager()
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
	c.taggedMetricsHandler.Counter(metrics.TaskQueueStartedCounter.Name()).Record(1)
}

// Stop does not unload the queue from its partition. It is intended to be called by the partition manager when
// unloading a queues. For stopping and unloading a queue call unloadFromPartitionManager instead.
func (c *taskQueueManagerImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&c.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}
	// Maybe try to write one final update of ack level and GC some tasks.
	// Skip the update if we never initialized (ackLevel will be -1 in that case).
	// Also skip if we're stopping due to lost ownership (the update will fail in that case).
	// Ignore any errors.
	// Note that it's fine to GC even if the update ack level fails because we did match the
	// tasks, the next owner will just read over an empty range.
	ackLevel := c.taskAckManager.getAckLevel()
	if ackLevel >= 0 && !c.skipFinalUpdate.Load() {
		ctx, cancel := c.newIOContext()
		defer cancel()

		_ = c.db.UpdateState(ctx, ackLevel)
		c.taskGC.RunNow(ctx, ackLevel)
	}
	c.liveness.Stop()
	c.taskWriter.Stop()
	c.taskReader.Stop()
	// Set user data state on stop to wake up anyone blocked on the user data changed channel.
	c.db.setUserDataState(userDataClosed)
	c.logger.Info("", tag.LifeCycleStopped)
	c.taggedMetricsHandler.Counter(metrics.TaskQueueStoppedCounter.Name()).Record(1)
}

// managesSpecificVersionSet returns true if this is a tqm for a specific version set in the build-id-based versioning
// feature. Note that this is a different concept from the overall task queue having versioning data associated with it,
// which is the usual meaning of "versioned task queue". These task queues are not interacted with directly outside of
// a single matching node.
func (c *taskQueueManagerImpl) managesSpecificVersionSet() bool {
	return c.taskQueueID.VersionSet() != ""
}

func (c *taskQueueManagerImpl) SetInitializedError(err error) {
	c.initializedError.Set(struct{}{}, err)
	if err != nil {
		// We can't recover from here without starting over, so unload the whole task queue.
		// Skip final update since we never initialized.
		c.skipFinalUpdate.Store(true)
		c.unloadFromPartitionManager()
	}
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
		c.liveness.markAlive()
	}

	// TODO: make this work for versioned queues too
	if c.QueueID().IsRoot() && c.QueueID().VersionSet() == "" && !c.HasPollerAfter(time.Now().Add(-noPollerThreshold)) {
		// Only checks recent pollers in the root partition
		c.taggedMetricsHandler.Counter(metrics.NoRecentPollerTasksPerTaskQueueCounter.Name()).Record(1)
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

	// Ensure that tasks with the "default" versioning directive get spooled in the unversioned queue as they are not
	// associated with any version set until their execution is touched by a version specific worker.
	// "compatible" tasks OTOH are associated with a specific version set and should be stored along with all tasks for
	// that version set.
	// The task queue default set is dynamic and applies only at dispatch time. Putting "default" tasks into version set
	// specific queues could cause them to get stuck behind "compatible" tasks when they should be able to progress
	// independently.
	if taskInfo.VersionDirective.GetUseDefault() != nil {
		err = c.partitionMgr.defaultQueue.SpoolTask(params)
	} else {
		err = c.SpoolTask(params)
	}
	return false, err
}

func (c *taskQueueManagerImpl) SpoolTask(params addTaskParams) error {
	_, err := c.taskWriter.appendTask(params.execution, params.taskInfo)
	c.signalIfFatal(err)
	if err == nil {
		c.taskReader.Signal()
	}
	return err
}

// PollTask blocks waiting for a task.
// Returns error when context deadline is exceeded
// maxDispatchPerSecond is the max rate at which tasks are allowed
// to be dispatched from this task queue to pollers
func (c *taskQueueManagerImpl) PollTask(
	ctx context.Context,
	pollMetadata *pollMetadata,
) (*internalTask, error) {
	c.liveness.markAlive()

	c.currentPolls.Add(1)
	defer c.currentPolls.Add(-1)

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
		return c.matcher.PollForQuery(ctx, pollMetadata)
	}

	task, err := c.matcher.Poll(ctx, pollMetadata)
	if err != nil {
		return nil, err
	}

	task.namespace = c.partitionMgr.namespaceName
	task.backlogCountHint = c.taskAckManager.getBacklogCountHint
	return task, nil
}

func (c *taskQueueManagerImpl) MarkAlive() {
	c.liveness.markAlive()
}

// DispatchSpooledTask dispatches a task to a poller. When there are no pollers to pick
// up the task or if rate limit is exceeded, this method will return error. Task
// *will not* be persisted to db
func (c *taskQueueManagerImpl) DispatchSpooledTask(
	ctx context.Context,
	task *internalTask,
	userDataChanged <-chan struct{},
) error {
	return c.matcher.MustOffer(ctx, task, userDataChanged)
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

func (c *taskQueueManagerImpl) UpdatePollerInfo(id pollerIdentity, pollMetadata *pollMetadata) {
	if c.pollerHistory != nil {
		c.pollerHistory.updatePollerInfo(id, pollMetadata)
	}
}

// GetAllPollerInfo returns all pollers that polled from this taskqueue in last few minutes
func (c *taskQueueManagerImpl) GetAllPollerInfo() []*taskqueuepb.PollerInfo {
	if c.pollerHistory == nil {
		return nil
	}
	return c.pollerHistory.getPollerInfo(time.Time{})
}

func (c *taskQueueManagerImpl) HasPollerAfter(accessTime time.Time) bool {
	if c.currentPolls.Load() > 0 {
		return true
	}
	if c.pollerHistory == nil {
		return false
	}
	recentPollers := c.pollerHistory.getPollerInfo(accessTime)
	return len(recentPollers) > 0
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
			// We still can't lose the old task, so we just unload the entire task queue
			c.logger.Error("Persistent store operation failure",
				tag.StoreOperationStopTaskQueue,
				tag.Error(err),
				tag.WorkflowTaskQueueName(c.taskQueueID.FullName()),
				tag.WorkflowTaskQueueType(c.taskQueueID.taskType))
			// Skip final update since persistence is having problems.
			c.skipFinalUpdate.Store(true)
			c.unloadFromPartitionManager()
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
	if params.forwardedFrom == "" && c.config.TestDisableSyncMatch() {
		return false, nil
	}
	childCtx, cancel := newChildContext(ctx, c.config.SyncMatchWaitDuration(), time.Second)
	defer cancel()

	// Use fake TaskId for sync match as it hasn't been allocated yet
	fakeTaskIdWrapper := &persistencespb.AllocatedTaskInfo{
		Data:   params.taskInfo,
		TaskId: syncMatchTaskId,
	}

	task := newInternalTask(fakeTaskIdWrapper, nil, params.source, params.forwardedFrom, true)
	return c.matcher.Offer(childCtx, task)
}

// newChildContext creates a child context with desired timeout.
// if tailroom is non-zero, then child context timeout will be
// the minOf(parentCtx.Deadline()-tailroom, timeout). Use this
// method to create child context when childContext cannot use
// all of parent's deadline but instead there is a need to leave
// some time for parent to do some post-work
func newChildContext(
	parent context.Context,
	timeout time.Duration,
	tailroom time.Duration,
) (context.Context, context.CancelFunc) {
	if parent.Err() != nil {
		return parent, func() {}
	}
	deadline, ok := parent.Deadline()
	if !ok {
		return context.WithTimeout(parent, timeout)
	}
	remaining := time.Until(deadline) - tailroom
	if remaining < timeout {
		timeout = max(0, remaining)
	}
	return context.WithTimeout(parent, timeout)
}

func (c *taskQueueManagerImpl) isFowardingAllowed(taskQueue *taskQueueID, kind enumspb.TaskQueueKind) bool {
	return !taskQueue.IsRoot() && kind != enumspb.TASK_QUEUE_KIND_STICKY
}

func (c *taskQueueManagerImpl) QueueID() *taskQueueID {
	return c.taskQueueID
}

func (c *taskQueueManagerImpl) callerInfoContext(ctx context.Context) context.Context {
	ns, _ := c.namespaceRegistry.GetNamespaceName(c.taskQueueID.namespaceID)
	return headers.SetCallerInfo(ctx, headers.NewBackgroundCallerInfo(ns.String()))
}

func (c *taskQueueManagerImpl) newIOContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), ioTimeout)
	return c.callerInfoContext(ctx), cancel
}

func (c *taskQueueManagerImpl) unloadFromPartitionManager() {
	c.partitionMgr.unloadDbQueue(c)
}
