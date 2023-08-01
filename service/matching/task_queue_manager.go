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
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
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
		baseTqm       taskQueueManager
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
		// GetTask blocks waiting for a task Returns error when context deadline is exceeded
		// maxDispatchPerSecond is the max rate at which tasks are allowed to be dispatched
		// from this task queue to pollers
		GetTask(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error)
		// SpoolTask spools a task to persistence to be matched asynchronously when a poller is available.
		SpoolTask(params addTaskParams) error
		// DispatchSpooledTask dispatches a task to a poller. When there are no pollers to pick
		// up the task, this method will return error. Task will not be persisted to db
		DispatchSpooledTask(ctx context.Context, task *internalTask, userDataChanged chan struct{}) error
		// DispatchQueryTask will dispatch query to local or remote poller. If forwarded then result or error is returned,
		// if dispatched to local poller then nil and nil is returned.
		DispatchQueryTask(ctx context.Context, taskID string, request *matchingservice.QueryWorkflowRequest) (*matchingservice.QueryWorkflowResponse, error)
		// GetUserData returns the versioned user data for this task queue
		GetUserData() (*persistencespb.VersionedTaskQueueUserData, chan struct{}, error)
		// UpdateUserData updates user data for this task queue and replicates across clusters if necessary.
		// Extra care should be taken to avoid mutating the existing data in the update function.
		UpdateUserData(ctx context.Context, options UserDataUpdateOptions, updateFn UserDataUpdateFunc) error
		UpdatePollerInfo(pollerIdentity, *pollMetadata)
		GetAllPollerInfo() []*taskqueuepb.PollerInfo
		HasPollerAfter(accessTime time.Time) bool
		// DescribeTaskQueue returns information about the target task queue
		DescribeTaskQueue(includeTaskQueueStatus bool) *matchingservice.DescribeTaskQueueResponse
		String() string
		QueueID() *taskQueueID
		TaskQueueKind() enumspb.TaskQueueKind
		LongPollExpirationInterval() time.Duration
		RedirectToVersionedQueueForAdd(context.Context, *taskqueuespb.TaskVersionDirective) (*taskQueueID, chan struct{}, error)
		RedirectToVersionedQueueForPoll(*commonpb.WorkerVersionCapabilities) (*taskQueueID, error)
	}

	// Single task queue in memory state
	taskQueueManagerImpl struct {
		status      int32
		engine      *matchingEngineImpl
		taskQueueID *taskQueueID
		stickyInfo
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
		namespace            namespace.Name
		taggedMetricsHandler metrics.Handler // namespace/taskqueue tagged metric scope
		// pollerHistory stores poller which poll from this taskqueue in last few minutes
		pollerHistory    *pollerHistory
		currentPolls     atomic.Int64
		goroGroup        goro.Group
		initializedError *future.FutureImpl[struct{}]
		// userDataReady is fulfilled once versioning data is fetched from the root partition. If this TQ is
		// the root partition, it is fulfilled as soon as it is fetched from db.
		userDataReady *future.FutureImpl[struct{}]
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
	e *matchingEngineImpl,
	taskQueue *taskQueueID,
	stickyInfo stickyInfo,
	config *Config,
	opts ...taskQueueManagerOpt,
) (taskQueueManager, error) {
	namespaceEntry, err := e.namespaceRegistry.GetNamespaceByID(taskQueue.namespaceID)
	if err != nil {
		return nil, err
	}
	nsName := namespaceEntry.Name()

	taskQueueConfig := newTaskQueueConfig(taskQueue, config, nsName)

	db := newTaskQueueDB(e.taskManager, e.matchingClient, taskQueue.namespaceID, taskQueue, stickyInfo.kind, e.logger)
	logger := log.With(e.logger,
		tag.WorkflowTaskQueueName(taskQueue.FullName()),
		tag.WorkflowTaskQueueType(taskQueue.taskType),
		tag.WorkflowNamespace(nsName.String()))
	throttledLogger := log.With(e.throttledLogger,
		tag.WorkflowTaskQueueName(taskQueue.FullName()),
		tag.WorkflowTaskQueueType(taskQueue.taskType),
		tag.WorkflowNamespace(nsName.String()))
	taggedMetricsHandler := metrics.GetPerTaskQueueScope(
		e.metricsHandler.WithTags(metrics.OperationTag(metrics.MatchingTaskQueueMgrScope), metrics.TaskQueueTypeTag(taskQueue.taskType)),
		nsName.String(),
		taskQueue.FullName(),
		stickyInfo.kind,
	)
	tlMgr := &taskQueueManagerImpl{
		status:               common.DaemonStatusInitialized,
		engine:               e,
		namespaceRegistry:    e.namespaceRegistry,
		matchingClient:       e.matchingClient,
		metricsHandler:       e.metricsHandler,
		clusterMeta:          e.clusterMeta,
		taskQueueID:          taskQueue,
		stickyInfo:           stickyInfo,
		logger:               logger,
		throttledLogger:      throttledLogger,
		db:                   db,
		taskAckManager:       newAckManager(e.logger),
		taskGC:               newTaskGC(db, taskQueueConfig),
		config:               taskQueueConfig,
		namespace:            nsName,
		taggedMetricsHandler: taggedMetricsHandler,
		initializedError:     future.NewFuture[struct{}](),
		userDataReady:        future.NewFuture[struct{}](),
	}
	// poller history is only kept for the base task queue manager
	if !tlMgr.managesSpecificVersionSet() {
		tlMgr.pollerHistory = newPollerHistory()
	}

	tlMgr.liveness = newLiveness(
		clock.NewRealTimeSource(),
		taskQueueConfig.MaxTaskQueueIdleTime,
		tlMgr.unloadFromEngine,
	)
	tlMgr.taskWriter = newTaskWriter(tlMgr)
	tlMgr.taskReader = newTaskReader(tlMgr)

	var fwdr *Forwarder
	if tlMgr.isFowardingAllowed(taskQueue, stickyInfo.kind) {
		// Forward without version set, the target will resolve the correct version set from
		// the build id itself. TODO: check if we still need this here after tqm refactoring
		forwardTaskQueue := newTaskQueueIDWithVersionSet(taskQueue, "")
		fwdr = newForwarder(&taskQueueConfig.forwarderConfig, forwardTaskQueue, stickyInfo.kind, e.matchingClient)
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
		c.skipFinalUpdate.Store(true)
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
	if c.db.DbStoresUserData() {
		c.goroGroup.Go(c.loadUserData)
	} else {
		c.goroGroup.Go(c.fetchUserData)
	}
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
	c.goroGroup.Cancel()
	c.logger.Info("", tag.LifeCycleStopped)
	c.taggedMetricsHandler.Counter(metrics.TaskQueueStoppedCounter.GetMetricName()).Record(1)
	// This may call Stop again, but the status check above makes that a no-op.
	c.unloadFromEngine()
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
		c.unloadFromEngine()
	}
}

// Sets user data enabled/disabled and marks the future ready (if it's not ready yet).
// userDataState controls whether GetUserData return an error, and which.
// futureError is the error to set on the ready future. If this is non-nil, the task queue will
// be unloaded.
// Note that this must only be called from a single goroutine since the Ready/Set sequence is
// potentially racy otherwise.
func (c *taskQueueManagerImpl) SetUserDataState(userDataState userDataState, futureError error) {
	// Always set state enabled/disabled even if we're not setting the future since we only set
	// the future once but the enabled/disabled state may change over time.
	c.db.setUserDataState(userDataState)

	if !c.userDataReady.Ready() {
		c.userDataReady.Set(struct{}{}, futureError)
		if futureError != nil {
			// We can't recover from here without starting over, so unload the whole task queue.
			// Skip final update since we never initialized.
			c.skipFinalUpdate.Store(true)
			c.unloadFromEngine()
		}
	}
}

func (c *taskQueueManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	_, err := c.initializedError.Get(ctx)
	if err != nil {
		return err
	}
	_, err = c.userDataReady.Get(ctx)
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

	// Ensure that tasks with the "default" versioning directive get spooled in the unversioned queue as they are not
	// associated with any version set until their execution is touched by a version specific worker.
	// "compatible" tasks OTOH are associated with a specific version set and should be stored along with all tasks for
	// that version set.
	// The task queue default set is dynamic and applies only at dispatch time. Putting "default" tasks into version set
	// specific queues could cause them to get stuck behind "compatible" tasks when they should be able to progress
	// independently.
	if taskInfo.VersionDirective.GetUseDefault() != nil {
		err = params.baseTqm.SpoolTask(params)
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

// GetTask blocks waiting for a task.
// Returns error when context deadline is exceeded
// maxDispatchPerSecond is the max rate at which tasks are allowed
// to be dispatched from this task queue to pollers
func (c *taskQueueManagerImpl) GetTask(
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

	task.namespace = c.namespace
	task.backlogCountHint = c.taskAckManager.getBacklogCountHint
	return task, nil
}

// DispatchSpooledTask dispatches a task to a poller. When there are no pollers to pick
// up the task or if rate limit is exceeded, this method will return error. Task
// *will not* be persisted to db
func (c *taskQueueManagerImpl) DispatchSpooledTask(
	ctx context.Context,
	task *internalTask,
	userDataChanged chan struct{},
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

// GetUserData returns the user data for the task queue if any.
// Note: can return nil value with no error.
func (c *taskQueueManagerImpl) GetUserData() (*persistencespb.VersionedTaskQueueUserData, chan struct{}, error) {
	return c.db.GetUserData()
}

// UpdateUserData updates user data for this task queue and replicates across clusters if necessary.
func (c *taskQueueManagerImpl) UpdateUserData(ctx context.Context, options UserDataUpdateOptions, updateFn UserDataUpdateFunc) error {
	newData, shouldReplicate, err := c.db.UpdateUserData(ctx, updateFn, options.KnownVersion, options.TaskQueueLimitPerBuildId)
	if err != nil {
		return err
	}
	if !shouldReplicate {
		return nil
	}

	// Only replicate if namespace is global and has at least 2 clusters registered.
	ns, err := c.namespaceRegistry.GetNamespaceByID(c.db.namespaceID)
	if err != nil {
		return err
	}
	if ns.ReplicationPolicy() != namespace.ReplicationPolicyMultiCluster {
		return nil
	}

	_, err = c.matchingClient.ReplicateTaskQueueUserData(ctx, &matchingservice.ReplicateTaskQueueUserDataRequest{
		NamespaceId: c.db.namespaceID.String(),
		TaskQueue:   c.taskQueueID.BaseNameString(),
		UserData:    newData.GetData(),
	})
	if err != nil {
		c.logger.Error("Failed to publish a replication task after updating task queue user data", tag.Error(err))
		return serviceerror.NewUnavailable("storing task queue user data succeeded but publishing to the namespace replication queue failed, please try again")
	}
	return err
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
		timeout = util.Max(0, remaining)
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
	return c.kind
}

func (c *taskQueueManagerImpl) LongPollExpirationInterval() time.Duration {
	return c.config.LongPollExpirationInterval()
}

func (c *taskQueueManagerImpl) RedirectToVersionedQueueForPoll(caps *commonpb.WorkerVersionCapabilities) (*taskQueueID, error) {
	if !caps.GetUseVersioning() {
		// Either this task queue is versioned, or there are still some workflows running on
		// the "unversioned" set.
		return c.taskQueueID, nil
	}
	// We don't need the userDataChanged channel here because polls have a timeout and the
	// client will retry, so if we're blocked on the wrong matcher it'll just take one poll
	// timeout to fix itself.
	userData, _, err := c.GetUserData()
	if err != nil {
		return nil, err
	}
	data := userData.GetData().GetVersioningData()

	if c.kind == enumspb.TASK_QUEUE_KIND_STICKY {
		// In the sticky case we don't redirect, but we may kick off this worker if there's a newer one.
		unknownBuild, err := checkVersionForStickyPoll(data, caps)
		if err != nil {
			return nil, err
		}
		if unknownBuild {
			c.recordUnknownBuildPoll(caps.BuildId)
		}
		return c.taskQueueID, nil
	}

	versionSet, unknownBuild, err := lookupVersionSetForPoll(data, caps)
	if err != nil {
		return nil, err
	}
	if unknownBuild {
		c.recordUnknownBuildPoll(caps.BuildId)
	}
	return newTaskQueueIDWithVersionSet(c.taskQueueID, versionSet), nil
}

func (c *taskQueueManagerImpl) RedirectToVersionedQueueForAdd(ctx context.Context, directive *taskqueuespb.TaskVersionDirective) (*taskQueueID, chan struct{}, error) {
	var buildId string
	switch dir := directive.GetValue().(type) {
	case *taskqueuespb.TaskVersionDirective_UseDefault:
		// leave buildId = "", lookupVersionSetForAdd understands that to mean "default"
	case *taskqueuespb.TaskVersionDirective_BuildId:
		buildId = dir.BuildId
	default:
		// Unversioned task, leave on unversioned queue.
		return c.taskQueueID, nil, nil
	}

	// Have to look up versioning data.
	userData, userDataChanged, err := c.GetUserData()
	if err != nil {
		if errors.Is(err, errUserDataDisabled) && buildId == "" {
			// When user data disabled, send "default" tasks to unversioned queue.
			return c.taskQueueID, userDataChanged, nil
		}
		return nil, nil, err
	}
	data := userData.GetData().GetVersioningData()

	if c.kind == enumspb.TASK_QUEUE_KIND_STICKY {
		// In the sticky case we don't redirect, but we may kick off this worker if there's a newer one.
		unknownBuild, err := checkVersionForStickyAdd(data, buildId)
		if err != nil {
			return nil, nil, err
		}
		if unknownBuild {
			c.recordUnknownBuildTask(buildId)
			// Don't bother persisting the unknown build id in this case: sticky tasks have a
			// short timeout, so it doesn't matter if they get lost.
		}
		return c.taskQueueID, userDataChanged, nil
	}

	versionSet, unknownBuild, err := lookupVersionSetForAdd(data, buildId)
	if err == errEmptyVersioningData { // nolint:goerr113
		// default was requested for an unversioned queue
		return c.taskQueueID, userDataChanged, nil
	} else if err != nil {
		return nil, nil, err
	}
	if unknownBuild {
		c.recordUnknownBuildTask(buildId)
		// Send rpc to root partition to persist the unknown build id before we return success.
		_, err = c.matchingClient.UpdateWorkerBuildIdCompatibility(ctx, &matchingservice.UpdateWorkerBuildIdCompatibilityRequest{
			NamespaceId: c.taskQueueID.namespaceID.String(),
			TaskQueue:   c.taskQueueID.Root().FullName(),
			Operation: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_PersistUnknownBuildId{
				PersistUnknownBuildId: buildId,
			},
		})
		if err != nil {
			return nil, nil, err
		}
	}
	return newTaskQueueIDWithVersionSet(c.taskQueueID, versionSet), userDataChanged, nil
}

func (c *taskQueueManagerImpl) recordUnknownBuildPoll(buildId string) {
	c.logger.Warn("unknown build id in poll", tag.BuildId(buildId))
	c.taggedMetricsHandler.Counter(metrics.UnknownBuildPollsCounter.GetMetricName()).Record(1)
}

func (c *taskQueueManagerImpl) recordUnknownBuildTask(buildId string) {
	c.logger.Warn("unknown build id in task", tag.BuildId(buildId))
	c.taggedMetricsHandler.Counter(metrics.UnknownBuildTasksCounter.GetMetricName()).Record(1)
}

func (c *taskQueueManagerImpl) callerInfoContext(ctx context.Context) context.Context {
	ns, _ := c.namespaceRegistry.GetNamespaceName(c.taskQueueID.namespaceID)
	return headers.SetCallerInfo(ctx, headers.NewBackgroundCallerInfo(ns.String()))
}

func (c *taskQueueManagerImpl) newIOContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), ioTimeout)
	return c.callerInfoContext(ctx), cancel
}

func (c *taskQueueManagerImpl) loadUserData(ctx context.Context) error {
	ctx = c.callerInfoContext(ctx)

	hasLoadedUserData := false

	for ctx.Err() == nil {
		if !c.config.LoadUserData() {
			// if disabled, mark disabled and ready
			c.SetUserDataState(userDataDisabled, nil)
			hasLoadedUserData = false // load again if re-enabled
		} else if !hasLoadedUserData {
			// otherwise try to load from db once
			err := c.db.loadUserData(ctx)
			c.SetUserDataState(userDataEnabled, err)
			hasLoadedUserData = err == nil
		} else {
			// if already loaded, set enabled
			c.SetUserDataState(userDataEnabled, nil)
		}
		common.InterruptibleSleep(ctx, c.config.GetUserDataLongPollTimeout())
	}

	return nil
}

func (c *taskQueueManagerImpl) userDataFetchSource() (string, error) {
	if c.kind == enumspb.TASK_QUEUE_KIND_STICKY {
		// Sticky queues get data from their corresponding normal queue
		if c.normalName == "" {
			// Older SDKs don't send the normal name. That's okay, they just can't use versioning.
			return "", errMissingNormalQueueName
		}
		return c.normalName, nil
	}

	degree := c.config.ForwarderMaxChildrenPerNode()
	parent, err := c.taskQueueID.Parent(degree)
	if err == tqname.ErrNoParent { // nolint:goerr113
		// we're the root activity task queue, ask the root workflow task queue
		return c.taskQueueID.FullName(), nil
	} else if err != nil {
		// invalid degree
		return "", err
	}
	return parent.FullName(), nil
}

func (c *taskQueueManagerImpl) fetchUserData(ctx context.Context) error {
	ctx = c.callerInfoContext(ctx)

	if c.managesSpecificVersionSet() {
		// tqm for specific version set doesn't have its own user data
		c.SetUserDataState(userDataSpecificVersion, nil)
		return nil
	}

	// otherwise fetch from parent partition

	fetchSource, err := c.userDataFetchSource()
	if err != nil {
		if err == errMissingNormalQueueName { // nolint:goerr113
			// pretend we have no user data. this is a sticky queue so the only effect is that we can't
			// kick off versioned pollers.
			c.SetUserDataState(userDataEnabled, nil)
		}
		return err
	}

	// hasFetchedUserData is true if we have gotten a successful reply to GetTaskQueueUserData.
	// It's used to control whether we do a long poll or a simple get.
	hasFetchedUserData := false

	op := func(ctx context.Context) error {
		if !c.config.LoadUserData() {
			// if disabled, mark disabled and ready, but allow retries so that we notice if
			// it's re-enabled
			c.SetUserDataState(userDataDisabled, nil)
			return errUserDataDisabled
		}

		knownUserData, _, err := c.GetUserData()
		if err != nil {
			// Start with a non-long poll after re-enabling after disable, so that we don't have to wait the
			// full long poll interval before calling SetUserDataStatus to enable again.
			// Leave knownUserData as nil and GetVersion will return 0.
			hasFetchedUserData = false
		}

		callCtx, cancel := context.WithTimeout(ctx, c.config.GetUserDataLongPollTimeout())
		defer cancel()

		res, err := c.matchingClient.GetTaskQueueUserData(callCtx, &matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              c.taskQueueID.namespaceID.String(),
			TaskQueue:                fetchSource,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: knownUserData.GetVersion(),
			WaitNewData:              hasFetchedUserData,
		})
		if err != nil {
			var unimplErr *serviceerror.Unimplemented
			var failedPrecondErr *serviceerror.FailedPrecondition
			if errors.As(err, &unimplErr) {
				// This might happen during a deployment. The older version couldn't have had any user data,
				// so we act as if it just returned an empty response and set ourselves ready.
				// Return the error so that we backoff with retry, and do not set hasFetchedUserData so that
				// we don't do a long poll next time.
				c.SetUserDataState(userDataEnabled, nil)
			} else if errors.As(err, &failedPrecondErr) {
				// This means the parent has the LoadUserData switch turned off. Act like our switch is off also.
				c.SetUserDataState(userDataDisabled, nil)
			}
			return err
		}
		// If the root partition returns nil here, then that means our data matched, and we don't need to update.
		// If it's nil because it never existed, then we'd never have any data.
		// It can't be nil due to removing versions, as that would result in a non-nil container with
		// nil inner fields.
		if res.GetUserData() != nil {
			c.db.setUserDataForNonOwningPartition(res.GetUserData())
		}
		hasFetchedUserData = true
		c.SetUserDataState(userDataEnabled, nil)
		return nil
	}

	minWaitTime := c.config.GetUserDataMinWaitTime

	for ctx.Err() == nil {
		start := time.Now()
		_ = backoff.ThrottleRetryContext(ctx, op, c.config.GetUserDataRetryPolicy, nil)
		elapsed := time.Since(start)

		// In general, we want to start a new call immediately on completion of the previous
		// one. But if the remote is broken and returns success immediately, we might end up
		// spinning. So enforce a minimum wait time that increases as long as we keep getting
		// very fast replies.
		if elapsed < minWaitTime {
			common.InterruptibleSleep(ctx, minWaitTime-elapsed)
			// Don't let this get near our call timeout, otherwise we can't tell the difference
			// between a fast reply and a timeout.
			minWaitTime = util.Min(minWaitTime*2, c.config.GetUserDataLongPollTimeout()/2)
		} else {
			minWaitTime = c.config.GetUserDataMinWaitTime
		}
	}

	return ctx.Err()
}
