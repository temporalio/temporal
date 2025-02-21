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
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/pborman/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/worker/deployment"
	"google.golang.org/protobuf/types/known/durationpb"
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

type (
	taskQueueManagerOpt func(*physicalTaskQueueManagerImpl)

	addTaskParams struct {
		taskInfo    *persistencespb.TaskInfo
		forwardInfo *taskqueuespb.TaskForwardInfo
	}
	// physicalTaskQueueManagerImpl manages a set of physical queues that comprise one logical
	// queue, corresponding to a single versioned queue of a task queue partition.
	// TODO(pri): rename this
	physicalTaskQueueManagerImpl struct {
		status       int32
		partitionMgr *taskQueuePartitionManagerImpl
		queue        *PhysicalTaskQueueKey
		config       *taskQueueConfig

		// This context is valid for lifetime of this physicalTaskQueueManagerImpl.
		// It can be used to notify when the task queue is closing.
		tqCtx       context.Context
		tqCtxCancel context.CancelFunc

		backlogLock       sync.Mutex
		backlogs          []*backlogManagerImpl // backlog managers are 1:1 with subqueues
		backlogByPriority map[int32]*backlogManagerImpl
		backlog0          *backlogManagerImpl // this is == backlogs[0] but does not require the lock to read

		liveness          *liveness
		oldMatcher        *TaskMatcher // TODO(pri): old matcher cleanup
		priMatcher        *priTaskMatcher
		matcher           matcherInterface // TODO(pri): old matcher cleanup
		namespaceRegistry namespace.Registry
		logger            log.Logger
		throttledLogger   log.ThrottledLogger
		matchingClient    matchingservice.MatchingServiceClient
		clusterMeta       cluster.Metadata
		metricsHandler    metrics.Handler // namespace/taskqueue tagged metric scope
		// pollerHistory stores poller which poll from this taskqueue in last few minutes
		pollerHistory              *pollerHistory
		currentPolls               atomic.Int64
		taskValidator              taskValidator
		tasksAddedInIntervals      *taskTracker
		tasksDispatchedInIntervals *taskTracker
		// deploymentWorkflowStarted keeps track if we have already registered the task queue worker
		// in the deployment.
		deploymentLock              sync.Mutex // TODO (Shivam): Rename after the pre-release versioning API's are removed.
		deploymentRegistered        bool       // TODO (Shivam): Rename after the pre-release versioning API's are removed.
		deploymentVersionRegistered bool       // TODO (Shivam): Rename after the pre-release versioning API's are removed.
		deploymentRegisterError     error      // last "too many ..." error we got when registering // TODO (Shivam): Rename after the pre-release versioning API's are removed.

		firstPoll time.Time
	}

	// TODO(pri): old matcher cleanup
	matcherInterface interface {
		Start()
		Stop()
		UpdateRatelimit(rpsPtr float64)
		Rate() float64
		Poll(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error)
		PollForQuery(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error)
		OfferQuery(ctx context.Context, task *internalTask) (*matchingservice.QueryWorkflowResponse, error)
		OfferNexusTask(ctx context.Context, task *internalTask) (*matchingservice.DispatchNexusTaskResponse, error)
		ReprocessAllTasks()
	}
)

var _ physicalTaskQueueManager = (*physicalTaskQueueManagerImpl)(nil)

var (
	errRemoteSyncMatchFailed  = serviceerror.NewCanceled("remote sync match failed")
	errMissingNormalQueueName = errors.New("missing normal queue name")
)

func newPhysicalTaskQueueManager(
	partitionMgr *taskQueuePartitionManagerImpl,
	queue *PhysicalTaskQueueKey,
	opts ...taskQueueManagerOpt,
) (*physicalTaskQueueManagerImpl, error) {
	e := partitionMgr.engine
	config := partitionMgr.config
	buildIdTagValue := queue.Version().MetricsTagValue()
	logger := log.With(partitionMgr.logger, tag.WorkerBuildId(buildIdTagValue))
	throttledLogger := log.With(partitionMgr.throttledLogger, tag.WorkerBuildId(buildIdTagValue))
	taggedMetricsHandler := partitionMgr.metricsHandler.WithTags(
		metrics.OperationTag(metrics.MatchingTaskQueueMgrScope),
		metrics.WorkerBuildIdTag(buildIdTagValue, config.BreakdownMetricsByBuildID()))

	tqCtx, tqCancel := context.WithCancel(partitionMgr.callerInfoContext(context.Background()))

	pqMgr := &physicalTaskQueueManagerImpl{
		status:                     common.DaemonStatusInitialized,
		partitionMgr:               partitionMgr,
		queue:                      queue,
		config:                     config,
		tqCtx:                      tqCtx,
		tqCtxCancel:                tqCancel,
		backlogByPriority:          make(map[int32]*backlogManagerImpl),
		namespaceRegistry:          e.namespaceRegistry,
		matchingClient:             e.matchingRawClient,
		clusterMeta:                e.clusterMeta,
		logger:                     logger,
		throttledLogger:            throttledLogger,
		metricsHandler:             taggedMetricsHandler,
		tasksAddedInIntervals:      newTaskTracker(clock.NewRealTimeSource()),
		tasksDispatchedInIntervals: newTaskTracker(clock.NewRealTimeSource()),
	}

	pqMgr.pollerHistory = newPollerHistory(partitionMgr.config.PollerHistoryTTL())

	pqMgr.liveness = newLiveness(
		clock.NewRealTimeSource(),
		config.MaxTaskQueueIdleTime,
		func() { pqMgr.UnloadFromPartitionManager(unloadCauseIdle) },
	)

	pqMgr.taskValidator = newTaskValidator(
		tqCtx,
		pqMgr.clusterMeta,
		pqMgr.namespaceRegistry,
		pqMgr.partitionMgr.engine.historyClient,
	)
	pqMgr.backlog0 = newBacklogManager(
		tqCtx,
		pqMgr,
		0,
		config,
		e.taskManager,
		logger,
		throttledLogger,
		e.matchingRawClient,
		taggedMetricsHandler,
	)
	pqMgr.backlogs = []*backlogManagerImpl{pqMgr.backlog0}

	if config.NewMatcher {
		var fwdr *priForwarder
		var err error
		if !queue.Partition().IsRoot() && queue.Partition().Kind() != enumspb.TASK_QUEUE_KIND_STICKY {
			// Every DB Queue needs its own forwarder so that the throttles do not interfere
			fwdr, err = newPriForwarder(&config.forwarderConfig, queue, e.matchingRawClient)
			if err != nil {
				return nil, err
			}
		}
		pqMgr.priMatcher = newPriTaskMatcher(tqCtx, config, queue.partition, fwdr, pqMgr.taskValidator, pqMgr.metricsHandler)
		pqMgr.matcher = pqMgr.priMatcher
	} else {
		var fwdr *Forwarder
		var err error
		if !queue.Partition().IsRoot() && queue.Partition().Kind() != enumspb.TASK_QUEUE_KIND_STICKY {
			// Every DB Queue needs its own forwarder so that the throttles do not interfere
			fwdr, err = newForwarder(&config.forwarderConfig, queue, e.matchingRawClient)
			if err != nil {
				return nil, err
			}
		}
		pqMgr.oldMatcher = newTaskMatcher(config, fwdr, pqMgr.metricsHandler)
		pqMgr.matcher = pqMgr.oldMatcher
	}
	for _, opt := range opts {
		opt(pqMgr)
	}
	return pqMgr, nil
}

func (c *physicalTaskQueueManagerImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&c.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}
	c.liveness.Start()
	c.backlog0.Start() // this will call LoadSubqueues after initializing
	c.matcher.Start()
	c.logger.Info("Started physicalTaskQueueManager", tag.LifeCycleStarted, tag.Cause(c.config.loadCause.String()))
	c.metricsHandler.Counter(metrics.TaskQueueStartedCounter.Name()).Record(1)
	c.partitionMgr.engine.updatePhysicalTaskQueueGauge(c.partitionMgr.ns, c.partitionMgr.partition, c.queue.version, 1)
}

// Stop does not unload the queue from its partition. It is intended to be called by the partition manager when
// unloading a queues. For stopping and unloading a queue call UnloadFromPartitionManager instead.
func (c *physicalTaskQueueManagerImpl) Stop(unloadCause unloadCause) {
	if !atomic.CompareAndSwapInt32(
		&c.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}
	c.backlogLock.Lock()
	for _, b := range c.backlogs {
		// this may attempt to write one final ack update, do this before canceling tqCtx
		b.Stop()
	}
	c.backlogLock.Unlock()
	c.matcher.Stop()
	c.liveness.Stop()
	c.tqCtxCancel()
	c.logger.Info("Stopped physicalTaskQueueManager", tag.LifeCycleStopped, tag.Cause(unloadCause.String()))
	c.metricsHandler.Counter(metrics.TaskQueueStoppedCounter.Name()).Record(1)
	c.partitionMgr.engine.updatePhysicalTaskQueueGauge(c.partitionMgr.ns, c.partitionMgr.partition, c.queue.version, -1)
}

func (c *physicalTaskQueueManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	return c.backlog0.WaitUntilInitialized(ctx)
}

// LoadSubqueues is called once on startup and then again each time the set of subqueues changes.
func (c *physicalTaskQueueManagerImpl) LoadSubqueues(subqueues []*persistencespb.SubqueueKey) {
	if !c.config.NewMatcher {
		return
	}

	c.backlogLock.Lock()
	defer c.backlogLock.Unlock()

	c.loadSubqueuesLocked(subqueues)
}

func (c *physicalTaskQueueManagerImpl) loadSubqueuesLocked(subqueues []*persistencespb.SubqueueKey) {
	// TODO(pri): This assumes that subqueues never shrinks, and priority/fairness index of
	// existing subqueues never changes. If we change that, this logic will need to change.
	for i, s := range subqueues {
		if i >= len(c.backlogs) {
			b := newBacklogManager(
				c.tqCtx,
				c,
				i,
				c.config,
				c.partitionMgr.engine.taskManager,
				c.logger,
				c.throttledLogger,
				c.partitionMgr.engine.matchingRawClient,
				c.metricsHandler,
			)
			b.Start()
			c.backlogs = append(c.backlogs, b)
		}
		c.backlogByPriority[s.Priority] = c.backlogs[i]
	}
}

func (c *physicalTaskQueueManagerImpl) getBacklogForPriority(priority int32) *backlogManagerImpl {
	if !c.config.NewMatcher {
		return c.backlog0
	}

	levels := c.config.PriorityLevels()
	if priority == 0 {
		priority = defaultPriorityLevel(levels)
	}
	if priority < 1 {
		// this should have been rejected much earlier, but just clip it here
		priority = 1
	} else if priority > int32(levels) {
		priority = int32(levels)
	}

	c.backlogLock.Lock()
	defer c.backlogLock.Unlock()

	if b, ok := c.backlogByPriority[priority]; ok {
		return b
	}

	// We need to allocate a new subqueue. Note this is doing io under backlogLock,
	// but we want to serialize these updates.
	// TODO(pri): maybe we can improve that
	subqueues, err := c.backlog0.db.AllocateSubqueue(c.tqCtx, priority)
	if err != nil {
		c.backlog0.signalIfFatal(err)
		// If we failed to write the metadata update, just use backlog0. If err was a
		// fatal error (most likely case), the subsequent call to SpoolTask will fail.
		return c.backlog0
	}

	c.loadSubqueuesLocked(subqueues)

	// this should be here now
	if b, ok := c.backlogByPriority[priority]; ok {
		return b
	}

	// if something went wrong, return 0
	return c.backlog0
}

func (c *physicalTaskQueueManagerImpl) SpoolTask(taskInfo *persistencespb.TaskInfo) error {
	c.liveness.markAlive()
	b := c.getBacklogForPriority(taskInfo.Priority.GetPriorityKey())
	return b.SpoolTask(taskInfo)
}

// PollTask blocks waiting for a task.
// Returns error when context deadline is exceeded
// maxDispatchPerSecond is the max rate at which tasks are allowed
// to be dispatched from this task queue to pollers
func (c *physicalTaskQueueManagerImpl) PollTask(
	ctx context.Context,
	pollMetadata *pollMetadata,
) (*internalTask, error) {
	c.liveness.markAlive()

	c.currentPolls.Add(1)
	defer c.currentPolls.Add(-1)

	namespaceId := namespace.ID(c.queue.NamespaceId())
	namespaceEntry, err := c.namespaceRegistry.GetNamespaceByID(namespaceId)
	if err != nil {
		return nil, err
	}

	// [cleanup-wv-pre-release]
	if c.partitionMgr.engine.config.EnableDeployments(namespaceEntry.Name().String()) {
		if err = c.ensureRegisteredInDeployment(ctx, namespaceEntry, pollMetadata); err != nil {
			return nil, err
		}
	}

	if c.partitionMgr.engine.config.EnableDeploymentVersions(namespaceEntry.Name().String()) {
		if err = c.ensureRegisteredInDeploymentVersion(ctx, namespaceEntry, pollMetadata); err != nil {
			return nil, err
		}
	}

	// the desired global rate limit for the task queue comes from the
	// poller, which lives inside the client side worker. There is
	// one rateLimiter for this entire task queue and as we get polls,
	// we update the ratelimiter rps if it has changed from the last
	// value. Last poller wins if different pollers provide different values
	if rps := pollMetadata.taskQueueMetadata.GetMaxTasksPerSecond(); rps != nil {
		c.matcher.UpdateRatelimit(rps.Value)
	}

	if !namespaceEntry.ActiveInCluster(c.clusterMeta.GetCurrentClusterName()) {
		return c.matcher.PollForQuery(ctx, pollMetadata)
	}

	for {
		task, err := c.matcher.Poll(ctx, pollMetadata)
		if err != nil {
			return nil, err
		}

		// It's possible to get an expired task here: taskReader checks for expiration when
		// reading from persistence, and physicalTaskQueueManager checks for expiration in
		// ProcessSpooledTask, but the one task blocked in the matcher could expire while it's
		// there. In that case, go back for another task.
		// If we didn't do this, the task would be rejected when we call RecordXTaskStarted on
		// history, but this is more efficient.
		if task.event != nil && IsTaskExpired(task.event.AllocatedTaskInfo) {
			c.metricsHandler.Counter(metrics.ExpiredTasksPerTaskQueueCounter.Name()).Record(1)
			task.finish(nil, false)
			continue
		}

		task.namespace = c.partitionMgr.ns.Name()
		task.backlogCountHint = c.backlogCountHint

		if pollMetadata.forwardedFrom == "" && // only track the original polls, not forwarded ones.
			(!task.isStarted() || !task.started.hasEmptyResponse()) { // Need to filter out the empty "started" ones
			c.tasksDispatchedInIntervals.incrementTaskCount()
		}
		return task, nil
	}
}

func (c *physicalTaskQueueManagerImpl) backlogCountHint() (total int64) {
	c.backlogLock.Lock()
	defer c.backlogLock.Unlock()

	for _, b := range c.backlogs {
		total += b.BacklogCountHint()
	}
	return
}

func (c *physicalTaskQueueManagerImpl) MarkAlive() {
	c.liveness.markAlive()
}

// DispatchSpooledTask dispatches a task to a poller. When there are no pollers to pick
// up the task or if rate limit is exceeded, this method will return error. Task
// *will not* be persisted to db
// TODO(pri): old matcher cleanup
func (c *physicalTaskQueueManagerImpl) DispatchSpooledTask(
	ctx context.Context,
	task *internalTask,
	userDataChanged <-chan struct{},
) error {
	return c.oldMatcher.MustOffer(ctx, task, userDataChanged)
}

// TODO(pri): old matcher cleanup
func (c *physicalTaskQueueManagerImpl) ProcessSpooledTask(
	ctx context.Context,
	task *internalTask,
) error {
	if !c.taskValidator.maybeValidate(task.event.AllocatedTaskInfo, c.queue.TaskType()) {
		task.finish(nil, false)
		c.metricsHandler.Counter(metrics.ExpiredTasksPerTaskQueueCounter.Name()).Record(1)
		// Don't try to set read level here because it may have been advanced already.
		return nil
	}
	return c.partitionMgr.ProcessSpooledTask(ctx, task, c.queue)
}

func (c *physicalTaskQueueManagerImpl) AddSpooledTask(task *internalTask) error {
	return c.partitionMgr.AddSpooledTask(c.tqCtx, task, c.queue)
}

func (c *physicalTaskQueueManagerImpl) AddSpooledTaskToMatcher(task *internalTask) {
	c.priMatcher.AddTask(task)
}

func (c *physicalTaskQueueManagerImpl) UserDataChanged() {
	c.matcher.ReprocessAllTasks()
}

// DispatchQueryTask will dispatch query to local or remote poller. If forwarded then result or error is returned,
// if dispatched to local poller then nil and nil is returned.
func (c *physicalTaskQueueManagerImpl) DispatchQueryTask(
	ctx context.Context,
	taskId string,
	request *matchingservice.QueryWorkflowRequest,
) (*matchingservice.QueryWorkflowResponse, error) {
	task := newInternalQueryTask(taskId, request)
	if !task.isForwarded() {
		c.tasksAddedInIntervals.incrementTaskCount()
	}
	return c.matcher.OfferQuery(ctx, task)
}

func (c *physicalTaskQueueManagerImpl) DispatchNexusTask(
	ctx context.Context,
	taskId string,
	request *matchingservice.DispatchNexusTaskRequest,
) (*matchingservice.DispatchNexusTaskResponse, error) {
	deadline, _ := ctx.Deadline() // If not set by user, our client will set a default.
	var opDeadline time.Time
	if header := nexus.Header(request.GetRequest().GetHeader()); header != nil {
		if opTimeoutHeader := header.Get(nexus.HeaderOperationTimeout); opTimeoutHeader != "" {
			opTimeout, err := time.ParseDuration(opTimeoutHeader)
			if err != nil {
				// Operation-Timeout header is not required so don't fail request on parsing errors.
				c.logger.Warn(fmt.Sprintf("unable to parse %v header: %v", nexus.HeaderOperationTimeout, opTimeoutHeader), tag.Error(err), tag.WorkflowNamespaceID(request.NamespaceId))
			} else {
				opDeadline = time.Now().Add(opTimeout)
			}
		}
	}
	task := newInternalNexusTask(taskId, deadline, opDeadline, request)
	if !task.isForwarded() {
		c.tasksAddedInIntervals.incrementTaskCount()
	}
	return c.matcher.OfferNexusTask(ctx, task)
}

func (c *physicalTaskQueueManagerImpl) UpdatePollerInfo(id pollerIdentity, pollMetadata *pollMetadata) {
	c.pollerHistory.updatePollerInfo(id, pollMetadata)
}

// GetAllPollerInfo returns all pollers that polled from this taskqueue in last few minutes
func (c *physicalTaskQueueManagerImpl) GetAllPollerInfo() []*taskqueuepb.PollerInfo {
	if c.pollerHistory == nil {
		return nil
	}
	return c.pollerHistory.getPollerInfo(time.Time{})
}

func (c *physicalTaskQueueManagerImpl) HasPollerAfter(accessTime time.Time) bool {
	if c.currentPolls.Load() > 0 {
		return true
	}
	if c.pollerHistory == nil {
		return false
	}
	recentPollers := c.pollerHistory.getPollerInfo(accessTime)
	return len(recentPollers) > 0
}

// LegacyDescribeTaskQueue returns information about the target taskqueue, right now this API returns the
// pollers which polled this taskqueue in last few minutes and status of taskqueue's ackManager
// (readLevel, ackLevel, backlogCountHint and taskIDBlock).
func (c *physicalTaskQueueManagerImpl) LegacyDescribeTaskQueue(includeTaskQueueStatus bool) *matchingservice.DescribeTaskQueueResponse {
	response := &matchingservice.DescribeTaskQueueResponse{
		DescResponse: &workflowservice.DescribeTaskQueueResponse{
			Pollers: c.GetAllPollerInfo(),
		},
	}
	if includeTaskQueueStatus {
		// TODO(pri): return only backlog 0, need a new api to include info for all subqueues
		response.DescResponse.TaskQueueStatus = c.backlog0.BacklogStatus()
		response.DescResponse.TaskQueueStatus.RatePerSecond = c.matcher.Rate()
	}
	return response
}

func (c *physicalTaskQueueManagerImpl) GetStats() *taskqueuepb.TaskQueueStats {
	c.backlogLock.Lock()
	defer c.backlogLock.Unlock()

	var approxCount int64
	var maxAge time.Duration
	for _, b := range c.backlogs {
		approxCount += b.db.getApproximateBacklogCount()
		// using this and not matcher's because it reports only the age of the current physical
		// queue backlog (not including the redirected backlogs) which is consistent with the
		// ApproximateBacklogCount metric.
		maxAge = max(maxAge, b.BacklogHeadAge())
	}
	return &taskqueuepb.TaskQueueStats{
		ApproximateBacklogCount: approxCount,
		ApproximateBacklogAge:   durationpb.New(maxAge),
		TasksAddRate:            c.tasksAddedInIntervals.rate(),
		TasksDispatchRate:       c.tasksDispatchedInIntervals.rate(),
	}
}

func (c *physicalTaskQueueManagerImpl) GetInternalTaskQueueStatus() *taskqueuespb.InternalTaskQueueStatus {
	// TODO(pri): return only backlog 0, need a new api to include info for all subqueues
	b := c.backlog0
	return &taskqueuespb.InternalTaskQueueStatus{
		ReadLevel: b.taskAckManager.getReadLevel(),
		AckLevel:  b.taskAckManager.getAckLevel(),
		TaskIdBlock: &taskqueuepb.TaskIdBlock{
			StartId: b.taskWriter.taskIDBlock.start,
			EndId:   b.taskWriter.taskIDBlock.end,
		},
		ReadBufferLength: b.ReadBufferLength(),
	}
}

func (c *physicalTaskQueueManagerImpl) TrySyncMatch(ctx context.Context, task *internalTask) (bool, error) {
	if !task.isForwarded() {
		// request sent by history service
		c.liveness.markAlive()
		c.tasksAddedInIntervals.incrementTaskCount()
		if disable, _ := testhooks.Get[bool](c.partitionMgr.engine.testHooks, testhooks.MatchingDisableSyncMatch); disable {
			return false, nil
		}
	}

	if c.priMatcher != nil {
		return c.priMatcher.Offer(ctx, task)
	}

	childCtx, cancel := newChildContext(ctx, c.config.SyncMatchWaitDuration(), time.Second)
	defer cancel()

	return c.oldMatcher.Offer(childCtx, task)
}

// [cleanup-wv-pre-release]
func (c *physicalTaskQueueManagerImpl) ensureRegisteredInDeployment(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	pollMetadata *pollMetadata,
) error {
	workerDeployment := worker_versioning.DeploymentFromCapabilities(pollMetadata.workerVersionCapabilities, pollMetadata.deploymentOptions)
	if workerDeployment == nil {
		return nil
	}
	if !c.partitionMgr.engine.config.EnableDeployments(namespaceEntry.Name().String()) {
		return errDeploymentsNotAllowed
	}

	// lock so that only one poll does the update and the rest wait for it
	c.deploymentLock.Lock()
	defer c.deploymentLock.Unlock()

	if c.deploymentRegistered {
		// deployment already registered
		return nil
	}

	if c.deploymentRegisterError != nil {
		// deployment not possible due to registration limits
		return c.deploymentRegisterError
	}

	userData, _, err := c.partitionMgr.GetUserDataManager().GetUserData()
	if err != nil {
		return err
	}

	deploymentData := userData.GetData().GetPerType()[int32(c.queue.TaskType())].GetDeploymentData()
	if hasDeploymentVersion(deploymentData, worker_versioning.DeploymentVersionFromDeployment(workerDeployment)) {
		// already registered in user data, we can assume the workflow is running.
		// TODO: consider replication scenarios where user data is replicated before
		// the deployment workflow.
		return nil
	}

	// we need to update the deployment workflow to tell it about this task queue
	// TODO: add some backoff here if we got an error last time

	if c.firstPoll.IsZero() {
		c.firstPoll = c.partitionMgr.engine.timeSource.Now()
	}
	err = c.partitionMgr.engine.deploymentStoreClient.RegisterTaskQueueWorker(
		ctx, namespaceEntry, workerDeployment, c.queue.TaskQueueFamily().Name(), c.queue.TaskType(), c.firstPoll,
		"matching service", uuid.New())
	if err != nil {
		var errTooMany deployment.ErrMaxTaskQueuesInDeployment
		if errors.As(err, &errTooMany) {
			c.deploymentRegisterError = errTooMany
		}
		return err
	}

	// the deployment workflow will register itself in this task queue's user data.
	// wait for it to propagate here.
	for {
		userData, userDataChanged, err := c.partitionMgr.GetUserDataManager().GetUserData()
		if err != nil {
			return err
		}
		deploymentData := userData.GetData().GetPerType()[int32(c.queue.TaskType())].GetDeploymentData()
		if hasDeploymentVersion(deploymentData, worker_versioning.DeploymentVersionFromDeployment(workerDeployment)) {
			break
		}
		select {
		case <-userDataChanged:
		case <-ctx.Done():
			c.logger.Error("timed out waiting for deployment to appear in user data")
			return ctx.Err()
		}
	}

	c.deploymentRegistered = true
	return nil
}

func (c *physicalTaskQueueManagerImpl) ensureRegisteredInDeploymentVersion(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	pollMetadata *pollMetadata,
) error {
	workerDeployment := worker_versioning.DeploymentFromCapabilities(pollMetadata.workerVersionCapabilities, pollMetadata.deploymentOptions)
	if workerDeployment == nil {
		return nil
	}
	if !c.partitionMgr.engine.config.EnableDeploymentVersions(namespaceEntry.Name().String()) {
		return errMissingDeploymentVersion
	}

	// lock so that only one poll does the update and the rest wait for it
	c.deploymentLock.Lock()
	defer c.deploymentLock.Unlock()

	if c.deploymentVersionRegistered {
		// deployment version already registered
		return nil
	}

	if c.deploymentRegisterError != nil {
		// deployment not possible due to registration limits
		return c.deploymentRegisterError
	}

	userData, _, err := c.partitionMgr.GetUserDataManager().GetUserData()
	if err != nil {
		return err
	}

	deploymentData := userData.GetData().GetPerType()[int32(c.queue.TaskType())].GetDeploymentData()
	if findDeploymentVersion(deploymentData, worker_versioning.DeploymentVersionFromDeployment(workerDeployment)) != -1 {
		// already registered in user data, we can assume the workflow is running.
		// TODO: consider replication scenarios where user data is replicated before
		// the deployment workflow.
		return nil
	}

	// we need to update the deployment workflow to tell it about this task queue
	// TODO: add some backoff here if we got an error last time

	err = c.partitionMgr.engine.workerDeploymentClient.RegisterTaskQueueWorker(
		ctx, namespaceEntry, workerDeployment.SeriesName, workerDeployment.BuildId, c.queue.TaskQueueFamily().Name(), c.queue.TaskType(),
		"matching service", uuid.New())
	if err != nil {
		var errTooMany deployment.ErrMaxTaskQueuesInDeployment
		if errors.As(err, &errTooMany) {
			c.deploymentRegisterError = errTooMany
		}
		return err
	}

	// the deployment workflow will register itself in this task queue's user data.
	// wait for it to propagate here.
	for {
		userData, userDataChanged, err := c.partitionMgr.GetUserDataManager().GetUserData()
		if err != nil {
			return err
		}
		deploymentData := userData.GetData().GetPerType()[int32(c.queue.TaskType())].GetDeploymentData()
		if findDeploymentVersion(deploymentData, worker_versioning.DeploymentVersionFromDeployment(workerDeployment)) >= 0 {
			break
		}
		select {
		case <-userDataChanged:
		case <-ctx.Done():
			c.logger.Error("timed out waiting for worker deployment version to appear in user data")
			return ctx.Err()
		}
	}

	c.deploymentVersionRegistered = true
	return nil
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

func (c *physicalTaskQueueManagerImpl) QueueKey() *PhysicalTaskQueueKey {
	return c.queue
}

func (c *physicalTaskQueueManagerImpl) UnloadFromPartitionManager(unloadCause unloadCause) {
	c.partitionMgr.unloadPhysicalQueue(c, unloadCause)
}

func (c *physicalTaskQueueManagerImpl) ShouldEmitGauges() bool {
	return c.config.BreakdownMetricsByTaskQueue() &&
		c.config.BreakdownMetricsByPartition() &&
		(!c.queue.IsVersioned() || c.config.BreakdownMetricsByBuildID())
}
