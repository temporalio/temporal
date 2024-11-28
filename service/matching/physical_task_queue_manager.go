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

	// The duration of each mini-bucket in the circularTaskBuffer
	intervalSize = 5
	// The total duration which is used to calculate the rate of tasks added/dispatched
	totalIntervalSize = 30
)

type (
	taskQueueManagerOpt func(*physicalTaskQueueManagerImpl)

	addTaskParams struct {
		taskInfo    *persistencespb.TaskInfo
		forwardInfo *taskqueuespb.TaskForwardInfo
	}
	// physicalTaskQueueManagerImpl manages a single DB-level (aka physical) task queue in memory
	physicalTaskQueueManagerImpl struct {
		status            int32
		partitionMgr      *taskQueuePartitionManagerImpl
		queue             *PhysicalTaskQueueKey
		config            *taskQueueConfig
		backlogMgr        *backlogManagerImpl
		liveness          *liveness
		matcher           *TaskMatcher // for matching a task producer with a poller
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
		deploymentLock          sync.Mutex
		deploymentRegistered    bool
		deploymentRegisterError error // last "too many ..." error we got when registering
		firstPoll               time.Time
	}
)

// a circular array of a fixed size for tracking tasks
type circularTaskBuffer struct {
	buffer     []int
	currentPos int
}

func newCircularTaskBuffer(size int) circularTaskBuffer {
	return circularTaskBuffer{
		buffer: make([]int, size),
	}
}

func (cb *circularTaskBuffer) incrementTaskCount() {
	cb.buffer[cb.currentPos]++
}

func (cb *circularTaskBuffer) advance() {
	cb.currentPos = (cb.currentPos + 1) % len(cb.buffer)
	cb.buffer[cb.currentPos] = 0 // Reset the task count for the new interval
}

// returns the total number of tasks in the buffer
func (cb *circularTaskBuffer) totalTasks() int {
	totalTasks := 0
	for _, count := range cb.buffer {
		totalTasks += count
	}
	return totalTasks
}

type taskTracker struct {
	lock              sync.Mutex
	clock             clock.TimeSource
	startTime         time.Time     // time when taskTracker was initialized
	bucketStartTime   time.Time     // the starting time of a bucket in the buffer
	bucketSize        time.Duration // the duration of each bucket in the buffer
	numberOfBuckets   int           // the total number of buckets in the buffer
	totalIntervalSize time.Duration // the number of seconds over which rate of tasks are added/dispatched
	tasksInInterval   circularTaskBuffer
}

func newTaskTracker(timeSource clock.TimeSource) *taskTracker {
	return &taskTracker{
		clock:             timeSource,
		startTime:         timeSource.Now(),
		bucketStartTime:   timeSource.Now(),
		bucketSize:        time.Duration(intervalSize) * time.Second,
		numberOfBuckets:   (totalIntervalSize / intervalSize) + 1,
		totalIntervalSize: time.Duration(totalIntervalSize) * time.Second,
		tasksInInterval:   newCircularTaskBuffer((totalIntervalSize / intervalSize) + 1),
	}
}

// advanceAndResetTracker advances the trackers position and clears out any expired intervals
// This method must be called with taskTracker's lock held.
func (s *taskTracker) advanceAndResetTracker(elapsed time.Duration) {
	// Calculate the number of intervals elapsed since the start interval time
	intervalsElapsed := int(elapsed / s.bucketSize)

	for i := 0; i < min(intervalsElapsed, s.numberOfBuckets); i++ {
		s.tasksInInterval.advance() // advancing our circular buffer's position until we land on the right interval
	}
	s.bucketStartTime = s.bucketStartTime.Add(time.Duration(intervalsElapsed) * s.bucketSize)
}

// incrementTaskCount adds/removes tasks from the current time that falls in the appropriate interval
func (s *taskTracker) incrementTaskCount() {
	s.lock.Lock()
	defer s.lock.Unlock()
	currentTime := s.clock.Now()

	// Calculate elapsed time from the latest start interval time
	elapsed := currentTime.Sub(s.bucketStartTime)
	s.advanceAndResetTracker(elapsed)
	s.tasksInInterval.incrementTaskCount()
}

// rate returns the rate of tasks added/dispatched in a given interval
func (s *taskTracker) rate() float32 {
	s.lock.Lock()
	defer s.lock.Unlock()
	currentTime := s.clock.Now()

	// Calculate elapsed time from the latest start interval time
	elapsed := currentTime.Sub(s.bucketStartTime)
	s.advanceAndResetTracker(elapsed)
	totalTasks := s.tasksInInterval.totalTasks()

	elapsedTime := min(currentTime.Sub(s.bucketStartTime)+s.totalIntervalSize,
		currentTime.Sub(s.startTime))

	if elapsedTime <= 0 {
		return 0
	}

	// rate per second
	return float32(totalTasks) / float32(elapsedTime.Seconds())
}

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

	pqMgr := &physicalTaskQueueManagerImpl{
		status:                     common.DaemonStatusInitialized,
		partitionMgr:               partitionMgr,
		namespaceRegistry:          e.namespaceRegistry,
		matchingClient:             e.matchingRawClient,
		clusterMeta:                e.clusterMeta,
		queue:                      queue,
		logger:                     logger,
		throttledLogger:            throttledLogger,
		config:                     config,
		metricsHandler:             taggedMetricsHandler,
		tasksAddedInIntervals:      newTaskTracker(clock.NewRealTimeSource()),
		tasksDispatchedInIntervals: newTaskTracker(clock.NewRealTimeSource()),
	}
	pqMgr.pollerHistory = newPollerHistory()

	pqMgr.liveness = newLiveness(
		clock.NewRealTimeSource(),
		config.MaxTaskQueueIdleTime,
		func() { pqMgr.UnloadFromPartitionManager(unloadCauseIdle) },
	)

	pqMgr.taskValidator = newTaskValidator(pqMgr.newIOContext, pqMgr.clusterMeta, pqMgr.namespaceRegistry, pqMgr.partitionMgr.engine.historyClient)
	pqMgr.backlogMgr = newBacklogManager(
		pqMgr,
		config,
		e.taskManager,
		logger,
		throttledLogger,
		e.matchingRawClient,
		taggedMetricsHandler,
		partitionMgr.callerInfoContext,
	)

	var fwdr *Forwarder
	var err error
	if !queue.Partition().IsRoot() && queue.Partition().Kind() != enumspb.TASK_QUEUE_KIND_STICKY {
		// Every DB Queue needs its own forwarder so that the throttles do not interfere
		fwdr, err = newForwarder(&config.forwarderConfig, queue, e.matchingRawClient)
		if err != nil {
			return nil, err
		}
	}
	pqMgr.matcher = newTaskMatcher(config, fwdr, pqMgr.metricsHandler)
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
	c.backlogMgr.Start()
	c.logger.Info("Started physicalTaskQueueManager", tag.LifeCycleStarted, tag.Cause(c.config.loadCause.String()))
	c.metricsHandler.Counter(metrics.TaskQueueStartedCounter.Name()).Record(1)
	c.partitionMgr.engine.updatePhysicalTaskQueueGauge(c, 1)
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
	c.backlogMgr.Stop()
	c.matcher.Stop()
	c.liveness.Stop()
	c.logger.Info("Stopped physicalTaskQueueManager", tag.LifeCycleStopped, tag.Cause(unloadCause.String()))
	c.metricsHandler.Counter(metrics.TaskQueueStoppedCounter.Name()).Record(1)
	c.partitionMgr.engine.updatePhysicalTaskQueueGauge(c, -1)
}

func (c *physicalTaskQueueManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	return c.backlogMgr.WaitUntilInitialized(ctx)
}

func (c *physicalTaskQueueManagerImpl) SpoolTask(taskInfo *persistencespb.TaskInfo) error {
	c.liveness.markAlive()
	return c.backlogMgr.SpoolTask(taskInfo)
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

	if err = c.ensureRegisteredInDeployment(ctx, namespaceEntry, pollMetadata); err != nil {
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
		task.backlogCountHint = c.backlogMgr.BacklogCountHint

		if pollMetadata.forwardedFrom == "" && // only track the original polls, not forwarded ones.
			(!task.isStarted() || !task.started.hasEmptyResponse()) { // Need to filter out the empty "started" ones
			c.tasksDispatchedInIntervals.incrementTaskCount()
		}
		return task, nil
	}
}

func (c *physicalTaskQueueManagerImpl) MarkAlive() {
	c.liveness.markAlive()
}

// DispatchSpooledTask dispatches a task to a poller. When there are no pollers to pick
// up the task or if rate limit is exceeded, this method will return error. Task
// *will not* be persisted to db
func (c *physicalTaskQueueManagerImpl) DispatchSpooledTask(
	ctx context.Context,
	task *internalTask,
	userDataChanged <-chan struct{},
) error {
	return c.matcher.MustOffer(ctx, task, userDataChanged)
}

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
	return c.partitionMgr.ProcessSpooledTask(ctx, task, c.queue.Version().BuildId())
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
		response.DescResponse.TaskQueueStatus = c.backlogMgr.BacklogStatus()
		response.DescResponse.TaskQueueStatus.RatePerSecond = c.matcher.Rate()
	}
	return response
}

func (c *physicalTaskQueueManagerImpl) GetStats() *taskqueuepb.TaskQueueStats {
	return &taskqueuepb.TaskQueueStats{
		ApproximateBacklogCount: c.backlogMgr.db.getApproximateBacklogCount(),
		ApproximateBacklogAge:   durationpb.New(c.backlogMgr.taskReader.getBacklogHeadAge()), // using this and not matcher's
		// because it reports only the age of the current physical queue backlog (not including the redirected backlogs) which is consistent
		// with the ApproximateBacklogCount metric.
		TasksAddRate:      c.tasksAddedInIntervals.rate(),
		TasksDispatchRate: c.tasksDispatchedInIntervals.rate(),
	}
}

func (c *physicalTaskQueueManagerImpl) GetInternalTaskQueueStatus() *taskqueuespb.InternalTaskQueueStatus {
	return &taskqueuespb.InternalTaskQueueStatus{
		ReadLevel:        c.backlogMgr.taskAckManager.getReadLevel(),
		AckLevel:         c.backlogMgr.taskAckManager.getAckLevel(),
		TaskIdBlock:      &taskqueuepb.TaskIdBlock{StartId: c.backlogMgr.taskWriter.taskIDBlock.start, EndId: c.backlogMgr.taskWriter.taskIDBlock.end},
		ReadBufferLength: int64(len(c.backlogMgr.taskReader.taskBuffer)),
	}
}

func (c *physicalTaskQueueManagerImpl) String() string {
	buf := new(bytes.Buffer)
	if c.queue.TaskType() == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
		buf.WriteString("Activity")
	} else {
		buf.WriteString("Workflow")
	}
	_, _ = fmt.Fprintf(buf, "Backlog=%s\n", c.backlogMgr.String())

	return buf.String()
}

func (c *physicalTaskQueueManagerImpl) TrySyncMatch(ctx context.Context, task *internalTask) (bool, error) {
	if !task.isForwarded() {
		// request sent by history service
		c.liveness.markAlive()
		c.tasksAddedInIntervals.incrementTaskCount()
		if c.config.TestDisableSyncMatch() {
			return false, nil
		}
	}
	childCtx, cancel := newChildContext(ctx, c.config.SyncMatchWaitDuration(), time.Second)
	defer cancel()

	return c.matcher.Offer(childCtx, task)
}

func (c *physicalTaskQueueManagerImpl) ensureRegisteredInDeployment(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	pollMetadata *pollMetadata,
) error {
	workerDeployment := worker_versioning.DeploymentFromCapabilities(pollMetadata.workerVersionCapabilities)
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
	if findDeployment(deploymentData, workerDeployment) >= 0 {
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
		if findDeployment(deploymentData, workerDeployment) >= 0 {
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

func (c *physicalTaskQueueManagerImpl) newIOContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), ioTimeout)
	return c.partitionMgr.callerInfoContext(ctx), cancel
}

func (c *physicalTaskQueueManagerImpl) UnloadFromPartitionManager(unloadCause unloadCause) {
	c.partitionMgr.unloadPhysicalQueue(c, unloadCause)
}

func (c *physicalTaskQueueManagerImpl) ShouldEmitGauges() bool {
	return c.config.BreakdownMetricsByTaskQueue() &&
		c.config.BreakdownMetricsByPartition() &&
		(!c.queue.IsVersioned() || c.config.BreakdownMetricsByBuildID())
}
